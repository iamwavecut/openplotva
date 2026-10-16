//! Source pictures of an image edit, prepared for the prompt optimizer the way the
//! FLUX.3 prompt upsampler sees its references.

use std::{fmt, future::Future, io::Cursor, pin::Pin};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt;
use image::{
    DynamicImage, ImageDecoder, ImageReader, Limits, Rgb, RgbImage, codecs::jpeg::JpegEncoder,
    imageops::FilterType, metadata::Orientation,
};
use openplotva_telegram::TelegramClient;

use crate::image_jobs::ImageEditRequest;

const MAX_REFERENCE_BYTES: usize = 20 * 1024 * 1024;
const MAX_REFERENCE_SIDE: u32 = 16_384;
const MAX_REFERENCE_DECODER_ALLOC_BYTES: u64 = 512 * 1024 * 1024;
const REFERENCE_JPEG_QUALITY: u8 = 92;
const TELEGRAM_FILE_URL_PREFIX: &str = "https://api.telegram.org/file/bot";

/// Future returned by [`ImageEditReferenceLoader::load_edit_references`].
pub type ImageEditReferencesFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<u8>>, String>> + Send + 'a>>;

/// Loads the encoded source pictures of an edit request, in request order.
pub trait ImageEditReferenceLoader: fmt::Debug + Send + Sync {
    fn load_edit_references<'a>(
        &'a self,
        request: &'a ImageEditRequest,
    ) -> ImageEditReferencesFuture<'a>;
}

/// Downloads edit pictures through the bot's Telegram client. Telegram file URLs carry
/// the bot token, so only their file path is used and errors never repeat the URL.
#[derive(Clone)]
pub struct TelegramImageEditReferenceLoader {
    client: TelegramClient,
}

impl TelegramImageEditReferenceLoader {
    #[must_use]
    pub const fn new(client: TelegramClient) -> Self {
        Self { client }
    }

    async fn file_path(&self, file_id: &str) -> Result<String, String> {
        let file: carapax::types::File = self
            .client
            .execute(carapax::types::GetFile::new(file_id))
            .await
            .map_err(|error| format!("get telegram file: {error}"))?;
        file.file_path
            .map(|path| path.trim().to_owned())
            .filter(|path| !path.is_empty())
            .ok_or_else(|| "telegram file_path is empty".to_owned())
    }

    async fn download(&self, file_path: &str) -> Result<Vec<u8>, String> {
        let mut stream = self
            .client
            .download_file(file_path)
            .await
            .map_err(|_| "download telegram file failed".to_owned())?;
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| "download telegram file chunk failed".to_owned())?;
            if data.len().saturating_add(chunk.len()) > MAX_REFERENCE_BYTES {
                return Err("telegram image exceeds 20 MiB".to_owned());
            }
            data.extend_from_slice(&chunk);
        }
        Ok(data)
    }
}

impl fmt::Debug for TelegramImageEditReferenceLoader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramImageEditReferenceLoader")
            .finish_non_exhaustive()
    }
}

impl ImageEditReferenceLoader for TelegramImageEditReferenceLoader {
    fn load_edit_references<'a>(
        &'a self,
        request: &'a ImageEditRequest,
    ) -> ImageEditReferencesFuture<'a> {
        Box::pin(async move {
            let inputs: Vec<&str> = request
                .photo_urls
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .collect();
            if inputs.is_empty() {
                let file_id = request.photo_file_id.trim();
                if file_id.is_empty() {
                    return Ok(Vec::new());
                }
                let path = self.file_path(file_id).await?;
                return Ok(vec![self.download(&path).await?]);
            }
            let mut images = Vec::with_capacity(inputs.len());
            for input in inputs {
                images.push(match telegram_file_path(input) {
                    Some(path) => self.download(path).await?,
                    None => decode_inline_image(input)?,
                });
            }
            Ok(images)
        })
    }
}

fn telegram_file_path(url: &str) -> Option<&str> {
    let rest = url.strip_prefix(TELEGRAM_FILE_URL_PREFIX)?;
    let (_, path) = rest.split_once('/')?;
    (!path.is_empty()).then_some(path)
}

fn decode_inline_image(value: &str) -> Result<Vec<u8>, String> {
    if value.starts_with("http://") || value.starts_with("https://") {
        return Err("unsupported reference image URL".to_owned());
    }
    let encoded = value
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(','))
        .map_or(value, |(_, data)| data);
    BASE64_STANDARD
        .decode(encoded.trim())
        .map_err(|error| format!("decode reference image: {error}"))
}

/// Data URLs of `images` as the FLUX.3 prompt upsampler sees its references: upright,
/// alpha flattened onto white, resized for the number of pictures, JPEG at quality 92.
/// Fails when any picture cannot be prepared, so the request's picture numbering never
/// shifts.
pub fn upsampler_reference_data_urls(images: &[Vec<u8>]) -> Result<Vec<String>, String> {
    let count = images.len();
    images
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            upsampler_reference_data_url(bytes, count)
                .map_err(|error| format!("reference image {}: {error}", index + 1))
        })
        .collect()
}

fn upsampler_reference_data_url(bytes: &[u8], count: usize) -> Result<String, String> {
    if bytes.len() > MAX_REFERENCE_BYTES {
        return Err("image exceeds 20 MiB".to_owned());
    }
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|error| format!("read image: {error}"))?;
    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_REFERENCE_SIDE);
    limits.max_image_height = Some(MAX_REFERENCE_SIDE);
    limits.max_alloc = Some(MAX_REFERENCE_DECODER_ALLOC_BYTES);
    reader.limits(limits);
    let mut decoder = reader
        .into_decoder()
        .map_err(|error| format!("decode image: {error}"))?;
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    let mut image =
        DynamicImage::from_decoder(decoder).map_err(|error| format!("decode image: {error}"))?;
    image.apply_orientation(orientation);
    let rgb = flatten_onto_white(&image);
    let (width, height) =
        openplotva_media::upsampler_reference_size(rgb.width(), rgb.height(), count)
            .ok_or_else(|| format!("unsupported image size {}x{}", rgb.width(), rgb.height()))?;
    let rgb = if rgb.dimensions() == (width, height) {
        rgb
    } else {
        image::imageops::resize(&rgb, width, height, FilterType::Lanczos3)
    };
    let mut jpeg = Vec::new();
    DynamicImage::ImageRgb8(rgb)
        .write_with_encoder(JpegEncoder::new_with_quality(
            &mut jpeg,
            REFERENCE_JPEG_QUALITY,
        ))
        .map_err(|error| format!("encode image: {error}"))?;
    Ok(format!(
        "data:image/jpeg;base64,{}",
        BASE64_STANDARD.encode(jpeg)
    ))
}

fn flatten_onto_white(image: &DynamicImage) -> RgbImage {
    if !image.color().has_alpha() {
        return image.to_rgb8();
    }
    let rgba = image.to_rgba8();
    RgbImage::from_fn(rgba.width(), rgba.height(), |x, y| {
        let [red, green, blue, alpha] = rgba.get_pixel(x, y).0;
        let alpha = u16::from(alpha);
        let blend = |channel: u8| {
            u8::try_from((u16::from(channel) * alpha + 255 * (255 - alpha) + 127) / 255)
                .unwrap_or(u8::MAX)
        };
        Rgb([blend(red), blend(green), blend(blue)])
    })
}

#[cfg(test)]
mod tests {
    use image::{GenericImageView, ImageFormat, Rgba, RgbaImage};

    use super::*;

    fn png(width: u32, height: u32, pixel: Rgba<u8>) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        DynamicImage::ImageRgba8(RgbaImage::from_pixel(width, height, pixel))
            .write_to(&mut bytes, ImageFormat::Png)
            .expect("encode png");
        bytes.into_inner()
    }

    fn decoded(data_url: &str) -> DynamicImage {
        let encoded = data_url
            .strip_prefix("data:image/jpeg;base64,")
            .expect("jpeg data url");
        image::load_from_memory(&BASE64_STANDARD.decode(encoded).expect("base64"))
            .expect("decode jpeg")
    }

    #[test]
    fn references_are_resized_for_the_picture_count() {
        let one = upsampler_reference_data_urls(&[png(1000, 1000, Rgba([10, 20, 30, 255]))])
            .expect("one reference");
        assert_eq!(decoded(&one[0]).dimensions(), (992, 992));

        let pictures = vec![png(1280, 960, Rgba([0, 0, 0, 255])); 3];
        let three = upsampler_reference_data_urls(&pictures).expect("three references");
        assert_eq!(three.len(), 3);
        assert_eq!(decoded(&three[2]).dimensions(), (576, 416));
    }

    #[test]
    fn transparent_pixels_become_white() {
        let urls =
            upsampler_reference_data_urls(&[png(64, 64, Rgba([0, 0, 0, 0]))]).expect("reference");
        let pixel = decoded(&urls[0]).to_rgb8().get_pixel(32, 32).0;
        assert!(pixel.iter().all(|channel| *channel > 245), "{pixel:?}");
    }

    #[test]
    fn one_unreadable_picture_fails_the_whole_set() {
        let error = upsampler_reference_data_urls(&[
            png(64, 64, Rgba([0, 0, 0, 255])),
            b"not an image".to_vec(),
        ])
        .expect_err("broken second picture");
        assert!(error.starts_with("reference image 2:"), "{error}");
    }

    #[test]
    fn telegram_file_urls_resolve_to_their_file_path() {
        assert_eq!(
            telegram_file_path("https://api.telegram.org/file/bot123:abc/photos/file_7.jpg"),
            Some("photos/file_7.jpg")
        );
        assert_eq!(
            telegram_file_path("https://example.com/file/bot1/a.jpg"),
            None
        );
    }

    #[test]
    fn inline_pictures_decode_from_base64_or_data_urls() {
        let bytes = png(8, 8, Rgba([1, 2, 3, 255]));
        let encoded = BASE64_STANDARD.encode(&bytes);
        assert_eq!(decode_inline_image(&encoded).expect("raw base64"), bytes);
        assert_eq!(
            decode_inline_image(&format!("data:image/png;base64,{encoded}")).expect("data url"),
            bytes
        );
        assert!(decode_inline_image("https://example.com/a.png").is_err());
    }
}
