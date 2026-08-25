use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use thiserror::Error;
use url::Url;

use crate::{escape_telegram_html_text, is_valid_telegram_html};

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MarkdownToTelegramHtmlError {
    #[error("generated advertising HTML is not valid for Telegram")]
    InvalidOutput,
    #[error("raw HTML is not accepted in advertising markdown")]
    RawHtml,
    #[error("unsupported advertising markdown construct: {0}")]
    Unsupported(&'static str),
    #[error("advertising link uses an unsupported URL scheme")]
    UnsafeLink,
}

pub fn telegram_html_from_markdown(markdown: &str) -> Result<String, MarkdownToTelegramHtmlError> {
    let mut output = String::with_capacity(markdown.len());
    let parser = Parser::new_ext(markdown, Options::ENABLE_STRIKETHROUGH);
    let mut lists = Vec::new();
    let mut block_quote_depth = 0_usize;

    for event in parser {
        match event {
            Event::Start(Tag::Paragraph) => {}
            Event::End(TagEnd::Paragraph) if lists.is_empty() && block_quote_depth == 0 => {
                end_block(&mut output);
            }
            Event::End(TagEnd::Paragraph) => {}
            Event::Start(Tag::Heading { .. }) => output.push_str("<b>"),
            Event::End(TagEnd::Heading(_)) => {
                output.push_str("</b>");
                end_block(&mut output);
            }
            Event::Start(Tag::BlockQuote(_)) => {
                block_quote_depth += 1;
                output.push_str("<blockquote>");
            }
            Event::End(TagEnd::BlockQuote(_)) => {
                block_quote_depth = block_quote_depth.saturating_sub(1);
                output.push_str("</blockquote>");
                end_block(&mut output);
            }
            Event::Start(Tag::CodeBlock(kind)) => {
                output.push_str("<pre><code");
                if let Some(language) = code_language(&kind) {
                    output.push_str(" class=\"language-");
                    output.push_str(language);
                    output.push('"');
                }
                output.push('>');
            }
            Event::End(TagEnd::CodeBlock) => {
                output.push_str("</code></pre>");
                end_block(&mut output);
            }
            Event::Start(Tag::List(start)) => lists.push(start),
            Event::End(TagEnd::List(_)) => {
                lists.pop();
                if lists.is_empty() {
                    end_block(&mut output);
                }
            }
            Event::Start(Tag::Item) => {
                if let Some(next) = lists.last_mut() {
                    match next {
                        Some(value) => {
                            output.push_str(&value.to_string());
                            output.push_str(". ");
                            *value = value.saturating_add(1);
                        }
                        None => output.push_str("• "),
                    }
                }
            }
            Event::End(TagEnd::Item) => push_line_break(&mut output),
            Event::Start(Tag::Emphasis) => output.push_str("<i>"),
            Event::End(TagEnd::Emphasis) => output.push_str("</i>"),
            Event::Start(Tag::Strong) => output.push_str("<b>"),
            Event::End(TagEnd::Strong) => output.push_str("</b>"),
            Event::Start(Tag::Strikethrough) => output.push_str("<s>"),
            Event::End(TagEnd::Strikethrough) => output.push_str("</s>"),
            Event::Start(Tag::Link { dest_url, .. }) => {
                let url = safe_link(&dest_url)?;
                output.push_str("<a href=\"");
                output.push_str(&escape_html_attribute(&url));
                output.push_str("\">");
            }
            Event::End(TagEnd::Link) => output.push_str("</a>"),
            Event::Start(Tag::Image { .. }) | Event::End(TagEnd::Image) => {
                return Err(MarkdownToTelegramHtmlError::Unsupported("image"));
            }
            Event::Text(text) => output.push_str(&escape_telegram_html_text(&text)),
            Event::Code(code) => {
                output.push_str("<code>");
                output.push_str(&escape_telegram_html_text(&code));
                output.push_str("</code>");
            }
            Event::SoftBreak | Event::HardBreak => output.push('\n'),
            Event::Html(_) | Event::InlineHtml(_) => {
                return Err(MarkdownToTelegramHtmlError::RawHtml);
            }
            _ => {}
        }
    }

    let output = output.trim().to_owned();
    if !is_valid_telegram_html(&output) {
        return Err(MarkdownToTelegramHtmlError::InvalidOutput);
    }
    Ok(output)
}

fn safe_link(raw: &str) -> Result<String, MarkdownToTelegramHtmlError> {
    let trimmed = raw.trim();
    let url = Url::parse(trimmed).map_err(|_| MarkdownToTelegramHtmlError::UnsafeLink)?;
    match url.scheme() {
        "http" | "https" | "tg" => Ok(trimmed.to_owned()),
        _ => Err(MarkdownToTelegramHtmlError::UnsafeLink),
    }
}

fn code_language<'a>(kind: &'a CodeBlockKind<'a>) -> Option<&'a str> {
    let CodeBlockKind::Fenced(info) = kind else {
        return None;
    };
    let language = info.split_ascii_whitespace().next()?;
    (!language.is_empty()
        && language
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')))
    .then_some(language)
}

fn push_line_break(output: &mut String) {
    if !output.ends_with('\n') {
        output.push('\n');
    }
}

fn end_block(output: &mut String) {
    push_line_break(output);
    if !output.ends_with("\n\n") {
        output.push('\n');
    }
}

fn escape_html_attribute(value: &str) -> String {
    escape_telegram_html_text(value).replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::{MarkdownToTelegramHtmlError, telegram_html_from_markdown};

    #[test]
    fn converts_links_and_emphasis_without_rewriting_the_url() {
        assert_eq!(
            telegram_html_from_markdown(
                "**Скидка**: [открыть предложение](https://ads.example/redirect?id=42&src=plotva) — 2 < 3"
            )
            .expect("valid advertising markdown"),
            "<b>Скидка</b>: <a href=\"https://ads.example/redirect?id=42&amp;src=plotva\">открыть предложение</a> — 2 &lt; 3"
        );
    }

    #[test]
    fn converts_supported_inline_formatting_and_breaks() {
        assert_eq!(
            telegram_html_from_markdown("_Курсив_ ~~нет~~ `x < 3 & y`  \nдальше")
                .expect("supported inline markdown"),
            "<i>Курсив</i> <s>нет</s> <code>x &lt; 3 &amp; y</code>\nдальше"
        );
    }

    #[test]
    fn rejects_raw_html_instead_of_interpreting_it() {
        assert_eq!(
            telegram_html_from_markdown("скидка <b>только сегодня</b>"),
            Err(MarkdownToTelegramHtmlError::RawHtml)
        );
    }

    #[test]
    fn rejects_unsafe_link_schemes() {
        assert_eq!(
            telegram_html_from_markdown("[получить скидку](javascript:alert(1))"),
            Err(MarkdownToTelegramHtmlError::UnsafeLink)
        );
    }

    #[test]
    fn converts_headings_quotes_lists_and_fenced_code() {
        let markdown = "# Акция\n\n> Пока действует предложение\n\n1. Первый шаг\n2. Второй шаг\n\n```rust\nlet cheaper = 2 < 3;\n```";

        assert_eq!(
            telegram_html_from_markdown(markdown).expect("supported block markdown"),
            "<b>Акция</b>\n\n<blockquote>Пока действует предложение</blockquote>\n\n1. Первый шаг\n2. Второй шаг\n\n<pre><code class=\"language-rust\">let cheaper = 2 &lt; 3;\n</code></pre>"
        );
    }

    #[test]
    fn validates_but_does_not_normalize_redirect_urls() {
        assert_eq!(
            telegram_html_from_markdown("[перейти](https://ads.example)")
                .expect("valid redirect URL"),
            "<a href=\"https://ads.example\">перейти</a>"
        );
    }

    #[test]
    fn rejects_markdown_images() {
        assert_eq!(
            telegram_html_from_markdown("![баннер](https://ads.example/banner.png)"),
            Err(MarkdownToTelegramHtmlError::Unsupported("image"))
        );
    }
}
