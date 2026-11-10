//! Rich layouts, durable final delivery, and temporary draft transport.

#[cfg(test)]
use openplotva_storage::database as sqlx;
use openplotva_telegram::{escape_telegram_html_text as esc, format_rich_html, sanitize_rich_html};

/// Persists final rich messages through the existing Telegram outbox.
#[derive(Clone, Debug)]
pub struct RichOutbox {
    store: openplotva_storage::PostgresTelegramOutboxStore,
    bot_id: i64,
}

impl RichOutbox {
    #[must_use]
    pub fn new(store: openplotva_storage::PostgresTelegramOutboxStore, bot_id: i64) -> Self {
        Self { store, bot_id }
    }

    // Retain persisted utility keys so replay finds previously queued ad deliveries.
    fn batch_id(&self, source: &str) -> String {
        format!("gradius-utility:v1:{}:{source}", self.bot_id)
    }

    pub async fn queued(
        &self,
        source: &str,
        job_id: Option<i64>,
    ) -> Result<Option<crate::telegram_outbox::QueuedBatchReceipt>, openplotva_storage::StorageError>
    {
        Ok(self
            .store
            .queued_batch(self.bot_id, &self.batch_id(source), job_id)
            .await?
            .map(rich_outbox_receipt))
    }

    pub async fn enqueue(
        &self,
        source: &str,
        thread_id: Option<i64>,
        method: impl Into<openplotva_telegram::TelegramOutboundMethod>,
        job_id: Option<i64>,
    ) -> Result<crate::telegram_outbox::QueuedBatchReceipt, RichOutboxError> {
        use openplotva_storage::{
            TelegramDeliveryPolicy, TelegramOutboxBatchInput, TelegramOutboxContentPolicy,
            TelegramOutboxPartInput,
        };
        use openplotva_telegram::OutboundCommand;
        use openplotva_telegram::TelegramOutboundMethod;
        let mut method = method.into();
        let (chat_id, trigger_message_id, delivery_policy) = match &mut method {
            TelegramOutboundMethod::SendRichMessage(message) => {
                if message.options.message_thread_id != thread_id {
                    return Err(RichOutboxError::InvalidThread);
                }
                message.html = format_rich_html(&message.html);
                (
                    message.chat_id,
                    message.options.reply_to_message_id,
                    TelegramDeliveryPolicy::Create,
                )
            }
            TelegramOutboundMethod::EditRichMessage(message) => {
                message.html = format_rich_html(&message.html);
                (
                    message.chat_id,
                    Some(message.message_id),
                    TelegramDeliveryPolicy::TargetIdempotent,
                )
            }
            _ => return Err(RichOutboxError::UnsupportedMethod),
        };
        let thread_id = thread_id
            .map(i32::try_from)
            .transpose()
            .map_err(|_| RichOutboxError::InvalidThread)?;
        let command = OutboundCommand::try_from_method(method)?;
        let (kind, version, payload) = command.into_storage_parts()?;
        let parts = vec![TelegramOutboxPartInput {
            method_kind: kind.to_owned(),
            payload_version: version,
            payload,
            blob: None,
            available_at: time::OffsetDateTime::now_utc(),
            expires_at: None,
        }];
        let queued = self
            .store
            .enqueue_batch(&TelegramOutboxBatchInput {
                content_policy: TelegramOutboxContentPolicy::ReusePrepared,
                batch_id: self.batch_id(source),
                bot_id: self.bot_id,
                chat_id: Some(chat_id),
                thread_id,
                ordering_key: crate::telegram_outbox::telegram_outbox_ordering_key(
                    self.bot_id,
                    chat_id,
                    thread_id,
                ),
                causation_update_id: None,
                dialog_job_id: job_id,
                trigger_message_id,
                delivery_policy,
                protected: true,
                priority: openplotva_taskman::DEFAULT_PRIORITY,
                parts,
            })
            .await?;
        Ok(rich_outbox_receipt(queued))
    }
}

fn rich_outbox_receipt(
    queued: openplotva_storage::QueuedTelegramOutboxBatch,
) -> crate::telegram_outbox::QueuedBatchReceipt {
    let delivered = queued.parts.iter().all(|part| part.state == "delivered");
    crate::telegram_outbox::QueuedBatchReceipt::durable_outbox(
        queued.batch_id,
        queued
            .parts
            .into_iter()
            .map(|part| part.operation_id)
            .collect(),
        delivered,
    )
}

#[derive(Debug, thiserror::Error)]
pub enum RichOutboxError {
    #[error("final rich delivery requires a rich send or edit command")]
    UnsupportedMethod,
    #[error(transparent)]
    Storage(#[from] openplotva_storage::StorageError),
    #[error(transparent)]
    Codec(#[from] openplotva_telegram::OutboundCommandCodecError),
    #[error("rich message topic ID is outside the supported range")]
    InvalidThread,
}

#[cfg(test)]
pub(crate) fn unreachable_outbox() -> std::sync::Arc<RichOutbox> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(100))
        .connect_lazy("postgres://test:test@127.0.0.1:1/test")
        .expect("fixed test DSN");
    std::sync::Arc::new(RichOutbox::new(
        openplotva_storage::PostgresTelegramOutboxStore::new(pool),
        7,
    ))
}

/// Escape text and turn newlines into `<br>` for use inside a rich block.
fn esc_multiline(value: &str) -> String {
    esc(value).replace('\n', "<br>")
}

/// A single currency row for [`compose_rates_table`].
#[derive(Clone, Debug)]
pub struct RateRow {
    pub label: String,
    pub value: String,
    pub delta: String,
}

/// Compose the currency-rates rich table (replaces the whitespace pseudo-grid).
#[must_use]
pub fn compose_rates_table(header: &str, rows: &[RateRow], footer: &str) -> String {
    let mut html = String::new();
    html.push_str(&format!("<p>{}</p>", esc(header)));
    html.push_str("<table bordered striped><tr><th>Пара</th><th>Курс</th><th>Динамика</th></tr>");
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td align=\"right\">{}</td><td align=\"right\">{}</td></tr>",
            esc(&row.label),
            esc(&row.value),
            esc(&row.delta),
        ));
    }
    html.push_str("</table>");
    if !footer.trim().is_empty() {
        html.push_str(&format!("<footer>{}</footer>", esc(footer)));
    }
    sanitize_rich_html(&html)
}

/// Inputs for [`compose_song_message`].
#[derive(Clone, Debug)]
pub struct SongMessage<'a> {
    pub title: &'a str,
    /// Compact style line (genre · BPM · key · vocals).
    pub styles: &'a str,
    pub audio_url: &'a str,
    pub lyrics: &'a str,
    /// Pre-built rich HTML for the author/credit footer (already escaped/sanitized fragments).
    pub footer_html: &'a str,
}

/// Compose a song as a single rich message: title, styles, inline audio, collapsible
/// lyrics, then the author footer.
#[must_use]
pub fn compose_song_message(song: &SongMessage<'_>) -> String {
    let mut html = String::new();
    html.push_str(&format!("<h3>🎵 {}</h3>", esc(song.title)));
    if !song.styles.trim().is_empty() {
        html.push_str(&format!("<p>🎛 <i>{}</i></p>", esc(song.styles)));
    }
    html.push_str(&format!("<audio src=\"{}\"></audio>", esc(song.audio_url)));
    if !song.lyrics.trim().is_empty() {
        html.push_str(&format!(
            "<details><summary>📜 Текст песни</summary><blockquote>{}</blockquote></details>",
            esc_multiline(song.lyrics),
        ));
    }
    if !song.footer_html.trim().is_empty() {
        html.push_str(&format!("<footer>{}</footer>", song.footer_html));
    }
    sanitize_rich_html(&html)
}

/// One leaderboard standing for [`compose_leaderboard`].
#[derive(Clone, Debug)]
pub struct LeaderboardRow {
    pub rank_title: String,
    pub name: String,
    pub wins: i64,
}

/// Compose the yearly check-in leaderboard as a rich table with medals for the top three.
#[must_use]
pub fn compose_leaderboard(theme: &str, rows: &[LeaderboardRow]) -> String {
    let mut html = String::new();
    html.push_str(&format!("<h3>🏆 {} — Лидеры года</h3>", esc(theme)));
    html.push_str(
        "<table bordered striped><tr><th>#</th><th>Титул</th><th>Имя</th><th>Побед</th></tr>",
    );
    for (index, row) in rows.iter().enumerate() {
        let place = match index {
            0 => "🥇".to_owned(),
            1 => "🥈".to_owned(),
            2 => "🥉".to_owned(),
            other => (other + 1).to_string(),
        };
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td align=\"right\">{}</td></tr>",
            place,
            esc(&row.rank_title),
            esc(&row.name),
            row.wins,
        ));
    }
    html.push_str("</table>");
    sanitize_rich_html(&html)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rates_table_renders_joke_as_plain_text() {
        let rows = vec![
            RateRow {
                label: "USD/RUB".to_owned(),
                value: "78.42".to_owned(),
                delta: "+0.15".to_owned(),
            },
            RateRow {
                label: "EUR/RUB".to_owned(),
                value: "85.10".to_owned(),
                delta: "-0.20".to_owned(),
            },
        ];
        let html = compose_rates_table("Курсы", &rows, "источник: ЦБ РФ");
        assert_eq!(
            html,
            "<p>Курсы</p><table bordered striped><tr><th>Пара</th><th>Курс</th><th>Динамика</th></tr>\
<tr><td>USD/RUB</td><td align=\"right\">78.42</td><td align=\"right\">+0.15</td></tr>\
<tr><td>EUR/RUB</td><td align=\"right\">85.10</td><td align=\"right\">-0.20</td></tr></table>\
<footer>источник: ЦБ РФ</footer>"
        );
    }

    #[test]
    fn song_has_audio_details_and_footer_in_order() {
        let song = SongMessage {
            title: "Песня",
            styles: "synthwave · 102 BPM · female vocals",
            audio_url: "https://uploader.example.com/x.mp3",
            lyrics: "строка 1\nстрока 2",
            footer_html: "за авторством <i>Автор</i>",
        };
        let html = compose_song_message(&song);
        let audio = html.find("<audio").expect("song should include audio");
        let details = html.find("<details>").expect("song should include details");
        let footer = html.find("<footer>").expect("song should include footer");
        assert!(audio < details && details < footer);
        assert!(html.contains("<p>🎛 <i>synthwave · 102 BPM · female vocals</i></p>"));
        assert!(html.contains(r#"<audio src="https://uploader.example.com/x.mp3"></audio>"#));
        assert!(html.contains("<blockquote>строка 1<br/>строка 2</blockquote>"));
        assert!(html.contains("<footer>за авторством <i>Автор</i></footer>"));
        assert!(
            !html.contains("Теги генерации"),
            "the full tag list stays in generated_songs, not in the message"
        );

        let instrumental = SongMessage { lyrics: "", ..song };
        let html = compose_song_message(&instrumental);
        assert!(!html.contains("<details>"));
        assert!(html.contains("<audio"));
    }

    #[test]
    fn leaderboard_has_medals() {
        let rows = vec![
            LeaderboardRow {
                rank_title: "Король".to_owned(),
                name: "Алиса".to_owned(),
                wins: 12,
            },
            LeaderboardRow {
                rank_title: "Принц".to_owned(),
                name: "Боб".to_owned(),
                wins: 9,
            },
        ];
        let html = compose_leaderboard("Кот дня", &rows);
        assert!(html.contains("<h3>🏆 Кот дня — Лидеры года</h3>"));
        assert!(
            html.contains("<td>🥇</td><td>Король</td><td>Алиса</td><td align=\"right\">12</td>")
        );
        assert!(html.contains("<td>🥈</td>"));
    }

    #[test]
    fn escapes_dynamic_values() {
        let rows = vec![RateRow {
            label: "<b>x</b>".to_owned(),
            value: "1 & 2".to_owned(),
            delta: "<".to_owned(),
        }];
        let html = compose_rates_table("a<b", &rows, "");
        assert!(html.contains("<p>a&lt;b</p>"));
        assert!(!html.contains("<h3>"));
        assert!(html.contains("<td>&lt;b&gt;x&lt;/b&gt;</td>"));
        assert!(html.contains("<td align=\"right\">1 &amp; 2</td>"));
    }
}
