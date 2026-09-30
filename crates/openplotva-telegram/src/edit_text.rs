use carapax::types::{
    EditMessageText, InlineKeyboardMarkup, InputText, LinkPreviewOptions, ParseMode,
};
use serde::{Deserialize, Serialize};

/// Plain text edit envelope retained for durable replay independently of SDK form internals.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EditTextMessagePlan {
    pub chat_id: i64,
    pub message_id: i64,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_mode: Option<ParseMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_markup: Option<InlineKeyboardMarkup>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_preview_options: Option<LinkPreviewOptions>,
}

impl EditTextMessagePlan {
    pub fn for_chat_message(chat_id: i64, message_id: i64, text: impl Into<String>) -> Self {
        Self {
            chat_id,
            message_id,
            text: text.into(),
            parse_mode: None,
            reply_markup: None,
            link_preview_options: None,
        }
    }

    pub fn with_parse_mode(mut self, mode: ParseMode) -> Self {
        self.parse_mode = Some(mode);
        self
    }

    pub fn with_reply_markup(mut self, markup: InlineKeyboardMarkup) -> Self {
        self.reply_markup = Some(markup);
        self
    }

    pub fn with_link_preview_options(mut self, options: LinkPreviewOptions) -> Self {
        self.link_preview_options = Some(options);
        self
    }

    pub fn to_carapax(&self) -> EditMessageText {
        let mut text = InputText::from(self.text.clone());
        if let Some(mode) = self.parse_mode {
            text = text.with_format(mode);
        }
        let mut method = EditMessageText::for_chat_message(self.chat_id, self.message_id, text);
        if let Some(markup) = self.reply_markup.clone() {
            method = method.with_reply_markup(markup);
        }
        if let Some(options) = self.link_preview_options.clone() {
            method = method.with_link_preview_options(options);
        }
        method
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persisted_edit_keeps_the_same_sdk_wire_payload() {
        let plan = EditTextMessagePlan::for_chat_message(-100, 77, "<b>edited</b>")
            .with_parse_mode(ParseMode::Html)
            .with_link_preview_options(LinkPreviewOptions::disabled());
        let expected = serde_json::to_value(&plan).expect("persisted plan");
        let actual = crate::test_api::bot_api_payload(plan.to_carapax())
            .await
            .expect("SDK request");
        assert_eq!(actual, expected);
    }
}
