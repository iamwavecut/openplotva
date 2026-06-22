use serde::Deserialize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarSubscriptionPayment {
    pub telegram_payment_charge_id: String,
    pub user_id: i64,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub language_code: Option<String>,
    pub is_premium: Option<bool>,
    pub invoice_payload: String,
    pub amount_stars: i64,
    pub paid_at_unix: i64,
    pub subscription_period_seconds: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarSubscriptionPaymentsPage {
    pub payments: Vec<StarSubscriptionPayment>,
    pub transaction_count: usize,
}

#[derive(Clone, Debug)]
pub struct StarTransactionsClient {
    http: reqwest::Client,
    bot_key: String,
    base_url: String,
}

impl StarTransactionsClient {
    #[must_use]
    pub fn new(bot_key: String, base_url: &str) -> Self {
        let base_url = if base_url.trim().is_empty() {
            "https://api.telegram.org"
        } else {
            base_url.trim()
        };
        Self {
            http: reqwest::Client::new(),
            bot_key,
            base_url: base_url.trim_end_matches('/').to_owned(),
        }
    }

    pub async fn get_star_transactions(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<StarSubscriptionPayment>, StarTransactionsError> {
        Ok(self
            .get_star_transactions_page(offset, limit)
            .await?
            .payments)
    }

    pub async fn get_star_transactions_page(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<StarSubscriptionPaymentsPage, StarTransactionsError> {
        let url = format!("{}/bot{}/getStarTransactions", self.base_url, self.bot_key);
        let response: BotApiResponse<StarTransactionsResult> = self
            .http
            .post(url)
            .json(&serde_json::json!({
                "offset": offset,
                "limit": limit,
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if !response.ok {
            return Err(StarTransactionsError::Telegram(
                response
                    .description
                    .unwrap_or_else(|| "getStarTransactions returned ok=false".to_owned()),
            ));
        }
        let Some(result) = response.result else {
            return Ok(StarSubscriptionPaymentsPage {
                payments: Vec::new(),
                transaction_count: 0,
            });
        };
        let transaction_count = result.transactions.len();
        Ok(StarSubscriptionPaymentsPage {
            payments: result
                .transactions
                .into_iter()
                .filter_map(parse_star_subscription_payment)
                .collect(),
            transaction_count,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StarTransactionsError {
    #[error("Telegram getStarTransactions request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Telegram getStarTransactions failed: {0}")]
    Telegram(String),
}

#[must_use]
pub fn parse_star_subscription_payment(
    value: serde_json::Value,
) -> Option<StarSubscriptionPayment> {
    parse_star_subscription_payment_raw(serde_json::from_value(value).ok()?)
}

fn parse_star_subscription_payment_raw(
    transaction: RawStarTransaction,
) -> Option<StarSubscriptionPayment> {
    if transaction.receiver.is_some() {
        return None;
    }
    let source = transaction.source?;
    if source.kind != "user" || source.transaction_type.as_deref() != Some("invoice_payment") {
        return None;
    }
    let invoice_payload = source.invoice_payload?;
    if !matches!(
        super::classify_payment_payload(&invoice_payload),
        super::PaymentPayloadKind::Subscription
    ) {
        return None;
    }
    Some(StarSubscriptionPayment {
        telegram_payment_charge_id: transaction.id,
        user_id: source.user.id,
        first_name: source.user.first_name,
        last_name: source.user.last_name,
        username: source.user.username,
        language_code: source.user.language_code,
        is_premium: source.user.is_premium,
        invoice_payload,
        amount_stars: transaction.amount,
        paid_at_unix: transaction.date,
        subscription_period_seconds: source
            .subscription_period
            .filter(|seconds| *seconds > 0)
            .unwrap_or(super::SUBSCRIPTION_PERIOD_SECONDS),
    })
}

#[derive(Debug, Deserialize)]
struct BotApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StarTransactionsResult {
    transactions: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RawStarTransaction {
    id: String,
    amount: i64,
    date: i64,
    source: Option<RawTransactionPartnerUser>,
    receiver: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RawTransactionPartnerUser {
    #[serde(rename = "type")]
    kind: String,
    transaction_type: Option<String>,
    user: RawStarUser,
    invoice_payload: Option<String>,
    subscription_period: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RawStarUser {
    id: i64,
    first_name: String,
    last_name: Option<String>,
    username: Option<String>,
    language_code: Option<String>,
    is_premium: Option<bool>,
}
