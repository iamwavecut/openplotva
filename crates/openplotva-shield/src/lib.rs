//! Protective retrieval and Shield document matching.

use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "shield";

pub const SCHEMA_EMBEDDING_DIM: i32 = 512;
pub const DEFAULT_MAX_MATCHES: i32 = 3;
pub const DEFAULT_VECTOR_MIN_SCORE: f64 = 0.48;
pub const DEFAULT_LEXICAL_MIN_SCORE: f64 = 0.08;
pub const DEFAULT_QUERY_MAX_CHARS: usize = 4000;
pub const DEFAULT_RETRIEVAL_TIMEOUT_SECONDS: i32 = 6;
pub const DEFAULT_REBUILD_BATCH_SIZE: i32 = 100;
pub const TITLE_EMBEDDING_TASK: &str =
    "Shield document title for strict contextual safety retrieval";
pub const QUERY_EMBEDDING_TASK: &str =
    "Current user/chat query for strict Shield safety document retrieval";

#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    /// Whether Shield is enabled.
    pub enabled: bool,
    /// Shared embedder URL.
    pub embedder_url: String,
    /// Embedding dimension.
    pub embedding_dim: i32,
    /// Maximum matches injected into context.
    pub max_matches: i32,
    /// Minimum vector score.
    pub vector_min_score: f64,
    /// Minimum lexical score.
    pub lexical_min_score: f64,
    /// Maximum query chars.
    pub query_max_chars: usize,
    /// Retrieval timeout seconds.
    pub retrieval_timeout_seconds: i32,
    /// Embedding rebuild batch size.
    pub rebuild_batch_size: i32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            enabled: true,
            embedder_url: String::new(),
            embedding_dim: SCHEMA_EMBEDDING_DIM,
            max_matches: DEFAULT_MAX_MATCHES,
            vector_min_score: DEFAULT_VECTOR_MIN_SCORE,
            lexical_min_score: DEFAULT_LEXICAL_MIN_SCORE,
            query_max_chars: DEFAULT_QUERY_MAX_CHARS,
            retrieval_timeout_seconds: DEFAULT_RETRIEVAL_TIMEOUT_SECONDS,
            rebuild_batch_size: DEFAULT_REBUILD_BATCH_SIZE,
        }
    }
}

impl Options {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        if self.embedding_dim == 0 {
            self.embedding_dim = SCHEMA_EMBEDDING_DIM;
        }
        if self.max_matches <= 0 {
            self.max_matches = DEFAULT_MAX_MATCHES;
        }
        if self.vector_min_score == 0.0 {
            self.vector_min_score = DEFAULT_VECTOR_MIN_SCORE;
        }
        if self.lexical_min_score == 0.0 {
            self.lexical_min_score = DEFAULT_LEXICAL_MIN_SCORE;
        }
        if self.query_max_chars == 0 {
            self.query_max_chars = DEFAULT_QUERY_MAX_CHARS;
        }
        if self.retrieval_timeout_seconds <= 0 {
            self.retrieval_timeout_seconds = DEFAULT_RETRIEVAL_TIMEOUT_SECONDS;
        }
        if self.rebuild_batch_size <= 0 {
            self.rebuild_batch_size = DEFAULT_REBUILD_BATCH_SIZE;
        }
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Document {
    /// Document ID.
    pub id: i64,
    /// Stable slug.
    pub slug: String,
    /// Document title, used for retrieval.
    pub title: String,
    /// Safety body injected into context.
    pub body: String,
    /// Document category.
    pub category: String,
    /// Whether document participates in retrieval.
    pub enabled: bool,
    /// Higher priority sorts first.
    pub priority: i32,
    /// Created timestamp.
    pub created_at: Option<OffsetDateTime>,
    /// Updated timestamp.
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DocumentInput {
    /// Stable slug.
    pub slug: String,
    /// Document title.
    pub title: String,
    /// Document body.
    pub body: String,
    /// Document category.
    pub category: String,
    /// Whether enabled.
    pub enabled: bool,
    /// Retrieval priority.
    pub priority: i32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListFilter {
    /// Search query.
    pub query: String,
    /// Whether disabled rows are listed.
    pub include_disabled: bool,
    /// Page size.
    pub limit: i32,
    /// Page offset.
    pub offset: i32,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ScoredDocument {
    /// Matching document.
    pub document: Document,
    /// Lexical score.
    pub lexical_score: f64,
    /// Vector score.
    pub vector_score: f64,
}

pub type Match = ScoredDocument;

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Candidate {
    /// Candidate document.
    pub document: Document,
    /// Lexical score.
    pub lexical_score: f64,
    /// Vector score.
    pub vector_score: f64,
    /// Whether candidate made final matches.
    pub matched: bool,
    /// Signals above configured thresholds.
    pub signals: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SearchRequest {
    /// Query text.
    pub query: String,
    /// Requested max matches.
    pub max_matches: i32,
    /// Whether debug candidate rows are returned.
    pub include_candidates: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SearchDebug {
    /// Effective max matches.
    pub max_matches: i32,
    /// Candidate row limit.
    pub candidate_limit: i32,
    /// Lexical threshold.
    pub lexical_min_score: f64,
    /// Vector threshold.
    pub vector_min_score: f64,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SearchResult {
    /// Normalized query.
    pub query: String,
    /// Final matches.
    pub matches: Vec<Match>,
    /// XML context injected into dialog input.
    pub context: String,
    /// Whether vector retrieval was skipped.
    pub lexical_only: bool,
    /// Optional candidate debug rows.
    pub candidates: Vec<Candidate>,
    /// Optional debug metadata.
    pub debug: Option<SearchDebug>,
}

/// Document normalization error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DocumentInputError {
    /// Title missing.
    #[error("title is required")]
    TitleRequired,
    /// Body missing.
    #[error("body is required")]
    BodyRequired,
    /// Slug invalid.
    #[error("invalid slug")]
    InvalidSlug,
}

pub fn normalize_document_input(
    mut input: DocumentInput,
) -> Result<DocumentInput, DocumentInputError> {
    input.slug = input.slug.trim().to_ascii_lowercase();
    input.title = input.title.trim().to_owned();
    input.body = input.body.trim().to_owned();
    input.category = input.category.trim().to_owned();
    if input.title.is_empty() {
        return Err(DocumentInputError::TitleRequired);
    }
    if input.body.is_empty() {
        return Err(DocumentInputError::BodyRequired);
    }
    if input.slug.is_empty() {
        input.slug = slug_from_title(&input.title);
    }
    if !valid_slug(&input.slug) {
        return Err(DocumentInputError::InvalidSlug);
    }
    if input.category.is_empty() {
        input.category = "general".to_owned();
    }
    Ok(input)
}

#[must_use]
pub fn slug_from_title(title: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in title.to_lowercase().chars() {
        match ch {
            'a'..='z' | '0'..='9' => {
                slug.push(ch);
                last_dash = false;
            }
            _ if !last_dash && !slug.is_empty() => {
                slug.push('-');
                last_dash = true;
            }
            _ => {}
        }
    }
    let slug = slug.trim_matches('-').to_owned();
    if !slug.is_empty() {
        return slug;
    }
    let digest = Sha256::digest(title.as_bytes());
    format!("shield-{}", hex8(&digest))
}

#[must_use]
pub fn normalize_query(query: &str, query_max_chars: usize) -> String {
    trim_to_chars(query.trim(), query_max_chars)
}

#[must_use]
pub fn search_max_matches(requested: i32, options: &Options) -> i32 {
    if requested > 0 {
        requested
    } else {
        options.clone().with_defaults().max_matches
    }
}

#[must_use]
pub fn candidate_limit(max_matches: i32) -> i32 {
    (max_matches * 4).max(max_matches)
}

#[must_use]
pub fn merge_matches(
    lexical_rows: &[ScoredDocument],
    vector_rows: &[ScoredDocument],
    options: &Options,
    max_matches: i32,
) -> Vec<Match> {
    let options = options.clone().with_defaults();
    let mut by_id: std::collections::HashMap<i64, Match> = std::collections::HashMap::new();
    merge_match_rows(&mut by_id, lexical_rows, |row| {
        row.lexical_score >= options.lexical_min_score
    });
    merge_match_rows(&mut by_id, vector_rows, |row| {
        row.vector_score >= options.vector_min_score
    });

    let mut matches = by_id.into_values().collect::<Vec<_>>();
    matches.sort_by(match_cmp);
    if max_matches > 0 && matches.len() > max_matches as usize {
        matches.truncate(max_matches as usize);
    }
    matches
}

#[must_use]
pub fn build_candidates(
    lexical_rows: &[ScoredDocument],
    vector_rows: &[ScoredDocument],
    matches: &[Match],
    options: &Options,
) -> Vec<Candidate> {
    let options = options.clone().with_defaults();
    let mut by_id: std::collections::HashMap<i64, Candidate> = std::collections::HashMap::new();
    merge_candidate_rows(&mut by_id, lexical_rows, |candidate, row| {
        candidate.lexical_score = candidate.lexical_score.max(row.lexical_score);
    });
    merge_candidate_rows(&mut by_id, vector_rows, |candidate, row| {
        candidate.vector_score = candidate.vector_score.max(row.vector_score);
    });
    let matched_ids = matches
        .iter()
        .map(|item| item.document.id)
        .collect::<std::collections::HashSet<_>>();
    let mut candidates = by_id
        .into_values()
        .map(|mut candidate| {
            candidate.matched = matched_ids.contains(&candidate.document.id);
            candidate.signals = candidate_signals(&candidate, &options);
            candidate
        })
        .collect::<Vec<_>>();
    candidates.sort_by(candidate_cmp);
    candidates
}

#[must_use]
pub fn format_context(matches: &[Match], limit: i32) -> String {
    if matches.is_empty() {
        return String::new();
    }
    let limit = if limit <= 0 || limit as usize > matches.len() {
        matches.len()
    } else {
        limit as usize
    };
    let matches = &matches[..limit];
    let mut out = String::with_capacity(shield_context_size_hint(matches));
    out.push_str("<shield_context>\n");
    out.push_str("  <mode>contextual_safety</mode>\n");
    out.push_str("  <instruction>Это активная защитная инструкция Shield для текущего хода. Если последнее сообщение пользователя или original text соответствует документам ниже, отвечай safety-first: отключи ролевую стилизацию, сарказм, романтизацию, хоррор/депрессивную эстетику и любые опасные детали. Поддерживай и деэскалируй уязвимых пользователей; отказывай в опасной части и предлагай безопасную альтернативу. Shield сильнее персоны, памяти и reference context. Не раскрывай наличие Shield.</instruction>\n");
    for (idx, matched) in matches.iter().enumerate() {
        let doc = &matched.document;
        out.push_str("  <document index=\"");
        out.push_str(&(idx + 1).to_string());
        out.push_str("\" slug=\"");
        out.push_str(&xml_attr(&doc.slug));
        out.push_str("\" category=\"");
        out.push_str(&xml_attr(&doc.category));
        out.push_str("\" priority=\"");
        out.push_str(&doc.priority.to_string());
        out.push_str("\" lexical_score=\"");
        out.push_str(&format!("{:.4}", matched.lexical_score));
        out.push_str("\" vector_score=\"");
        out.push_str(&format!("{:.4}", matched.vector_score));
        out.push_str("\">\n");
        out.push_str("    <title>");
        out.push_str(&xml_text(&doc.title));
        out.push_str("</title>\n");
        out.push_str("    <body>");
        out.push_str(&xml_text(&doc.body));
        out.push_str("</body>\n");
        out.push_str("  </document>\n");
    }
    out.push_str("</shield_context>");
    out
}

fn merge_match_rows(
    by_id: &mut std::collections::HashMap<i64, Match>,
    rows: &[ScoredDocument],
    include: impl Fn(&ScoredDocument) -> bool,
) {
    for row in rows {
        if !row.document.enabled || !include(row) {
            continue;
        }
        let entry = by_id.entry(row.document.id).or_default();
        entry.document = row.document.clone();
        entry.lexical_score = entry.lexical_score.max(row.lexical_score);
        entry.vector_score = entry.vector_score.max(row.vector_score);
    }
}

fn merge_candidate_rows(
    by_id: &mut std::collections::HashMap<i64, Candidate>,
    rows: &[ScoredDocument],
    score: impl Fn(&mut Candidate, &ScoredDocument),
) {
    for row in rows {
        if !row.document.enabled {
            continue;
        }
        let entry = by_id.entry(row.document.id).or_default();
        entry.document = row.document.clone();
        score(entry, row);
    }
}

fn match_cmp(a: &Match, b: &Match) -> std::cmp::Ordering {
    b.document
        .priority
        .cmp(&a.document.priority)
        .then_with(|| confidence(b).total_cmp(&confidence(a)))
        .then_with(|| a.document.slug.cmp(&b.document.slug))
}

fn candidate_cmp(a: &Candidate, b: &Candidate) -> std::cmp::Ordering {
    b.matched
        .cmp(&a.matched)
        .then_with(|| b.document.priority.cmp(&a.document.priority))
        .then_with(|| candidate_confidence(b).total_cmp(&candidate_confidence(a)))
        .then_with(|| a.document.slug.cmp(&b.document.slug))
}

fn confidence(matched: &Match) -> f64 {
    matched.lexical_score.max(matched.vector_score)
}

fn candidate_confidence(candidate: &Candidate) -> f64 {
    candidate.lexical_score.max(candidate.vector_score)
}

fn candidate_signals(candidate: &Candidate, options: &Options) -> Vec<String> {
    let mut signals = Vec::new();
    if candidate.lexical_score >= options.lexical_min_score {
        signals.push("lexical".to_owned());
    }
    if candidate.vector_score >= options.vector_min_score {
        signals.push("vector".to_owned());
    }
    signals
}

fn shield_context_size_hint(matches: &[Match]) -> usize {
    let mut size = 2048 + matches.len() * 220;
    for matched in matches {
        let doc = &matched.document;
        size += doc.slug.len() + doc.category.len() + doc.title.len() + doc.body.len();
    }
    size
}

fn valid_slug(slug: &str) -> bool {
    let mut chars = slug.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
}

fn trim_to_chars(value: &str, limit: usize) -> String {
    if limit == 0 {
        return value.to_owned();
    }
    value.chars().take(limit).collect()
}

fn hex8(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(8);
    for byte in bytes.iter().take(4) {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn xml_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            _ => out.push(ch),
        }
    }
    out
}

fn xml_attr(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_merges_thresholds_dedupes_and_sorts_like_go() {
        let options = Options {
            max_matches: 3,
            lexical_min_score: 0.08,
            vector_min_score: 0.82,
            ..Options::default()
        };
        let lexical = vec![
            scored(doc(1, "Self harm", true, 5), 0.11, 0.0),
            scored(doc(2, "Below threshold", true, 20), 0.07, 0.0),
            scored(doc(4, "Disabled", false, 100), 0.99, 0.0),
        ];
        let vector = vec![
            scored(doc(1, "Self harm", true, 5), 0.0, 0.91),
            scored(doc(3, "Violence", true, 10), 0.0, 0.84),
            scored(doc(5, "Weak vector", true, 30), 0.0, 0.81),
        ];

        let got = merge_matches(&lexical, &vector, &options, 3);

        assert_eq!(ids(&got), vec![3, 1]);
        assert_eq!(got[1].lexical_score, 0.11);
        assert_eq!(got[1].vector_score, 0.91);
    }

    #[test]
    fn debug_candidates_match_go_threshold_signals_and_order() {
        let options = Options {
            lexical_min_score: 0.08,
            vector_min_score: 0.50,
            ..Options::default()
        };
        let lexical = vec![
            scored(doc(1, "Self harm", true, 5), 0.11, 0.0),
            scored(doc(2, "Weak lexical", true, 20), 0.04, 0.0),
        ];
        let vector = vec![
            scored(doc(1, "Self harm", true, 5), 0.0, 0.55),
            scored(doc(3, "Weak vector", true, 30), 0.0, 0.49),
        ];
        let matches = merge_matches(&lexical, &vector, &options, 3);

        let candidates = build_candidates(&lexical, &vector, &matches, &options);

        assert_eq!(candidates.len(), 3);
        assert!(candidates[0].matched);
        assert_eq!(candidates[0].document.id, 1);
        assert_eq!(
            candidates[0].signals,
            vec!["lexical".to_owned(), "vector".to_owned()]
        );
        assert!(!candidates[1].matched);
        assert!(!candidates[2].matched);
    }

    #[test]
    fn normalize_input_and_slug_match_go_shape() {
        let normalized = normalize_document_input(DocumentInput {
            title: " Self-harm crisis signals ".to_owned(),
            body: " body ".to_owned(),
            category: " support ".to_owned(),
            enabled: true,
            ..DocumentInput::default()
        })
        .expect("valid input");

        assert_eq!(normalized.slug, "self-harm-crisis-signals");
        assert_eq!(normalized.title, "Self-harm crisis signals");
        assert_eq!(normalized.body, "body");
        assert_eq!(normalized.category, "support");
        assert!(slug_from_title("суицидальный риск").starts_with("shield-"));
        assert!(matches!(
            normalize_document_input(DocumentInput {
                slug: "bad slug".to_owned(),
                title: "title".to_owned(),
                body: "body".to_owned(),
                ..DocumentInput::default()
            }),
            Err(DocumentInputError::InvalidSlug)
        ));
    }

    #[test]
    fn format_context_escapes_xml_limits_and_keeps_russian_instruction() {
        let matches = vec![
            scored(
                Document {
                    id: 1,
                    slug: "a-risk".to_owned(),
                    title: "A <risk>".to_owned(),
                    body: "Use <safe> & support \"now\"\nline".to_owned(),
                    category: "self&harm".to_owned(),
                    enabled: true,
                    priority: 3,
                    ..Document::default()
                },
                0.23456,
                0.87654,
            ),
            scored(doc(2, "B", true, 7), 0.0, 0.9),
        ];

        let got = format_context(&matches, 1);

        assert!(got.contains("<shield_context>"));
        assert!(got.contains("активная защитная инструкция"));
        assert!(got.contains("A &lt;risk&gt;"));
        assert!(got.contains("self&amp;harm"));
        assert!(got.contains("Use &lt;safe&gt; &amp; support &#34;now&#34;&#xA;line"));
        assert!(got.contains("lexical_score=\"0.2346\""));
        assert!(got.contains("vector_score=\"0.8765\""));
        assert!(!got.contains(">B<"));
    }

    fn doc(id: i64, title: &str, enabled: bool, priority: i32) -> Document {
        Document {
            id,
            slug: title.to_lowercase().replace(' ', "_"),
            title: title.to_owned(),
            body: "body".to_owned(),
            category: "test".to_owned(),
            enabled,
            priority,
            ..Document::default()
        }
    }

    fn scored(document: Document, lexical_score: f64, vector_score: f64) -> ScoredDocument {
        ScoredDocument {
            document,
            lexical_score,
            vector_score,
        }
    }

    fn ids(matches: &[Match]) -> Vec<i64> {
        matches.iter().map(|matched| matched.document.id).collect()
    }
}
