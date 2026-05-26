use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value, json};

use crate::ToolParseError;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlotvaFinalResponse {
    /// Dialog step, usually `final_response`.
    #[serde(default)]
    pub step: String,
    /// Final answer text.
    #[serde(default)]
    pub answer: String,
}

#[must_use]
pub const fn plotva_known_keys() -> &'static [&'static str] {
    &["answer", "response"]
}

pub fn decode_plotva_final_response_with_salvage(
    raw: &str,
) -> Result<PlotvaFinalResponse, ToolParseError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ToolParseError::new("empty json payload"));
    }

    if let Ok(decoded) = serde_json::from_str::<PlotvaFinalResponse>(trimmed) {
        return Ok(decoded);
    }
    if let Some(extracted) = trim_to_json_object(trimmed)
        && extracted != trimmed
        && let Ok(decoded) = serde_json::from_str::<PlotvaFinalResponse>(&extracted)
    {
        return Ok(decoded);
    }

    let value = salvage_json_object_value(trimmed, plotva_known_keys())
        .ok_or_else(|| ToolParseError::new("strict decode failed and salvage failed"))?;
    Ok(PlotvaFinalResponse {
        step: find_json_key_value(&value, "step")
            .and_then(coerce_string_value)
            .unwrap_or_default(),
        answer: find_json_key_value(&value, "answer")
            .and_then(coerce_string_value)
            .unwrap_or_default(),
    })
}

/// Return a compact JSON object containing the salvaged `answer`.
#[must_use]
pub fn salvage_json_object_text(raw: &str, known_keys: &[&str]) -> Option<String> {
    let keys = normalize_known_keys(known_keys);
    let source = trim_to_json_object(raw).unwrap_or_else(|| raw.trim().to_owned());
    let root = parse_root_object(&source);
    let mut value = match root {
        Some(value) if contains_known_key(&value, &keys) => value,
        _ => {
            let answer = salvage_answer_only(raw, &keys)?;
            return build_answer_only_json(&answer);
        }
    };

    let mut answer = extract_answer(&value);
    if answer.trim().is_empty()
        && let Some(fallback) = salvage_answer_only(raw, &keys)
    {
        answer = fallback;
    }
    let answer = answer.trim();
    if answer.is_empty() {
        return None;
    }

    if let Value::Object(map) = &mut value {
        map.insert("answer".to_owned(), Value::String(answer.to_owned()));
    }
    build_answer_only_json(answer)
}

/// Return a salvaged JSON object when it contains one of the known keys.
#[must_use]
pub fn salvage_json_object_value(raw: &str, known_keys: &[&str]) -> Option<Value> {
    let keys = normalize_known_keys(known_keys);
    let source = trim_to_json_object(raw).unwrap_or_else(|| raw.trim().to_owned());
    let mut root = parse_root_object(&source)?;
    if !contains_known_key(&root, &keys) {
        return salvage_answer_only(raw, &keys)
            .and_then(|answer| build_answer_value(&answer))
            .map(Value::Object);
    }
    let answer = extract_answer(&root);
    if let Value::Object(map) = &mut root {
        if !answer.trim().is_empty() {
            map.insert("answer".to_owned(), Value::String(answer.trim().to_owned()));
        } else if let Some(fallback) = salvage_answer_only(raw, &keys) {
            map.insert(
                "answer".to_owned(),
                Value::String(fallback.trim().to_owned()),
            );
        }
    }
    Some(root)
}

#[must_use]
pub fn parse_json_object_tolerant(raw: &str, last_wins: bool) -> Option<Value> {
    let start = raw.find('{')?;
    let mut parser = TolerantParser {
        raw,
        index: start,
        last_wins,
    };
    parser
        .parse_object()
        .filter(|map| !map.is_empty())
        .map(Value::Object)
}

/// Recursively find a key value in deterministic key order.
#[must_use]
pub fn find_json_key_value<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if let Some(value) = map.get(key) {
                return Some(value);
            }
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            keys.into_iter()
                .find_map(|nested_key| find_json_key_value(&map[nested_key], key))
        }
        Value::Array(items) => items.iter().find_map(|item| find_json_key_value(item, key)),
        _ => None,
    }
}

#[must_use]
pub fn coerce_string_value(value: &Value) -> Option<String> {
    match extract_scalar(value)? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

#[must_use]
pub fn coerce_bool_value(value: &Value) -> Option<bool> {
    match extract_scalar(value)? {
        Value::Bool(value) => Some(*value),
        Value::String(value) => parse_bool(value),
        other => coerce_f64_value(other).map(|value| value != 0.0),
    }
}

#[must_use]
pub fn coerce_i64_value(value: &Value) -> Option<i64> {
    match extract_scalar(value)? {
        Value::Bool(true) => Some(1),
        Value::Bool(false) => Some(0),
        Value::Number(value) => value
            .as_i64()
            .or_else(|| value.as_f64().map(|value| value as i64)),
        Value::String(value) => value
            .trim()
            .parse::<i64>()
            .ok()
            .or_else(|| value.trim().parse::<f64>().ok().map(|value| value as i64)),
        _ => None,
    }
}

#[must_use]
pub fn coerce_u64_value(value: &Value) -> Option<u64> {
    match extract_scalar(value)? {
        Value::Bool(true) => Some(1),
        Value::Bool(false) => Some(0),
        Value::Number(value) => value
            .as_u64()
            .or_else(|| value.as_f64().map(|value| value as u64)),
        Value::String(value) => value
            .trim()
            .parse::<u64>()
            .ok()
            .or_else(|| value.trim().parse::<f64>().ok().map(|value| value as u64)),
        _ => None,
    }
}

#[must_use]
pub fn coerce_f64_value(value: &Value) -> Option<f64> {
    match extract_scalar(value)? {
        Value::Bool(true) => Some(1.0),
        Value::Bool(false) => Some(0.0),
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn normalize_known_keys(keys: &[&str]) -> Vec<String> {
    let mut out = Vec::with_capacity(keys.len().max(plotva_known_keys().len()));
    for key in keys.iter().copied().filter_map(non_empty_trimmed) {
        if !out.iter().any(|existing| existing == key) {
            out.push(key.to_owned());
        }
    }
    if out.is_empty() {
        return plotva_known_keys()
            .iter()
            .map(|key| (*key).to_owned())
            .collect();
    }
    out
}

fn non_empty_trimmed(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn parse_root_object(raw: &str) -> Option<Value> {
    if let Ok(value) = serde_json::from_str::<Value>(raw)
        && matches!(value, Value::Object(ref map) if !map.is_empty())
    {
        return Some(value);
    }
    parse_json_object_tolerant(raw, true)
}

fn contains_known_key(value: &Value, keys: &[String]) -> bool {
    keys.iter()
        .any(|key| find_json_key_value(value, key).is_some())
}

fn extract_answer(value: &Value) -> String {
    for key in ["answer", "response"] {
        if let Some(answer) = find_json_key_value(value, key).and_then(coerce_string_value) {
            let answer = answer.trim();
            if !answer.is_empty() {
                return answer.to_owned();
            }
        }
    }
    String::new()
}

fn build_answer_only_json(answer: &str) -> Option<String> {
    let answer = answer.trim();
    if answer.is_empty() {
        return None;
    }
    Some(json!({ "answer": answer }).to_string())
}

fn build_answer_value(answer: &str) -> Option<Map<String, Value>> {
    let answer = answer.trim();
    if answer.is_empty() {
        return None;
    }
    let mut map = Map::new();
    map.insert("answer".to_owned(), Value::String(answer.to_owned()));
    Some(map)
}

fn extract_scalar(value: &Value) -> Option<&Value> {
    match value {
        Value::String(_) | Value::Bool(_) | Value::Number(_) => Some(value),
        Value::Object(map) => {
            if let Some(value) = map.get("value").and_then(extract_scalar) {
                return Some(value);
            }
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            keys.into_iter().find_map(|key| extract_scalar(&map[key]))
        }
        Value::Array(items) => items.iter().find_map(extract_scalar),
        Value::Null => None,
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim() {
        "1" => Some(true),
        "0" => Some(false),
        value if value.eq_ignore_ascii_case("t") || value.eq_ignore_ascii_case("true") => {
            Some(true)
        }
        value if value.eq_ignore_ascii_case("f") || value.eq_ignore_ascii_case("false") => {
            Some(false)
        }
        _ => None,
    }
}

fn salvage_answer_only(raw: &str, known_keys: &[String]) -> Option<String> {
    salvage_answer_by_prefix(raw, "answer\":\"", known_keys)
        .or_else(|| salvage_answer_by_prefix(raw, "answer:\"", known_keys))
        .or_else(|| {
            let start = find_answer_start(raw)?;
            let end = find_answer_end(raw, start, known_keys);
            (end > start)
                .then(|| raw[start..end].trim().to_owned())
                .filter(|answer| !answer.is_empty())
        })
}

fn salvage_answer_by_prefix(raw: &str, prefix: &str, known_keys: &[String]) -> Option<String> {
    let start = raw.find(prefix)? + prefix.len();
    let end = find_answer_end(raw, start, known_keys);
    (end > start)
        .then(|| raw[start..end].trim().to_owned())
        .filter(|answer| !answer.is_empty())
}

fn find_answer_start(raw: &str) -> Option<usize> {
    let mut position = 0;
    loop {
        let idx = next_answer_candidate(raw, position)?;
        if let Some(start) = quoted_answer_value_start(raw, idx) {
            return Some(start);
        }
        position = idx + "answer".len();
    }
}

fn next_answer_candidate(raw: &str, position: usize) -> Option<usize> {
    let mut position = position;
    loop {
        let idx = raw[position..].find("answer")? + position;
        if idx == 0 || !is_word_byte(raw.as_bytes()[idx - 1]) {
            return Some(idx);
        }
        position = idx + "answer".len();
    }
}

fn quoted_answer_value_start(raw: &str, answer_idx: usize) -> Option<usize> {
    let mut index = skip_answer_key_decorators(raw, answer_idx + "answer".len());
    if raw.as_bytes().get(index) != Some(&b':') {
        return None;
    }
    index = skip_spaces(raw, index + 1);
    raw.as_bytes()
        .get(index)
        .is_some_and(|byte| is_quote_byte(*byte))
        .then_some(index + 1)
}

fn skip_answer_key_decorators(raw: &str, mut index: usize) -> usize {
    while let Some(byte) = raw.as_bytes().get(index)
        && (is_quote_byte(*byte) || is_space_byte(*byte))
    {
        index += 1;
    }
    index
}

fn find_answer_end(raw: &str, start: usize, known_keys: &[String]) -> usize {
    let mut index = start;
    while let Some(relative) = raw[index..].find('"') {
        let quote = index + relative;
        if !is_escaped(raw, quote) && is_answer_terminator(raw, quote, known_keys) {
            return quote;
        }
        index = quote + 1;
    }
    raw.len()
}

fn is_answer_terminator(raw: &str, quote: usize, known_keys: &[String]) -> bool {
    let after = &raw[quote..];
    if after.starts_with("\",\"") && has_known_key_after(raw, quote + 2, known_keys) {
        return true;
    }
    if after.starts_with("\"}") {
        return true;
    }
    if after.starts_with("\"\n") && has_known_key_after(raw, quote + 2, known_keys) {
        return true;
    }
    is_answer_terminator_after_quote(raw, skip_spaces(raw, quote + 1), known_keys)
}

fn is_answer_terminator_after_quote(raw: &str, index: usize, known_keys: &[String]) -> bool {
    let Some(byte) = raw.as_bytes().get(index).copied() else {
        return true;
    };
    match byte {
        b'}' => true,
        b',' => has_known_key_after(raw, index + 1, known_keys),
        b'"' => has_known_key_after(raw, index, known_keys),
        _ => false,
    }
}

fn has_known_key_after(raw: &str, index: usize, known_keys: &[String]) -> bool {
    let index = skip_spaces(raw, index);
    let Some(byte) = raw.as_bytes().get(index).copied() else {
        return true;
    };
    if byte != b'"' {
        return known_keys.iter().any(|key| {
            raw[index..].starts_with(key) && is_key_boundary(raw.as_bytes(), index + key.len())
        });
    }
    known_keys
        .iter()
        .any(|key| raw[index..].starts_with(&format!("\"{key}\"")))
}

fn is_key_boundary(raw: &[u8], index: usize) -> bool {
    raw.get(index).is_none_or(|byte| {
        matches!(
            byte,
            b':' | b'"' | b' ' | b'\t' | b'\n' | b'\r' | b',' | b'}' | b']'
        )
    })
}

fn trim_to_json_object(raw: &str) -> Option<String> {
    let mut scan = ObjectSpanScan::default();
    for (index, _) in raw.char_indices() {
        if scan.consume(raw, index) {
            return Some(raw[scan.start?..=index].to_owned());
        }
    }
    scan.start.map(|start| raw[start..].trim().to_owned())
}

#[derive(Default)]
struct ObjectSpanScan {
    start: Option<usize>,
    in_string: bool,
    depth: i32,
}

impl ObjectSpanScan {
    fn consume(&mut self, raw: &str, index: usize) -> bool {
        let byte = raw.as_bytes()[index];
        if byte == b'"' && !is_escaped(raw, index) {
            self.in_string = !self.in_string;
            return false;
        }
        if self.in_string {
            return false;
        }
        if self.start.is_none() {
            if byte == b'{' {
                self.start = Some(index);
                self.depth = 1;
            }
            return false;
        }
        match byte {
            b'{' => self.depth += 1,
            b'}' => {
                self.depth -= 1;
                return self.depth == 0;
            }
            _ => {}
        }
        false
    }
}

struct TolerantParser<'a> {
    raw: &'a str,
    index: usize,
    last_wins: bool,
}

impl TolerantParser<'_> {
    fn parse_object(&mut self) -> Option<Map<String, Value>> {
        if !self.consume_byte(b'{') {
            return None;
        }
        let mut map = Map::new();
        while self.index < self.raw.len() {
            self.skip_whitespace();
            if self.index >= self.raw.len() {
                break;
            }
            if self.consume_byte(b'}') {
                return (!map.is_empty()).then_some(map);
            }
            if self.consume_byte(b',') {
                continue;
            }
            let Some((key, value)) = self.parse_object_pair() else {
                continue;
            };
            if self.last_wins || !map.contains_key(&key) {
                map.insert(key, value);
            }
            self.skip_whitespace();
            self.consume_byte(b',');
        }
        (!map.is_empty()).then_some(map)
    }

    fn parse_object_pair(&mut self) -> Option<(String, Value)> {
        let key = self.parse_key().filter(|key| !key.is_empty())?;
        self.skip_whitespace();
        if self.peek_byte() != Some(b':') {
            self.seek_to_colon();
        }
        if !self.consume_byte(b':') {
            self.skip_bad_pair();
            return None;
        }
        let Some(value) = self.parse_value() else {
            self.skip_bad_pair();
            return None;
        };
        if matches!(value, Value::Object(ref map) if map.is_empty()) {
            return None;
        }
        Some((key, value))
    }

    fn parse_key(&mut self) -> Option<String> {
        self.skip_whitespace();
        match self.peek_byte()? {
            b'"' => self.parse_string_with_terminators(b":"),
            _ => self.parse_bare_key(),
        }
    }

    fn parse_value(&mut self) -> Option<Value> {
        self.skip_whitespace();
        match self.peek_byte()? {
            b'{' => self.parse_object().map(Value::Object),
            b'[' => Some(Value::Array(self.parse_array())),
            b'"' => self
                .parse_string_with_terminators(b",}]")
                .map(Value::String),
            b't' | b'f' | b'n' => self.parse_literal(),
            byte if is_number_start(byte) => self.parse_number(),
            _ => self.parse_loose_string_value(b"}]").map(Value::String),
        }
    }

    fn parse_array(&mut self) -> Vec<Value> {
        if !self.consume_byte(b'[') {
            return Vec::new();
        }
        let mut values = Vec::new();
        while self.index < self.raw.len() {
            self.skip_whitespace();
            if self.consume_byte(b']') {
                return values;
            }
            if self.consume_byte(b',') {
                continue;
            }
            if let Some(value) = self.parse_value() {
                if !matches!(value, Value::Object(ref map) if map.is_empty()) {
                    values.push(value);
                }
                self.skip_whitespace();
                self.consume_byte(b',');
            } else {
                self.skip_bad_array_value();
            }
        }
        values
    }

    fn parse_string_with_terminators(&mut self, terminators: &[u8]) -> Option<String> {
        if !self.consume_byte(b'"') {
            return None;
        }
        let mut out = String::new();
        while self.index < self.raw.len() {
            let ch = self.next_char()?;
            match ch {
                '"' => {
                    let quote = self.index - 1;
                    if quote_terminates_string(self.raw, quote, terminators) {
                        return Some(out);
                    }
                    out.push('"');
                }
                '\\' => {
                    if let Some(escaped) = self.parse_escape() {
                        out.push(escaped);
                    } else {
                        return Some(out);
                    }
                }
                other => out.push(other),
            }
        }
        Some(out)
    }

    fn parse_escape(&mut self) -> Option<char> {
        let next = self.next_char()?;
        if next == 'u' {
            let end = self.index.checked_add(4)?;
            if let Some(hex) = self.raw.get(self.index..end)
                && let Ok(value) = u32::from_str_radix(hex, 16)
                && let Some(ch) = char::from_u32(value)
            {
                self.index = end;
                return Some(ch);
            }
        }
        Some(match next {
            'n' => '\n',
            't' => '\t',
            'r' => '\r',
            '"' => '"',
            '\\' => '\\',
            other => other,
        })
    }

    fn parse_bare_key(&mut self) -> Option<String> {
        let start = self.index;
        while let Some(byte) = self.peek_byte() {
            if !is_bare_key_byte(byte) {
                break;
            }
            self.index += 1;
        }
        (self.index > start).then(|| self.raw[start..self.index].to_owned())
    }

    fn parse_number(&mut self) -> Option<Value> {
        let start = self.index;
        self.consume_byte(b'-');
        self.consume_digits();
        if self.consume_byte(b'.') {
            self.consume_digits();
        }
        if self.consume_any(b"eE") {
            self.consume_any(b"+-");
            self.consume_digits();
        }
        if self.index == start {
            return None;
        }
        parse_number_literal(&self.raw[start..self.index])
    }

    fn parse_literal(&mut self) -> Option<Value> {
        for (literal, value) in [
            ("true", Value::Bool(true)),
            ("false", Value::Bool(false)),
            ("null", Value::Null),
        ] {
            if self.raw[self.index..].starts_with(literal) {
                self.index += literal.len();
                return Some(value);
            }
        }
        None
    }

    fn parse_loose_string_value(&mut self, end_delims: &[u8]) -> Option<String> {
        let start = self.index;
        while let Some(byte) = self.peek_byte() {
            if byte == b',' || end_delims.contains(&byte) {
                break;
            }
            self.index += char_len_at(self.raw, self.index);
        }
        let value = self.raw[start..self.index].trim();
        (!value.is_empty()).then(|| value.to_owned())
    }

    fn consume_digits(&mut self) {
        while self.peek_byte().is_some_and(is_digit) {
            self.index += 1;
        }
    }

    fn consume_any(&mut self, bytes: &[u8]) -> bool {
        self.peek_byte().is_some_and(|byte| bytes.contains(&byte)) && {
            self.index += 1;
            true
        }
    }

    fn consume_byte(&mut self, byte: u8) -> bool {
        if self.peek_byte() != Some(byte) {
            return false;
        }
        self.index += 1;
        true
    }

    fn skip_whitespace(&mut self) {
        self.index = skip_spaces(self.raw, self.index);
    }

    fn seek_to_colon(&mut self) {
        while let Some(byte) = self.peek_byte() {
            if matches!(byte, b':' | b',' | b'}') {
                return;
            }
            self.index += char_len_at(self.raw, self.index);
        }
    }

    fn skip_bad_pair(&mut self) {
        self.skip_to_delimiter(b'}', b',');
        self.consume_byte(b',');
    }

    fn skip_bad_array_value(&mut self) {
        self.skip_to_delimiter(b']', b',');
        self.consume_byte(b',');
    }

    fn skip_to_delimiter(&mut self, end1: u8, end2: u8) {
        let mut scan = DelimiterScan::default();
        while let Some(byte) = self.peek_byte() {
            if scan.consume_quote(self.raw, self.index) {
                self.index += 1;
                continue;
            }
            if scan.in_string {
                self.index += char_len_at(self.raw, self.index);
                continue;
            }
            if scan.should_stop(byte, end1, end2) {
                return;
            }
            scan.track_depth(byte);
            self.index += char_len_at(self.raw, self.index);
        }
    }

    fn next_char(&mut self) -> Option<char> {
        let ch = self.raw[self.index..].chars().next()?;
        self.index += ch.len_utf8();
        Some(ch)
    }

    fn peek_byte(&self) -> Option<u8> {
        self.raw.as_bytes().get(self.index).copied()
    }
}

#[derive(Default)]
struct DelimiterScan {
    in_string: bool,
    depth_object: i32,
    depth_array: i32,
}

impl DelimiterScan {
    fn consume_quote(&mut self, raw: &str, index: usize) -> bool {
        if raw.as_bytes().get(index) != Some(&b'"') || is_escaped(raw, index) {
            return false;
        }
        self.in_string = !self.in_string;
        true
    }

    fn should_stop(&self, byte: u8, end1: u8, end2: u8) -> bool {
        match byte {
            b'}' => self.depth_object == 0,
            b']' => self.depth_array == 0,
            _ => self.depth_object == 0 && self.depth_array == 0 && (byte == end1 || byte == end2),
        }
    }

    fn track_depth(&mut self, byte: u8) {
        match byte {
            b'{' => self.depth_object += 1,
            b'}' => self.depth_object -= 1,
            b'[' => self.depth_array += 1,
            b']' => self.depth_array -= 1,
            _ => {}
        }
    }
}

fn parse_number_literal(value: &str) -> Option<Value> {
    if !value.contains(['.', 'e', 'E'])
        && let Ok(value) = value.parse::<i64>()
    {
        return Some(Value::Number(Number::from(value)));
    }
    value
        .parse::<f64>()
        .ok()
        .and_then(Number::from_f64)
        .map(Value::Number)
}

fn quote_terminates_string(raw: &str, quote_index: usize, terminators: &[u8]) -> bool {
    let index = skip_spaces(raw, quote_index + 1);
    raw.as_bytes()
        .get(index)
        .is_none_or(|byte| terminators.contains(byte))
}

fn skip_spaces(raw: &str, mut index: usize) -> usize {
    while raw
        .as_bytes()
        .get(index)
        .is_some_and(|byte| is_space_byte(*byte))
    {
        index += 1;
    }
    index
}

fn char_len_at(raw: &str, index: usize) -> usize {
    raw[index..].chars().next().map_or(1, char::len_utf8)
}

fn is_escaped(raw: &str, index: usize) -> bool {
    let mut count = 0;
    let mut cursor = index;
    while cursor > 0 && raw.as_bytes()[cursor - 1] == b'\\' {
        count += 1;
        cursor -= 1;
    }
    count % 2 == 1
}

fn is_quote_byte(byte: u8) -> bool {
    byte == b'"' || byte == b'\''
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_space_byte(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

fn is_digit(byte: u8) -> bool {
    byte.is_ascii_digit()
}

fn is_number_start(byte: u8) -> bool {
    byte == b'-' || is_digit(byte)
}

fn is_bare_key_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_json_object_with_salvage_preserves_quoted_answer() -> Result<(), ToolParseError> {
        let decoded = decode_plotva_final_response_with_salvage(
            r#"{"step":"final_response","answer":"Он сказал "Привет" и ушел"}"#,
        )?;

        assert_eq!(decoded.step, "final_response");
        assert_eq!(decoded.answer, r#"Он сказал "Привет" и ушел"#);
        Ok(())
    }

    #[test]
    fn salvage_json_object_text_handles_wrapped_and_answer_only_inputs() {
        for (raw, want) in [
            (
                r#"{"answer":"Он сказал "Привет" и ушел"}{"answer":"Он сказал "Привет" и ушел"}"#,
                r#"Он сказал "Привет" и ушел"#,
            ),
            (
                r#"noise >>> {"answer":"Так и запишем: "да"."} <<< tail"#,
                r#"Так и запишем: "да"."#,
            ),
            (
                r#"garbage answer":"Он сказал "Это не JSON", а потом ушел","response":oops"#,
                r#"Он сказал "Это не JSON", а потом ушел"#,
            ),
        ] {
            let out = salvage_json_object_text(raw, plotva_known_keys()).expect("salvaged");
            let payload = serde_json::from_str::<Value>(&out).expect("valid json");
            assert_eq!(
                payload["answer"].as_str().map(str::trim),
                Some(want),
                "{raw}"
            );
        }
    }

    #[test]
    fn numeric_and_bool_coercions_preserve_loose_inputs() {
        for (input, want_i64, want_u64, want_f64) in [
            (Value::String("42".to_owned()), 42, 42, 42.0),
            (Value::String("42.9".to_owned()), 42, 42, 42.9),
            (json!(7.5), 7, 7, 7.5),
            (Value::Bool(true), 1, 1, 1.0),
            (json!({"value": 9}), 9, 9, 9.0),
        ] {
            assert_eq!(coerce_i64_value(&input), Some(want_i64));
            assert_eq!(coerce_u64_value(&input), Some(want_u64));
            assert_eq!(coerce_f64_value(&input), Some(want_f64));
        }

        for (input, want) in [
            (Value::String("true".to_owned()), true),
            (Value::String("0".to_owned()), false),
            (json!(1), true),
            (json!({"value": false}), false),
        ] {
            assert_eq!(coerce_bool_value(&input), Some(want));
        }
    }

    #[test]
    fn find_json_key_value_keeps_sorted_nested_search() {
        let root = json!({
            "z": {"answer": "late"},
            "a": {"answer": {"value": 7}},
        });

        let value = find_json_key_value(&root, "answer").expect("answer");
        assert_eq!(coerce_i64_value(value), Some(7));
    }

    #[test]
    fn tolerant_parser_preserves_number_forms_and_escapes() {
        let root = parse_json_object_tolerant(
            r#"{answer:"ok", bare-key.name_1:"value", whole:42, decimal:-12.5, exponent:1.25e+3}"#,
            true,
        )
        .expect("parsed");
        assert_eq!(root["bare-key.name_1"], "value");
        assert_eq!(root["whole"], 42);
        assert_eq!(root["decimal"], -12.5);
        assert_eq!(root["exponent"], 1250.0);

        let root = parse_json_object_tolerant(
            r#"{answer:"line\nsmile:\u263a quote:\" slash:\\", response:"ok"}"#,
            true,
        )
        .expect("parsed");
        assert_eq!(root["answer"], "line\nsmile:\u{263a} quote:\" slash:\\");
    }
}
