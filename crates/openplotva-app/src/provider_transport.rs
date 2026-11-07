//! Resolves persisted provider credentials before constructing a transport.

use openplotva_storage::{StorageError, llm_routing::open_key};

pub(crate) fn resolve_provider_api_key(
    api_key_ref: Option<&str>,
    api_key_encrypted: Option<&[u8]>,
) -> Result<Option<String>, StorageError> {
    let env_key = api_key_ref
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .and_then(|name| std::env::var(name).ok());
    select_api_key(env_key, api_key_encrypted, || {
        std::env::var("MASTER_KEY").unwrap_or_default()
    })
}

fn select_api_key(
    env_key: Option<String>,
    sealed: Option<&[u8]>,
    master_key: impl FnOnce() -> String,
) -> Result<Option<String>, StorageError> {
    if let Some(key) = env_key.filter(|key| !key.trim().is_empty()) {
        return Ok(Some(key));
    }
    sealed
        .map(|sealed| open_key(&master_key(), sealed))
        .transpose()
        .map(|key| key.filter(|key| !key.trim().is_empty()))
}

use crate::routed_attempts::RoutedAttempt;
use openplotva_config::AppConfig;
use openplotva_llm::{
    aifarm::{AifarmClientConfig, GatewayRequestFields, normalize_chat_completions_url},
    provider_schema::Protocol,
    retry::{FailureReason, ProviderError},
};

pub(crate) fn openai_client_for_attempt(
    config: &AppConfig,
    attempt: &RoutedAttempt,
    mut client: AifarmClientConfig,
) -> Result<AifarmClientConfig, ProviderError> {
    if attempt.protocol != Some(Protocol::OpenAiCompat) {
        return Err(ProviderError::new(
            &attempt.provider_name,
            FailureReason::ProviderUnavailable,
            "selected protocol does not support OpenAI chat completions",
        ));
    }
    client.api_key = resolve_provider_api_key(
        attempt.provider_api_key_ref.as_deref(),
        attempt.provider_api_key_encrypted.as_deref(),
    )
    .map_err(|error| {
        ProviderError::wrap(
            &attempt.provider_name,
            FailureReason::ProviderUnavailable,
            error,
        )
    })?
    .unwrap_or_else(|| builtin_api_key(config, &attempt.provider_name));
    client.default_model = transport_model_for_attempt(attempt);
    if is_openrouter(attempt) {
        client.request_timeout = std::time::Duration::from_secs(
            config.open_router.request_timeout_seconds.max(1) as u64,
        );
    }
    client.direct_url.clear();
    let (service_name, endpoint_name) = attempt.discovery_names();
    if let Some(endpoint) = attempt.endpoint() {
        if service_name.is_some() || endpoint_name.is_some() {
            client.base_url = endpoint.to_owned();
        } else {
            client.direct_url = normalize_chat_completions_url(endpoint);
        }
    } else if is_openrouter(attempt) {
        client.direct_url = "https://openrouter.ai/api/v1/chat/completions".to_owned();
    } else if service_name.is_none()
        && endpoint_name.is_none()
        && !attempt.provider_name.eq_ignore_ascii_case("aifarm")
    {
        return Err(ProviderError::new(
            &attempt.provider_name,
            FailureReason::ProviderUnavailable,
            "provider has neither an endpoint nor a discovery service",
        ));
    }
    if let Some(service) = service_name {
        client.service_name = service.to_owned();
    }
    if let Some(endpoint) = endpoint_name {
        client.endpoint_name = endpoint.to_owned();
    }
    client.runtime_hint = attempt.provider_runtime_hint.clone().unwrap_or_default();
    client.supports_message_name = attempt.supports_message_name();
    client.supports_cache_control = attempt.supports_cache_control();
    client.gateway_fields = GatewayRequestFields::from_overrides(&attempt.overrides.extra);
    if let Some(timeout) = attempt
        .provider_config
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .filter(|timeout| *timeout > 0)
    {
        client.request_timeout = std::time::Duration::from_millis(timeout);
    }
    Ok(client)
}

pub(crate) fn gemini_endpoint_for_attempt(
    config: &AppConfig,
    attempt: &RoutedAttempt,
) -> Result<(String, String), ProviderError> {
    if attempt.protocol != Some(Protocol::Genkit) {
        return Err(ProviderError::new(
            &attempt.provider_name,
            FailureReason::ProviderUnavailable,
            "selected protocol does not support Gemini requests",
        ));
    }
    let key = resolve_provider_api_key(
        attempt.provider_api_key_ref.as_deref(),
        attempt.provider_api_key_encrypted.as_deref(),
    )
    .map_err(|error| {
        ProviderError::wrap(
            &attempt.provider_name,
            FailureReason::ProviderUnavailable,
            error,
        )
    })?
    .unwrap_or_else(|| builtin_api_key(config, &attempt.provider_name));
    if key.trim().is_empty() {
        return Err(ProviderError::new(
            &attempt.provider_name,
            FailureReason::ProviderUnavailable,
            "provider API key is not configured",
        ));
    }
    let endpoint = attempt
        .endpoint()
        .map(str::to_owned)
        .unwrap_or_else(|| openplotva_llm::gemini::GeminiDialogConfig::default().base_url);
    Ok((key, endpoint))
}

pub(crate) fn aifarm_discovery_provider_name(service: &str, config: &AppConfig) -> String {
    let service = service.trim();
    if service.is_empty() || service == config.llm.dialog.discovery_service_name {
        return "aifarm".to_owned();
    }
    if service.eq_ignore_ascii_case(crate::agent_runtime::DEFAULT_LOCAL_REASONER_SERVICE_NAME) {
        return crate::agent_runtime::LOCAL_REASONER_PROVIDER_NAME.to_owned();
    }
    if service.eq_ignore_ascii_case(crate::agent_runtime::VIBETHINKER_SERVICE_NAME) {
        return crate::agent_runtime::VIBETHINKER_PROVIDER_NAME.to_owned();
    }
    format!(
        "aifarm-{}",
        service
            .trim_start_matches("llm-openai-")
            .trim_start_matches("llm-")
    )
}

fn builtin_api_key(config: &AppConfig, provider: &str) -> String {
    let dialog = &config.llm.dialog;
    match provider.to_ascii_lowercase().as_str() {
        "aifarm" => dialog.api_key.clone(),
        "openrouter" | "openrouter-free" => config.open_router.key.clone(),
        "genkit" | "gemini" => {
            crate::runtime_gemini_cache::resolve_google_ai_key(&config.google_ai)
        }
        "vram-cloud" => dialog.aifarm_pool_api_key.clone(),
        "vmlx" => dialog.vmlx_api_key.clone(),
        "nvidia" => dialog.nvidia_api_key.clone(),
        _ => {
            let local = crate::agent_runtime::local_reasoner_named_provider_config(config);
            let spec = config
                .llm
                .providers
                .iter()
                .find(|entry| {
                    entry.name.eq_ignore_ascii_case(provider)
                        || aifarm_discovery_provider_name(&entry.discovery_service_name, config)
                            .eq_ignore_ascii_case(provider)
                })
                .or_else(|| {
                    (aifarm_discovery_provider_name(&local.discovery_service_name, config)
                        .eq_ignore_ascii_case(provider))
                    .then_some(&local)
                });
            spec.map(|spec| {
                if spec.api_key.trim().is_empty() {
                    dialog.api_key.clone()
                } else {
                    spec.api_key.clone()
                }
            })
            .unwrap_or_default()
        }
    }
}

pub(crate) fn transport_model_for_attempt(attempt: &RoutedAttempt) -> String {
    let name = attempt.model_name.trim();
    if is_openrouter(attempt) {
        strip_provider_prefix(name, "openrouter/")
            .unwrap_or(name)
            .to_owned()
    } else {
        name.to_owned()
    }
}

pub(crate) fn is_openrouter(attempt: &RoutedAttempt) -> bool {
    attempt.protocol == Some(Protocol::OpenAiCompat)
        && (attempt.provider_name.eq_ignore_ascii_case("openrouter")
            || attempt
                .provider_name
                .eq_ignore_ascii_case("openrouter-free")
            || attempt
                .endpoint()
                .and_then(|endpoint| url::Url::parse(endpoint).ok())
                .is_some_and(|url| url.host_str() == Some("openrouter.ai")))
}

pub(crate) fn strip_provider_prefix<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
}

pub(crate) fn redactor_config_for_attempt(
    attempt: &RoutedAttempt,
    mut config: openplotva_llm::redaction::DiscoveryRedactorConfig,
) -> Result<openplotva_llm::redaction::DiscoveryRedactorConfig, ProviderError> {
    if attempt.protocol != Some(Protocol::PrivacyFilter) {
        return Err(ProviderError::new(
            &attempt.provider_name,
            FailureReason::ProviderUnavailable,
            "selected protocol does not support privacy filtering",
        ));
    }
    if let Some(endpoint) = attempt.endpoint() {
        config.base_url = endpoint.to_owned();
    }
    let (service, endpoint) = attempt.discovery_names();
    if let Some(service) = service {
        config.service_name = service.to_owned();
    }
    if let Some(endpoint) = endpoint {
        config.endpoint_name = endpoint.to_owned();
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_storage::llm_routing::seal_key;

    fn attempt() -> RoutedAttempt {
        RoutedAttempt {
            protocol: Some(Protocol::OpenAiCompat),
            provider_id: 1,
            model_id: 2,
            provider_name: "custom".into(),
            model_name: "model".into(),
            provider_runtime_hint: None,
            provider_endpoint: Some("https://provider.test/v1".into()),
            discovery_service_name: None,
            discovery_endpoint_name: None,
            provider_api_key_ref: None,
            provider_api_key_encrypted: None,
            model_base_url: None,
            embedding_dim: None,
            provider_config: serde_json::json!({}),
            model_config: serde_json::json!({}),
            overrides: Default::default(),
            variant: None,
        }
    }

    #[test]
    fn redaction_uses_the_selected_protocol_and_shared_discovery_overrides() {
        use openplotva_llm::redaction::DiscoveryRedactorConfig;
        let mut route = attempt();
        assert_eq!(
            redactor_config_for_attempt(&route, DiscoveryRedactorConfig::default())
                .expect_err("wrong protocol")
                .reason(),
            FailureReason::ProviderUnavailable
        );
        route.protocol = Some(Protocol::PrivacyFilter);
        route.provider_name = "renamed-privacy-provider".into();
        route.model_base_url = Some("  ".into());
        route.discovery_service_name = Some("dedicated".into());
        route.provider_config =
            serde_json::json!({"service_name":"legacy", "endpoint_name":"legacy-redact"});
        route.model_config =
            serde_json::json!({"service_name":"model-service", "endpoint_name":"model-redact"});
        let config = redactor_config_for_attempt(&route, DiscoveryRedactorConfig::default())
            .expect("config");
        assert_eq!(config.base_url, "https://provider.test/v1");
        assert_eq!(config.service_name, "model-service");
        assert_eq!(config.endpoint_name, "model-redact");
        route.model_base_url = Some("https://model.test".into());
        route.model_config = serde_json::json!({"service_name":" ", "endpoint_name":""});
        let config = redactor_config_for_attempt(&route, DiscoveryRedactorConfig::default())
            .expect("fallback config");
        assert_eq!(config.base_url, "https://model.test");
        assert_eq!(config.service_name, "dedicated");
        assert_eq!(config.endpoint_name, "legacy-redact");
    }

    #[test]
    fn resolved_route_carries_endpoint_capabilities_and_gateway_overrides() {
        let config = AppConfig::from_raw(Default::default()).expect("config");
        let mut route = attempt();
        route.model_base_url = Some("https://override.test/v1".into());
        route.provider_config = serde_json::json!({"supports_cache_control": false, "supports_message_name": false, "timeout_ms": 1234});
        route.overrides.extra = serde_json::json!({"reasoning": {"enabled": false}});
        let client =
            openai_client_for_attempt(&config, &route, Default::default()).expect("client");
        assert_eq!(
            client.direct_url,
            "https://override.test/v1/chat/completions"
        );
        assert_eq!(client.request_timeout.as_millis(), 1234);
        assert!(!client.supports_cache_control);
        assert!(!client.supports_message_name);
        assert_eq!(
            client.gateway_fields.reasoning,
            Some(serde_json::json!({"enabled": false}))
        );
        route.model_base_url = Some("   ".into());
        assert_eq!(
            openai_client_for_attempt(&config, &route, Default::default())
                .expect("fallback endpoint")
                .direct_url,
            "https://provider.test/v1/chat/completions"
        );
    }

    #[test]
    fn an_incomplete_custom_route_cannot_inherit_an_unrelated_discovery_service() {
        let config = AppConfig::from_raw(Default::default()).expect("config");
        let mut route = attempt();
        route.provider_endpoint = None;
        assert!(openai_client_for_attempt(&config, &route, Default::default()).is_err());
    }

    #[test]
    fn corrupt_saved_key_never_uses_the_builtin_account() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            openrouter_key: Some("builtin-account".into()),
            ..Default::default()
        })
        .expect("config");
        let mut route = attempt();
        route.provider_name = "openrouter".into();
        route.provider_api_key_encrypted = Some(b"corrupt".to_vec());
        assert!(openai_client_for_attempt(&config, &route, Default::default()).is_err());
    }

    #[test]
    fn nonempty_environment_key_wins_without_opening_ciphertext() {
        let key = select_api_key(Some("env-key".into()), Some(b"invalid"), || {
            panic!("unused master")
        });
        assert_eq!(key.expect("key").as_deref(), Some("env-key"));
    }

    #[test]
    fn missing_or_empty_environment_uses_the_encrypted_key() {
        let sealed = seal_key("test-master", "stored-key").expect("seal");
        for env in [None, Some(String::new()), Some("  ".into())] {
            assert_eq!(
                select_api_key(env, Some(&sealed), || "test-master".into())
                    .expect("open")
                    .as_deref(),
                Some("stored-key")
            );
        }
    }

    #[test]
    fn corrupt_or_wrong_account_ciphertext_is_an_error() {
        let sealed = seal_key("test-master", "stored-key").expect("seal");
        assert!(select_api_key(None, Some(b"invalid"), || "test-master".into()).is_err());
        assert!(select_api_key(None, Some(&sealed), || "other-master".into()).is_err());
        assert_eq!(
            select_api_key(None, None, || panic!("unused master")).expect("absent"),
            None
        );
    }
}
