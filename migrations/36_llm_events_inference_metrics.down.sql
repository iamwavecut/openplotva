-- Source SHA-256: 83189e9ddfbb7772843013c726f64ea7e5add6dbcdc85cfec66836378c82152a

DROP INDEX IF EXISTS idx_llm_request_events_model_bucket;
DROP INDEX IF EXISTS idx_llm_request_events_provider_created_at;

ALTER TABLE llm_request_events
	DROP COLUMN IF EXISTS inference_params,
	DROP COLUMN IF EXISTS response_format,
	DROP COLUMN IF EXISTS tool_mode,
	DROP COLUMN IF EXISTS candidate_count,
	DROP COLUMN IF EXISTS top_k,
	DROP COLUMN IF EXISTS top_p,
	DROP COLUMN IF EXISTS temperature,
	DROP COLUMN IF EXISTS max_tokens,
	DROP COLUMN IF EXISTS effective_total_tps,
	DROP COLUMN IF EXISTS effective_output_tps,
	DROP COLUMN IF EXISTS generation_tps,
	DROP COLUMN IF EXISTS generation_ms,
	DROP COLUMN IF EXISTS generation_tokens,
	DROP COLUMN IF EXISTS prompt_tps,
	DROP COLUMN IF EXISTS prompt_eval_ms,
	DROP COLUMN IF EXISTS prompt_eval_tokens,
	DROP COLUMN IF EXISTS tool_use_prompt_tokens,
	DROP COLUMN IF EXISTS thoughts_tokens,
	DROP COLUMN IF EXISTS cached_tokens,
	DROP COLUMN IF EXISTS total_tokens,
	DROP COLUMN IF EXISTS output_tokens,
	DROP COLUMN IF EXISTS input_tokens,
	DROP COLUMN IF EXISTS request_kind,
	DROP COLUMN IF EXISTS provider;
