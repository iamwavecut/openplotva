//! Logging and tracing setup for OpenPlotva.

use openplotva_config::ObservabilityConfig;
use tracing_subscriber::EnvFilter;

/// Initialize process-wide tracing.
pub fn init(config: &ObservabilityConfig) {
    let filter = EnvFilter::try_new(&config.log_filter).unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
