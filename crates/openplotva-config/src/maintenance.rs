use std::{fmt, net::IpAddr};

use serde::{Deserialize, Serialize};

use crate::{ConfigError, RawConfig, parse_bool, parse_i64_list_or_default, parse_u16};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    #[serde(skip)]
    pub token: String,
    pub notify_user_id: i64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "127.0.0.1".into(),
            port: 9092,
            token: String::new(),
            notify_user_id: 0,
        }
    }
}

impl fmt::Debug for MaintenanceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaintenanceConfig")
            .field("enabled", &self.enabled)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("token", &"<redacted>")
            .field("notify_user_id", &self.notify_user_id)
            .finish()
    }
}

impl MaintenanceConfig {
    pub(crate) fn from_raw(raw: &RawConfig) -> Result<Self, ConfigError> {
        let config = Self {
            enabled: parse_bool(
                "MAINTENANCE_ENABLED",
                raw.maintenance_enabled.clone(),
                false,
            )?,
            host: raw
                .maintenance_host
                .clone()
                .unwrap_or_else(|| "127.0.0.1".into()),
            port: parse_u16("MAINTENANCE_PORT", raw.maintenance_port.clone(), 9092)?,
            token: raw.maintenance_token.clone().unwrap_or_default(),
            notify_user_id: raw
                .maintenance_notify_user_id
                .as_deref()
                .unwrap_or("0")
                .parse()
                .map_err(|_| ConfigError::InvalidMaintenance {
                    reason: "MAINTENANCE_NOTIFY_USER_ID must be a positive administrator id",
                })?,
        };
        if config.host.parse::<IpAddr>().is_err() || config.port == 0 {
            return Err(ConfigError::InvalidMaintenance {
                reason: "MAINTENANCE_HOST must be an IP address and MAINTENANCE_PORT must be nonzero",
            });
        }
        if config.enabled {
            if !(32..=256).contains(&config.token.len())
                || !config.token.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(ConfigError::InvalidMaintenance {
                    reason: "MAINTENANCE_TOKEN must contain 32 to 256 non-whitespace ASCII bytes",
                });
            }
            let admins =
                parse_i64_list_or_default("ADMINS_ADMIN_IDS", raw.admins_admin_ids.clone(), "")?;
            if config.notify_user_id <= 0 || !admins.contains(&config.notify_user_id) {
                return Err(ConfigError::InvalidMaintenance {
                    reason: "MAINTENANCE_NOTIFY_USER_ID must identify a configured private-chat administrator",
                });
            }
        }
        Ok(config)
    }
}
