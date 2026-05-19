use std::env;

use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry};

use crate::error::KafkaAppError;

pub const LOG_FORMAT_ENV_VAR: &str = "KAFKA_BASICS_LOG_FORMAT";

pub fn init_tracing() -> Result<(), KafkaAppError> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,rdkafka=warn"));
    let format = env::var(LOG_FORMAT_ENV_VAR).unwrap_or_else(|_| "compact".to_string());

    match format.as_str() {
        "compact" => Registry::default()
            .with(env_filter)
            .with(fmt::layer().compact())
            .try_init()
            .map_err(KafkaAppError::Tracing),
        "json" => Registry::default()
            .with(env_filter)
            .with(fmt::layer().json())
            .try_init()
            .map_err(KafkaAppError::Tracing),
        _ => Err(KafkaAppError::InvalidLogFormat(format)),
    }
}
