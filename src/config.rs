use std::fs;
use std::path::Path;

use rdkafka::config::ClientConfig;
use serde::Deserialize;
use thiserror::Error;

pub const CONFIG_PATH: &str = "config/app.toml";
const CONFIG_ENV_VAR: &str = "KAFKA_BASICS_CONFIG";

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub producer: ProducerConfig,
    pub consumer: ConsumerConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub broker: String,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProducerConfig {
    pub message_timeout_ms: u64,
    pub event_count: u32,
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub auto_offset_reset: String,
    pub enable_auto_commit: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file at {path}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse TOML config")]
    Parse(#[from] toml::de::Error),
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        if let Ok(path) = std::env::var(CONFIG_ENV_VAR) {
            return Self::load_from(Path::new(&path));
        }

        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        Self::load_from(&manifest_dir.join(CONFIG_PATH))
    }

    fn load_from(path: &Path) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;

        Ok(toml::from_str(&raw)?)
    }

    pub fn kafka_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.kafka.broker);
        config
    }

    pub fn producer_client_config(&self) -> ClientConfig {
        let message_timeout_ms = self.producer.message_timeout_ms.to_string();
        let mut config = self.kafka_client_config();
        config.set("message.timeout.ms", &message_timeout_ms);
        config
    }

    pub fn consumer_client_config(&self, group_id: &str, enable_auto_commit: bool) -> ClientConfig {
        let enable_auto_commit = enable_auto_commit.to_string();
        let mut config = self.kafka_client_config();
        config
            .set("group.id", group_id)
            .set("enable.auto.commit", &enable_auto_commit)
            .set("auto.offset.reset", &self.consumer.auto_offset_reset);
        config
    }
}
