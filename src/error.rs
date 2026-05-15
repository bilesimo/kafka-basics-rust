use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use thiserror::Error;

use crate::config::ConfigError;

#[derive(Debug, Error)]
pub enum KafkaAppError {
    #[error("failed to load app config")]
    Config(#[from] ConfigError),
    #[error("kafka operation failed")]
    Kafka(#[from] KafkaError),
    #[error("failed to serialize order event")]
    Json(#[from] serde_json::Error),
    #[error("received invalid order event at partition {partition}, offset {offset}")]
    InvalidOrderEvent {
        partition: i32,
        offset: i64,
        #[source]
        source: serde_json::Error,
    },
    #[error("message payload was not valid UTF-8")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("timed out after collecting {received} of {expected} messages")]
    Timeout { expected: usize, received: usize },
    #[error("topic operation {operation} failed for {topic}: {code:?}")]
    TopicOperation {
        operation: &'static str,
        topic: String,
        code: RDKafkaErrorCode,
    },
    #[error(
        "topic {topic} already exists with {actual_partitions} partitions and replication factor {actual_replication_factor}, expected {expected_partitions} partitions and replication factor {expected_replication_factor}"
    )]
    TopicConfigurationMismatch {
        topic: String,
        expected_partitions: i32,
        actual_partitions: usize,
        expected_replication_factor: i32,
        actual_replication_factor: usize,
    },
}
