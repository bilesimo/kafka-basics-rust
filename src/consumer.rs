use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;

use crate::config::{AppConfig, ConfigError};
use crate::error::KafkaAppError;
use crate::order_event::OrderEvent;

#[derive(Debug, Clone, PartialEq)]
pub struct ConsumedRecord {
    pub key: Option<String>,
    pub partition: i32,
    pub offset: i64,
    pub payload: String,
    pub event: OrderEvent,
}

#[derive(Debug, Clone)]
pub struct OrderConsumer {
    config: AppConfig,
}

impl OrderConsumer {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    pub fn from_config() -> Result<Self, ConfigError> {
        Ok(Self::new(AppConfig::load()?))
    }

    pub fn default_group_id(&self) -> &str {
        &self.config.consumer.group_id
    }

    pub async fn consume_n(
        &self,
        topic: &str,
        group_id: &str,
        max_messages: usize,
        commit: bool,
        idle_timeout: Duration,
    ) -> Result<Vec<ConsumedRecord>, KafkaAppError> {
        let consumer = self.stream_consumer_with_auto_commit(group_id, false)?;
        consumer.subscribe(&[topic])?;
        let records = Self::collect_messages(consumer, max_messages, commit, idle_timeout).await?;

        if records.len() < max_messages {
            return Err(KafkaAppError::Timeout {
                expected: max_messages,
                received: records.len(),
            });
        }

        Ok(records)
    }

    pub fn stream_consumer(&self, group_id: &str) -> Result<StreamConsumer, KafkaAppError> {
        self.stream_consumer_with_auto_commit(group_id, self.config.consumer.enable_auto_commit)
    }

    fn stream_consumer_with_auto_commit(
        &self,
        group_id: &str,
        enable_auto_commit: bool,
    ) -> Result<StreamConsumer, KafkaAppError> {
        Ok(self
            .config
            .consumer_client_config(group_id, enable_auto_commit)
            .create()?)
    }

    pub async fn collect_messages(
        consumer: StreamConsumer,
        max_messages: usize,
        commit: bool,
        idle_timeout: Duration,
    ) -> Result<Vec<ConsumedRecord>, KafkaAppError> {
        let mut records = Vec::new();

        while records.len() < max_messages {
            let message = match tokio::time::timeout(idle_timeout, consumer.recv()).await {
                Ok(result) => result?,
                Err(_) => break,
            };

            let key = message
                .key_view::<str>()
                .transpose()?
                .map(std::string::ToString::to_string);
            let payload = message
                .payload_view::<str>()
                .transpose()?
                .unwrap_or("")
                .to_string();
            let event = serde_json::from_str::<OrderEvent>(&payload).map_err(|source| {
                KafkaAppError::InvalidOrderEvent {
                    partition: message.partition(),
                    offset: message.offset(),
                    source,
                }
            })?;

            records.push(ConsumedRecord {
                key,
                partition: message.partition(),
                offset: message.offset(),
                payload,
                event,
            });

            if commit {
                consumer.commit_message(&message, CommitMode::Sync)?;
            }
        }

        Ok(records)
    }
}
