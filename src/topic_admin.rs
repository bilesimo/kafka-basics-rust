use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use tracing::{debug, info};

use crate::config::{AppConfig, ConfigError};
use crate::error::KafkaAppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: usize,
}

#[derive(Debug, Clone)]
pub struct TopicAdmin {
    config: AppConfig,
}

impl TopicAdmin {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    pub fn from_config() -> Result<Self, ConfigError> {
        Ok(Self::new(AppConfig::load()?))
    }

    pub fn default_topic(&self) -> &str {
        &self.config.kafka.topic
    }

    pub async fn create_topic(
        &self,
        topic: &str,
        partitions: i32,
        replication_factor: i32,
    ) -> Result<(), KafkaAppError> {
        info!(topic, partitions, replication_factor, "creating topic");
        let admin = self.admin_client()?;
        let topics = [NewTopic::new(
            topic,
            partitions,
            TopicReplication::Fixed(replication_factor),
        )];
        let results = admin.create_topics(&topics, &AdminOptions::new()).await?;

        for result in results {
            match result {
                Ok(_) => {}
                Err((name, RDKafkaErrorCode::TopicAlreadyExists)) if name == topic => {
                    let description = self.describe_topic(topic)?;

                    if description.partition_count != partitions as usize
                        || description.replication_factor != replication_factor as usize
                    {
                        return Err(KafkaAppError::TopicConfigurationMismatch {
                            topic: topic.to_string(),
                            expected_partitions: partitions,
                            actual_partitions: description.partition_count,
                            expected_replication_factor: replication_factor,
                            actual_replication_factor: description.replication_factor,
                        });
                    }
                }
                Err((name, code)) => {
                    return Err(KafkaAppError::TopicOperation {
                        operation: "create",
                        topic: name,
                        code,
                    });
                }
            }
        }

        info!(
            topic,
            partitions, replication_factor, "topic create request completed"
        );
        Ok(())
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<(), KafkaAppError> {
        info!(topic, "deleting topic");
        let admin = self.admin_client()?;
        let results = admin.delete_topics(&[topic], &AdminOptions::new()).await?;

        for result in results {
            match result {
                Ok(_) => {}
                Err((name, RDKafkaErrorCode::UnknownTopicOrPartition)) if name == topic => {}
                Err((name, code)) => {
                    return Err(KafkaAppError::TopicOperation {
                        operation: "delete",
                        topic: name,
                        code,
                    });
                }
            }
        }

        info!(topic, "topic delete request completed");
        Ok(())
    }

    pub fn describe_topic(&self, topic: &str) -> Result<TopicDescription, KafkaAppError> {
        debug!(topic, "describing topic");
        let consumer: BaseConsumer = self.config.kafka_client_config().create()?;
        let metadata =
            consumer.fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)))?;
        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .ok_or_else(|| KafkaAppError::TopicOperation {
                operation: "describe",
                topic: topic.to_string(),
                code: RDKafkaErrorCode::UnknownTopicOrPartition,
            })?;

        if let Some(error) = topic_metadata.error() {
            return Err(KafkaAppError::TopicOperation {
                operation: "describe",
                topic: topic.to_string(),
                code: error.into(),
            });
        }

        let replication_factor = topic_metadata
            .partitions()
            .iter()
            .map(|partition| partition.replicas().len())
            .max()
            .unwrap_or(0);

        let description = TopicDescription {
            name: topic.to_string(),
            partition_count: topic_metadata.partitions().len(),
            replication_factor,
        };

        info!(
            topic,
            partition_count = description.partition_count,
            replication_factor = description.replication_factor,
            "topic metadata loaded"
        );

        Ok(description)
    }

    fn admin_client(&self) -> Result<AdminClient<DefaultClientContext>, KafkaAppError> {
        Ok(self.config.kafka_client_config().create()?)
    }
}
