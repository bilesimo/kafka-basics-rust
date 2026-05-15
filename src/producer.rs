use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::config::{AppConfig, ConfigError};
use crate::error::KafkaAppError;
use crate::order_event::OrderEvent;

#[derive(Debug, Clone)]
pub struct RecordToProduce {
    pub key: Option<String>,
    pub partition: Option<i32>,
    pub event: OrderEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProducedRecord {
    pub key: Option<String>,
    pub partition: i32,
    pub offset: i64,
    pub event: OrderEvent,
}

#[derive(Debug, Clone)]
pub struct OrderProducer {
    config: AppConfig,
}

impl OrderProducer {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    pub fn from_config() -> Result<Self, ConfigError> {
        Ok(Self::new(AppConfig::load()?))
    }

    pub fn default_topic(&self) -> &str {
        &self.config.kafka.topic
    }

    pub fn event_count(&self) -> u32 {
        self.config.producer.event_count
    }

    pub fn sample_events(&self) -> Vec<RecordToProduce> {
        (0..self.config.producer.event_count)
            .map(|index| {
                let event = OrderEvent::sample(index);
                let key = Some(event.order_id.clone());
                RecordToProduce {
                    key,
                    partition: None,
                    event,
                }
            })
            .collect()
    }

    pub async fn produce_records(
        &self,
        topic: &str,
        records: &[RecordToProduce],
    ) -> Result<Vec<ProducedRecord>, KafkaAppError> {
        let producer = self.future_producer()?;
        let mut deliveries = Vec::with_capacity(records.len());

        for record in records {
            let payload = serde_json::to_string(&record.event)?;
            let mut future_record = FutureRecord::to(topic).payload(&payload);

            if let Some(key) = record.key.as_deref() {
                future_record = future_record.key(key);
            }

            if let Some(partition) = record.partition {
                future_record = future_record.partition(partition);
            }

            let delivery = producer.send(future_record, Timeout::Never).await;

            match delivery {
                Ok(report) => deliveries.push(ProducedRecord {
                    key: record.key.clone(),
                    partition: report.partition,
                    offset: report.offset,
                    event: record.event.clone(),
                }),
                Err((error, _)) => return Err(KafkaAppError::Kafka(error)),
            }
        }

        Ok(deliveries)
    }

    fn future_producer(&self) -> Result<FutureProducer, KafkaAppError> {
        Ok(self.config.producer_client_config().create()?)
    }
}
