use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kafka_basics_rust::config::AppConfig;
use kafka_basics_rust::consumer::{ConsumedRecord, OrderConsumer};
use kafka_basics_rust::error::KafkaAppError;
use kafka_basics_rust::order_event::OrderEvent;
use kafka_basics_rust::producer::{OrderProducer, RecordToProduce};
use kafka_basics_rust::topic_admin::TopicAdmin;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();

    format!("{prefix}_{nanos}")
}

async fn create_test_topic(
    admin: &TopicAdmin,
    prefix: &str,
    partitions: i32,
) -> Result<String, Box<dyn std::error::Error>> {
    let topic = unique_name(prefix);
    admin.create_topic(&topic, partitions, 1).await?;
    Ok(topic)
}

fn statuses(records: &[ConsumedRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| record.event.status.clone())
        .collect()
}

fn test_config(enable_auto_commit: bool) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let mut config = AppConfig::load()?;
    config.consumer.enable_auto_commit = enable_auto_commit;
    Ok(config)
}

async fn produce_raw_payload(
    config: &AppConfig,
    topic: &str,
    key: &str,
    payload: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let producer: FutureProducer = config.producer_client_config().create()?;
    producer
        .send(
            FutureRecord::to(topic).key(key).payload(payload),
            Timeout::Never,
        )
        .await
        .map_err(|(error, _)| error)?;

    Ok(())
}

async fn wait_for_group_assignment(
    consumers: &[&StreamConsumer],
    expected_partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let poll_slice = Duration::from_millis(100);

    loop {
        for consumer in consumers {
            let _ = tokio::time::timeout(poll_slice, consumer.recv()).await;
        }

        let assignments = consumers
            .iter()
            .map(|consumer| consumer.assignment())
            .collect::<Result<Vec<_>, _>>()?;

        let total_assigned = assignments
            .iter()
            .map(|assignment| assignment.count())
            .sum::<usize>();
        let each_has_work = assignments.iter().all(|assignment| assignment.count() > 0);

        if total_assigned == expected_partitions && each_has_work {
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for group assignment; total_assigned={total_assigned}, expected_partitions={expected_partitions}"
            )
            .into());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn app_config_loads() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load()?;

    assert_eq!(config.kafka.broker, "localhost:9092");
    assert_eq!(config.kafka.topic, "orders");
    assert_eq!(config.consumer.group_id, "orders-rust-consumer");

    Ok(())
}

#[tokio::test]
async fn topic_creation_and_metadata_work() -> Result<(), Box<dyn std::error::Error>> {
    let admin = TopicAdmin::from_config()?;
    let topic = create_test_topic(&admin, "metadata_orders", 3).await?;

    let description = admin.describe_topic(&topic)?;

    assert_eq!(description.name, topic);
    assert_eq!(description.partition_count, 3);
    assert_eq!(description.replication_factor, 1);

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn describe_missing_topic_returns_an_error() -> Result<(), Box<dyn std::error::Error>> {
    let admin = TopicAdmin::from_config()?;
    let missing_topic = unique_name("missing_topic");

    let result = admin.describe_topic(&missing_topic);

    assert!(matches!(
        result,
        Err(KafkaAppError::TopicOperation {
            operation: "describe",
            ..
        })
    ));

    Ok(())
}

#[tokio::test]
async fn create_topic_detects_existing_topic_shape_mismatches()
-> Result<(), Box<dyn std::error::Error>> {
    let admin = TopicAdmin::from_config()?;
    let topic = create_test_topic(&admin, "mismatch_orders", 1).await?;

    let result = admin.create_topic(&topic, 2, 1).await;

    assert!(matches!(
        result,
        Err(KafkaAppError::TopicConfigurationMismatch {
            expected_partitions: 2,
            actual_partitions: 1,
            expected_replication_factor: 1,
            actual_replication_factor: 1,
            ..
        })
    ));

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn same_key_stays_in_one_partition_and_keeps_order() -> Result<(), Box<dyn std::error::Error>>
{
    let admin = TopicAdmin::from_config()?;
    let producer = OrderProducer::from_config()?;
    let consumer = OrderConsumer::from_config()?;
    let topic = create_test_topic(&admin, "ordering_orders", 3).await?;

    let records = vec![
        RecordToProduce {
            key: Some("order-123".to_string()),
            partition: None,
            event: OrderEvent::new("order-123", "u1", 49.9, "created"),
        },
        RecordToProduce {
            key: Some("order-123".to_string()),
            partition: None,
            event: OrderEvent::new("order-123", "u1", 49.9, "paid"),
        },
        RecordToProduce {
            key: Some("order-123".to_string()),
            partition: None,
            event: OrderEvent::new("order-123", "u1", 49.9, "shipped"),
        },
        RecordToProduce {
            key: Some("order-999".to_string()),
            partition: None,
            event: OrderEvent::new("order-999", "u2", 10.0, "created"),
        },
    ];

    producer.produce_records(&topic, &records).await?;

    let consumed = consumer
        .consume_n(
            &topic,
            &unique_name("ordering_group"),
            records.len(),
            false,
            Duration::from_secs(2),
        )
        .await?;

    let keyed: Vec<_> = consumed
        .into_iter()
        .filter(|record| record.key.as_deref() == Some("order-123"))
        .collect();

    assert_eq!(keyed.len(), 3);
    assert!(
        keyed
            .windows(2)
            .all(|window| window[0].partition == window[1].partition)
    );
    assert_eq!(statuses(&keyed), vec!["created", "paid", "shipped"]);

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn consume_n_does_not_persist_offsets_when_commit_is_false_even_if_auto_commit_is_enabled()
-> Result<(), Box<dyn std::error::Error>> {
    let config = test_config(true)?;
    let admin = TopicAdmin::new(config.clone());
    let producer = OrderProducer::new(config.clone());
    let consumer = OrderConsumer::new(config);
    let topic = create_test_topic(&admin, "manual_commit_orders", 1).await?;
    let group_id = unique_name("manual_commit_group");

    let records = vec![
        RecordToProduce {
            key: Some("order-1".to_string()),
            partition: None,
            event: OrderEvent::new("order-1", "u1", 10.0, "created"),
        },
        RecordToProduce {
            key: Some("order-2".to_string()),
            partition: None,
            event: OrderEvent::new("order-2", "u2", 20.0, "paid"),
        },
    ];
    producer.produce_records(&topic, &records).await?;

    let first_read = consumer
        .consume_n(&topic, &group_id, 1, false, Duration::from_secs(2))
        .await?;
    assert_eq!(first_read[0].event.order_id, "order-1");

    let second_read = consumer
        .consume_n(
            &topic,
            &group_id,
            records.len(),
            false,
            Duration::from_secs(2),
        )
        .await?;

    assert_eq!(second_read.len(), records.len());
    assert_eq!(second_read[0].event.order_id, "order-1");
    assert_eq!(second_read[1].event.order_id, "order-2");

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn consumers_in_same_group_share_the_work() -> Result<(), Box<dyn std::error::Error>> {
    let admin = TopicAdmin::from_config()?;
    let producer = OrderProducer::from_config()?;
    let consumer = OrderConsumer::from_config()?;
    let topic = create_test_topic(&admin, "shared_group_orders", 3).await?;
    let group_id = unique_name("shared_group");

    let consumer_a = consumer.stream_consumer(&group_id)?;
    let consumer_b = consumer.stream_consumer(&group_id)?;
    consumer_a.subscribe(&[&topic])?;
    consumer_b.subscribe(&[&topic])?;

    wait_for_group_assignment(&[&consumer_a, &consumer_b], 3).await?;

    let records = vec![
        RecordToProduce {
            key: None,
            partition: Some(0),
            event: OrderEvent::new("order-a0", "u1", 10.0, "created"),
        },
        RecordToProduce {
            key: None,
            partition: Some(0),
            event: OrderEvent::new("order-a1", "u1", 11.0, "paid"),
        },
        RecordToProduce {
            key: None,
            partition: Some(1),
            event: OrderEvent::new("order-b0", "u2", 20.0, "created"),
        },
        RecordToProduce {
            key: None,
            partition: Some(1),
            event: OrderEvent::new("order-b1", "u2", 21.0, "paid"),
        },
        RecordToProduce {
            key: None,
            partition: Some(2),
            event: OrderEvent::new("order-c0", "u3", 30.0, "created"),
        },
        RecordToProduce {
            key: None,
            partition: Some(2),
            event: OrderEvent::new("order-c1", "u3", 31.0, "paid"),
        },
    ];

    producer.produce_records(&topic, &records).await?;

    let (result_a, result_b) = tokio::join!(
        OrderConsumer::collect_messages(consumer_a, records.len(), false, Duration::from_secs(2)),
        OrderConsumer::collect_messages(consumer_b, records.len(), false, Duration::from_secs(2))
    );

    let messages_a = result_a?;
    let messages_b = result_b?;

    assert!(!messages_a.is_empty());
    assert!(!messages_b.is_empty());

    let total = messages_a.len() + messages_b.len();
    assert_eq!(total, records.len());

    let unique_offsets: HashSet<_> = messages_a
        .iter()
        .chain(messages_b.iter())
        .map(|record| (record.partition, record.offset))
        .collect();

    assert_eq!(unique_offsets.len(), records.len());

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn consumers_in_different_groups_read_independently() -> Result<(), Box<dyn std::error::Error>>
{
    let admin = TopicAdmin::from_config()?;
    let producer = OrderProducer::from_config()?;
    let consumer = OrderConsumer::from_config()?;
    let topic = create_test_topic(&admin, "independent_groups_orders", 2).await?;

    let records = vec![
        RecordToProduce {
            key: Some("order-1".to_string()),
            partition: None,
            event: OrderEvent::new("order-1", "u1", 10.0, "created"),
        },
        RecordToProduce {
            key: Some("order-2".to_string()),
            partition: None,
            event: OrderEvent::new("order-2", "u2", 20.0, "created"),
        },
        RecordToProduce {
            key: Some("order-3".to_string()),
            partition: None,
            event: OrderEvent::new("order-3", "u3", 30.0, "created"),
        },
    ];

    producer.produce_records(&topic, &records).await?;

    let group_a = consumer
        .consume_n(
            &topic,
            &unique_name("analytics_group"),
            records.len(),
            false,
            Duration::from_secs(2),
        )
        .await?;
    let group_b = consumer
        .consume_n(
            &topic,
            &unique_name("billing_group"),
            records.len(),
            false,
            Duration::from_secs(2),
        )
        .await?;

    assert_eq!(group_a.len(), records.len());
    assert_eq!(group_b.len(), records.len());
    assert_eq!(statuses(&group_a), statuses(&group_b));

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn committed_offsets_control_restart_position() -> Result<(), Box<dyn std::error::Error>> {
    let admin = TopicAdmin::from_config()?;
    let producer = OrderProducer::from_config()?;
    let consumer = OrderConsumer::from_config()?;
    let topic = create_test_topic(&admin, "offset_resume_orders", 1).await?;
    let group_id = unique_name("offset_resume_group");

    let first_batch = vec![
        RecordToProduce {
            key: Some("order-1".to_string()),
            partition: None,
            event: OrderEvent::new("order-1", "u1", 10.0, "created"),
        },
        RecordToProduce {
            key: Some("order-2".to_string()),
            partition: None,
            event: OrderEvent::new("order-2", "u2", 20.0, "paid"),
        },
    ];
    producer.produce_records(&topic, &first_batch).await?;

    let first_read = consumer
        .consume_n(
            &topic,
            &group_id,
            first_batch.len(),
            true,
            Duration::from_secs(2),
        )
        .await?;
    assert_eq!(first_read.len(), first_batch.len());

    let second_batch = vec![RecordToProduce {
        key: Some("order-3".to_string()),
        partition: None,
        event: OrderEvent::new("order-3", "u3", 30.0, "shipped"),
    }];
    producer.produce_records(&topic, &second_batch).await?;

    let resumed = consumer
        .consume_n(&topic, &group_id, 1, true, Duration::from_secs(2))
        .await?;

    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].event.order_id.as_str(), "order-3");

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn consume_n_returns_an_error_for_invalid_order_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load()?;
    let admin = TopicAdmin::new(config.clone());
    let consumer = OrderConsumer::new(config.clone());
    let topic = create_test_topic(&admin, "invalid_payload_orders", 1).await?;

    produce_raw_payload(&config, &topic, "order-1", "{\"order_id\":").await?;

    let result = consumer
        .consume_n(
            &topic,
            &unique_name("invalid_payload_group"),
            1,
            false,
            Duration::from_secs(2),
        )
        .await;

    assert!(matches!(
        result,
        Err(KafkaAppError::InvalidOrderEvent { .. })
    ));

    admin.delete_topic(&topic).await?;

    Ok(())
}

#[tokio::test]
async fn consume_n_times_out_when_not_enough_messages_arrive()
-> Result<(), Box<dyn std::error::Error>> {
    let admin = TopicAdmin::from_config()?;
    let producer = OrderProducer::from_config()?;
    let consumer = OrderConsumer::from_config()?;
    let topic = create_test_topic(&admin, "consumer_timeout_orders", 1).await?;

    let records = vec![RecordToProduce {
        key: Some("order-1".to_string()),
        partition: None,
        event: OrderEvent::new("order-1", "u1", 10.0, "created"),
    }];
    producer.produce_records(&topic, &records).await?;

    let result = consumer
        .consume_n(
            &topic,
            &unique_name("timeout_group"),
            2,
            false,
            Duration::from_millis(500),
        )
        .await;

    assert!(matches!(
        result,
        Err(KafkaAppError::Timeout {
            expected: 2,
            received: 1
        })
    ));

    admin.delete_topic(&topic).await?;

    Ok(())
}
