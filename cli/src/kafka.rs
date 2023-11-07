pub mod admin {
    use rdkafka::admin::AdminClient;
    use rdkafka::client::DefaultClientContext;
    use rdkafka::error::KafkaResult;
    use rdkafka::ClientConfig;

    pub fn create_admin_client(
        bootstrap_address: String,
    ) -> KafkaResult<AdminClient<DefaultClientContext>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_address)
            .set("client.id", "stream-shift-cli")
            .create()
    }
}

pub mod consumer {
    use std::collections::HashMap;
    use std::error::Error;
    use std::time::Duration;

    use iso8601_timestamp::Timestamp;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::error::KafkaResult;
    use rdkafka::message::BorrowedMessage;
    use rdkafka::Offset;
    use rdkafka::{ClientConfig, TopicPartitionList};
    use uuid::Uuid;

    use crate::DEFAULT_TIMEOUT;

    const INVALID_TOPIC_OFFSET: i64 = -1001;

    pub fn create_transient_consumer(bootstrap_address: String) -> KafkaResult<BaseConsumer> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_address)
            .set("group.id", format!("stream-shift-cli-{}", Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
    }

    pub fn tail_limit(consumer: &BaseConsumer, limit: usize) -> Vec<BorrowedMessage> {
        consumer
            .iter()
            .take(limit)
            .map(|message| message.unwrap())
            .collect::<Vec<_>>()
    }

    pub fn tail<'a>(
        consumer: &'a BaseConsumer,
        topic: &'a str,
        limit: &Option<i64>,
        since: &Option<Timestamp>,
    ) -> Result<impl Iterator<Item = BorrowedMessage<'a>>, Box<dyn Error>> {
        let metadata = consumer
            .client()
            .fetch_metadata(Some(topic), DEFAULT_TIMEOUT)?;

        let partitions = metadata.topics()[0].partitions();
        let offsets: HashMap<i32, (i64, i64)> = partitions
            .iter()
            .map(|partition| {
                (
                    partition.id(),
                    consumer
                        .fetch_watermarks(topic, partition.id(), DEFAULT_TIMEOUT)
                        .unwrap(),
                )
            })
            .collect();

        let mut assignment = TopicPartitionList::new();
        match since {
            None => {
                for (partition, (start, end)) in &offsets {
                    match limit {
                        Some(limit) if end - start > *limit => assignment.add_partition_offset(
                            topic,
                            *partition,
                            Offset::OffsetTail(*limit),
                        )?,
                        _ => assignment.add_partition_offset(
                            topic,
                            *partition,
                            Offset::Offset(*start),
                        )?,
                    }
                }
            }
            Some(ts) => {
                for partition in offsets.keys() {
                    assignment.add_partition_offset(
                        topic,
                        *partition,
                        Offset::Offset(
                            ts.duration_since(Timestamp::UNIX_EPOCH)
                                .whole_milliseconds() as i64,
                        ),
                    )?
                }

                assignment = consumer.offsets_for_times(assignment, DEFAULT_TIMEOUT)?;
            }
        }

        consumer.assign(&assignment)?;

        Ok(ConsumeAllIterator {
            consumer,
            topic,
            offsets,
        })
    }

    struct ConsumeAllIterator<'a> {
        consumer: &'a BaseConsumer,
        topic: &'a str,
        offsets: HashMap<i32, (i64, i64)>,
    }

    impl<'a> ConsumeAllIterator<'a> {
        fn has_more(&self) -> bool {
            let position = self.consumer.position().unwrap();
            let partitions = position.elements_for_topic(self.topic);

            let mut at_end = true;
            for partition in partitions {
                let offset = partition.offset().to_raw().unwrap();
                at_end = offset == INVALID_TOPIC_OFFSET
                    || offset == self.offsets.get(&partition.partition()).unwrap().1
            }

            !at_end
        }
    }

    impl<'a> Iterator for ConsumeAllIterator<'a> {
        type Item = BorrowedMessage<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            let mut message = self.consumer.poll(Duration::from_millis(150));
            while message.is_none() && self.has_more() {
                message = self.consumer.poll(Duration::from_millis(15));
            }

            message.map(|message| message.unwrap())
        }
    }
}
