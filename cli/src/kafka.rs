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

    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::error::KafkaResult;
    use rdkafka::message::BorrowedMessage;
    use rdkafka::ClientConfig;
    use uuid::Uuid;

    use crate::DEFAULT_TIMEOUT;

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
    ) -> Result<impl Iterator<Item = BorrowedMessage<'a>>, Box<dyn Error>> {
        let metadata = consumer
            .client()
            .fetch_metadata(Some(topic), DEFAULT_TIMEOUT)?;

        let partitions = metadata.topics()[0].partitions();
        let end_offsets: HashMap<i32, i64> = partitions
            .iter()
            .map(|partition| {
                (
                    partition.id(),
                    consumer
                        .fetch_watermarks(topic, partition.id(), DEFAULT_TIMEOUT)
                        .unwrap()
                        .1,
                )
            })
            .collect();

        Ok(ConsumeAllIterator {
            consumer,
            topic,
            end_offsets,
        })
    }

    struct ConsumeAllIterator<'a> {
        consumer: &'a BaseConsumer,
        topic: &'a str,
        end_offsets: HashMap<i32, i64>,
    }

    impl<'a> ConsumeAllIterator<'a> {
        fn has_more(&self) -> bool {
            let subscription = self.consumer.subscription().unwrap();
            let topics = subscription.to_topic_map();
            let mut at_end = true;
            for ((topic, partition), offset) in topics {
                if topic == self.topic {
                    println!(
                        "topic: {}, partition: {}, position: {:?}",
                        topic, partition, offset
                    );

                    at_end = offset.to_raw().unwrap() == *self.end_offsets.get(&partition).unwrap()
                }
            }

            at_end
        }
    }

    impl<'a> Iterator for ConsumeAllIterator<'a> {
        type Item = BorrowedMessage<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            let mut message = self.consumer.poll(Duration::from_secs(60));
            while message.is_none() && self.has_more() {
                message = self.consumer.poll(Duration::from_millis(15));
            }

            message.map(|message| message.unwrap())
        }
    }
}
