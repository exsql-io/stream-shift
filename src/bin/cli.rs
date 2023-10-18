use std::error::Error;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use tabled::settings::{Settings, Style};
use tabled::{Table, Tabled};
use uuid::Uuid;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Kafka boostrap address
    #[arg(short, long, value_name = "bootstrap-address")]
    bootstrap_address: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Topic {
        #[command(subcommand)]
        action: TopicCommandActions,
    },
}

#[derive(Subcommand)]
enum TopicCommandActions {
    List {},
    Tail {
        #[arg(long, value_name = "name")]
        name: String,
        #[arg(short, value_name = "n")]
        n: Option<usize>,
        #[arg(short, long, value_name = "follow")]
        follow: Option<bool>,
    },
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Tabled)]
struct TabledTopic {
    name: String,
    partitions: usize,
}

#[derive(Tabled)]
struct TabledMessage {
    key: String,
    value: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Topic { action } => match action {
            TopicCommandActions::List {} => {
                let mut config = ClientConfig::new();
                let admin: AdminClient<DefaultClientContext> = config
                    .set("bootstrap.servers", cli.bootstrap_address)
                    .set("client.id", "stream-shift-cli")
                    .create()?;

                let client = admin.inner();
                let describe = client.fetch_metadata(None, DEFAULT_TIMEOUT)?;

                let mut topics = Vec::new();
                for topic in describe.topics() {
                    topics.push(TabledTopic {
                        name: topic.name().to_string(),
                        partitions: topic.partitions().len(),
                    });
                }

                let table_config = Settings::default().with(Style::psql());
                println!("{}", Table::new(topics).with(table_config));

                drop(admin)
            }
            TopicCommandActions::Tail { name, n, follow } => {
                let mut config = ClientConfig::new();
                let consumer: BaseConsumer = config
                    .set("bootstrap.servers", cli.bootstrap_address)
                    .set("group.id", format!("stream-shift-cli-{}", Uuid::new_v4()))
                    .set("enable.auto.commit", "false")
                    .set("auto.offset.reset", "earliest")
                    .create()?;

                let n = n.unwrap_or(10);
                let _follow = follow.unwrap_or(false);

                consumer.subscribe(&[name])?;

                let mut messages = Vec::new();
                for message in consumer.iter().take(n) {
                    let message = message.unwrap();
                    let key: &str = message.key_view().unwrap().unwrap();
                    let value: &[u8] = message.payload().unwrap();

                    messages.push(TabledMessage {
                        key: key.to_string(),
                        value: String::from_utf8_lossy(value).to_string(),
                    });
                }

                let table_config = Settings::default().with(Style::psql());
                println!("{}\n", Table::new(messages).with(table_config));

                drop(consumer)
            }
        },
    }

    Ok(())
}
