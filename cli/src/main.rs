use std::error::Error;

use clap::error::{ErrorKind, RichFormatter};
use clap::{Parser, Subcommand};
use itertools::Itertools;
use k_board::{Keyboard, Keys};
use rdkafka::consumer::BaseConsumer;
use rdkafka::Message;
use tabled::Tabled;

use stream_shift_cli::kafka::*;
use stream_shift_cli::rendering::console;
use stream_shift_cli::*;

const TOPIC_LIST_HEADERS: [&str; 2] = ["name", "partitions"];

const TOPIC_TAIL_HEADERS: [&str; 2] = ["key", "value"];

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
        #[arg(short, long, value_name = "name")]
        name: String,
        #[arg(short, long, value_name = "since")]
        since: Option<String>,
        #[arg(short, long, value_name = "follow")]
        follow: Option<bool>,
        #[arg(short, long, value_name = "limit")]
        limit: Option<usize>,
    },
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
                let admin = admin::create_admin_client(cli.bootstrap_address)?;
                let client = admin.inner();
                let describe = client.fetch_metadata(None, DEFAULT_TIMEOUT)?;

                let mut topics = Vec::new();
                for topic in describe.topics() {
                    topics.push(vec![
                        topic.name().to_string(),
                        topic.partitions().len().to_string(),
                    ])
                }

                println!("{}", console::render(TOPIC_LIST_HEADERS.to_vec(), topics));

                drop(admin)
            }
            TopicCommandActions::Tail {
                name,
                since,
                limit,
                follow,
            } => {
                if since.is_some() && limit.is_some() {
                    let err = clap::Error::raw(
                        ErrorKind::InvalidValue,
                        "Can't use both `since` and `limit` arguments.",
                    );

                    err.apply::<RichFormatter>().exit()
                }

                let consumer: BaseConsumer =
                    consumer::create_transient_consumer(cli.bootstrap_address)?;

                let _follow = follow.unwrap_or(false);
                let timestamp = match since {
                    None => None,
                    Some(value) => iso8601_timestamp::Timestamp::parse(value),
                };

                for chunk in consumer::tail(
                    &consumer,
                    name,
                    &limit.map(|limit| limit as i64),
                    &timestamp,
                )?
                .chunks(80)
                .into_iter()
                {
                    let mut messages = Vec::new();
                    for message in chunk {
                        let key: &str = message.key_view().unwrap().unwrap();
                        let value: &[u8] = message.payload().unwrap();

                        messages.push(vec![
                            key.to_string(),
                            String::from_utf8_lossy(value).to_string(),
                        ]);
                    }

                    std::process::Command::new("clear").status().unwrap();
                    println!("{}", console::render(TOPIC_TAIL_HEADERS.to_vec(), messages));
                    print!(":");

                    for key in Keyboard::new() {
                        match key {
                            Keys::Enter => break,
                            Keys::Letter('q') | Keys::Letter('Q') => return Ok(()),
                            _ => {}
                        }
                    }
                }

                drop(consumer)
            }
        },
    }

    Ok(())
}
