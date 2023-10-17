use std::error::Error;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rdkafka::admin::AdminClient;
use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;
use tabled::settings::{Settings, Style};
use tabled::{Table, Tabled};

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
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Tabled)]
struct Topic {
    name: String,
    partitions: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Topic { action } => match action {
            TopicCommandActions::List {} => {
                let mut config = ClientConfig::new();
                config.set("bootstrap.servers", cli.bootstrap_address);

                let admin = AdminClient::from_config(&config)?;
                let client = admin.inner();
                let describe = client.fetch_metadata(None, DEFAULT_TIMEOUT)?;

                let mut topics = Vec::new();
                for topic in describe.topics() {
                    topics.push(Topic {
                        name: topic.name().to_string(),
                        partitions: topic.partitions().len(),
                    });
                }

                let table_config = Settings::default().with(Style::psql());
                println!("Topics: \n{}", Table::new(topics).with(table_config))
            }
        },
    }

    Ok(())
}
