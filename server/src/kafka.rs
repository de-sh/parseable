/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::{
    collections::HashMap,
    env::{self, VarError},
    fmt::Display,
    num::ParseIntError,
    str::FromStr,
};

use chrono::Utc;
use log::{debug, error, info};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
    util::Timeout,
    ClientConfig, Message, TopicPartitionList,
};
use tokio::spawn;

use crate::{
    event::{
        error::EventError,
        format::{self, EventFormat},
        Event,
    },
    handlers::http::ingest::{create_stream_if_not_exists, PostError},
    metadata::{error::stream_info::MetadataError, STREAM_INFO},
    storage::StreamType,
};

const KAFKA_TOPIC: &str = "KAFKA_TOPIC";

#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Error loading environment variable {0}")]
    Env(&'static str),
    #[error("Kafka error {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("RDKafka error {0}")]
    RDKafka(#[from] rdkafka::error::RDKafkaError),
    #[error("Error parsing int {1} for environment variable {0}")]
    ParseInt(&'static str, ParseIntError),
    #[error("Post error: #{0}")]
    Post(#[from] PostError),
    #[error("Metadata error: #{0}")]
    Metadata(#[from] MetadataError),
    #[error("Event error: #{0}")]
    Event(#[from] EventError),
    #[error("JSON error: #{0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid SSL protocol: #{0}")]
    InvalidSslProtocol(String),
    #[error("Invalid unicode for environment variable {0}")]
    EnvNotUnicode(&'static str),
}

enum SslProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl Display for SslProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SslProtocol::Plaintext => "plaintext",
            SslProtocol::Ssl => "ssl",
            SslProtocol::SaslPlaintext => "sasl_plaintext",
            SslProtocol::SaslSsl => "sasl_ssl",
        })
    }
}
impl FromStr for SslProtocol {
    type Err = KafkaError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "plaintext" => Ok(SslProtocol::Plaintext),
            "ssl" => Ok(SslProtocol::Ssl),
            "sasl_plaintext" => Ok(SslProtocol::SaslPlaintext),
            "sasl_ssl" => Ok(SslProtocol::SaslSsl),
            _ => Err(KafkaError::InvalidSslProtocol(s.to_string())),
        }
    }
}

fn load_env_or_err(key: &'static str) -> Result<String, KafkaError> {
    env::var(key).map_err(|_| KafkaError::Env(key))
}

fn get_flag_env_val(key: &'static str) -> Result<Option<bool>, KafkaError> {
    let raw = env::var(key);
    match raw {
        Ok(val) => Ok(Some(val != "0" && val != "false")),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(KafkaError::EnvNotUnicode(key)),
    }
}

fn setup_consumer() -> Result<StreamConsumer, KafkaError> {
    let hosts = load_env_or_err("KAFKA_HOSTS")?;
    let topic = load_env_or_err("KAFKA_TOPIC")?;

    let mut conf = ClientConfig::new();
    conf.set("bootstrap.servers", &hosts);

    if let Ok(val) = env::var("KAFKA_CLIENT_ID") {
        conf.set("client.id", &val);
    }

    if let Some(val) = get_flag_env_val("a")? {
        conf.set("api.version.request", val.to_string());
    }
    if let Ok(val) = env::var("KAFKA_GROUP") {
        conf.set("group.id", &val);
    }

    if let Ok(val) = env::var("KAFKA_SECURITY_PROTOCOL") {
        let mapped: SslProtocol = val.parse()?;
        conf.set("security.protocol", mapped.to_string());
    }
    let consumer: StreamConsumer = conf.create()?;

    if let Ok(vals_raw) = env::var("KAFKA_PARTITIONS") {
        let vals = vals_raw
            .split(',')
            .map(i32::from_str)
            .collect::<Result<Vec<i32>, ParseIntError>>()
            .map_err(|raw| KafkaError::ParseInt("KAFKA_PARTITIONS", raw))?;

        let mut parts = TopicPartitionList::new();
        for val in vals {
            parts.add_partition(&topic, val);
        }
        consumer.seek_partitions(parts, Timeout::Never)?;
    }
    Ok(consumer)
}

async fn ingest_message<'a>(stream_name: &str, msg: BorrowedMessage<'a>) -> Result<(), KafkaError> {
    debug!("{}: Message: {:?}", stream_name, msg);
    let Some(payload) = msg.payload() else {
        debug!("{} No payload for stream", stream_name);
        return Ok(());
    };
    let schema = STREAM_INFO.raw_schema(stream_name)?;
    let event = format::json::Event {
        data: serde_json::from_slice(payload)?,
        tags: "".to_owned(),
        metadata: "".to_owned(),
    };

    debug!("Generated event: {:?}", event.data);
    let (rb, is_first) = event.into_recordbatch(schema, None, None).unwrap();

    Event {
        rb,
        stream_name: stream_name.to_string(),
        origin_format: "json",
        origin_size: payload.len() as u64,
        is_first_event: is_first,
        parsed_timestamp: Utc::now().naive_utc(),
        time_partition: None,
        custom_partition_values: HashMap::new(),
        stream_type: StreamType::UserDefined,
    }
    .process()
    .await?;

    Ok(())
}

pub async fn setup_integration() -> Result<(), KafkaError> {
    let Ok(stream_name) = env::var(KAFKA_TOPIC) else {
        return Ok(());
    };

    info!("Setup kafka integration for {stream_name}");
    create_stream_if_not_exists(&stream_name, &StreamType::UserDefined.to_string()).await?;

    spawn(async move {
        let consumer = setup_consumer().unwrap();
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    if let Err(e) = ingest_message(&stream_name, msg).await {
                        error!("Couldn't ingest message from kafka: {e}")
                    }
                }
                Err(e) => error!("{e}"),
            }
        }
    });
    info!("Done Setup kafka integration");

    Ok(())
}
