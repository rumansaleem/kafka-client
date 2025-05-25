use futures::FutureExt;
use rdkafka::{Offset, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Emitter, State};
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::core::config::{ApplicationState, ClusterConfig};

use crate::kafka::admin::{self, get_topics_offsets, ConfigProperty};
use crate::kafka::consumer::{
    ConsumerGroup, ConsumerGroupOffsetDescription, KafkaConsumer, MessageEnvelope,
};
use crate::kafka::metadata::ClusterMetadata;
use crate::kafka::util::TopicOffsetsMap;

#[tauri::command]
pub fn get_current_cluster(app_config: State<ApplicationState>) -> ClusterConfig {
    app_config.config.lock().unwrap().default_cluster_config()
}

#[tauri::command(async)]
pub fn get_topics(app_config: State<ApplicationState>) -> Result<ClusterMetadata, String> {
    KafkaConsumer::connect(
        app_config
            .config
            .lock()
            .unwrap()
            .default_cluster_config()
            .bootstrap_servers,
    )
    .fetch_metadata()
}

#[tauri::command(async)]
pub async fn fetch_topic_configs(
    app_config: State<'_, ApplicationState>,
    topics: Vec<String>,
) -> Result<HashMap<String, Vec<ConfigProperty>>, String> {
    let bootstrap_servers = app_config
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;

    admin::get_topic_configs(bootstrap_servers, topics).await
}

#[tauri::command(async)]
pub async fn alter_topic_configs(
    app_config: State<'_, ApplicationState>,
    topic: &str,
    configs: HashMap<&str, &str>,
) -> Result<(), String> {
    let bootstrap_servers = app_config
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;
    println!("Topic: {}, configs: {:?}", topic, configs);
    admin::alter_topic_configs(bootstrap_servers, topic, configs).await
}

#[tauri::command(async)]
pub async fn delete_topic(
    app_config: State<'_, ApplicationState>,
    topic: &str,
) -> Result<String, String> {
    let bootstrap_servers = app_config
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;

    admin::delete_topic(bootstrap_servers, topic).await
}

#[tauri::command(async)]
pub fn get_group_offsets(
    app_config: State<ApplicationState>,
    group_name: String,
) -> Result<Vec<ConsumerGroupOffsetDescription>, String> {
    let servers = app_config
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;

    KafkaConsumer::connect_config(HashMap::from([
        ("bootstrap.servers".to_owned(), servers.join(",")),
        ("group.id".to_owned(), group_name.clone()),
    ]))
    .get_committed_offsets()
}

#[tauri::command(async)]
pub fn get_groups(app_config: State<ApplicationState>) -> Result<Vec<ConsumerGroup>, String> {
    KafkaConsumer::connect(
        app_config
            .config
            .lock()
            .unwrap()
            .default_cluster_config()
            .bootstrap_servers,
    )
    .get_groups_list()
}

#[tauri::command(async)]
pub async fn create_topic(
    app_config: State<'_, ApplicationState>,
    topic: &str,
    partitions: i32,
    replication: i32,
    configs: HashMap<String, String>,
) -> Result<String, String> {
    let bootstrap_servers = app_config
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;

    let result = admin::create_topic(
        bootstrap_servers,
        topic,
        partitions,
        replication,
        configs,
        None,
    )
    .await;

    match result {
        Ok(out) => Ok(out),
        Err((data, _err_code)) => Err(data),
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum GroupOffset {
    Beginning,
    End,
    Tail(i64),
    Offset(i64),
}
impl Into<Offset> for GroupOffset {
    fn into(self) -> Offset {
        match self {
            Self::End => Offset::End,
            Self::Beginning => Offset::Beginning,
            Self::Tail(offset) => Offset::OffsetTail(offset),
            Self::Offset(timestamp) => Offset::Offset(timestamp),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "content")]
pub enum FetchOffset {
    Beginning,
    End,
    Timestamp(i64),
}
impl Display for FetchOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Beginning => f.write_str("Beginning"),
            Self::End => f.write_str("End"),
            Self::Timestamp(t) => f.write_fmt(format_args!("Timestamp({})", t)),
        }
    }
}
impl Into<Offset> for FetchOffset {
    fn into(self) -> Offset {
        match self {
            Self::End => Offset::End,
            Self::Beginning => Offset::Beginning,
            Self::Timestamp(timestamp) => Offset::Offset(timestamp),
        }
    }
}

#[tauri::command(async)]
pub async fn create_group_offsets(
    app_config: State<'_, ApplicationState>,
    group_id: &str,
    topics: Vec<&str>,
    initial_offset: GroupOffset,
) -> Result<(), String> {
    let servers = app_config
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;

    admin::create_consumer_group(servers, group_id, topics, initial_offset).await
}

#[tauri::command(async)]
pub async fn delete_consumer_group(
    app_config: State<'_, ApplicationState>,
    group: &str,
) -> Result<String, String> {
    let bootstrap_servers = app_config
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;

    admin::delete_consumer_group(bootstrap_servers, group).await
}

#[tauri::command]
pub async fn stop_consumer(
    state: State<'_, ApplicationState>,
    consumer_id: String,
) -> Result<(), String> {
    let cancel = {
        let map = state
            .active_consumers
            .lock()
            .map_err(|err| err.to_string())?;
        let cancel = map.get(&consumer_id).cloned().ok_or(format!(
            "there is no such consumer running on channel: '{}'",
            consumer_id
        ))?;
        cancel
    };

    let _ = cancel.send(()).await.map_err(|err| err.to_string())?;

    state
        .active_consumers
        .lock()
        .and_then(|mut map| Ok(map.remove(&consumer_id)))
        .map_err(|err| err.to_string())?;
    Ok(())
}

#[tauri::command]
pub fn get_all_active_consumers(app_state: State<ApplicationState>) -> Result<Vec<String>, String> {
    let consumers: Vec<String> = app_state
        .active_consumers
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect();

    Ok(consumers)
}

#[tauri::command(async)]
pub async fn consume_topic_by_timestamp(
    app_handle: AppHandle,
    app_state: State<'_, ApplicationState>,
    topic: &str,
    start: FetchOffset,
    end: Option<FetchOffset>,
) -> Result<(String, TopicOffsetsMap), String> {
    let bootstrap_servers = app_state
        .config
        .lock()
        .unwrap()
        .default_cluster_config()
        .bootstrap_servers;
    let mut stream = KafkaConsumer::connect(bootstrap_servers);

    let offsets_map = stream
        .assign_offsets_by_timestamp(topic, start.clone().into())
        .await?;
    let now_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_err| Duration::from_millis(0))
        .as_millis();

    let event_name = format!("consumer_{now_epoch}/{topic}/{start}");
    let (sender, mut receiver) = mpsc::channel(1);

    app_state
        .active_consumers
        .lock()
        .unwrap()
        .insert(event_name.clone(), sender);

    let out_ev = event_name.clone();
    let consumed_topic = topic.to_owned();
    let mut partitions_current_offsets = offsets_map
        .get(topic)
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<HashMap<i32, i64>>();

    println!("Spawning Thread to consume messages");
    tokio::spawn(async move {
        // TODO: we can subscribe to frontend event before hitting this command, and gen consumer id at frontend
        sleep(Duration::from_secs(1)).await; // Let frontend subscribe to events.
        let consumed_topic = consumed_topic.as_str();
        let end_offsets = end
            .filter(|e| !matches!(e, FetchOffset::Beginning))
            .and_then(|end| {
                get_topics_offsets(
                    stream.client(),
                    vec![&consumed_topic],
                    end.into(),
                    Offset::End,
                )
                .map(|tpl| {
                    tpl.to_topic_map()
                        .iter()
                        .map(|((_, partition), val)| match val {
                            Offset::Offset(o) => (partition.to_owned(), o.to_owned()),
                            _ => (partition.to_owned(), 0),
                        })
                        .collect::<HashMap<i32, i64>>()
                })
                .ok()
            });

        loop {
            tokio::select! {
              _ = receiver.recv() => {
                app_handle.emit::<Option<MessageEnvelope<String, String>>>(&event_name, None).expect("Failed to emit event");
                break;
              },
              result = stream.get_next_message().fuse() => {
                let message = result.expect("Could not get next message");
                let (consumed_partition, consumed_offset) = (message.partition, message.offset);

                // Optionally, check if the received message is beyond the end offset, dont emit
                let is_message_beyond_end_offset = end_offsets.as_ref().is_some_and(|end| {
                  return &consumed_offset >= end.get(&consumed_partition).unwrap_or_else(|| &0);
                });
                if !is_message_beyond_end_offset {
                  // println!("Emitted message on channel `{}`: {:?}", event_name, message);
                  app_handle.emit(&event_name, Some(message)).expect("Failed to emit event");
                } else {
                  println!("Message on `{}` at `{}` ignored due to beyond end offset!", consumed_partition, consumed_offset);
                }

                // Update current offsets.
                partitions_current_offsets.entry(consumed_partition).and_modify(move |offset| *offset=consumed_offset+1);

                // Optionally, Check if all current offsets have reached the end.
                let all_partitions_ended = end_offsets.as_ref().is_some_and(|offsets| {
                  return offsets.iter().all(
                    |(part, end_offset)| partitions_current_offsets.get(part).unwrap_or_else(|| &0) >= end_offset
                  );
                });
                if all_partitions_ended {
                  app_handle.emit::<Option<MessageEnvelope<String, String>>>(&event_name, None).expect("Failed to emit event");
                  println!("Consumer on `{}` Ended!", event_name);
                  break;
                }
              }
            }
        }
    });

    Ok((out_ev, offsets_map))
}
