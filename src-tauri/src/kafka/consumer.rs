use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use itertools::Itertools;
use rdkafka::{
    bindings::rd_kafka_OffsetSpec_t,
    client::Client,
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    groups::{GroupInfo, GroupMemberInfo},
    message::{Headers, OwnedMessage},
    util::Timeout,
    ClientConfig, Message, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Cursor, time::Duration};

use super::{
    admin::get_topic_partition_offsets,
    metadata::ClusterMetadata,
    util::{from_topic_partition_list_to_map, read_str, TopicOffsetsMap},
};

#[derive(Serialize, Deserialize, Clone)]
pub struct TopicPartitionOffset {
    topic: String,
    offsets: Vec<(i32, i64)>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MessageEnvelope<K, P> {
    pub key: K,
    pub partition: i32,
    pub offset: i64,
    pub headers: HashMap<String, String>,
    pub payload: P,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroup {
    name: String,
    state: String,
    protocol: String,
    protocol_type: String,
    members: Vec<ConsumerGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
    id: String,
    client_id: String,
    client_host: String,
    metadata: Vec<u8>,
    assignments: Vec<MemberAssignment>,
}

impl ConsumerGroup {
    pub fn from(group: &GroupInfo) -> Self {
        let members: Vec<ConsumerGroupMember> = group
            .members()
            .into_iter()
            .map(|member| ConsumerGroupMember::from(member))
            .filter_map(|res| res.ok())
            .collect();

        ConsumerGroup {
            name: group.name().to_string(),
            state: group.state().to_string(),
            protocol: group.protocol().to_string(),
            protocol_type: group.protocol_type().to_string(),
            members,
        }
    }
}

impl ConsumerGroupMember {
    pub fn from(member: &GroupMemberInfo) -> Result<Self, String> {
        let member_metadata = member
            .metadata()
            .map(|meta| meta.to_vec())
            .unwrap_or(vec![]);
        ConsumerGroupMember::parse_member_assignment(member.assignment()).map(|mem_assignments| {
            ConsumerGroupMember {
                id: member.id().to_string(),
                client_id: member.client_id().to_string(),
                client_host: member.client_host().to_string(),
                metadata: member_metadata,
                assignments: mem_assignments,
            }
        })
    }

    fn parse_member_assignment(payload: Option<&[u8]>) -> Result<Vec<MemberAssignment>, String> {
        if payload.is_none() {
            return Ok(vec![]);
        }

        let mut cursor = Cursor::new(payload.unwrap());
        let _version = cursor
            .read_i16::<BigEndian>()
            .map_err(|e| format!("{}", e))?;
        let assign_len = cursor
            .read_i32::<BigEndian>()
            .map_err(|e| format!("{}", e))?;
        let mut assigns = Vec::with_capacity(assign_len as usize);
        for _ in 0..assign_len {
            let topic = read_str(&mut cursor)
                .map_err(|e| format!("{}", e))?
                .to_string();
            let partition_len = cursor
                .read_i32::<BigEndian>()
                .map_err(|e| format!("{}", e))?;
            let mut partitions = Vec::with_capacity(partition_len as usize);
            for _ in 0..partition_len {
                let partition = cursor
                    .read_i32::<BigEndian>()
                    .map_err(|e| format!("{}", e))?;
                partitions.push(partition);
            }
            assigns.push(MemberAssignment { topic, partitions })
        }
        Ok(assigns)
    }
}
pub struct KafkaConsumer {
    consumer: StreamConsumer,
    metadata: Option<ClusterMetadata>,
}

impl KafkaConsumer {
    pub fn connect(bootstrap_servers: Vec<String>) -> Self {
        let config: HashMap<String, String> = HashMap::from([
            ("bootstrap.servers".into(), bootstrap_servers.join(",")),
            ("group.id".into(), "runtime".into()),
            ("enable.auto.commit".into(), "false".into()),
        ]);

        KafkaConsumer::connect_config(config)
    }

    pub fn client(&self) -> &Client<DefaultConsumerContext> {
        self.consumer.client()
    }

    pub fn connect_config(config: HashMap<String, String>) -> Self {
        let mut client_config = ClientConfig::new();

        for (key, value) in config.clone().into_iter() {
            client_config.set(key, value);
        }

        Self {
            consumer: client_config
                .create::<StreamConsumer>()
                .expect(format!("Error while connecting using config {:#?}", config).as_str()),
            metadata: None,
        }
    }

    pub fn get_metadata(&mut self) -> Result<ClusterMetadata, String> {
        match &self.metadata {
            Some(metadata) => Result::Ok(metadata.to_owned()),
            None => self
                .fetch_metadata()
                .and_then(|meta| Ok(self.update_metadata(meta))),
        }
    }

    pub fn get_committed_offsets(mut self) -> Result<Vec<ConsumerGroupOffsetDescription>, String> {
        let metadata = self.get_metadata()?;

        let mut tpl_stored = TopicPartitionList::new();
        for topic in metadata.topics {
            for partition in topic.partitions {
                tpl_stored
                    .add_partition_offset(topic.name.as_str(), partition.id, Offset::Stored)
                    .unwrap();
            }
        }

        let mut tpl_end = tpl_stored.clone();
        tpl_end.set_all_offsets(Offset::End).unwrap();
        let end_offsets = unsafe {
            get_topic_partition_offsets(self.consumer.client(), &tpl_end)
                .map(from_topic_partition_list_to_map)?
        };

        let mut tpl_beginning = tpl_stored.clone();
        tpl_beginning.set_all_offsets(Offset::Beginning).unwrap();
        let start_offsets = unsafe {
            get_topic_partition_offsets(self.consumer.client(), &tpl_beginning)
                .map(from_topic_partition_list_to_map)?
        };

        let stored_offset = self
            .consumer
            .committed_offsets(tpl_stored, Timeout::Never)
            .map_err(|err| err.to_string())
            .map(from_topic_partition_list_to_map)?;

        Ok(from_offset_map_tuple_to_description_vec(
            start_offsets,
            end_offsets,
            stored_offset,
        ))
    }

    pub fn get_groups_list(&self) -> Result<Vec<ConsumerGroup>, String> {
        self.consumer
            .fetch_group_list(None, Timeout::After(Duration::from_secs(300)))
            .map_err(|err| err.to_string())
            .map(|group_list| {
                group_list
                    .groups()
                    .into_iter()
                    .map(|group| ConsumerGroup::from(group))
                    .collect()
            })
    }

    fn update_metadata(&mut self, metadata: ClusterMetadata) -> ClusterMetadata {
        self.metadata = Some(metadata.clone());
        metadata
    }

    pub fn fetch_metadata(&self) -> Result<ClusterMetadata, String> {
        self.consumer
            .fetch_metadata(None, Duration::from_secs(2))
            .map(|data| ClusterMetadata::from(&data))
            .map_err(|err| err.to_string())
    }

    pub async fn assign_offsets_by_timestamp(
        &mut self,
        topic: &str,
        offset: Offset,
    ) -> Result<TopicOffsetsMap, String> {
        let mut start_offset_timestamp_list = TopicPartitionList::new();
        let topic_partitions = self
            .get_metadata()
            .map(|metadata| {
                metadata.topics.into_iter().find_map(|item| {
                    if item.name == topic {
                        Some(item.partitions)
                    } else {
                        None
                    }
                })
            })
            .and_then(|top| top.ok_or_else(|| "Invalid topic name".to_owned()))
            .map_err(|err| err.to_string())?;

        for partition in topic_partitions {
            start_offset_timestamp_list
                .add_partition_offset(topic, partition.id, offset)
                .map_err(|err| err.to_string())?;
        }

        let start_offsets_list = match offset {
            Offset::Offset(_) => self
                .consumer
                .offsets_for_times(start_offset_timestamp_list, Duration::from_secs(2))
                .map_err(|err| err.to_string())?,
            Offset::Beginning => unsafe {
                get_topic_partition_offsets(self.consumer.client(), &start_offset_timestamp_list)?
            },
            Offset::End => unsafe {
                get_topic_partition_offsets(self.consumer.client(), &start_offset_timestamp_list)?
            },
            _ => panic!("unsupported Offset type {:?}", offset),
        };

        self.consumer
            .assign(&start_offsets_list)
            .map_err(|err| err.to_string())?;

        Ok(from_topic_partition_list_to_map(start_offsets_list))
    }

    pub async fn get_next_message(&self) -> Result<MessageEnvelope<String, String>, String> {
        self.consumer
            .recv()
            .await
            .map(|x| x.detach())
            .map(Self::convert_message)
            .map_err(|err| err.to_string())
    }

    fn convert_message(message: OwnedMessage) -> MessageEnvelope<String, String> {
        let key = message
            .key()
            .map(String::from_utf8_lossy)
            .unwrap_or_default()
            .to_string();
        let timestamp = message.timestamp().to_millis().unwrap_or_default();
        let headers = message
            .headers()
            .map(|h| {
                let mut map = HashMap::with_capacity(h.count());
                for i in 0..h.count() {
                    let header = h.get(i);
                    let key = header.key;
                    let val = header.value.unwrap_or_else(|| &[] as &[u8]);

                    map.insert(key.to_string(), String::from_utf8_lossy(val).to_string());
                }
                map
            })
            .unwrap_or_default();
        let payload = message
            .payload()
            .map(String::from_utf8_lossy)
            .map(|v| v.to_string())
            .unwrap_or_default();
        MessageEnvelope {
            key,
            partition: message.partition(),
            offset: message.offset(),
            headers,
            payload,
            timestamp,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupOffsetDescription {
    topic: String,
    partitions: Vec<ConsumerGroupPartitionOffsets>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupPartitionOffsets {
    partition: i32,
    start_offset: i64,
    end_offset: i64,
    current_offset: i64,
}
impl ConsumerGroupPartitionOffsets {
    pub fn lag(self) -> i64 {
        self.end_offset - self.current_offset
    }
}
fn from_offset_map_tuple_to_description_vec(
    start_offsets_map: TopicOffsetsMap,
    end_offsets_map: TopicOffsetsMap,
    current_offsets_map: TopicOffsetsMap,
) -> Vec<ConsumerGroupOffsetDescription> {
    current_offsets_map
        .keys()
        .into_iter()
        .map(|topic| {
            let start_part_map = start_offsets_map
                .get(topic)
                .map(|partition_list| {
                    partition_list
                        .to_owned()
                        .into_iter()
                        .collect::<HashMap<i32, i64>>()
                })
                .unwrap_or_default();

            let end_part_map = end_offsets_map
                .get(topic)
                .map(|partition_list| {
                    partition_list
                        .to_owned()
                        .into_iter()
                        .collect::<HashMap<i32, i64>>()
                })
                .unwrap_or_default();

            let invalid_offset: i64 = -1;
            current_offsets_map
                .get(topic)
                .map(|list| ConsumerGroupOffsetDescription {
                    topic: topic.to_string(),
                    partitions: list
                        .to_owned()
                        .into_iter()
                        .sorted_by(|part1, part2| part1.0.cmp(&part2.0))
                        .map(|(part, offset)| ConsumerGroupPartitionOffsets {
                            partition: part,
                            start_offset: *(start_part_map.get(&part).unwrap_or(&invalid_offset)),
                            end_offset: *(end_part_map.get(&part).unwrap_or(&invalid_offset)),
                            current_offset: offset,
                        })
                        .collect(),
                })
                .unwrap()
        })
        .collect()
}
