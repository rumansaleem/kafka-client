use std::{borrow::Borrow, collections::HashMap, ffi::CStr, ptr::slice_from_raw_parts, time::Duration};
use rdkafka::{
    admin::{AdminClient, AdminOptions, AlterConfig, ConfigEntry, ConfigResource, ConfigSource as KafkaConfigSource, NewTopic, OwnedResourceSpecifier, ResourceSpecifier, TopicReplication, TopicResult}, bindings::{rd_kafka_AdminOptions_new, rd_kafka_ListOffsets, rd_kafka_ListOffsetsResultInfo_topic_partition, rd_kafka_ListOffsets_result_infos, rd_kafka_event_ListOffsets_result, rd_kafka_event_destroy, rd_kafka_event_error, rd_kafka_event_error_string, rd_kafka_queue_destroy, rd_kafka_queue_new, rd_kafka_queue_poll}, client::{Client, DefaultClientContext}, config::FromClientConfig, consumer::{BaseConsumer, CommitMode, Consumer}, error::IsError, statistics::Topic, topic_partition_list::TopicPartitionListElem, types::RDKafkaErrorCode, util::Timeout, ClientConfig, ClientContext, Offset, TopicPartitionList
};
use serde::{Deserialize, Serialize};

use crate::core::commands::GroupOffset;

fn create_admin_client(bootstrap_servers: Vec<String>, config: ClientConfig) -> AdminClient<DefaultClientContext> {
    AdminClient::from_config(
        config.to_owned()
            .set("bootstrap.servers", bootstrap_servers.join(","))
            .set("group.id", "runtime")
            .set("enable.auto.commit", "false")
    )
    .expect("Error while creating admin client")
}

fn create_base_consumer(bootstrap_servers: Vec<String>, config: &mut ClientConfig) -> BaseConsumer {
    config.to_owned()
        .set("bootstrap.servers", bootstrap_servers.join(","))
        .create()
        .expect("Error creating client")
}

pub async fn create_topic(
    bootstrap_servers: Vec<String>,
    topic: &str,
    partitions: i32,
    replication_factor: i32,
    topic_config: HashMap<String, String>,
    options: Option<AdminOptions>,
) -> TopicResult {
    let client = create_admin_client(bootstrap_servers, ClientConfig::default());
    let new_topic = NewTopic {
        config: topic_config.iter()
            .map(|(key, val)| (key.as_str(), val.as_str()))
            .collect(),
        name: topic,
        num_partitions: partitions,
        replication: TopicReplication::Fixed(replication_factor),
    };
    
    let out = client
        .create_topics(
            vec![new_topic.borrow()],
            options.unwrap_or_default().borrow(),
        )
        .await
        .and_then(|val| Ok(val.first().unwrap().to_owned()))
        .expect("Could not get Result");
    return out.clone();
}
pub async fn alter_topic_configs(bootstrap_servers: Vec<String>, topic: &str, configs: HashMap<&str, &str>) -> Result<(), String> {
    let admin = create_admin_client(bootstrap_servers, ClientConfig::default());
    let alter_config = AlterConfig {
        specifier: ResourceSpecifier::Topic(topic),
        entries: configs
    };
    let alter_configs = vec![alter_config];
    admin.alter_configs(&alter_configs, &AdminOptions::default()).await
        .map(|_val| ())
        .map_err(|err| err.to_string())
}


pub async fn get_topic_configs(bootstrap_servers: Vec<String>, topics:Vec<String>) -> Result<HashMap<String, Vec<ConfigProperty>>, String> {
    let admin = create_admin_client(bootstrap_servers, ClientConfig::default());    
    let resource_specifiers: Vec<ResourceSpecifier> = topics.iter().map(|topic| ResourceSpecifier::Topic(topic)).collect();
    let results = admin.describe_configs(&resource_specifiers, &AdminOptions::default())
        .await
        .map_err(|err| err.to_string())?;

    
    let configs_list = results.into_iter()
        .map(|result| result.map_err(|err| err.to_string()))
        .collect::<Result<Vec<ConfigResource>, String>>()?;


    let configs = configs_list.into_iter().filter_map(|res| match res.specifier {
        OwnedResourceSpecifier::Topic(topic) => Some((topic, res.entries.into_iter().map(|entry| ConfigProperty::from(&entry)).collect())),
        _ => None
    }).collect();

    Ok(configs)
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all="camelCase")]
pub struct ConfigProperty {
    /// The name of the configuration parameter.
    pub name: String,
    /// The value of the configuration parameter.
    pub value: Option<String>,
    /// The source of the configuration parameter.
    pub source: ConfigSource,
    /// Whether the configuration parameter is read only.
    pub is_read_only: bool,
    /// Whether the configuration parameter currently has the default value.
    pub is_default: bool,
    /// Whether the configuration parameter contains sensitive data.
    pub is_sensitive: bool,
}
impl From<&ConfigEntry> for ConfigProperty {
    fn from(value: &ConfigEntry) -> Self {
        Self {
            name: value.name.to_owned(),
            value: value.value.to_owned(),
            source: ConfigSource::from(&value.source),
            is_read_only: value.is_read_only,
            is_default: value.is_default,
            is_sensitive: value.is_sensitive,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum ConfigSource {
    Unknown,
    Default,
    DynamicTopic,
    DynamicBroker,
    DynamicDefaultBroker,
    StaticBroker,
} 
impl From<&KafkaConfigSource> for ConfigSource {
    fn from(value: &KafkaConfigSource) -> Self {
        match value {
            KafkaConfigSource::Unknown => Self::Unknown,
            KafkaConfigSource::Default => Self::Default,
            KafkaConfigSource::DynamicTopic => Self::DynamicTopic,
            KafkaConfigSource::DynamicBroker => Self::DynamicBroker,
            KafkaConfigSource::DynamicDefaultBroker => Self::DynamicDefaultBroker,
            KafkaConfigSource::StaticBroker => Self::StaticBroker,
        }
    }
}
  
pub async fn delete_topic(bootstrap_servers: Vec<String>, topic: &str) -> Result<String, String> {
    // TODO: make sure topic is not in use by any consumer group assignments
    // if topic_in_use {
    //   return Err(format!("Topic '{}' has partitions assigned to consumer groups"));
    // }

    let admin = create_admin_client(bootstrap_servers, ClientConfig::default());
    let results = admin.delete_topics(&[topic], &AdminOptions::default())
    .await
    .map_err(|err| err.to_string())?;

    let result = results.first().unwrap().to_owned();
        
    result.map_err(|(err_str, err_code)| format!("[{}]: {}", err_code, err_str))
}


pub async fn create_consumer_group(
    bootstrap_servers: Vec<String>,
    group_id: &str,
    topics: Vec<&str>,
    initial_offset: GroupOffset,
) -> Result<(), String> {
    let client = create_base_consumer(bootstrap_servers, ClientConfig::default()
        .set("group.id", group_id)
        .set("enable.auto.offset.store", "false")
    );

    // TODO: Improve this validation by checking committed offsets
    let all_groups = client.fetch_group_list(None, Timeout::After(Duration::from_secs(5))).map_err(|err| err.to_string())?;
    let group_already_exists = all_groups.groups().iter().any(|g_info| g_info.name() == group_id);
    if group_already_exists {
        return Err(format!("{} group offsets already exists", group_id));
    }


    let offsets = get_topics_offsets(client.client(), topics, initial_offset.into(), Offset::End)?;

    client.commit(&offsets, CommitMode::Sync)
        .map_err(|err| err.to_string())
}
pub async fn delete_consumer_group(bootstrap_servers: Vec<String>, group: &str) -> Result<String, String> {
    // TODO: make sure there are no group assignments
    // if active_members_present {
    //   return Err(format!("Topic '{}' has partitions assigned to consumer groups"));
    // }

    let admin = create_admin_client(bootstrap_servers, ClientConfig::default());
    let results = admin.delete_groups(&[group], &AdminOptions::default())
    .await
    .map_err(|err: rdkafka::error::KafkaError| err.to_string())?;

    let result = results.first().unwrap().to_owned();
        
    result.map_err(|(err_str, err_code)| format!("[{}]: {}", err_code, err_str))
}

pub fn get_topics_offsets<C: ClientContext>(client: &Client<C>, topics: Vec<&str>, offset: Offset, fallback_offset: Offset) -> Result<TopicPartitionList, String> {
    // Fetch all topic/paritions with latest metadata.
    let mut tpl = TopicPartitionList::new();
    for topic_name in &topics {
        let meta = client.fetch_metadata(Some(topic_name), Timeout::After(Duration::from_secs(5)))
            .map_err(|err| err.to_string())?;
        let topics_meta  = meta.topics().first().filter(|t| t.partitions().len() > 0);
        if let Some(topic) = topics_meta {
            let start_partitions = 0;
            let total_partitions = topic.partitions().into_iter().map(|p| p.id()).max().unwrap();
            tpl.add_partition_range(topic.name(), start_partitions, total_partitions);
        }
    }
    let _ = tpl.set_all_offsets(offset).map_err(|err| err.to_string())?;

    let offset_list = unsafe {
        get_topic_partition_offsets(client, &tpl)?
    };
    
    let invalid_offsets: Vec<TopicPartitionListElem> = offset_list.elements().into_iter()
        .filter(|el| matches!(el.offset(), Offset::Invalid))
        .collect();
   
    if invalid_offsets.len() == 0 {
        return Ok(offset_list);
    }
   
    // To commit offset maybe just the Offset::End fallback should work. TODO: Confirm this!
    if let Offset::Offset(_) = offset {
        tpl.set_all_offsets(fallback_offset).map_err(|err| err.to_string())?;
        let fallback_map = unsafe {
            get_topic_partition_offsets(client, &tpl)
            .map(|list| list.to_topic_map())?
        };
        
        let mut updated_offset_list = offset_list.clone();
        for tpl_el in invalid_offsets {
            let (topic, partition) = (tpl_el.topic(), tpl_el.partition());
            let fallback_offset = fallback_map.get(&(topic.to_string(), partition)).unwrap_or_else(|| &Offset::Invalid);
            updated_offset_list.set_partition_offset(topic, partition, *fallback_offset)
                .map_err(|err| err.to_string())?;
        }
        return Ok(updated_offset_list);
     } 

    Ok(offset_list)
}


pub unsafe fn get_topic_partition_offsets<C: ClientContext>(client: &Client<C>, topic_partition_list: &TopicPartitionList) -> Result<TopicPartitionList, String> {
    let native_client = client.native_ptr();
    let q = rd_kafka_queue_new(native_client);
    let o = rd_kafka_AdminOptions_new(
        native_client,
        rdkafka::types::RDKafkaAdminOp::RD_KAFKA_ADMIN_OP_LISTOFFSETS,
    );
    rd_kafka_ListOffsets(native_client, topic_partition_list.ptr(), o, q);

    let event = rd_kafka_queue_poll(q, 5000);
    if event.is_null() {
        return Err(format!("No event received from rd_kafka_queue_poll"));
    }
    let result = rd_kafka_event_ListOffsets_result(event);
    if result.is_null() {
        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(q);
        return Err(format!("No result received from rd_kafka_event_ListOffsets_result"));
    }

    let err = rd_kafka_event_error(event);
    if err.is_error() {
        let msg = rd_kafka_event_error_string(event);
        let err_str = CStr::from_ptr(msg).to_string_lossy().into_owned();
        return Err(format!("Error: {}", err_str))
    }

    let mut len: usize = 0;
    let raw = &mut len as *mut usize;
    let list_offset_result_infos = rd_kafka_ListOffsets_result_infos(result, raw);
    if list_offset_result_infos.is_null() {
        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(q);
        return Err(format!("Failed to get list_offset_result_infos"));
    }

    let s = &*slice_from_raw_parts(list_offset_result_infos, len);
    let mut new_tpl = TopicPartitionList::new();
    for inf in s {
        let top_part = *rd_kafka_ListOffsetsResultInfo_topic_partition(*inf);
        let topic_str = CStr::from_ptr(top_part.topic).to_string_lossy().into_owned();
        let res = new_tpl.add_partition_offset(
            &topic_str,
            top_part.partition,
            rdkafka::Offset::Offset(top_part.offset),
        );
        if let Err(err) = res {
            eprintln!("Error setting offset '{}': {:#?}", top_part.offset, err);
        }
    }
    
    rd_kafka_event_destroy(event);
    rd_kafka_queue_destroy(q);
    Ok(new_tpl)
}
