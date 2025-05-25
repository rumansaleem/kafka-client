import { invoke } from "@tauri-apps/api/core";

// Kafka Admin
export type CreateTopicRequest = {
  topic: string;
  partitions: number;
  replication: number;
  configs: Record<string, string>;
};
export function createTopic(topicCreateRequest: CreateTopicRequest): Promise<string> {
  return invoke<string>("create_topic", topicCreateRequest);
}

export function deleteTopic(topic: string): Promise<string> {
  return invoke("delete_topic", { topic });
}

export function getTopicConfigs(topic: string): Promise<ConfigEntry[]> {
  return getAllTopicsConfigs([topic]).then(record => record[topic]);
}

export function getAllTopicsConfigs(topics: string[]): Promise<Record<string, ConfigEntry[]>> {
  return invoke<Record<string, ConfigEntry[]>>("fetch_topic_configs", { topics });
}

export function alterTopicConfigs(topic: string, configs: Record<string, string>): Promise<void> {
  return invoke("alter_topic_configs", { topic, configs });
}

export type ConfigEntry = {
  name: string;
  value?: string;
  isDefault: boolean;
  isReadOnly: boolean;
  isSensitive: boolean;
  source:
    | "Unknown"
    | "Default"
    | "DynamicTopic"
    | "DynamicBroker"
    | "StaticBroker"
    | "DynamicDefaultBroker";
};

// Topics & Broker Metadata
export type PartitionInfo = {
  id: number;
  isr: number[];
  replicas: number[];
  leader: number;
};
export type TopicInfo = {
  name: string;
  partitions: PartitionInfo[];
};

export type BrokerInfo = {
  id: number;
  host: string;
  port: number;
};

export type ClusterMetadata = {
  originating_broker_id: number;
  topics: TopicInfo[];
  brokers: BrokerInfo[];
};
export function getClusterMetadata(): Promise<ClusterMetadata> {
  return invoke<ClusterMetadata>("get_topics");
}

export type TopicGroupOffsets = {
  topic: string;
  partitions: {
    partition: number;
    startOffset: number;
    endOffset: number;
    currentOffset: number;
  }[];
};
export function getGroupOffsets(groupName: string): Promise<TopicGroupOffsets[]> {
  return invoke<TopicGroupOffsets[]>("get_group_offsets", { groupName });
}

export type GroupOffset =
  | { type: "Beginning" }
  | { type: "End" }
  | { type: "Tail"; content: number }
  | { type: "Offset"; content: number };
export function createConsumerGroup(
  groupId: string,
  topics: string[],
  initialOffset: GroupOffset
): Promise<void> {
  return invoke<void>("create_group_offsets", { groupId, topics, initialOffset });
}

// Consumer Groups Metadata
export type MemberAssignment = {
  topic: string;
  partitions: number[];
};

export type ConsumerGroup = {
  name: string;
  state: string;
  protocol: string;
  protocol_type: string;
  members: ConsumerGroupMember[];
};

export type ConsumerGroupMember = {
  id: string;
  client_id: string;
  client_host: string;
  metadata: Uint8Array;
  assignments: MemberAssignment[];
};

export function getConsumerGroups(): Promise<ConsumerGroup[]> {
  return invoke<ConsumerGroup[]>("get_groups");
}

export function deleteConsumerGroup(group: string): Promise<string> {
  return invoke("delete_consumer_group", { group });
}

// Consumers
export type MessageEnvelope = {
  key: string;
  offset: number;
  partition: number;
  timestamp: number;
  payload: string;
  headers: Record<string, string>;
};

export type JsonMessageEnvelope = MessageEnvelope & { payloadJson: Record<string, unknown> | null };
export type FetchOffset =
  | { type: "Beginning" }
  | { type: "End" }
  | { type: "Timestamp"; content: number };
export function consumeTopicBetweenOffsets(topic: string, start: FetchOffset, end?: FetchOffset) {
  return invoke<[string, Record<string, [number, number][]>]>("consume_topic_by_timestamp", {
    topic,
    start,
    end
  });
}

export function stopConsumer(consumerId: string) {
  return invoke<void>("stop_consumer", { consumerId });
}
