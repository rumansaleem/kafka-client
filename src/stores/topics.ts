import {
  alterTopicConfigs,
  ConfigEntry,
  createTopic as adminCreateTopic,
  deleteTopic as adminDeleteTopic,
  CreateTopicRequest,
  getAllTopicsConfigs,
  getTopicConfigs,
  TopicInfo
} from "@/lib/kafka";
import { Fzf } from "fzf";
import { defineStore } from "pinia";
import { computed, ref } from "vue";
import { useClusterMetadata } from "./metadata";

export const useTopics = defineStore("topics", () => {
  let allTopics = ref<TopicInfo[]>([]);
  const isEmpty = computed(() => allTopics.value.length === 0);
  const configLoading = ref(false);
  const configLoadedAt = ref(-Infinity);
  let configEntries = ref<Record<string, ConfigEntry[]>>({});

  const topicsIndex = computed(
    () => new Fzf(allTopics.value, { selector: topic => topic.name, sort: true })
  );

  function filterTopics(search: string) {
    return topicsIndex.value
      .find(search)
      .sort((a, b) => b.score - a.score)
      .map(e => e.item);
  }

  function isValidTopic(topic: string) {
    return allTopics.value.some(t => t.name === topic);
  }

  const topicConfigs = computed(() => (topic: string) => configEntries.value[topic]);
  const nonDefaultTopicConfigs = computed(() => (topic: string) => {
    console.log("computing nonDefaultTopicConfigs", topic);
    return topicConfigs.value(topic)?.filter(item => !item.isDefault && item.source != "Default");
  });

  async function fetchAllTopicConfigs() {
    const topicNames = allTopics.value.map(topic => topic.name);
    console.log(`Fetching all topic configs: ${topicNames}`);
    try {
      configLoading.value = true;
      configEntries.value = await getAllTopicsConfigs(topicNames);
      console.log("Fetched Configs: ", configEntries.value);
      configLoadedAt.value = Date.now();
      return configEntries.value;
    } catch (err) {
      console.error(err);
    } finally {
      configLoading.value = false;
    }
  }

  async function createTopic(newTopic: CreateTopicRequest) {
    if (!newTopic.topic) {
      return;
    }

    const { fetchMetadata } = useClusterMetadata();

    const topicName = await adminCreateTopic(newTopic);
    const { topics } = await fetchMetadata();
    return topics.find(topic => topic.name === topicName);
  }

  async function deleteTopic(topic: string) {
    const { fetchMetadata } = useClusterMetadata();

    const deletedTopic = await adminDeleteTopic(topic);
    await fetchMetadata();

    return deletedTopic;
  }

  async function fetchTopicConfigs(topic: string) {
    try {
      configLoading.value = true;
      configEntries.value[topic] = await getTopicConfigs(topic);
      configLoadedAt.value = Date.now();

      return configEntries;
    } catch (err) {
      console.error(err);
    } finally {
      configLoading.value = false;
    }
  }

  async function addTopicConfigs(topic: string, newConfigs: Record<string, string>) {
    const existingConfigs = Object.fromEntries(
      nonDefaultTopicConfigs.value(topic).map(i => [i.name, i.value ?? ""])
    );

    await alterTopicConfigs(topic, { ...existingConfigs, ...newConfigs });
    return await fetchTopicConfigs(topic);
  }
  async function removeTopicConfigs(topic: string, configName: string) {
    const removedConfigs = Object.fromEntries(
      nonDefaultTopicConfigs
        .value(topic)
        .filter(i => i.name != configName)
        .map(i => [i.name, i.value || ""])
    );
    await alterTopicConfigs(topic, removedConfigs);

    return await fetchTopicConfigs(topic);
  }

  function setTopicsMetadata(metadata: TopicInfo[]) {
    allTopics.value = metadata;
    fetchAllTopicConfigs();
  }

  return {
    allTopics,
    createTopic,
    deleteTopic,
    isEmpty,
    topicsIndex,
    isValidTopic,
    filterTopics,
    setTopicsMetadata,
    configLoading,
    topicConfigs,
    nonDefaultTopicConfigs,
    addTopicConfigs,
    removeTopicConfigs
  };
});
