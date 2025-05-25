import { getClusterMetadata } from "@/lib/kafka";
import { defineStore } from "pinia";
import { ref } from "vue";
import { useTopics } from "./topics";
import { useBrokers } from "./brokers";
import { FreeBrush } from "@unovis/ts";

const DEFAULT_TTL = 5 * 60 * 1000; // 5 mins

export const useClusterMetadata = defineStore("metadata", () => {
  const isLoading = ref(false);
  const lastError = ref("");
  const lastLoadTime = ref(-Infinity);
  const originatingBrokerId = ref(0);

  function fetchMetadata() {
    console.log("Fetching Metadata");
    isLoading.value = true;
    return getClusterMetadata()
      .catch(err => {
        lastError.value = err;
        throw err;
      })
      .then(data => {
        lastLoadTime.value = Date.now();
        const { setTopicsMetadata } = useTopics();
        const { setBrokers } = useBrokers();
        console.log("Fetched Metadata: ", data);
        setTopicsMetadata(data.topics);
        setBrokers(data.brokers);
        originatingBrokerId.value = data.originating_broker_id;
        lastError.value = "";
        return data;
      })
      .finally(() => {
        isLoading.value = false;
      });
  }

  function loadMetadata(ttl = DEFAULT_TTL) {
    const metadataAge = Date.now() - lastLoadTime.value;
    console.log(`Load Metadata, age: ${metadataAge}, ttl: ${ttl}, Fetch: ${metadataAge > ttl}`);
    if (metadataAge > ttl) {
      return fetchMetadata();
    }

    return Promise.resolve();
  }

  return { isLoading, error: lastError, loadMetadata, fetchMetadata };
});
