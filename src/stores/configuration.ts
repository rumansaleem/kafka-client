import { ClusterConfig, getConfigClusters } from "@/lib/config";
import { defineStore } from "pinia";
import { computed, reactive, toRef } from "vue";

type AppConfig = {
  clusters: ClusterConfig[];
  currentClusterName: string;
};
const DEFAULT_APP_CONFIG: AppConfig = {
  clusters: [],
  currentClusterName: ""
};
export const useAppConfig = defineStore("appConfig", () => {
  const config = reactive<AppConfig>(DEFAULT_APP_CONFIG);
  const cluster = computed(() => config.clusters.find(c => c.name === config.currentClusterName));

  async function loadConfig() {
    const clusters = await getConfigClusters();
    config.clusters = clusters;
    config.currentClusterName = clusters.length > 0 ? clusters[0].name : "";
  }

  async function addNewCluster(cluster: ClusterConfig, isCurrent = false) {
    config.clusters.push(cluster);
    if (isCurrent) {
      config.currentClusterName = cluster.name;
    }
  }

  return {
    clusters: toRef(config, "clusters"),
    cluster,
    loadConfig,
    addNewCluster
  };
});
