<script setup lang="ts">
import { onMounted, ref } from "vue";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import TopicList from "@/components/Topics/TopicList.vue";
import TopicView from "@/components/Topics/TopicView.vue";
import { TopicInfo } from "@/lib/kafka";
import { ClusterConfig } from "@/lib/config";
import { useClusterMetadata } from "@/stores/metadata";
import { useAppConfig } from "@/stores/configuration";

const {loadConfig, setCurrentCluster} = useAppConfig();
const {loadMetadata} = useClusterMetadata();
const selectedTopic = ref<TopicInfo>();
onMounted(async () => {
  await loadConfig();
  await loadMetadata();
});
onMounted(async () => await getCurrentWebviewWindow().listen<ClusterConfig>("current-cluster-update", (event) => {
  setCurrentCluster(event.payload);
}))
</script>
<template>
  <div class="flex h-full">
    <aside class="bg-muted/40 text-foreground max-w-sm overflow-auto flex-none min-w-80">
      <TopicList v-model:selected-topic="selectedTopic" @refresh="loadMetadata()" />
    </aside>
    <main class="flex-1 h-full overflow-auto">
      <p class="p-2" v-if="selectedTopic == null">
        Please select a topic from topic list.
      </p>
      <TopicView v-else :topic="selectedTopic" />
    </main>
  </div>
</template>