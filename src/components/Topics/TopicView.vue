<script setup lang="ts">
import { PartitionInfo, TopicInfo } from "@/lib/kafka";
import { computed, ref, watchEffect } from "vue";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { PlusIcon } from "lucide-vue-next";
import { allTopicConfigs } from "@/lib/kafka/topicConfigs";
import {Badge} from "@/components/ui/badge";
import { DialogClose, DialogContent, DialogDescription, DialogTitle, DialogHeader, DialogTrigger, DialogFooter, Dialog } from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { useTopics } from "@/stores/topics";
import { useBrokers } from "@/stores/brokers";
import { storeToRefs } from "pinia";

const props = defineProps<{
  topic: TopicInfo;
}>();


function isReplicaInSync(partition: PartitionInfo, replica: number) {
  return partition.isr.includes(replica);
}
const { getBrokerLabel } = useBrokers();
const topicStore = useTopics();
const {configLoading, nonDefaultTopicConfigs } = storeToRefs(topicStore);
const {removeTopicConfigs, addTopicConfigs } = topicStore;
const nonDefaultConfigs = computed(() => nonDefaultTopicConfigs.value(props.topic.name));

const showAddConfigsDialog = ref(false);
const newConfig = ref(allTopicConfigs.at(0)?.name ?? "");
const selectedNewConfig = computed(() => allTopicConfigs.find(c => c.name === newConfig.value));
const newConfigValue = ref("");
watchEffect(() => {
  if (selectedNewConfig.value) {
    newConfigValue.value = selectedNewConfig.value.defaultValue
  }
});

function submitAddTopicConfig() {
  addTopicConfigs(props.topic.name, {[newConfig.value]: newConfigValue.value});
  newConfig.value = allTopicConfigs.at(0)?.name ?? "";
  showAddConfigsDialog.value = false;
}

</script>

<template>
  <header class="px-4 py-2 bg-muted shadow sticky top-0 flex items-center justify-between z-10">
    <div>
      <h4 class="text-lg font-semibold" v-if="topic" v-text="topic.name"></h4>
      <h6 class="text-sm uppercase tracking-wide text-muted-foreground">Paritions: {{ topic.partitions.length }}</h6>
    </div>
  </header>

  <main class="p-4">
    <Dialog v-model:open="showAddConfigsDialog">
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            Add Topic Config
          </DialogTitle>
          <DialogDescription>
            Add configuration options for : <code class="relative rounded bg-muted px-[0.3rem] py-[0.2rem] font-mono text-sm font-semibold" v-text="topic.name"></code>
          </DialogDescription>
        </DialogHeader>
        <form class="grid gap-5">
          <div class="grid gap-2">
            <Label for="topic-config-name">Config</Label>
            <Select id="topic-config-name" v-model="newConfig">
              <SelectTrigger>
                <SelectValue class="text-left" placeholder="Select a config" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem v-for="configOpt in allTopicConfigs" :key="configOpt.name" :value="configOpt.name">
                  <p v-text="configOpt.name"></p>
                  <p class="max-w-[450px] truncate text-muted-foreground" v-text="configOpt.description"></p>
                </SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label for="topic-config-value">Value</Label>
            <div class="space-y-2">
              <Input id="topic-config-value" type="text" v-model="newConfigValue" :default-value="selectedNewConfig?.defaultValue"/>
              <div class="text-xs space-y-1" v-if="selectedNewConfig">
                <p class="text-muted-foreground"><strong class="font-bold">Type:</strong> <span v-text="selectedNewConfig.type"></span></p>
                <p class="text-muted-foreground"><strong class="font-bold">Valid Values:</strong> <span v-text="selectedNewConfig.validValues"></span></p>
                <p class="text-muted-foreground"><strong class="font-bold">Default:</strong> <span v-text="selectedNewConfig.defaultValue"></span></p>
              </div>
            </div>
          </div>
        </form>
        <DialogFooter>
          <DialogClose as-child>
            <Button variant="secondary">Cancel</Button>
          </DialogClose>
          <Button @click="submitAddTopicConfig">Add Config</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
    <p v-if="configLoading" class="mb-4"><strong class="font-bold">configs:</strong> Loading... </p>
    <div v-else-if="nonDefaultConfigs.length > 0" class="-m-1 px-1 pt-1 mb-4 flex flex-wrap items-center">
      <strong class="font-bold">configs:</strong> 
      <Badge variant="outline" v-for="config in nonDefaultConfigs" :key="config.name" class="m-1 font-mono inline-flex items-baseline space-x-2">
        <span v-text="`${config.name}=${config.value}`"></span>
        <Button as="a" variant="link" size="xs" class="p-0 hover:text-red-500 cursor-pointer h-auto" @click="() => removeTopicConfigs(topic.name, config.name)">x</Button>
      </Badge>
      <Button variant="default" size="xs" class="h-6 w-6 p-0 rounded-full" @click="showAddConfigsDialog = true">
        <PlusIcon class="block w-3 h-3" />
      </Button>
    </div>
    <p v-else class="mb-4 space-x-1">
      <strong class="font-bold">configs:</strong>  
      <span class="text-muted-foreground">No additional configuration found for this topic. </span>
      <Button variant="default" size="xs" class="h-6 w-6 p-0 rounded-full" @click="() => showAddConfigsDialog = true">
        <PlusIcon class="block w-3 h-3" />
      </Button>
    </p>
    <h2 class="text-xl font-bold mb-2">Partitions</h2>
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead class="w-10">#</TableHead>
          <TableHead class="w-64">Leader</TableHead>
          <TableHead>Replicas</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <TableRow v-for="partition in topic.partitions" :key="partition.id">
          <TableCell v-text="partition.id"></TableCell>
          <TableCell v-text="getBrokerLabel(partition.leader)"></TableCell>
          <TableCell>
            <Badge :variant="isReplicaInSync(partition, replica) ? 'outline' : 'destructive'" v-for="replica in partition.replicas" v-text="getBrokerLabel(replica)"></Badge>
          </TableCell>
        </TableRow>
      </TableBody>
    </Table>
  </main>
</template>
