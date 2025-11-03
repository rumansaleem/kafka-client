<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useAppConfig } from "@/stores/configuration";
import {
  Dialog,
  DialogTrigger,
  DialogContent,
  DialogDescription,
  DialogHeader
} from "@/components/ui/dialog";
const isNewClusterDialogOpen = ref(false);
const newClusterName = ref<string>();
const newClusterBootstrapServers = ref<string>();
const { clusters, loadConfig, cluster, addNewCluster: addCluster } = useAppConfig();

function addNewCluster() {
  if (!newClusterName.value || !newClusterBootstrapServers.value) {
    return;
  }
  addCluster(
    newClusterName.value,
    newClusterBootstrapServers.value.split(",").map(s => s.trim()),
    clusters.length === 0 ? true : false
  );
  newClusterName.value = "";
  newClusterBootstrapServers.value = "";
  isNewClusterDialogOpen.value = false;
  loadConfig();
}

onMounted(async () => {
  await loadConfig();
});
</script>
<template>
  <div class="flex h-full">
    <aside class="bg-muted/40 text-foreground max-w-sm overflow-auto flex-none min-w-80">
      <div class="flex items-center justify-between leading-none py-1 px-2 border-t border-muted">
        <h4 class="uppercase text-xs tracking-wide font-bold">Clusters</h4>
        <div class="space-x-2">
          <Dialog v-model:open="isNewClusterDialogOpen">
            <DialogTrigger as-child>
              <Button variant="outline" size="xs"> <PlusIcon class="w-4 h-4" /> Add </Button>
            </DialogTrigger>
            <DialogContent class="sm:max-w-[425px]">
              <DialogHeader>
                <DialogTitle>Add New Cluster</DialogTitle>
                <DialogDescription>
                  Give cluster a name and bootstrap servers
                </DialogDescription>
              </DialogHeader>
              <div class="grid gap-5 py-4">
                <div class="grid gap-2">
                  <Label for="cluster_name">
                    Name
                  </Label>
                  <Input id="cluster_name" v-model="newClusterName" />
                </div>
                <div class="grid gap-1">
                  <Label for="cluster_bootstrap_servers">
                    Bootstrap Servers
                  </Label>
                  <Input
                    id="cluster_bootstrap_servers"
                    type="number"
                    v-model="newClusterBootstrapServers"
                  />
                </div>
              </div>
              <DialogFooter>
                <Button type="submit" @click="addNewCluster">
                  Add
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>

          <Button variant="outline" size="xs" @click="loadConfig">
            <ArrowPathIcon class="block w-4 h-4" />
          </Button>
        </div>
      </div>
      <Command v-if="clusters">
        <CommandInput id="search-input" placeholder="Clusters..." />
        <CommandList>
          <CommandEmpty>
            <p v-if="clusters.length > 0">
              No clusters match the search keywords. Try clearing search.
            </p>
            <p v-else>
              No clusters found.
              <Button @click="loadConfig" size="sm">
                Reload clusters
              </Button>
            </p>
          </CommandEmpty>
          <CommandItem
            v-for="clusterItem of clusters"
            :key="clusterItem.name"
            class="-mx-2 px-4 py-2 flex space-x-2 hover:bg-neutral-100 dark:hover:bg-neutral-800 ui-checked:bg-primary ui-checked:text-white cursor-pointer"
            :value="clusterItem.name"
          >
            <div class="flex-1">
              <code class="text-xs leading-none font-bold" v-text="clusterItem.name"></code>
              <p
                class="text-opacity-25 font-medium text-xs uppercase"
                v-text="clusterItem.bootstrap_servers"
              ></p>
            </div>
          </CommandItem>
        </CommandList>
      </Command>
    </aside>
    <div class="flex-1 h-full overflow-auto">
      <p class="p-2" v-if="cluster == null">
        Please select a cluster from topic list.
      </p>
      <div v-else>
        <h3>Custer: {{ cluster.name }}</h3>
        <p>Bootstrap servers: {{ cluster.bootstrap_servers }}</p>
      </div>
    </div>
  </div>
</template>
