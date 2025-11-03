import { load, Store } from "@tauri-apps/plugin-store";

let store: Store | null = null;
async function getStore(): Promise<Store> {
  if (store) return store;
  store = await load("config.json");
  return store;
}

export function getConfigClusters(): Promise<ClusterConfig[]> {
  return getStore().then(store => store.get<ClusterConfig[]>("clusters").then(data => data ?? []));
}

export async function addCluster(cluster: ClusterConfig): Promise<void> {
  const store = await getStore();
  const clusters = await getConfigClusters();
  clusters.push(cluster);
  await store.set("clusters", clusters);
  await store.save();
}

export type ClusterConfig = {
  name: string;
  bootstrap_servers: string[];
};
