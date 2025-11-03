import { RouteRecordRaw, createMemoryHistory, createRouter } from "vue-router";
import Topics from "./pages/Topics.vue";
import ConsumerGroups from "./pages/ConsumerGroups.vue";
import Clusters from "./pages/Clusters.vue";

const routes: RouteRecordRaw[] = [
  { path: "/", redirect: "/clusters" },
  { path: "/clusters", component: Clusters },
  { path: "/topics", component: Topics },
  { path: "/consumer-groups", component: ConsumerGroups }
];

export const router = createRouter({
  history: createMemoryHistory(),
  routes
});
