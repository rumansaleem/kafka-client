<script setup lang="ts">
import { RouterView } from "vue-router";
import Navbar from "./components/Navbar.vue";
import { Toaster } from "@/components/ui/toast";
import { onMounted, ref } from "vue";
import { Theme } from "@tauri-apps/api/window";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useAppConfig } from "@/stores/configuration";

const theme = ref<Theme | null>(null);
const { loadConfig } = useAppConfig();
onMounted(async () => {
  theme.value = await getCurrentWebviewWindow().theme();
  await loadConfig();
  return await getCurrentWebviewWindow().onThemeChanged(event => {
    console.log("App theme changed to", event);
    theme.value = event.payload;
  });
});
</script>
<template>
  <div class="relative flex flex-col h-screen overflow-hidden" :class="{ dark: theme === 'dark' }">
    <Navbar />
    <div class="flex-1 border-t border-muted overflow-auto">
      <RouterView />
    </div>
    <div class="grid gap-2">
      <Toaster />
    </div>
  </div>
</template>
