<script setup lang="ts">
import { RouterView } from "vue-router";
import Navbar from "./components/Navbar.vue";
import { Toaster } from "@/components/ui/toast";
</script>
<template>
  <div class="relative flex flex-col h-screen overflow-hidden">
    <Navbar />
    <div class="flex-1 border-t border-muted overflow-auto">
      <RouterView />
    </div>
    <div class="grid gap-2">
      <Toaster />
    </div>
  </div>
</template>
