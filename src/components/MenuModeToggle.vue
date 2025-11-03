<script lang="ts" setup>
import { Moon, Sun, SunMoon } from "lucide-vue-next";
import {
  MenubarContent,
  MenubarMenu,
  MenubarRadioGroup,
  MenubarRadioItem,
  MenubarTrigger
} from "./ui/menubar";
import { onMounted, ref } from "vue";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { setTheme as setAppTheme } from "@tauri-apps/api/app";
import { Theme } from "@tauri-apps/api/window";

const window = getCurrentWebviewWindow();
const theme = ref<Theme | "system">("system");
const setTheme = async (newTheme: Theme | "system") => {
  await setAppTheme(newTheme === "system" ? null : newTheme);
};

onMounted(async () => {
  const currentTheme = await window.theme();
  theme.value = currentTheme ?? "system";
  const unlisten = await window.onThemeChanged(event => {
    theme.value = event.payload;
    console.log("Theme changed to", event);
  });

  return () => {
    unlisten();
  };
});

interface ThemeItem {
  label: string;
  value: Theme | "system";
  icon: typeof Moon | typeof Sun | typeof SunMoon;
}
const themeList: Array<ThemeItem> = [
  {
    label: "Light",
    value: "light",
    icon: Sun
  },
  {
    label: "Dark",
    value: "dark",
    icon: Moon
  },
  {
    label: "System",
    value: "system",
    icon: SunMoon
  }
];
</script>

<template>
  <MenubarMenu>
    <MenubarTrigger>Theme</MenubarTrigger>
    <MenubarContent>
      <MenubarRadioGroup :model-value="theme">
        <MenubarRadioItem
          v-for="{ label, value, icon } in themeList"
          :key="value"
          :value="value"
          @click="setTheme(value)"
        >
          <component :is="icon" class="mr-2 h-4 w-4" />
          <span>{{ label }}</span>
        </MenubarRadioItem>
      </MenubarRadioGroup>
    </MenubarContent>
  </MenubarMenu>
</template>
