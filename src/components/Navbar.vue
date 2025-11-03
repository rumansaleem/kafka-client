<script lang="ts" setup>
import { WindowTitlebar } from "@tauri-controls/vue";
import MenuModeToggle from "./MenuModeToggle.vue";
import AboutDialog from "./AboutDialog.vue";
import {
  Menubar,
  MenubarMenu,
  MenubarTrigger,
  MenubarContent,
  MenubarItem,
  MenubarSeparator,
  MenubarShortcut,
  MenubarLabel,
  MenubarRadioGroup,
  MenubarRadioItem
} from "./ui/menubar";
import { Dialog, DialogTrigger } from "./ui/dialog";
import { Icons } from "./Icons";
import { onMounted } from "vue";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";

function closeWindow() {
  const appWindow = getCurrentWebviewWindow();
  appWindow.close();
}
onMounted(() => console.log("<Navbar />"));
</script>

<template>
  <WindowTitlebar controls-order="platform" :window-controls-props="{ platform: 'gnome' }">
    <Menubar class="rounded-none border-b border-none pl-2 lg:pl-3 dark:bg-black">
      <MenubarMenu>
        <div class="inline-flex h-fit w-fit items-center text-cyan-600">
          <Icons.kafka class="h-5" />
        </div>
      </MenubarMenu>
      <MenubarMenu>
        <MenubarTrigger class="font-bold">
          Kafka Client
        </MenubarTrigger>
        <Dialog :modal="false">
          <MenubarContent>
            <MenubarItem>
              <RouterLink to="/clusters">Clusters View</RouterLink>
            </MenubarItem>
            <MenubarItem>
              <RouterLink to="/topics">Topics View</RouterLink>
            </MenubarItem>
            <MenubarItem>
              <RouterLink to="/consumer-groups">Groups View</RouterLink>
            </MenubarItem>
            <DialogTrigger as-child>
              <MenubarItem>
                About App
              </MenubarItem>
            </DialogTrigger>
            <MenubarSeparator />
            <MenubarItem> Preferences... <MenubarShortcut>⌘,</MenubarShortcut> </MenubarItem>
            <MenubarShortcut />
            <MenubarItem @click="closeWindow">
              Quit <MenubarShortcut>⌘Q</MenubarShortcut>
            </MenubarItem>
          </MenubarContent>
          <AboutDialog />
        </Dialog>
      </MenubarMenu>
      <MenubarMenu>
        <MenubarTrigger>View</MenubarTrigger>
        <MenubarContent>
          <MenubarItem inset disabled>
            Show Status Bar
          </MenubarItem>
          <MenubarSeparator />
          <MenubarItem inset>
            Hide Sidebar
          </MenubarItem>
          <MenubarItem disabled inset>
            Enter Full Screen
          </MenubarItem>
        </MenubarContent>
      </MenubarMenu>
      <MenubarMenu>
        <MenubarTrigger>Account</MenubarTrigger>
        <MenubarContent>
          <MenubarLabel inset>
            Switch Account
          </MenubarLabel>
          <MenubarSeparator />
          <MenubarRadioGroup value="benoit">
            <MenubarRadioItem value="andy">
              Andy
            </MenubarRadioItem>
            <MenubarRadioItem value="benoit">
              Benoit
            </MenubarRadioItem>
            <MenubarRadioItem value="Luis">
              Luis
            </MenubarRadioItem>
          </MenubarRadioGroup>
          <MenubarSeparator />
          <MenubarItem inset>
            Manage Famliy...
          </MenubarItem>
          <MenubarSeparator />
          <MenubarItem inset>
            Add Account...
          </MenubarItem>
        </MenubarContent>
      </MenubarMenu>
      <MenuModeToggle />
    </Menubar>
  </WindowTitlebar>
</template>
