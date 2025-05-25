import { UserConfig, defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import vueDevTools from "vite-plugin-vue-devtools";
import path from "path";

const host = process.env.TAURI_DEV_HOST;

// https://vitejs.dev/config/
export default defineConfig(async () => {
  const config: UserConfig = {
    plugins: [vueDevTools(), vue()],

    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src")
      }
    },

    // Vite options tailored for Tauri development and only applied in `tauri dev` or `tauri build`
    //
    // 1. prevent vite from obscuring rust errors
    clearScreen: false,
    // 2. tauri expects a fixed port, fail if that port is not available
    server: {
      host: host || false,
      port: 1420,
      strictPort: true,
      watch: {
        // 3. tell vite to ignore watching `src-tauri`
        ignored: ["**/src-tauri/**"]
      },
      hmr: host
        ? {
            protocol: "ws",
            host: host,
            port: 1430
          }
        : undefined
    },

    // to access the Tauri environment variables set by the CLI with information about the current target
    envPrefix: [
      "VITE_",
      "TAURI_PLATFORM",
      "TAURI_ARCH",
      "TAURI_FAMILY",
      "TAURI_PLATFORM_VERSION",
      "TAURI_PLATFORM_TYPE",
      "TAURI_DEBUG"
    ],
    build: {
      // Tauri uses Chromium on Windows and WebKit on macOS and Linux
      target: process.env.TAURI_PLATFORM == "windows" ? "chrome105" : "safari13",
      // // don't minify for debug builds
      minify: !process.env.TAURI_DEBUG ? ("esbuild" as const) : false,
      // // produce sourcemaps for debug builds
      sourcemap: !!process.env.TAURI_DEBUG
    }
  };

  return config;
});
