import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: Object.fromEntries(["/v1", "/api", "/livez", "/readyz"].map((path) => [path, { target: process.env.EVENTGLASS_WEB_API_ORIGIN ?? "http://127.0.0.1:18080" }])),
  },
  build: { outDir: "dist", sourcemap: true },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: "./src/test/setup.ts",
    restoreMocks: true,
  },
});
