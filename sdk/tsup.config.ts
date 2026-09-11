import { defineConfig } from "tsup";

export default defineConfig({
  entry: { index: "src/index.ts" },
  format: ["esm", "iife"],
  globalName: "PulseRTC",
  dts: true,
  clean: true,
  minify: false,
  outExtension: ({ format }) => ({ js: format === "iife" ? ".global.js" : ".js" }),
});
