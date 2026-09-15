#!/usr/bin/env node
// Rebuild codemirror.bundle.js from source.
// Run: npm install && node build.mjs
// The resulting codemirror.bundle.js is committed to the repo.
// This script is NOT executed at Go build time.

import { build } from "esbuild";

await build({
  entryPoints: ["codemirror_entry.mjs"],
  bundle: true,
  minify: true,
  format: "iife",
  globalName: "CodeMirrorBundle",
  outfile: "codemirror.bundle.js",
  platform: "browser",
  target: ["es2020"],
});

console.log("codemirror.bundle.js built successfully");
