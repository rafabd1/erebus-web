import { build } from "esbuild";
import { cp, mkdir, rm } from "node:fs/promises";

await rm("dist", { recursive: true, force: true });
await mkdir("dist", { recursive: true });

await build({
  entryPoints: {
    background: "src/background.ts",
    popup: "src/popup.ts"
  },
  outdir: "dist",
  bundle: true,
  format: "esm",
  target: "chrome120",
  sourcemap: true,
  minify: false
});

await Promise.all([
  cp("manifest.json", "dist/manifest.json"),
  cp("src/popup.html", "dist/popup.html"),
  cp("src/popup.css", "dist/popup.css")
]);

