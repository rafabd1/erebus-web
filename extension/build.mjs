import { build } from "esbuild";
import { cp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import JSZip from "jszip";

await rm("dist", { recursive: true, force: true });
await mkdir("dist", { recursive: true });

await build({
  entryPoints: {
    background: "src/background.ts",
    offscreen: "src/offscreen.ts",
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
  cp("src/offscreen.html", "dist/offscreen.html"),
  cp("src/popup.html", "dist/popup.html"),
  cp("src/popup.css", "dist/popup.css")
]);

await mkdir("dist/firefox", { recursive: true });
await build({
  entryPoints: {
    "firefox-background": "src/firefox-background.ts",
    "firefox-content": "src/firefox-content.ts"
  },
  outdir: "dist/firefox",
  bundle: true,
  format: "iife",
  target: "firefox121",
  sourcemap: true,
  minify: false
});
await Promise.all([
  cp("manifest.firefox.json", "dist/firefox/manifest.json"),
  cp("dist/popup.js", "dist/firefox/popup.js"),
  cp("dist/popup.js.map", "dist/firefox/popup.js.map"),
  cp("src/popup.html", "dist/firefox/popup.html"),
  cp("src/popup.css", "dist/firefox/popup.css")
]);

await Promise.all([
  archiveDirectory(
    "dist",
    "dist/erebus-chromium-extension.zip",
    (relative) => !relative.startsWith("firefox/") && !relative.endsWith(".zip") && !relative.endsWith(".xpi")
  ),
  archiveDirectory("dist/firefox", "dist/erebus-firefox-extension.xpi")
]);

await writeFile(
  "dist/INSTALL.txt",
  [
    "Chromium (Chrome, Edge, Brave)",
    "1. Open the browser extension page and enable Developer mode.",
    "2. Extract erebus-chromium-extension.zip, choose Load unpacked, and select the extracted folder.",
    "3. In Erebus, create a Chromium extension pairing and import it from the extension popup.",
    "",
    "Firefox",
    "1. Open about:debugging#/runtime/this-firefox.",
    "2. Choose Load Temporary Add-on and select firefox/manifest.json or erebus-firefox-extension.xpi.",
    "3. Import the Erebus capture CA in Firefox Settings > Privacy & Security > Certificates > Authorities.",
    "4. In Erebus, create a Firefox extension pairing and import it from the extension popup."
  ].join("\n") + "\n"
);

async function archiveDirectory(sourceDirectory, outputFile, include = () => true) {
  const zip = new JSZip();
  await addDirectory(zip, sourceDirectory, "", include);
  const data = await zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE", compressionOptions: { level: 9 } });
  await writeFile(outputFile, data);
}

async function addDirectory(zip, sourceDirectory, relativeDirectory, include) {
  const directory = path.join(sourceDirectory, relativeDirectory);
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const relative = path.posix.join(relativeDirectory.replaceAll("\\", "/"), entry.name);
    if (!include(relative)) continue;
    if (entry.isDirectory()) {
      await addDirectory(zip, sourceDirectory, relative, include);
    } else if (entry.isFile()) {
      zip.file(relative, await readFile(path.join(sourceDirectory, relative)));
    }
  }
}

