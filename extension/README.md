# Erebus Web Capture extension

The extension controls and captures tabs selected by the user. Chromium uses the Chrome DevTools Protocol. Firefox uses its extension APIs. Both keep unsent events in IndexedDB and send idempotent HTTP and WebSocket batches to the local Erebus web runtime.

## Development

```powershell
pnpm install
pnpm typecheck
pnpm test
pnpm build
```

The build creates `dist/erebus-chromium-extension.zip` and `dist/erebus-firefox-extension.xpi`. Extract the Chromium ZIP before using **Load unpacked**. Firefox can load the XPI or `dist/firefox/manifest.json` temporarily. Start an extension capture in Erebus, then import its pairing record in the popup.

Opening DevTools on a captured tab may take ownership of the debugger connection. The extension records the resulting gap and shows `detached`; press **Start capture** again after closing DevTools.

The queue survives browser and service-worker restarts. Each item keeps its session and producer identity. The runtime address and token can be rebound for that session without rewriting the backlog. The popup locks the session while capture is active or data is queued. Send the backlog or use **Discard queued** before pairing another session. A capture event leaves the queue only after the runtime acknowledges its sequence as accepted or already present.

The extension caps the queue at 2,000 events or 256 MiB and keeps the newest 200 rejected events. If either limit drops data, the tab state records a capture gap and the runtime stores it with the session. Batches are also capped by encoded size so they stay below the runtime request limit.
