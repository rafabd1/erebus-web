# Erebus Web Capture extension

The extension captures only tabs selected by the user. It attaches through the Chrome DevTools Protocol, keeps unsent events in IndexedDB, and sends idempotent HTTP and WebSocket batches to the local Erebus web runtime.

## Development

```powershell
pnpm install
pnpm typecheck
pnpm test
pnpm build
```

Load `dist/` as an unpacked extension in Chrome or Edge. Start the Erebus web runtime, create a capture session, then enter its local URL, token, and session ID in the popup.

Opening DevTools on a captured tab may take ownership of the debugger connection. The extension records the resulting gap and shows `detached`; press **Start capture** again after closing DevTools.

The queue survives browser and service-worker restarts. Each item keeps its runtime, session, and producer identity. The popup locks those fields while capture is active or data is queued; the token can still be replaced. Send the backlog or use **Discard queued** before pairing another session. A capture event leaves the queue only after the runtime acknowledges its sequence as accepted or already present.

The extension caps the queue at 2,000 events or 256 MiB and keeps the newest 200 rejected events. If either limit drops data, the tab state records a capture gap. Batches are also capped by encoded size so they stay below the runtime request limit.
