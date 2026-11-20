import type { ExtensionState, PopupRequest, PopupResponse } from "./protocol";

const serverUrl = input("server-url");
const token = input("token");
const sessionId = input("session-id");
const connection = element("connection");
const tabStatus = element("tab-status");
const tabDetail = element("tab-detail");
const queued = element("queued");
const failed = element("failed");
const error = element("error");
const pairingLock = element("pairing-lock");
let latestState: ExtensionState | undefined;
const buttons = {
  save: button("save"),
  start: button("start"),
  stop: button("stop"),
  flush: button("flush"),
  discard: button("discard")
};

buttons.save.addEventListener("click", () =>
  run({
    type: "saveConfig",
    serverUrl: serverUrl.value,
    token: token.value,
    sessionId: sessionId.value
  })
);
buttons.start.addEventListener("click", () => run({ type: "startCurrentTab" }));
buttons.stop.addEventListener("click", () => run({ type: "stopCurrentTab" }));
buttons.flush.addEventListener("click", () => run({ type: "flushNow" }));
buttons.discard.addEventListener("click", () => {
  if (window.confirm("Discard every queued capture event? This cannot be undone.")) {
    void run({ type: "discardQueue" });
  }
});

void load();

async function load(): Promise<void> {
  setBusy(true);
  try {
    const response = await send({ type: "getState" });
    if (!response.ok) throw new Error(response.error);
    if (response.config) {
      serverUrl.value = response.config.serverUrl;
      token.value = response.config.token;
      sessionId.value = response.config.sessionId;
    }
    render(response.state);
  } catch (cause) {
    showError(message(cause));
  } finally {
    setBusy(false);
  }
}

async function run(request: PopupRequest): Promise<void> {
  setBusy(true);
  hideError();
  try {
    const response = await send(request);
    if (!response.ok) throw new Error(response.error);
    render(response.state);
  } catch (cause) {
    showError(message(cause));
  } finally {
    setBusy(false);
  }
}

function render(state: ExtensionState): void {
  latestState = state;
  connection.textContent = state.configured ? "Paired" : "Not paired";
  connection.classList.toggle("connected", state.configured);
  queued.textContent = String(state.queued);
  failed.textContent = String(state.deadLetters);
  buttons.discard.disabled = state.queued === 0;
  serverUrl.disabled = state.pairingLocked;
  sessionId.disabled = state.pairingLocked;
  pairingLock.hidden = !state.pairingLocked;
  pairingLock.textContent = state.pairingLocked
    ? "Runtime and session stay locked while capture is active or events are queued. The token can still be updated."
    : "";

  const active = state.tabs.find((tab) => tab.status === "capturing") ?? state.tabs[0];
  if (!active) {
    tabStatus.textContent = "Idle";
    tabStatus.className = "status";
    tabDetail.textContent = "No selected tab is being captured.";
    buttons.stop.disabled = true;
  } else {
    tabStatus.textContent = active.status;
    tabStatus.className = `status ${active.status}`;
    tabDetail.textContent = [
      active.title ?? active.url ?? `Tab ${active.tabId}`,
      `${active.captured} HTTP exchanges`,
      active.gaps.length > 0 ? `${active.gaps.length} capture gap(s)` : undefined,
      active.lastError
    ]
      .filter(Boolean)
      .join(" · ");
    buttons.stop.disabled = false;
  }

  if (state.lastFlushError) showError(state.lastFlushError);
}

async function send(request: PopupRequest): Promise<PopupResponse> {
  return chrome.runtime.sendMessage(request) as Promise<PopupResponse>;
}

function setBusy(busy: boolean): void {
  if (busy) {
    for (const control of Object.values(buttons)) control.disabled = true;
    return;
  }
  for (const control of Object.values(buttons)) control.disabled = false;
  if (latestState) {
    const active = latestState.tabs.find((tab) => tab.status === "capturing") ?? latestState.tabs[0];
    buttons.stop.disabled = !active;
    buttons.discard.disabled = latestState.queued === 0;
    serverUrl.disabled = latestState.pairingLocked;
    sessionId.disabled = latestState.pairingLocked;
  }
}

function showError(value: string): void {
  error.textContent = value;
  error.hidden = false;
}

function hideError(): void {
  error.hidden = true;
  error.textContent = "";
}

function input(id: string): HTMLInputElement {
  return document.getElementById(id) as HTMLInputElement;
}

function button(id: string): HTMLButtonElement {
  return document.getElementById(id) as HTMLButtonElement;
}

function element(id: string): HTMLElement {
  return document.getElementById(id) as HTMLElement;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

