import type { ExtensionState, PopupRequest, PopupResponse } from "./protocol";

const connection = element("connection");
const tabStatus = element("tab-status");
const tabDetail = element("tab-detail");
const queued = element("queued");
const failed = element("failed");
const error = element("error");
let latestState: ExtensionState | undefined;
const buttons = {
  reconnect: button("reconnect"),
  flush: button("flush"),
  discard: button("discard")
};

buttons.reconnect.addEventListener("click", () => run({ type: "reconnect" }));
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
  connection.textContent = state.configured
    ? state.automationConnected
      ? "Paired · agent online"
      : "Paired · waiting"
    : "Not paired";
  connection.classList.toggle("connected", state.configured);
  queued.textContent = String(state.queued);
  failed.textContent = String(state.deadLetters);
  buttons.discard.disabled = state.queued === 0;
  const active = state.tabs.find((tab) => tab.status === "capturing") ?? state.tabs[0];
  if (!active) {
    tabStatus.textContent = "Idle";
    tabStatus.className = "status";
    tabDetail.textContent = "No tab is controlled by Erebus.";
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
    buttons.discard.disabled = latestState.queued === 0;
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

function button(id: string): HTMLButtonElement {
  return document.getElementById(id) as HTMLButtonElement;
}

function element(id: string): HTMLElement {
  return document.getElementById(id) as HTMLElement;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

