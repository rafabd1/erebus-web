import type {
  ExtensionAutomationCommand,
  ExtensionAutomationResult,
  ExtensionAutomationTab,
  ExtensionState,
  PopupRequest,
  PopupResponse,
  RuntimeConfig,
  TabCaptureState
} from "./protocol";
import { configFromBootstrap, discoverErebus } from "./bootstrap";

const browserApi = (globalThis as typeof globalThis & { browser?: any }).browser;
const CONFIG_KEY = "runtimeConfig";
const TABS_KEY = "captureTabs";
const INSTALL_ID_KEY = "installId";
const AUTOMATION_POLL_MS = 20_000;
const AUTOMATION_RETRY_MS = 1_500;
const AUTOMATIC_PAIRING_RETRY_MS = 5_000;
const PRESET_VIEWPORTS: Record<string, { width: number; height: number }> = {
  "iphone-se": { width: 375, height: 667 },
  "iphone-14-pro-max": { width: 430, height: 932 },
  "iphone-15-pro": { width: 393, height: 852 },
  "pixel-7": { width: 412, height: 915 },
  "ipad-mini": { width: 768, height: 1024 },
  "ipad-pro-11": { width: 834, height: 1194 },
  "desktop-1280": { width: 1280, height: 720 },
  "desktop-1440": { width: 1440, height: 900 }
};

let tabStates = new Map<number, TabCaptureState>();
let automationConnected = false;
let automationGeneration = 0;
let automationController: AbortController | undefined;
let automaticPairingAttemptAt = 0;

void initialize();
browserApi.runtime.onStartup.addListener(() => void initialize());
browserApi.runtime.onInstalled.addListener(() => void initialize());
browserApi.tabs.onRemoved.addListener((tabId: number) => {
  if (!tabStates.delete(tabId)) return;
  void persistTabs();
});
browserApi.tabs.onUpdated.addListener((tabId: number, change: Record<string, unknown>, tab: any) => {
  const state = tabStates.get(tabId);
  if (!state) return;
  tabStates.set(tabId, {
    ...state,
    ...(typeof tab.title === "string" ? { title: tab.title } : {}),
    ...(typeof tab.url === "string" ? { url: tab.url } : {}),
    status: change.status === "loading" ? "attaching" : "capturing"
  });
  void persistTabs();
});
browserApi.runtime.onMessage.addListener((message: PopupRequest) => handlePopupMessage(message));

browserApi.proxy.onRequest.addListener(
  async (details: { tabId?: number; url?: string }) => {
    if (details.tabId === undefined || !tabStates.has(details.tabId)) return { type: "direct" };
    const config = await loadConfig();
    if (!config?.proxyUrl) return { type: "direct" };
    const proxy = parseProxy(config.proxyUrl);
    return {
      type: proxy.protocol === "https:" ? "https" : "http",
      host: proxy.hostname,
      port: Number(proxy.port),
      proxyDNS: true,
      failoverTimeout: 5
    };
  },
  { urls: ["<all_urls>"] }
);

async function initialize(): Promise<void> {
  const saved = await browserApi.storage.local.get([TABS_KEY]);
  const tabs = Array.isArray(saved[TABS_KEY]) ? (saved[TABS_KEY] as TabCaptureState[]) : [];
  tabStates = new Map(tabs.map((state) => [state.tabId, state]));
  await ensureAutomaticPairing();
  for (const state of tabs) {
    await setIndicator(state.tabId, state.status === "error" ? "!" : "LIVE");
  }
  restartAutomationLoop();
}

async function handlePopupMessage(message: PopupRequest): Promise<PopupResponse> {
  try {
    switch (message.type) {
      case "getState": {
        const config = await loadConfig();
        return {
          ok: true,
          state: currentState(config),
          ...(config ? { config: configForPopup(config) } : {})
        };
      }
      case "saveConfig": {
        const previous = await loadConfig();
        const serverUrl = normalizeServerUrl(message.serverUrl);
        const token = message.token.trim();
        const sessionId = message.sessionId.trim();
        const proxyUrl = message.proxyUrl?.trim();
        const caPem = message.caPem?.trim();
        if (!token || !sessionId) throw new Error("token and capture session are required");
        if (tabStates.size > 0 && previous?.sessionId !== sessionId) {
          throw new Error("stop controlled tabs before changing capture session");
        }
        if (proxyUrl) parseProxy(proxyUrl);
        const sameSession = previous?.sessionId === sessionId;
        const next: RuntimeConfig = {
          serverUrl,
          token,
          sessionId,
          producerId: sameSession ? previous.producerId : crypto.randomUUID(),
          nextSequence: sameSession ? previous.nextSequence : 1,
          ...(proxyUrl ? { proxyUrl } : {}),
          ...(caPem ? { caPem } : {})
        };
        await browserApi.storage.local.set({ [CONFIG_KEY]: next });
        restartAutomationLoop();
        return { ok: true, state: currentState(next) };
      }
      case "startCurrentTab": {
        const config = await requireConfig();
        if (!config.proxyUrl) {
          throw new Error("Firefox capture needs a proxy pairing record from an active Erebus session");
        }
        const tab = await activeTab();
        await controlTab(tab);
        return { ok: true, state: currentState(config) };
      }
      case "stopCurrentTab": {
        const tab = await activeTab();
        if (typeof tab.id === "number") await releaseTab(tab.id);
        return { ok: true, state: currentState(await loadConfig()) };
      }
      case "reconnect": {
        await ensureAutomaticPairing(true);
        restartAutomationLoop();
        return { ok: true, state: currentState(await loadConfig()) };
      }
      case "flushNow":
      case "discardQueue":
        return { ok: true, state: currentState(await loadConfig()) };
    }
  } catch (cause) {
    return {
      ok: false,
      error: errorMessage(cause),
      state: currentState(await loadConfig())
    };
  }
}

function currentState(config: RuntimeConfig | undefined): ExtensionState {
  return {
    configured: config !== undefined,
    tabs: [...tabStates.values()].sort((a, b) => a.tabId - b.tabId),
    queued: 0,
    deadLetters: 0,
    pairingLocked: tabStates.size > 0,
    automationConnected
  };
}

function configForPopup(config: RuntimeConfig): Omit<RuntimeConfig, "nextSequence"> {
  const { nextSequence: _nextSequence, ...value } = config;
  return value;
}

async function controlTab(tab: any): Promise<void> {
  if (
    typeof tab.id !== "number" ||
    (!/^https?:/i.test(tab.url ?? "") && tab.url !== "about:blank")
  ) {
    throw new Error("select an HTTP, HTTPS, or blank tab");
  }
  tabStates.set(tab.id, {
    tabId: tab.id,
    ...(tab.title ? { title: tab.title } : {}),
    ...(tab.url ? { url: tab.url } : {}),
    status: "capturing",
    captured: 0,
    gaps: [],
    startedAt: new Date().toISOString()
  });
  await persistTabs();
  await setIndicator(tab.id, "LIVE");
}

async function releaseTab(tabId: number): Promise<void> {
  tabStates.delete(tabId);
  await persistTabs();
  await browserApi.browserAction.setBadgeText({ tabId, text: "" }).catch(() => undefined);
  await browserApi.browserAction
    .setTitle({ tabId, title: "Erebus Browser Control" })
    .catch(() => undefined);
}

async function setIndicator(tabId: number, text: string): Promise<void> {
  await browserApi.browserAction.setBadgeBackgroundColor({ tabId, color: "#1f8f5f" });
  await browserApi.browserAction.setBadgeText({ tabId, text });
  await browserApi.browserAction.setTitle({
    tabId,
    title: "Erebus is controlling this tab and routing its traffic through the capture proxy"
  });
}

function restartAutomationLoop(): void {
  automationGeneration += 1;
  automationConnected = false;
  automationController?.abort();
  automationController = undefined;
  void ensureAutomationLoop();
}

async function ensureAutomationLoop(): Promise<void> {
  if (automationController) return;
  const generation = ++automationGeneration;
  const controller = new AbortController();
  automationController = controller;
  try {
    while (!controller.signal.aborted && automationGeneration === generation) {
      const config = await loadConfig();
      if (!config) return;
      try {
        await pairingRequest(
          config,
          "/v1/extension/heartbeat",
          {
            sessionId: config.sessionId,
            producerId: config.producerId,
            browser: "firefox",
            version: browserApi.runtime.getManifest().version,
            tabs: await automationTabs()
          },
          controller.signal
        );
        automationConnected = true;
        const command = await pairingRequest<ExtensionAutomationCommand | null>(
          config,
          "/v1/extension/commands/poll",
          {
            sessionId: config.sessionId,
            producerId: config.producerId,
            waitMs: AUTOMATION_POLL_MS
          },
          controller.signal
        );
        if (!command) continue;
        const result = await executeAutomation(command);
        await pairingRequest(
          config,
          "/v1/extension/commands/result",
          { sessionId: config.sessionId, producerId: config.producerId, ...result },
          controller.signal
        );
      } catch {
        if (controller.signal.aborted || automationGeneration !== generation) return;
        automationConnected = false;
        await ensureAutomaticPairing(true).catch(() => undefined);
        await delay(AUTOMATION_RETRY_MS);
      }
    }
  } finally {
    if (automationGeneration === generation) {
      automationController = undefined;
      automationConnected = false;
    }
  }
}

async function automationTabs(): Promise<ExtensionAutomationTab[]> {
  const tabs = await browserApi.tabs.query({});
  return tabs.flatMap((tab: any) =>
    !tab || typeof tab.id !== "number" || typeof tab.windowId !== "number"
      ? []
      : [
          {
            tabId: tab.id,
            windowId: tab.windowId,
            active: tab.active === true,
            attached:
              tabStates.get(tab.id)?.status === "capturing" ||
              tabStates.get(tab.id)?.status === "attaching",
            ...(tab.title ? { title: tab.title } : {}),
            ...(tab.url ? { url: tab.url } : {}),
            ...(tab.status ? { status: tab.status } : {})
          }
        ]
  );
}

async function ensureAutomaticPairing(force = false): Promise<RuntimeConfig | undefined> {
  const previous = await loadConfig();
  if (previous && !force) return previous;
  const now = Date.now();
  if (now - automaticPairingAttemptAt < AUTOMATIC_PAIRING_RETRY_MS) return previous;
  automaticPairingAttemptAt = now;
  const installId = await loadOrCreateInstallId();
  const record = await discoverErebus(
    installId,
    "firefox",
    browserApi.runtime.getManifest().version
  );
  if (!record) return previous;
  const next = configFromBootstrap(record, installId, previous);
  await browserApi.storage.local.set({ [CONFIG_KEY]: next });
  return next;
}

async function loadOrCreateInstallId(): Promise<string> {
  const saved = await browserApi.storage.local.get([INSTALL_ID_KEY]);
  const existing = saved[INSTALL_ID_KEY];
  if (typeof existing === "string" && existing.length >= 8) return existing;
  const installId = crypto.randomUUID();
  await browserApi.storage.local.set({ [INSTALL_ID_KEY]: installId });
  return installId;
}

async function executeAutomation(command: ExtensionAutomationCommand): Promise<ExtensionAutomationResult> {
  try {
    return { commandId: command.id, ok: true, result: await execute(command) };
  } catch (cause) {
    return { commandId: command.id, ok: false, error: automationError(cause) };
  }
}

async function execute(command: ExtensionAutomationCommand): Promise<unknown> {
  const input = command.input ?? {};
  if (command.operation === "tabsList") return { tabs: await listAutomationTabs() };
  if (command.operation === "open") {
    const url = optionalString(input.url) ?? "about:blank";
    const reuse = input.reuseExistingTab !== false && command.tabId !== undefined;
    const tab = reuse
      ? await browserApi.tabs.update(command.tabId, { url, active: input.open !== false })
      : await browserApi.tabs.create({ url, active: input.open !== false });
    await controlTab(tab);
    await waitForLoad(tab.id, command.timeoutMs);
    return status(tab.id);
  }
  const tabId = requiredTabId(command);
  await ensureControlled(tabId);
  switch (command.operation) {
    case "status":
      return status(tabId);
    case "tabClose":
      await browserApi.tabs.remove(tabId);
      return { closedTabId: tabId, tabId: null };
    case "navigate":
      await browserApi.tabs.update(tabId, { url: requiredString(input.url, "url") });
      await waitForLoad(tabId, optionalNumber(input.timeoutMs) ?? command.timeoutMs);
      return status(tabId);
    case "back":
      await browserApi.tabs.goBack(tabId);
      await waitForLoad(tabId, command.timeoutMs);
      return status(tabId);
    case "forward":
      await browserApi.tabs.goForward(tabId);
      await waitForLoad(tabId, command.timeoutMs);
      return status(tabId);
    case "reload":
      await browserApi.tabs.reload(tabId);
      await waitForLoad(tabId, command.timeoutMs);
      return status(tabId);
    case "resize":
      return resize(tabId, input);
    case "snapshot":
      return snapshot(tabId);
    case "click":
    case "type":
    case "press":
    case "scroll":
      await contentCommand(tabId, command.operation, input);
      return undefined;
    case "evaluate":
      return evaluate(tabId, input);
    case "waitFor":
      await waitForConditions(tabId, input, command.timeoutMs);
      return undefined;
    case "setColorScheme":
      throw unsupported("Firefox does not expose per-tab color-scheme emulation to extensions.");
    case "recordingStart":
    case "recordingStop":
      throw unsupported("Recording is available in the Erebus browser, not in the Firefox extension.");
    default:
      throw unsupported(`The Firefox extension does not support ${command.operation}.`);
  }
}

async function listAutomationTabs(): Promise<Record<string, unknown>[]> {
  const tabs = await automationTabs();
  return tabs.map((tab) => ({
    tabId: tab.tabId,
    url: tab.url ?? null,
    title: tab.title ?? null,
    active: tab.active,
    loading: tab.status === "loading"
  }));
}

async function ensureControlled(tabId: number): Promise<void> {
  if (tabStates.has(tabId)) return;
  await controlTab(await browserApi.tabs.get(tabId));
}

async function status(tabId: number): Promise<Record<string, unknown>> {
  const tab = await browserApi.tabs.get(tabId);
  const page = objectValue(await contentCommand(tabId, "status", {}).catch(() => undefined));
  const window = await browserApi.windows.get(tab.windowId).catch(() => ({}));
  return {
    available: true,
    visible: tab.active === true,
    tabId,
    url: tab.url ?? page.url ?? null,
    title: tab.title ?? page.title ?? null,
    loading: tab.status === "loading" || page.loading === true,
    viewportSetting: { mode: "fill" },
    viewport: {
      width: finiteInteger(page.width ?? window.width, 1),
      height: finiteInteger(page.height ?? window.height, 1)
    }
  };
}

async function resize(tabId: number, input: Record<string, unknown>): Promise<Record<string, unknown>> {
  const tab = await browserApi.tabs.get(tabId);
  const mode = requiredString(input.mode, "mode");
  if (mode === "fill") {
    return { tabId, setting: { mode: "fill" }, viewport: (await status(tabId)).viewport };
  }
  let width = optionalNumber(input.width);
  let height = optionalNumber(input.height);
  if (mode === "preset") {
    const preset = PRESET_VIEWPORTS[requiredString(input.preset, "preset")];
    if (!preset) throw new Error("unknown viewport preset");
    width = preset.width;
    height = preset.height;
    if (input.orientation === "landscape") [width, height] = [height, width];
  }
  if (!width || !height) throw new Error("viewport width and height are required");
  await browserApi.windows.update(tab.windowId, { width: Math.round(width), height: Math.round(height) });
  const current = await status(tabId);
  return {
    tabId,
    setting:
      mode === "freeform"
        ? { mode, width: input.width, height: input.height }
        : { mode, preset: input.preset, ...(input.orientation ? { orientation: input.orientation } : {}) },
    viewport: current.viewport
  };
}

async function snapshot(tabId: number): Promise<Record<string, unknown>> {
  const tab = await browserApi.tabs.get(tabId);
  const payload = objectValue(await contentCommand(tabId, "snapshot", {}));
  const screenshotUrl = await browserApi.tabs.captureVisibleTab(tab.windowId, { format: "png" });
  const data = typeof screenshotUrl === "string" ? screenshotUrl.split(",", 2)[1] : undefined;
  return {
    ...payload,
    consoleEntries: [],
    networkEntries: [],
    actionTimeline: [],
    ...(data
      ? {
          screenshot: {
            mimeType: "image/png",
            data,
            width: finiteInteger(payload.width, 1),
            height: finiteInteger(payload.height, 1)
          }
        }
      : {})
  };
}

async function evaluate(tabId: number, input: Record<string, unknown>): Promise<unknown> {
  const expression = requiredString(input.expression, "expression");
  const results = await browserApi.tabs.executeScript(tabId, {
    code: `Promise.resolve((${expression})).then((value) => value)`
  });
  return Array.isArray(results) ? results[0] : results;
}

async function waitForConditions(
  tabId: number,
  input: Record<string, unknown>,
  defaultTimeoutMs: number
): Promise<void> {
  const timeoutMs = optionalNumber(input.timeoutMs) ?? defaultTimeoutMs;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const probe = objectValue(await contentCommand(tabId, "waitProbe", input));
    if (probe.element === true && probe.text === true && probe.url === true) return;
    await delay(100);
  }
  throw taggedError("PreviewAutomationTimeoutError", "The browser condition did not match before timeout.");
}

async function contentCommand(
  tabId: number,
  operation: string,
  input: Record<string, unknown>
): Promise<unknown> {
  try {
    return await browserApi.tabs.sendMessage(tabId, { type: "erebusAutomation", operation, input });
  } catch (cause) {
    throw taggedError(
      "PreviewAutomationExecutionError",
      `Firefox could not access this page: ${errorMessage(cause)}`
    );
  }
}

async function waitForLoad(tabId: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const tab = await browserApi.tabs.get(tabId);
    if (tab.status !== "loading") return;
    await delay(100);
  }
  throw taggedError("PreviewAutomationTimeoutError", "Navigation did not become ready before timeout.");
}

async function pairingRequest<T = unknown>(
  config: RuntimeConfig,
  path: string,
  payload: unknown,
  signal: AbortSignal
): Promise<T> {
  const response = await fetch(`${config.serverUrl}${path}`, {
    method: "POST",
    headers: { authorization: `Bearer ${config.token}`, "content-type": "application/json" },
    body: JSON.stringify(payload),
    signal
  });
  if (!response.ok) throw new Error(`Erebus runtime returned HTTP ${response.status}`);
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

async function loadConfig(): Promise<RuntimeConfig | undefined> {
  const result = await browserApi.storage.local.get([CONFIG_KEY]);
  return result[CONFIG_KEY] as RuntimeConfig | undefined;
}

async function requireConfig(): Promise<RuntimeConfig> {
  const config = await loadConfig();
  if (!config) throw new Error("pair the extension with an Erebus capture session first");
  return config;
}

async function persistTabs(): Promise<void> {
  await browserApi.storage.local.set({ [TABS_KEY]: [...tabStates.values()] });
}

async function activeTab(): Promise<any> {
  const tabs = await browserApi.tabs.query({ active: true, currentWindow: true });
  const tab = tabs[0];
  if (!tab) throw new Error("no active browser tab");
  return tab;
}

function parseProxy(value: string): URL {
  const proxy = new URL(value);
  if (!/^https?:$/.test(proxy.protocol) || !proxy.hostname || !proxy.port) {
    throw new Error("Firefox capture proxy must be an HTTP or HTTPS URL with an explicit port");
  }
  return proxy;
}

function normalizeServerUrl(value: string): string {
  const parsed = new URL(value.trim());
  if (!/^https?:$/.test(parsed.protocol)) throw new Error("runtime URL must use HTTP or HTTPS");
  return parsed.href.replace(/\/$/, "");
}

function requiredTabId(command: ExtensionAutomationCommand): number {
  if (typeof command.tabId !== "number") throw new Error("tabId is required");
  return command.tabId;
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) throw new Error(`${name} is required`);
  return value;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function optionalNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function finiteInteger(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(1, Math.round(value)) : fallback;
}

function objectValue(value: unknown): Record<string, any> {
  return typeof value === "object" && value !== null ? (value as Record<string, any>) : {};
}

function unsupported(message: string): Error {
  return taggedError("PreviewAutomationUnsupportedClientError", message);
}

function taggedError(tag: string, message: string): Error {
  const error = new Error(message) as Error & { tag?: string };
  error.tag = tag;
  return error;
}

function automationError(cause: unknown): { tag: string; message: string; detail?: unknown } {
  if (cause instanceof Error) {
    const tagged = cause as Error & { tag?: string; detail?: unknown };
    return {
      tag: tagged.tag ?? "PreviewAutomationExecutionError",
      message: cause.message,
      ...(tagged.detail === undefined ? {} : { detail: tagged.detail })
    };
  }
  return { tag: "PreviewAutomationExecutionError", message: String(cause) };
}

function errorMessage(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
