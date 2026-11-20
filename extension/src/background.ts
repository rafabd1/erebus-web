import { bodyFromCdp, normalizeHeaders, unavailableBody } from "./normalization";
import { applyExtraInfo } from "./extra-info";
import type {
  CapturedBody,
  CapturedResponse,
  ExtensionState,
  HttpCaptureEvent,
  IngestResult,
  PopupRequest,
  PopupResponse,
  QueueItem,
  QueueKind,
  QueueTarget,
  RuntimeConfig,
  TabCaptureState,
  WebSocketFrameEvent
} from "./protocol";
import { pairingChangeAllowed, queueTarget, sameTarget, selectBatch } from "./queue-policy";
import { classifyAcknowledgements, retryDelay } from "./retry";
import {
  acknowledge,
  clearQueue,
  dueItems,
  enqueue,
  loadConfig,
  loadLastFlushError,
  loadTabStates,
  pruneStorage,
  queueUsage,
  queueCounts,
  saveConfig,
  saveLastFlushError,
  saveTabStates
} from "./storage";

const DEBUGGER_VERSION = "1.3";
const FLUSH_ALARM = "erebus-web-flush";
const FLUSH_SOON_ALARM = "erebus-web-flush-soon";
const FLUSH_BUDGET_MS = 2_000;
const DUE_CANDIDATES = 100;
const MAX_QUEUE_ITEMS = 2_000;
const MAX_QUEUE_BYTES = 256 * 1024 * 1024;
const MAX_DEAD_LETTERS = 200;
const MAX_GAPS_PER_TAB = 20;
const EXTRA_INFO_GRACE_MS = 35;

interface PendingRequest {
  requestId: string;
  redirectIndex: number;
  startedAt: number;
  wallTime?: number;
  method: string;
  url: string;
  headers: unknown;
  postData?: string;
  initiator?: string;
  resourceType?: string;
  response?: Record<string, unknown>;
  extraRequest?: Record<string, unknown>;
  extraResponse?: Record<string, unknown>;
  expectsResponseExtra?: boolean;
  redirect: boolean;
  completedAt?: number;
  error?: string;
}

interface PendingRequestChain {
  stages: PendingRequest[];
  requestExtras: Record<string, unknown>[];
  responseExtras: Record<string, unknown>[];
  finalizing: boolean;
}

interface PendingSocket {
  connectionId: string;
  url: string;
  initiator?: string;
  opened: boolean;
}

const pendingRequests = new Map<number, Map<string, PendingRequestChain>>();
const pendingSockets = new Map<number, Map<string, PendingSocket>>();
const recentlyFinalizedRequests = new Set<string>();
let tabStates = new Map<number, TabCaptureState>();
let sequenceChain = Promise.resolve(0);
let enqueueChain = Promise.resolve();
let flushing = false;
let intentionalDetaches = new Set<number>();
let queuedItemsEstimate = 0;
let queuedBytesEstimate = 0;
let queueUsageReady = false;

void initialize();

chrome.runtime.onInstalled.addListener(() => void initialize());
chrome.runtime.onStartup.addListener(() => void initialize());
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === FLUSH_ALARM || alarm.name === FLUSH_SOON_ALARM) void flushAll();
});

chrome.debugger.onEvent.addListener((source, method, params) => {
  if (source.tabId === undefined || !tabStates.has(source.tabId)) return;
  void handleDebuggerEvent(source.tabId, method, params as Record<string, unknown>).catch((error) =>
    markTabError(source.tabId!, errorMessage(error))
  );
});

chrome.debugger.onDetach.addListener((source, reason) => {
  if (source.tabId === undefined || !tabStates.has(source.tabId)) return;
  if (intentionalDetaches.delete(source.tabId)) return;
  void markUnexpectedDetach(source.tabId, reason);
});

chrome.tabs.onRemoved.addListener((tabId) => {
  if (!tabStates.has(tabId)) return;
  void closeRemovedTab(tabId);
});

chrome.runtime.onMessage.addListener((message: PopupRequest, _sender, sendResponse) => {
  void handlePopupMessage(message)
    .then(sendResponse)
    .catch(async (error) => {
      sendResponse({ ok: false, error: errorMessage(error), state: await currentState() } satisfies PopupResponse);
    });
  return true;
});

async function initialize(): Promise<void> {
  const [savedTabs] = await Promise.all([loadTabStates(), chrome.alarms.clear(FLUSH_ALARM)]);
  tabStates = new Map(savedTabs.map((state) => [state.tabId, state]));
  await refreshQueueUsage();
  await chrome.alarms.create(FLUSH_ALARM, { periodInMinutes: 0.5 });
  for (const state of savedTabs) {
    if (state.status === "capturing" || state.status === "attaching") {
      appendGap(state.tabId, "extension service worker restarted; in-flight requests may be incomplete");
      await attachTab(state.tabId, true).catch((error) => markTabError(state.tabId, errorMessage(error)));
    }
  }
  await flushAll();
}

async function handlePopupMessage(message: PopupRequest): Promise<PopupResponse> {
  switch (message.type) {
    case "getState": {
      const config = await loadConfig();
      return {
        ok: true,
        state: await currentState(),
        ...(config
          ? {
              config: {
                serverUrl: config.serverUrl,
                token: config.token,
                sessionId: config.sessionId,
                producerId: config.producerId
              }
            }
          : {})
      };
    }
    case "saveConfig": {
      const current = await loadConfig();
      const serverUrl = normalizeServerUrl(message.serverUrl);
      const token = message.token.trim();
      const sessionId = message.sessionId.trim();
      if (!token || !sessionId) throw new Error("token and capture session are required");
      const counts = await queueCounts();
      const locked = counts.queued > 0 || hasActiveCapture();
      if (!pairingChangeAllowed(current, { serverUrl, sessionId }, locked)) {
        throw new Error(
          counts.queued > 0
            ? `send or clear the ${counts.queued} queued capture event(s) before changing runtime or session`
            : "stop active tab capture before changing runtime or session"
        );
      }
      const destinationChanged =
        !current || current.serverUrl !== serverUrl || current.sessionId !== sessionId;
      await saveConfig({
        serverUrl,
        token,
        sessionId,
        producerId: destinationChanged ? crypto.randomUUID() : current.producerId,
        nextSequence: destinationChanged ? 1 : current.nextSequence
      });
      await saveLastFlushError();
      await flushAll();
      return { ok: true, state: await currentState() };
    }
    case "startCurrentTab": {
      const tab = await activeTab();
      await attachTab(tab.id!, false);
      return { ok: true, state: await currentState() };
    }
    case "stopCurrentTab": {
      const tab = await activeTab();
      await stopTab(tab.id!);
      return { ok: true, state: await currentState() };
    }
    case "flushNow": {
      await flushAll();
      return { ok: true, state: await currentState() };
    }
    case "discardQueue": {
      if (flushing) throw new Error("wait for the current send attempt to finish");
      await enqueueChain;
      const removed = await clearQueue();
      noteQueueRemoval(removed);
      await recordDiscardedItems(removed, "queued capture discarded by user");
      await saveLastFlushError();
      return { ok: true, state: await currentState() };
    }
  }
}

async function attachTab(tabId: number, restoring: boolean): Promise<void> {
  const config = await requireConfig();
  void config;
  const tab = await chrome.tabs.get(tabId);
  if (!tab.url || !/^https?:/i.test(tab.url)) throw new Error("select an HTTP or HTTPS tab");

  const previous = tabStates.get(tabId);
  tabStates.set(tabId, {
    tabId,
    ...(tab.title ? { title: tab.title } : {}),
    ...(tab.url ? { url: tab.url } : {}),
    status: "attaching",
    captured: previous?.captured ?? 0,
    gaps: previous?.gaps ?? [],
    startedAt: previous?.startedAt ?? new Date().toISOString()
  });
  await persistTabs();

  try {
    await chrome.debugger.attach({ tabId }, DEBUGGER_VERSION);
  } catch (error) {
    const message = errorMessage(error);
    if (!restoring || !message.toLowerCase().includes("already attached")) throw error;
  }
  await sendCommand(tabId, "Network.enable", {
    maxPostDataSize: 2 * 1024 * 1024,
    maxResourceBufferSize: 2 * 1024 * 1024,
    maxTotalBufferSize: 16 * 1024 * 1024
  });

  pendingRequests.set(tabId, new Map());
  pendingSockets.set(tabId, new Map());
  const { lastError: _lastError, ...activeState } = tabStates.get(tabId)!;
  tabStates.set(tabId, { ...activeState, status: "capturing" });
  await persistTabs();
  await chrome.action.setBadgeBackgroundColor({ tabId, color: "#1f8f5f" });
  await chrome.action.setBadgeText({ tabId, text: "REC" });
}

async function stopTab(tabId: number): Promise<void> {
  const state = tabStates.get(tabId);
  if (state) {
    const lost = inFlightCount(tabId);
    tabStates.set(tabId, {
      ...state,
      status: "detached",
      ...(lost > 0
        ? {
            gaps: [
              ...state.gaps,
              { at: new Date().toISOString(), reason: `capture stopped with ${lost} in-flight item(s)` }
            ].slice(-MAX_GAPS_PER_TAB)
          }
        : {})
    });
  }
  pendingRequests.delete(tabId);
  pendingSockets.delete(tabId);
  await persistTabs();
  try {
    intentionalDetaches.add(tabId);
    await chrome.debugger.detach({ tabId });
    setTimeout(() => intentionalDetaches.delete(tabId), 2_000);
  } catch {
    intentionalDetaches.delete(tabId);
    // The tab may already be closed or detached by DevTools.
  }
  await chrome.action.setBadgeText({ tabId, text: "" }).catch(() => undefined);
}

async function handleDebuggerEvent(
  tabId: number,
  method: string,
  params: Record<string, unknown>
): Promise<void> {
  switch (method) {
    case "Network.requestWillBeSent":
      await onRequestWillBeSent(tabId, params);
      break;
    case "Network.requestWillBeSentExtraInfo":
      onRequestExtraInfo(tabId, params);
      break;
    case "Network.responseReceived":
      onResponseReceived(tabId, params);
      break;
    case "Network.responseReceivedExtraInfo":
      onResponseExtraInfo(tabId, params);
      break;
    case "Network.loadingFinished":
      await onLoadingFinished(tabId, params);
      break;
    case "Network.loadingFailed":
      await onLoadingFailed(tabId, params);
      break;
    case "Network.webSocketCreated":
      await onWebSocketCreated(tabId, params);
      break;
    case "Network.webSocketHandshakeResponseReceived":
      await onWebSocketHandshake(tabId, params);
      break;
    case "Network.webSocketFrameSent":
      await onWebSocketFrame(tabId, params, "client_to_server");
      break;
    case "Network.webSocketFrameReceived":
      await onWebSocketFrame(tabId, params, "server_to_client");
      break;
    case "Network.webSocketClosed":
      await onWebSocketClosed(tabId, params);
      break;
    case "Network.webSocketFrameError":
      await onWebSocketError(tabId, params);
      break;
  }
}

async function onRequestWillBeSent(tabId: number, params: Record<string, unknown>): Promise<void> {
  const requestId = stringField(params, "requestId");
  const request = objectField(params, "request");
  if (!requestId || !request) return;
  recentlyFinalizedRequests.delete(requestKey(tabId, requestId));
  const chain = ensureRequestChain(tabId, requestId);
  const previous = chain.stages.at(-1);
  const redirect = objectField(params, "redirectResponse");
  const wallTime = numberField(params, "wallTime");
  if (previous && redirect) {
    previous.response = redirect;
    previous.redirect = true;
    const completedAt = numberField(params, "timestamp");
    if (completedAt !== undefined) previous.completedAt = completedAt;
    previous.expectsResponseExtra = params.redirectHasExtraInfo === true;
  }
  chain.stages.push({
    requestId,
    redirectIndex: chain.stages.length,
    startedAt: numberField(params, "timestamp") ?? performance.now() / 1000,
    ...(wallTime !== undefined ? { wallTime } : {}),
    method: String(request.method ?? "GET"),
    url: String(request.url ?? ""),
    headers: request.headers ?? {},
    ...(typeof request.postData === "string" ? { postData: request.postData } : {}),
    ...(objectField(params, "initiator") ? { initiator: JSON.stringify(params.initiator) } : {}),
    ...(typeof params.type === "string" ? { resourceType: params.type } : {}),
    redirect: false
  });
  applyChainExtraInfo(chain);
}

function onRequestExtraInfo(tabId: number, params: Record<string, unknown>): void {
  const requestId = stringField(params, "requestId");
  if (!requestId) return;
  if (recentlyFinalizedRequests.has(requestKey(tabId, requestId))) return;
  const chain = ensureRequestChain(tabId, requestId);
  chain.requestExtras.push(params);
  applyChainExtraInfo(chain);
}

function onResponseReceived(tabId: number, params: Record<string, unknown>): void {
  const request = lookupRequest(tabId, stringField(params, "requestId"));
  const response = objectField(params, "response");
  if (request && response) {
    request.response = response;
    request.expectsResponseExtra = params.hasExtraInfo === true;
    const chain = requestChain(tabId, stringField(params, "requestId"));
    if (chain) applyChainExtraInfo(chain);
  }
}

function onResponseExtraInfo(tabId: number, params: Record<string, unknown>): void {
  const requestId = stringField(params, "requestId");
  if (!requestId) return;
  if (recentlyFinalizedRequests.has(requestKey(tabId, requestId))) return;
  const chain = ensureRequestChain(tabId, requestId);
  chain.responseExtras.push(params);
  applyChainExtraInfo(chain);
}

async function onLoadingFinished(tabId: number, params: Record<string, unknown>): Promise<void> {
  const requestId = stringField(params, "requestId");
  const request = lookupRequest(tabId, requestId);
  if (!request) return;
  const completedAt = numberField(params, "timestamp");
  if (completedAt !== undefined) request.completedAt = completedAt;
  await finalizeRequestChain(tabId, requestId!);
}

async function onLoadingFailed(tabId: number, params: Record<string, unknown>): Promise<void> {
  const requestId = stringField(params, "requestId");
  const request = lookupRequest(tabId, requestId);
  if (!request) return;
  request.error = String(params.errorText ?? "network request failed");
  const completedAt = numberField(params, "timestamp");
  if (completedAt !== undefined) request.completedAt = completedAt;
  await finalizeRequestChain(tabId, requestId!);
}

async function finalizeRequestChain(tabId: number, requestId: string): Promise<void> {
  const chain = requestChain(tabId, requestId);
  if (!chain || chain.finalizing) return;
  chain.finalizing = true;
  await delay(EXTRA_INFO_GRACE_MS);
  const current = requestChain(tabId, requestId);
  if (!current) return;
  applyChainExtraInfo(current);
  pendingRequests.get(tabId)?.delete(requestId);
  const key = requestKey(tabId, requestId);
  recentlyFinalizedRequests.add(key);
  setTimeout(() => recentlyFinalizedRequests.delete(key), 5_000);
  for (const stage of current.stages) {
    await finalizeRequest(
      tabId,
      stage,
      stage.response,
      stage.error,
      stage.redirect,
      stage.completedAt
    );
  }
}

function applyChainExtraInfo(chain: PendingRequestChain): void {
  applyExtraInfo(chain.stages, chain.requestExtras, chain.responseExtras);
}

async function finalizeRequest(
  tabId: number,
  pending: PendingRequest,
  responseValue?: Record<string, unknown>,
  error?: string,
  redirect = false,
  completedAt?: number
): Promise<void> {
  const sequence = await nextSequence();
  const requestBody = await readRequestBody(tabId, pending, redirect);
  const responseBody = await readResponseBody(tabId, pending, responseValue, redirect);
  const response = responseValue ? capturedResponse(responseValue, pending.extraResponse, responseBody) : undefined;
  const state = tabStates.get(tabId);
  const event: HttpCaptureEvent = {
    sequence,
    capturedAt: timestampFromPending(pending),
    ...(completedAt !== undefined
      ? { durationMs: Math.max(0, Math.round((completedAt - pending.startedAt) * 1000)) }
      : {}),
    request: {
      method: pending.method,
      url: pending.url,
      headers: normalizeHeaders(pending.extraRequest?.headers ?? pending.headers),
      ...(requestBody ? { body: requestBody } : {})
    },
    ...(response ? { response } : {}),
    ...(error ? { error } : {}),
    tabId: String(tabId),
    ...(pending.initiator ? { initiator: pending.initiator } : {}),
    metadata: {
      source: "chrome-debugger",
      redirectIndex: pending.redirectIndex,
      resourceType: pending.resourceType,
      ...(state?.gaps.length ? { captureGaps: state.gaps.slice(-5) } : {})
    }
  };
  await enqueueEvent("http", sequence, event, tabId);
  incrementCaptured(tabId);
}

async function readRequestBody(
  tabId: number,
  pending: PendingRequest,
  redirect: boolean
): Promise<CapturedBody | undefined> {
  if (pending.postData !== undefined) return bodyFromCdp(pending.postData);
  if (!methodMayHaveBody(pending.method)) return undefined;
  if (redirect) return unavailableBody("Chromium did not expose this redirect request body");
  try {
    const result = await sendCommand(tabId, "Network.getRequestPostData", { requestId: pending.requestId });
    return typeof result.postData === "string" ? bodyFromCdp(result.postData) : undefined;
  } catch (error) {
    return unavailableBody(`Chromium did not retain request body: ${errorMessage(error)}`);
  }
}

async function readResponseBody(
  tabId: number,
  pending: PendingRequest,
  response: Record<string, unknown> | undefined,
  redirect: boolean
): Promise<CapturedBody | undefined> {
  if (!response || redirect || response.status === 204 || response.status === 304) return undefined;
  try {
    const result = await sendCommand(tabId, "Network.getResponseBody", { requestId: pending.requestId });
    if (typeof result.body !== "string") return undefined;
    return bodyFromCdp(result.body, result.base64Encoded === true);
  } catch (error) {
    return unavailableBody(`Chromium did not retain response body: ${errorMessage(error)}`);
  }
}

function capturedResponse(
  response: Record<string, unknown>,
  extra: Record<string, unknown> | undefined,
  body: CapturedBody | undefined
): CapturedResponse {
  const status = typeof extra?.statusCode === "number" ? extra.statusCode : Number(response.status ?? 0);
  return {
    status,
    headers: normalizeHeaders(extra?.headers ?? response.headers ?? {}),
    ...(body ? { body } : {})
  };
}

async function onWebSocketCreated(tabId: number, params: Record<string, unknown>): Promise<void> {
  const requestId = stringField(params, "requestId");
  const url = stringField(params, "url");
  if (!requestId || !url) return;
  const sockets = pendingSockets.get(tabId) ?? new Map<string, PendingSocket>();
  pendingSockets.set(tabId, sockets);
  const config = await requireConfig();
  sockets.set(requestId, {
    connectionId: `${config.producerId}:${tabId}:${requestId}`,
    url,
    ...(objectField(params, "initiator") ? { initiator: JSON.stringify(params.initiator) } : {}),
    opened: false
  });
}

async function onWebSocketHandshake(tabId: number, params: Record<string, unknown>): Promise<void> {
  const socket = lookupSocket(tabId, stringField(params, "requestId"));
  if (!socket || socket.opened) return;
  const response = objectField(params, "response");
  const headers = normalizeHeaders(response?.headers ?? {});
  const subprotocol = headers.find((header) => header.name.toLowerCase() === "sec-websocket-protocol")?.value;
  await enqueueWebSocket(tabId, {
    connectionId: socket.connectionId,
    kind: "open",
    url: socket.url,
    ...(subprotocol ? { subprotocol } : {})
  });
  socket.opened = true;
}

async function onWebSocketFrame(
  tabId: number,
  params: Record<string, unknown>,
  direction: "client_to_server" | "server_to_client"
): Promise<void> {
  const socket = lookupSocket(tabId, stringField(params, "requestId"));
  const response = objectField(params, "response");
  if (!socket || !response) return;
  if (!socket.opened) {
    await enqueueWebSocket(tabId, { connectionId: socket.connectionId, kind: "open", url: socket.url });
    socket.opened = true;
  }
  const opcode = Number(response.opcode ?? 1);
  const payloadData = typeof response.payloadData === "string" ? response.payloadData : "";
  await enqueueWebSocket(tabId, {
    connectionId: socket.connectionId,
    kind: "frame",
    direction,
    opcode,
    payload: bodyFromCdp(payloadData, opcode === 2)
  });
}

async function onWebSocketClosed(tabId: number, params: Record<string, unknown>): Promise<void> {
  const requestId = stringField(params, "requestId");
  const socket = lookupSocket(tabId, requestId);
  if (!socket) return;
  await enqueueWebSocket(tabId, { connectionId: socket.connectionId, kind: "close" });
  pendingSockets.get(tabId)?.delete(requestId!);
}

async function onWebSocketError(tabId: number, params: Record<string, unknown>): Promise<void> {
  const socket = lookupSocket(tabId, stringField(params, "requestId"));
  if (!socket) return;
  await enqueueWebSocket(tabId, {
    connectionId: socket.connectionId,
    kind: "error",
    error: String(params.errorMessage ?? "WebSocket frame error")
  });
}

async function enqueueWebSocket(
  tabId: number,
  event: Omit<WebSocketFrameEvent, "sequence" | "capturedAt">
): Promise<void> {
  const sequence = await nextSequence();
  await enqueueEvent("ws", sequence, {
    ...event,
    sequence,
    capturedAt: new Date().toISOString()
  }, tabId);
}

async function enqueueEvent(
  kind: QueueKind,
  sequence: number,
  event: HttpCaptureEvent | WebSocketFrameEvent,
  tabId: number
): Promise<void> {
  const config = await requireConfig();
  const byteSize = new TextEncoder().encode(JSON.stringify(event)).byteLength + 256;
  const item: QueueItem = {
    id: `${config.producerId}:${sequence}`,
    kind,
    sequence,
    event,
    target: queueTarget(config),
    byteSize,
    tabId,
    attempts: 0,
    nextAttemptAt: 0,
    createdAt: Date.now()
  };
  const operation = enqueueChain.then(async () => {
    if (!queueUsageReady) await refreshQueueUsage();
    await enqueue(item);
    queuedItemsEstimate += 1;
    queuedBytesEstimate += item.byteSize;
    if (queuedItemsEstimate <= MAX_QUEUE_ITEMS && queuedBytesEstimate <= MAX_QUEUE_BYTES) {
      return { dropped: [], deadLettersRemoved: 0 };
    }
    const pruned = await pruneStorage(MAX_QUEUE_ITEMS, MAX_QUEUE_BYTES, MAX_DEAD_LETTERS);
    await refreshQueueUsage();
    return pruned;
  });
  enqueueChain = operation.then(
    () => undefined,
    () => undefined
  );
  const pruned = await operation;
  if (pruned.dropped.length > 0) await recordQueueDrops(pruned.dropped);
  scheduleFlushSoon();
  if (!flushing) void flushAll();
}

async function flushAll(): Promise<void> {
  if (flushing) return;
  flushing = true;
  try {
    await enqueueChain;
    const config = await loadConfig();
    if (!config) return;
    const deadline = Date.now() + FLUSH_BUDGET_MS;
    while (Date.now() < deadline) {
      const http = await flushKind(config, "http");
      const websocket = await flushKind(config, "ws");
      if (!http && !websocket) break;
    }
    if (Date.now() >= deadline) scheduleFlushSoon();
    await saveLastFlushError();
  } catch (error) {
    await saveLastFlushError(errorMessage(error));
  } finally {
    flushing = false;
  }
}

async function flushKind(config: RuntimeConfig, kind: QueueKind): Promise<boolean> {
  const candidates = await dueItems(kind, Date.now(), DUE_CANDIDATES);
  const items = selectBatch(candidates);
  if (items.length === 0) return false;
  const target = items[0]!.target;
  if (!sameTarget(target, queueTarget(config))) {
    throw new Error(
      "queued capture belongs to a different runtime session; restore that pairing or clear the queue"
    );
  }
  const path = kind === "http" ? "/v1/ingest/http" : "/v1/ingest/ws";
  const body =
    kind === "http"
      ? { producerId: target.producerId, sessionId: target.sessionId, events: items.map((item) => item.event) }
      : { producerId: target.producerId, sessionId: target.sessionId, frames: items.map((item) => item.event) };

  let response: Response;
  try {
    response = await fetch(`${target.serverUrl}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });
  } catch (error) {
    await retryItems(items);
    throw error;
  }

  if (response.status === 429 || response.status >= 500) {
    await retryItems(items, retryAfterMs(response));
    throw new Error(`runtime returned HTTP ${response.status}`);
  }
  if ([400, 401, 403, 404, 409].includes(response.status)) {
    const detail = (await response.text()).slice(0, 200);
    await retryItems(items, 5_000);
    throw new Error(`runtime pairing rejected the batch (HTTP ${response.status}): ${detail}`);
  }
  if (!response.ok) {
    const error = `runtime returned HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`;
    await acknowledge([], items.map((item) => ({ item, error })), [], () => 0);
    noteQueueRemoval(items);
    await pruneStorage(MAX_QUEUE_ITEMS, MAX_QUEUE_BYTES, MAX_DEAD_LETTERS);
    throw new Error(error);
  }

  const payload = (await response.json()) as { results?: IngestResult[] } | IngestResult[];
  const results = Array.isArray(payload) ? payload : (payload.results ?? []);
  const classified = classifyAcknowledgements(items, results);
  await acknowledge(
    classified.remove,
    classified.reject,
    classified.retry,
    (item) => Date.now() + retryDelay(item.attempts)
  );
  noteQueueRemoval([...classified.remove, ...classified.reject.map((entry) => entry.item)]);
  if (classified.reject.length > 0) {
    const pruned = await pruneStorage(MAX_QUEUE_ITEMS, MAX_QUEUE_BYTES, MAX_DEAD_LETTERS);
    if (pruned.dropped.length > 0) await recordQueueDrops(pruned.dropped);
  }
  return true;
}

async function retryItems(items: QueueItem[], minimumDelay = 0): Promise<void> {
  await acknowledge([], [], items, (item) => Date.now() + Math.max(minimumDelay, retryDelay(item.attempts)));
}

async function nextSequence(): Promise<number> {
  const next = sequenceChain.then(async () => {
    const config = await requireConfig();
    const sequence = config.nextSequence;
    await saveConfig({ ...config, nextSequence: sequence + 1 });
    return sequence;
  });
  sequenceChain = next.catch(() => 0);
  return next;
}

async function currentState(): Promise<ExtensionState> {
  const [config, counts, lastFlushError] = await Promise.all([
    loadConfig(),
    queueCounts(),
    loadLastFlushError()
  ]);
  return {
    configured: config !== undefined,
    tabs: [...tabStates.values()].sort((a, b) => a.tabId - b.tabId),
    ...counts,
    pairingLocked: counts.queued > 0 || hasActiveCapture(),
    ...(lastFlushError ? { lastFlushError } : {})
  };
}

async function markUnexpectedDetach(tabId: number, reason: string): Promise<void> {
  const state = tabStates.get(tabId);
  if (!state) return;
  const lost = inFlightCount(tabId);
  const gap = {
    at: new Date().toISOString(),
    reason: `debugger detached: ${reason}${lost > 0 ? `; ${lost} in-flight item(s) lost` : ""}`
  };
  tabStates.set(tabId, {
    ...state,
    status: "detached",
    lastError: gap.reason,
    gaps: [...state.gaps, gap].slice(-MAX_GAPS_PER_TAB)
  });
  pendingRequests.delete(tabId);
  pendingSockets.delete(tabId);
  await persistTabs();
  await chrome.action.setBadgeBackgroundColor({ tabId, color: "#a64b3c" }).catch(() => undefined);
  await chrome.action.setBadgeText({ tabId, text: "!" }).catch(() => undefined);
}

async function closeRemovedTab(tabId: number): Promise<void> {
  const state = tabStates.get(tabId);
  if (!state) return;
  const lost = inFlightCount(tabId);
  tabStates.set(tabId, {
    ...state,
    status: "detached",
    lastError: "captured tab was closed",
    gaps: [
      ...state.gaps,
      {
        at: new Date().toISOString(),
        reason: `tab closed${lost > 0 ? ` with ${lost} in-flight item(s)` : ""}`
      }
    ].slice(-MAX_GAPS_PER_TAB)
  });
  pendingRequests.delete(tabId);
  pendingSockets.delete(tabId);
  await persistTabs();
}

async function recordQueueDrops(items: QueueItem[]): Promise<void> {
  const counts = new Map<number, number>();
  for (const item of items) {
    if (item.tabId !== undefined) counts.set(item.tabId, (counts.get(item.tabId) ?? 0) + 1);
  }
  for (const [tabId, count] of counts) {
    appendGap(tabId, `queue limit discarded ${count} oldest capture event(s)`);
  }
  await persistTabs();
  await saveLastFlushError(
    `capture queue reached its limit and discarded ${items.length} oldest event(s)`
  );
}

async function recordDiscardedItems(items: QueueItem[], reason: string): Promise<void> {
  const counts = new Map<number, number>();
  for (const item of items) {
    if (item.tabId !== undefined) counts.set(item.tabId, (counts.get(item.tabId) ?? 0) + 1);
  }
  for (const [tabId, count] of counts) appendGap(tabId, `${reason}: ${count} event(s)`);
  await persistTabs();
}

function appendGap(tabId: number, reason: string): void {
  const state = tabStates.get(tabId);
  if (!state) return;
  tabStates.set(tabId, {
    ...state,
    gaps: [...state.gaps, { at: new Date().toISOString(), reason }].slice(-MAX_GAPS_PER_TAB)
  });
}

async function markTabError(tabId: number, message: string): Promise<void> {
  const state = tabStates.get(tabId);
  if (!state) return;
  tabStates.set(tabId, { ...state, status: "error", lastError: message });
  await persistTabs();
}

function incrementCaptured(tabId: number): void {
  const state = tabStates.get(tabId);
  if (!state) return;
  tabStates.set(tabId, { ...state, captured: state.captured + 1 });
  void persistTabs();
}

function hasActiveCapture(): boolean {
  return [...tabStates.values()].some(
    (state) => state.status === "capturing" || state.status === "attaching"
  );
}

function inFlightCount(tabId: number): number {
  const requests = pendingRequests.get(tabId);
  const requestCount = requests
    ? [...requests.values()].reduce((total, chain) => total + chain.stages.length, 0)
    : 0;
  return requestCount + (pendingSockets.get(tabId)?.size ?? 0);
}

function scheduleFlushSoon(): void {
  void chrome.alarms.create(FLUSH_SOON_ALARM, { delayInMinutes: 0.05 });
}

async function refreshQueueUsage(): Promise<void> {
  const usage = await queueUsage();
  queuedItemsEstimate = usage.items;
  queuedBytesEstimate = usage.bytes;
  queueUsageReady = true;
}

function noteQueueRemoval(items: QueueItem[]): void {
  queuedItemsEstimate = Math.max(0, queuedItemsEstimate - items.length);
  queuedBytesEstimate = Math.max(
    0,
    queuedBytesEstimate - items.reduce((total, item) => total + queueItemSize(item), 0)
  );
}

function queueItemSize(item: QueueItem): number {
  return Number.isFinite(item.byteSize) && item.byteSize > 0
    ? item.byteSize
    : new TextEncoder().encode(JSON.stringify(item.event)).byteLength + 256;
}

function requestKey(tabId: number, requestId: string): string {
  return `${tabId}:${requestId}`;
}

async function persistTabs(): Promise<void> {
  await saveTabStates([...tabStates.values()]);
}

async function requireConfig(): Promise<RuntimeConfig> {
  const config = await loadConfig();
  if (!config) throw new Error("pair the extension with an Erebus capture session first");
  return config;
}

async function activeTab(): Promise<chrome.tabs.Tab> {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id) throw new Error("no active browser tab");
  return tab;
}

async function sendCommand(
  tabId: number,
  method: string,
  commandParams?: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const result = await chrome.debugger.sendCommand({ tabId }, method, commandParams);
  return (result ?? {}) as Record<string, unknown>;
}

function requestChain(tabId: number, requestId: string | undefined): PendingRequestChain | undefined {
  return requestId ? pendingRequests.get(tabId)?.get(requestId) : undefined;
}

function ensureRequestChain(tabId: number, requestId: string): PendingRequestChain {
  const requests = pendingRequests.get(tabId) ?? new Map<string, PendingRequestChain>();
  pendingRequests.set(tabId, requests);
  const existing = requests.get(requestId);
  if (existing) return existing;
  const chain: PendingRequestChain = {
    stages: [],
    requestExtras: [],
    responseExtras: [],
    finalizing: false
  };
  requests.set(requestId, chain);
  return chain;
}

function lookupRequest(tabId: number, requestId: string | undefined): PendingRequest | undefined {
  return requestChain(tabId, requestId)?.stages.at(-1);
}

function lookupSocket(tabId: number, requestId: string | undefined): PendingSocket | undefined {
  return requestId ? pendingSockets.get(tabId)?.get(requestId) : undefined;
}

function objectField(value: Record<string, unknown>, key: string): Record<string, unknown> | undefined {
  const field = value[key];
  return typeof field === "object" && field !== null ? (field as Record<string, unknown>) : undefined;
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  return typeof value[key] === "string" ? value[key] : undefined;
}

function numberField(value: Record<string, unknown>, key: string): number | undefined {
  return typeof value[key] === "number" ? value[key] : undefined;
}

function methodMayHaveBody(method: string): boolean {
  return !["GET", "HEAD", "OPTIONS", "TRACE"].includes(method.toUpperCase());
}

function timestampFromPending(pending: PendingRequest): string {
  return pending.wallTime ? new Date(pending.wallTime * 1000).toISOString() : new Date().toISOString();
}

function normalizeServerUrl(input: string): string {
  const url = new URL(input.trim());
  const host = url.hostname.toLowerCase();
  if (url.protocol !== "http:" || !["127.0.0.1", "localhost", "::1", "[::1]"].includes(host)) {
    throw new Error("runtime URL must be a local HTTP address");
  }
  url.pathname = url.pathname.replace(/\/$/, "");
  url.search = "";
  url.hash = "";
  return url.toString().replace(/\/$/, "");
}

function retryAfterMs(response: Response): number {
  const value = response.headers.get("Retry-After");
  if (!value) return 0;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const date = Date.parse(value);
  return Number.isNaN(date) ? 0 : Math.max(0, date - Date.now());
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
