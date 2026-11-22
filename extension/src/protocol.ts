export type BodyState = "complete" | "truncated" | "unavailable" | "skipped" | "evicted";

export interface HeaderField {
  name: string;
  value: string;
}

export interface CapturedBody {
  encoding: "utf8" | "base64";
  data: string;
  state: BodyState;
  reason?: string;
  originalSize?: number;
}

export interface CapturedRequest {
  method: string;
  url: string;
  headers: HeaderField[];
  body?: CapturedBody;
}

export interface CapturedResponse {
  status: number;
  headers: HeaderField[];
  body?: CapturedBody;
}

export interface HttpCaptureEvent {
  sequence: number;
  capturedAt: string;
  durationMs?: number;
  request: CapturedRequest;
  response?: CapturedResponse;
  error?: string;
  tabId: string;
  initiator?: string;
  parentSequence?: number;
  metadata: Record<string, unknown>;
}

export interface WebSocketFrameEvent {
  sequence: number;
  connectionId: string;
  capturedAt: string;
  kind: "open" | "frame" | "close" | "error";
  direction?: "client_to_server" | "server_to_client";
  opcode?: number;
  payload?: CapturedBody;
  url?: string;
  subprotocol?: string;
  closeCode?: number;
  closeReason?: string;
  error?: string;
}

export interface CaptureGapEvent {
  sessionId: string;
  source: "extension";
  droppedEvents: number;
  reason: string;
  observedAt: string;
  producerId: string;
  sequence: number;
}

export type QueueKind = "http" | "ws" | "gap";

export interface QueueTarget {
  serverUrl: string;
  sessionId: string;
  producerId: string;
}

export interface QueueItem<T = HttpCaptureEvent | WebSocketFrameEvent | CaptureGapEvent> {
  id: string;
  kind: QueueKind;
  sequence: number;
  event: T;
  target: QueueTarget;
  byteSize: number;
  tabId?: number;
  attempts: number;
  nextAttemptAt: number;
  createdAt: number;
}

export interface DeadLetter extends QueueItem {
  failedAt: number;
  error: string;
}

export interface IngestResult {
  sequence: number;
  status: "accepted" | "duplicate" | "rejected";
  observationId?: string;
  error?: string;
}

export interface RuntimeConfig {
  serverUrl: string;
  token: string;
  sessionId: string;
  producerId: string;
  nextSequence: number;
  proxyUrl?: string;
  caPem?: string;
}

export interface CaptureGap {
  at: string;
  reason: string;
}

export interface TabCaptureState {
  tabId: number;
  title?: string;
  url?: string;
  status: "attaching" | "capturing" | "detached" | "error";
  lastError?: string;
  captured: number;
  gaps: CaptureGap[];
  startedAt: string;
}

export interface ExtensionState {
  configured: boolean;
  tabs: TabCaptureState[];
  queued: number;
  deadLetters: number;
  pairingLocked: boolean;
  automationConnected: boolean;
  lastFlushError?: string;
}

export interface ExtensionAutomationTab {
  tabId: number;
  windowId: number;
  active: boolean;
  attached: boolean;
  title?: string;
  url?: string;
  status?: string;
}

export interface ExtensionAutomationCommand {
  id: string;
  tabId?: number;
  operation: string;
  input: Record<string, unknown>;
  timeoutMs: number;
  createdAtMs: number;
}

export interface ExtensionAutomationError {
  tag: string;
  message: string;
  detail?: unknown;
}

export interface ExtensionAutomationResult {
  commandId: string;
  ok: boolean;
  result?: unknown;
  error?: ExtensionAutomationError;
}

export type PopupRequest =
  | { type: "getState" }
  | {
      type: "saveConfig";
      serverUrl: string;
      token: string;
      sessionId: string;
      proxyUrl?: string;
      caPem?: string;
    }
  | { type: "startCurrentTab" }
  | { type: "stopCurrentTab" }
  | { type: "flushNow" }
  | { type: "discardQueue" };

export type PopupResponse =
  | { ok: true; state: ExtensionState; config?: Omit<RuntimeConfig, "nextSequence"> }
  | { ok: false; error: string; state?: ExtensionState };

