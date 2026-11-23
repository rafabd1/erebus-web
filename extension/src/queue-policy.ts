import type { QueueItem, QueueKind, QueueTarget, RuntimeConfig } from "./protocol";

export const MAX_BATCH_ITEMS = 50;
export const MAX_BATCH_BYTES = 12 * 1024 * 1024;

export function queueTarget(config: QueueTarget): QueueTarget {
  return {
    serverUrl: config.serverUrl,
    sessionId: config.sessionId,
    producerId: config.producerId
  };
}

export function sameTarget(left: QueueTarget, right: QueueTarget): boolean {
  // Capture-session ids are UUIDs and identify the durable destination.
  // The runtime port can change after a restart, so it must not split a
  // target or strand its queued evidence on the old URL.
  return left.sessionId === right.sessionId && left.producerId === right.producerId;
}

export function deliveryUrl(config: Pick<RuntimeConfig, "serverUrl">, path: string): string {
  return `${config.serverUrl}${path}`;
}

export function deliveryPath(kind: QueueKind): string {
  return kind === "http" ? "/v1/ingest/http" : kind === "ws" ? "/v1/ingest/ws" : "/v1/gaps";
}

export function reboundConfig(
  current: RuntimeConfig | undefined,
  proposed: Pick<RuntimeConfig, "serverUrl" | "token" | "sessionId"> &
    Partial<Pick<RuntimeConfig, "proxyUrl" | "caPem">>,
  producerId: () => string
): RuntimeConfig {
  const sameSession = current?.sessionId === proposed.sessionId;
  return {
    ...proposed,
    producerId: sameSession ? current.producerId : producerId(),
    nextSequence: sameSession ? current.nextSequence : 1
  };
}

export function selectBatch(
  candidates: QueueItem[],
  maxItems = MAX_BATCH_ITEMS,
  maxBytes = MAX_BATCH_BYTES
): QueueItem[] {
  const first = candidates[0];
  if (!first) return [];
  const selected: QueueItem[] = [];
  let bytes = 256;
  for (const item of candidates) {
    if (!sameTarget(item.target, first.target)) break;
    const size = Math.max(1, item.byteSize);
    if (selected.length > 0 && (selected.length >= maxItems || bytes + size > maxBytes)) break;
    selected.push(item);
    bytes += size;
  }
  return selected;
}

export function pairingChangeAllowed(
  current: QueueTarget | undefined,
  proposed: Pick<QueueTarget, "serverUrl" | "sessionId">,
  locked: boolean
): boolean {
  if (!current || !locked) return true;
  return current.sessionId === proposed.sessionId;
}
