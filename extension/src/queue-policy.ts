import type { QueueItem, QueueTarget } from "./protocol";

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
  return (
    left.serverUrl === right.serverUrl &&
    left.sessionId === right.sessionId &&
    left.producerId === right.producerId
  );
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
  return current.serverUrl === proposed.serverUrl && current.sessionId === proposed.sessionId;
}
