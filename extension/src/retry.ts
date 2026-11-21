import type { IngestResult, QueueItem } from "./protocol";

const MAX_BACKOFF_MS = 60_000;

export type DeliveryFailureAction = "retry" | "reject-batch" | "reject-target";

export function classifyDeliveryFailure(
  status: number,
  detail: string,
  attempts: number
): DeliveryFailureAction {
  if (status === 429 || status >= 500) return "retry";
  if (status === 410 || status === 404) return "reject-target";
  if (status === 401 || status === 403) return attempts >= 2 ? "reject-target" : "retry";
  if (status === 400) {
    return /(?:session|pairing).*(?:ended|deleted|does not exist|not found|revoked)/i.test(detail)
      ? "reject-target"
      : "reject-batch";
  }
  if (status === 409) return "reject-batch";
  return "reject-batch";
}

export function retryDelay(attempts: number, random = Math.random()): number {
  const base = Math.min(MAX_BACKOFF_MS, 500 * 2 ** Math.min(attempts, 7));
  return Math.round(base * (0.75 + random * 0.5));
}

export function classifyAcknowledgements(
  items: QueueItem[],
  results: IngestResult[]
): { remove: QueueItem[]; reject: Array<{ item: QueueItem; error: string }>; retry: QueueItem[] } {
  const bySequence = new Map(results.map((result) => [result.sequence, result]));
  const remove: QueueItem[] = [];
  const reject: Array<{ item: QueueItem; error: string }> = [];
  const retry: QueueItem[] = [];

  for (const item of items) {
    const result = bySequence.get(item.sequence);
    if (!result) {
      retry.push(item);
    } else if (result.status === "accepted" || result.status === "duplicate") {
      remove.push(item);
    } else {
      reject.push({ item, error: result.error ?? "runtime rejected capture event" });
    }
  }
  return { remove, reject, retry };
}

