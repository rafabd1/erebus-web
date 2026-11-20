import { describe, expect, it } from "vitest";

import type { QueueItem } from "../src/protocol";
import { classifyAcknowledgements, retryDelay } from "../src/retry";

const item = (sequence: number): QueueItem => ({
  id: `producer:${sequence}`,
  kind: "http",
  sequence,
  event: {} as never,
  target: { serverUrl: "http://127.0.0.1", sessionId: "session", producerId: "producer" },
  byteSize: 100,
  attempts: 0,
  nextAttemptAt: 0,
  createdAt: 0
});

describe("classifyAcknowledgements", () => {
  it("removes accepted and duplicate items, isolates rejections and retries missing ACKs", () => {
    const items = [item(1), item(2), item(3), item(4)];
    const result = classifyAcknowledgements(items, [
      { sequence: 1, status: "accepted" },
      { sequence: 2, status: "duplicate" },
      { sequence: 3, status: "rejected", error: "bad URL" }
    ]);
    expect(result.remove.map((entry) => entry.sequence)).toEqual([1, 2]);
    expect(result.reject[0]?.item.sequence).toBe(3);
    expect(result.retry.map((entry) => entry.sequence)).toEqual([4]);
  });
});

describe("retryDelay", () => {
  it("backs off and caps the delay", () => {
    expect(retryDelay(0, 0)).toBe(375);
    expect(retryDelay(20, 1)).toBe(75_000);
  });
});
