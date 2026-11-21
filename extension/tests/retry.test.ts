import { describe, expect, it } from "vitest";

import type { QueueItem } from "../src/protocol";
import { classifyAcknowledgements, classifyDeliveryFailure, retryDelay } from "../src/retry";

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

describe("classifyDeliveryFailure", () => {
  it("stops a revoked or missing target without retrying forever", () => {
    expect(classifyDeliveryFailure(410, "pairing session ended", 0)).toBe("reject-target");
    expect(classifyDeliveryFailure(404, "not found", 0)).toBe("reject-target");
    expect(classifyDeliveryFailure(400, "capture session does not exist", 0)).toBe("reject-target");
  });

  it("gives runtime rebinds a bounded window for authorization failures", () => {
    expect(classifyDeliveryFailure(401, "unauthorized", 0)).toBe("retry");
    expect(classifyDeliveryFailure(403, "forbidden", 1)).toBe("retry");
    expect(classifyDeliveryFailure(401, "unauthorized", 2)).toBe("reject-target");
  });

  it("rejects malformed or conflicting batches and retries transient failures", () => {
    expect(classifyDeliveryFailure(400, "invalid batch", 0)).toBe("reject-batch");
    expect(classifyDeliveryFailure(409, "sequence conflict", 0)).toBe("reject-batch");
    expect(classifyDeliveryFailure(429, "busy", 0)).toBe("retry");
    expect(classifyDeliveryFailure(503, "unavailable", 0)).toBe("retry");
  });
});
