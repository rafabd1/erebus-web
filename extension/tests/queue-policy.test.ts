import { describe, expect, it } from "vitest";

import type { QueueItem, QueueTarget } from "../src/protocol";
import { pairingChangeAllowed, selectBatch } from "../src/queue-policy";

const target: QueueTarget = {
  serverUrl: "http://127.0.0.1:8789",
  sessionId: "session-a",
  producerId: "producer-a"
};

function item(sequence: number, byteSize: number, destination = target): QueueItem {
  return {
    id: `${destination.producerId}:${sequence}`,
    kind: "http",
    sequence,
    event: {} as never,
    target: destination,
    byteSize,
    attempts: 0,
    nextAttemptAt: 0,
    createdAt: sequence
  };
}

describe("selectBatch", () => {
  it("stops before crossing the byte budget", () => {
    expect(selectBatch([item(1, 400), item(2, 400), item(3, 400)], 10, 1_100).map((entry) => entry.sequence))
      .toEqual([1, 2]);
  });

  it("never mixes runtime destinations", () => {
    const other = { ...target, sessionId: "session-b" };
    expect(selectBatch([item(1, 100), item(2, 100, other), item(3, 100)], 10, 10_000))
      .toHaveLength(1);
  });
});

describe("pairingChangeAllowed", () => {
  it("allows token rotation but blocks destination changes while locked", () => {
    expect(pairingChangeAllowed(target, target, true)).toBe(true);
    expect(
      pairingChangeAllowed(target, { serverUrl: target.serverUrl, sessionId: "session-b" }, true)
    ).toBe(false);
    expect(
      pairingChangeAllowed(target, { serverUrl: target.serverUrl, sessionId: "session-b" }, false)
    ).toBe(true);
  });
});
