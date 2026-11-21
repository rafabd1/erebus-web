import { describe, expect, it } from "vitest";

import type { QueueItem, QueueTarget, RuntimeConfig } from "../src/protocol";
import {
  deliveryPath,
  deliveryUrl,
  pairingChangeAllowed,
  reboundConfig,
  sameTarget,
  selectBatch
} from "../src/queue-policy";

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

  it("never mixes session or producer identities", () => {
    const other = { ...target, sessionId: "session-b" };
    expect(selectBatch([item(1, 100), item(2, 100, other), item(3, 100)], 10, 10_000))
      .toHaveLength(1);
  });

  it("keeps a batch together across a runtime URL rebind", () => {
    const rebound = { ...target, serverUrl: "http://127.0.0.1:9999" };
    expect(selectBatch([item(1, 100), item(2, 100, rebound)], 10, 10_000)).toHaveLength(2);
    expect(sameTarget(target, rebound)).toBe(true);
  });
});

describe("pairingChangeAllowed", () => {
  it("allows runtime rebinds but blocks session changes while locked", () => {
    expect(pairingChangeAllowed(target, target, true)).toBe(true);
    expect(
      pairingChangeAllowed(
        target,
        { serverUrl: "http://127.0.0.1:9999", sessionId: target.sessionId },
        true
      )
    ).toBe(true);
    expect(
      pairingChangeAllowed(target, { serverUrl: target.serverUrl, sessionId: "session-b" }, true)
    ).toBe(false);
    expect(
      pairingChangeAllowed(target, { serverUrl: target.serverUrl, sessionId: "session-b" }, false)
    ).toBe(true);
  });
});

describe("runtime rebind", () => {
  const current: RuntimeConfig = {
    ...target,
    token: "old-token",
    nextSequence: 42
  };

  it("preserves producer and sequence while switching transport", () => {
    const rebound = reboundConfig(
      current,
      {
        serverUrl: "http://127.0.0.1:9999",
        token: "new-token",
        sessionId: current.sessionId
      },
      () => "new-producer"
    );
    expect(rebound.producerId).toBe(current.producerId);
    expect(rebound.nextSequence).toBe(42);
    expect(deliveryUrl(rebound, "/v1/gaps")).toBe("http://127.0.0.1:9999/v1/gaps");
    expect(deliveryPath("gap")).toBe("/v1/gaps");
  });

  it("rotates producer and sequence for a different session", () => {
    const rebound = reboundConfig(
      current,
      { serverUrl: current.serverUrl, token: "new-token", sessionId: "session-b" },
      () => "new-producer"
    );
    expect(rebound.producerId).toBe("new-producer");
    expect(rebound.nextSequence).toBe(1);
  });
});
