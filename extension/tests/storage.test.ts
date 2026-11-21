import { IDBKeyRange, indexedDB } from "fake-indexeddb";
import { beforeEach, describe, expect, it } from "vitest";

import type { QueueItem } from "../src/protocol";
import { pairingChangeAllowed } from "../src/queue-policy";
import {
  clearQueue,
  deadLetterTarget,
  dueItems,
  enqueue,
  pruneStorage,
  queueCounts
} from "../src/storage";

Object.assign(globalThis, { indexedDB, IDBKeyRange });

function item(sequence: number, byteSize = 100): QueueItem {
  return {
    id: `producer:${sequence}`,
    kind: "http",
    sequence,
    event: {} as never,
    target: {
      serverUrl: "http://127.0.0.1:8789",
      sessionId: "session",
      producerId: "producer"
    },
    byteSize,
    attempts: 0,
    nextAttemptAt: 0,
    createdAt: sequence
  };
}

beforeEach(async () => {
  await clearQueue();
  await pruneStorage(Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER, 0);
});

describe("capture queue limits", () => {
  it("persists capture gaps in their own send queue", async () => {
    await enqueue({ ...item(1), kind: "gap" });

    expect((await dueItems("gap", Date.now(), 10)).map((entry) => entry.sequence)).toEqual([1]);
    expect(await dueItems("http", Date.now(), 10)).toEqual([]);
  });

  it("drops the oldest items and leaves the retained events sendable", async () => {
    await enqueue(item(1));
    await enqueue(item(2));
    await enqueue(item(3));

    const result = await pruneStorage(2, 10_000, 10);
    const retained = await dueItems("http", Date.now(), 10);

    expect(result.dropped.map((entry) => entry.sequence)).toEqual([1]);
    expect(retained.map((entry) => entry.sequence)).toEqual([2, 3]);
  });

  it("also enforces the encoded byte budget", async () => {
    await enqueue(item(1, 700));
    await enqueue(item(2, 700));

    const result = await pruneStorage(10, 1_000, 10);
    expect(result.dropped.map((entry) => entry.sequence)).toEqual([1]);
  });

  it("dead-letters an ended target atomically and unlocks a new pairing", async () => {
    const queued = [
      item(1),
      {
        ...item(2),
        kind: "ws" as const,
        target: { ...item(2).target, serverUrl: "http://127.0.0.1:9999" }
      },
      { ...item(3), kind: "gap" as const }
    ];
    for (const entry of queued) await enqueue(entry);

    const rejected = await deadLetterTarget(queued[0]!.target, "pairing session ended");
    const counts = await queueCounts();

    expect(rejected.map((entry) => entry.sequence)).toEqual([1, 2, 3]);
    expect(counts).toEqual({ queued: 0, deadLetters: 3 });
    expect(
      pairingChangeAllowed(
        queued[0]!.target,
        { ...queued[0]!.target, sessionId: "replacement-session" },
        counts.queued > 0
      )
    ).toBe(true);
  });
});
