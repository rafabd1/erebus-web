import { IDBKeyRange, indexedDB } from "fake-indexeddb";
import { beforeEach, describe, expect, it } from "vitest";

import type { QueueItem } from "../src/protocol";
import { clearQueue, dueItems, enqueue, pruneStorage } from "../src/storage";

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
});

describe("capture queue limits", () => {
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
});
