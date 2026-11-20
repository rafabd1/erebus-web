import type { DeadLetter, QueueItem, QueueKind, RuntimeConfig, TabCaptureState } from "./protocol";

const DB_NAME = "erebus-web-capture";
const DB_VERSION = 1;
const QUEUE_STORE = "queue";
const DEAD_STORE = "deadLetters";
const CONFIG_KEY = "runtimeConfig";
const TABS_KEY = "captureTabs";
const FLUSH_ERROR_KEY = "lastFlushError";

export interface PruneResult {
  dropped: QueueItem[];
  deadLettersRemoved: number;
}

let databasePromise: Promise<IDBDatabase> | undefined;

function database(): Promise<IDBDatabase> {
  databasePromise ??= new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onerror = () => reject(request.error ?? new Error("failed to open capture queue"));
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(QUEUE_STORE)) {
        const store = db.createObjectStore(QUEUE_STORE, { keyPath: "id" });
        store.createIndex("kindNextAttempt", ["kind", "nextAttemptAt"]);
        store.createIndex("createdAt", "createdAt");
      }
      if (!db.objectStoreNames.contains(DEAD_STORE)) {
        const store = db.createObjectStore(DEAD_STORE, { keyPath: "id" });
        store.createIndex("failedAt", "failedAt");
      }
    };
    request.onsuccess = () => resolve(request.result);
  });
  return databasePromise;
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("capture queue operation failed"));
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error ?? new Error("capture queue transaction failed"));
    transaction.onabort = () => reject(transaction.error ?? new Error("capture queue transaction aborted"));
  });
}

export async function enqueue(item: QueueItem): Promise<void> {
  const db = await database();
  const transaction = db.transaction(QUEUE_STORE, "readwrite");
  transaction.objectStore(QUEUE_STORE).put(item);
  await transactionDone(transaction);
}

export async function pruneStorage(
  maxQueueItems: number,
  maxQueueBytes: number,
  maxDeadLetters: number
): Promise<PruneResult> {
  const db = await database();
  const read = db.transaction([QUEUE_STORE, DEAD_STORE], "readonly");
  const queueRequest = read.objectStore(QUEUE_STORE).getAll();
  const deadRequest = read.objectStore(DEAD_STORE).getAll();
  const [queued, dead] = await Promise.all([
    requestResult(queueRequest) as Promise<QueueItem[]>,
    requestResult(deadRequest) as Promise<DeadLetter[]>
  ]);
  await transactionDone(read);

  queued.sort((left, right) => left.createdAt - right.createdAt || left.sequence - right.sequence);
  dead.sort((left, right) => left.failedAt - right.failedAt);
  let queueBytes = queued.reduce((total, item) => total + itemSize(item), 0);
  const dropped: QueueItem[] = [];
  while (queued.length > maxQueueItems || queueBytes > maxQueueBytes) {
    const item = queued.shift();
    if (!item) break;
    dropped.push(item);
    queueBytes -= itemSize(item);
  }
  const deadLettersRemoved = Math.max(0, dead.length - maxDeadLetters);
  const deadToRemove = dead.slice(0, deadLettersRemoved);
  if (dropped.length === 0 && deadToRemove.length === 0) {
    return { dropped, deadLettersRemoved };
  }

  const write = db.transaction([QUEUE_STORE, DEAD_STORE], "readwrite");
  const queueStore = write.objectStore(QUEUE_STORE);
  const deadStore = write.objectStore(DEAD_STORE);
  for (const item of dropped) queueStore.delete(item.id);
  for (const item of deadToRemove) deadStore.delete(item.id);
  await transactionDone(write);
  return { dropped, deadLettersRemoved };
}

export async function dueItems(kind: QueueKind, now: number, limit: number): Promise<QueueItem[]> {
  const db = await database();
  const transaction = db.transaction(QUEUE_STORE, "readonly");
  const index = transaction.objectStore(QUEUE_STORE).index("kindNextAttempt");
  const range = IDBKeyRange.bound([kind, 0], [kind, now]);
  const items: QueueItem[] = [];

  await new Promise<void>((resolve, reject) => {
    const request = index.openCursor(range);
    request.onerror = () => reject(request.error ?? new Error("failed to read capture queue"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor || items.length >= limit) {
        resolve();
        return;
      }
      items.push(cursor.value as QueueItem);
      cursor.continue();
    };
  });
  await transactionDone(transaction);
  return items;
}

export async function clearQueue(): Promise<QueueItem[]> {
  const db = await database();
  const transaction = db.transaction(QUEUE_STORE, "readwrite");
  const store = transaction.objectStore(QUEUE_STORE);
  const items = (await requestResult(store.getAll())) as QueueItem[];
  store.clear();
  await transactionDone(transaction);
  return items;
}

export async function acknowledge(
  remove: QueueItem[],
  reject: Array<{ item: QueueItem; error: string }>,
  retry: QueueItem[],
  nextAttemptAt: (item: QueueItem) => number
): Promise<void> {
  if (remove.length + reject.length + retry.length === 0) return;
  const db = await database();
  const transaction = db.transaction([QUEUE_STORE, DEAD_STORE], "readwrite");
  const queue = transaction.objectStore(QUEUE_STORE);
  const dead = transaction.objectStore(DEAD_STORE);

  for (const item of remove) queue.delete(item.id);
  for (const entry of reject) {
    queue.delete(entry.item.id);
    const deadLetter: DeadLetter = {
      ...entry.item,
      failedAt: Date.now(),
      error: entry.error
    };
    dead.put(deadLetter);
  }
  for (const item of retry) {
    queue.put({
      ...item,
      attempts: item.attempts + 1,
      nextAttemptAt: nextAttemptAt(item)
    });
  }
  await transactionDone(transaction);
}

export async function queueCounts(): Promise<{ queued: number; deadLetters: number }> {
  const db = await database();
  const transaction = db.transaction([QUEUE_STORE, DEAD_STORE], "readonly");
  const queuedRequest = transaction.objectStore(QUEUE_STORE).count();
  const deadRequest = transaction.objectStore(DEAD_STORE).count();
  const [queued, deadLetters] = await Promise.all([
    requestResult(queuedRequest),
    requestResult(deadRequest)
  ]);
  await transactionDone(transaction);
  return { queued, deadLetters };
}

export async function queueUsage(): Promise<{ items: number; bytes: number }> {
  const db = await database();
  const transaction = db.transaction(QUEUE_STORE, "readonly");
  const request = transaction.objectStore(QUEUE_STORE).getAll();
  const queued = (await requestResult(request)) as QueueItem[];
  await transactionDone(transaction);
  return {
    items: queued.length,
    bytes: queued.reduce((total, item) => total + itemSize(item), 0)
  };
}

export async function loadConfig(): Promise<RuntimeConfig | undefined> {
  const result = await chrome.storage.local.get(CONFIG_KEY);
  return result[CONFIG_KEY] as RuntimeConfig | undefined;
}

export async function saveConfig(config: RuntimeConfig): Promise<void> {
  await chrome.storage.local.set({ [CONFIG_KEY]: config });
}

export async function loadTabStates(): Promise<TabCaptureState[]> {
  const result = await chrome.storage.local.get(TABS_KEY);
  return (result[TABS_KEY] as TabCaptureState[] | undefined) ?? [];
}

export async function saveTabStates(states: TabCaptureState[]): Promise<void> {
  await chrome.storage.local.set({ [TABS_KEY]: states });
}

export async function loadLastFlushError(): Promise<string | undefined> {
  const result = await chrome.storage.local.get(FLUSH_ERROR_KEY);
  return result[FLUSH_ERROR_KEY] as string | undefined;
}

export async function saveLastFlushError(error?: string): Promise<void> {
  if (error === undefined) {
    await chrome.storage.local.remove(FLUSH_ERROR_KEY);
  } else {
    await chrome.storage.local.set({ [FLUSH_ERROR_KEY]: error });
  }
}

function itemSize(item: QueueItem): number {
  if (Number.isFinite(item.byteSize) && item.byteSize > 0) return item.byteSize;
  return new TextEncoder().encode(JSON.stringify(item.event)).byteLength + 256;
}

