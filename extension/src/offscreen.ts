export {};

interface ErebusRecordingState {
  tabId: number;
  startedAt: string;
  canvas: HTMLCanvasElement;
  context: CanvasRenderingContext2D;
  recorder: MediaRecorder | null;
  mimeType: string | null;
  chunks: Blob[];
}

type RecordingMessage =
  | { type: "erebus-recording-start"; tabId: number }
  | { type: "erebus-recording-frame"; tabId: number; data: string }
  | { type: "erebus-recording-stop"; tabId: number }
  | { type: "erebus-recording-discard"; tabId: number };

const recordings = new Map<number, ErebusRecordingState>();

chrome.runtime.onMessage.addListener((message: RecordingMessage, _sender, sendResponse) => {
  if (!message?.type?.startsWith("erebus-recording-")) return false;
  void handleMessage(message)
    .then((result) => sendResponse({ ok: true, result }))
    .catch((error) => sendResponse({ ok: false, error: errorMessage(error) }));
  return true;
});

async function handleMessage(message: RecordingMessage): Promise<unknown> {
  switch (message.type) {
    case "erebus-recording-start":
      return start(message.tabId);
    case "erebus-recording-frame":
      await drawFrame(message.tabId, message.data);
      return undefined;
    case "erebus-recording-stop":
      return stop(message.tabId);
    case "erebus-recording-discard":
      await discard(message.tabId);
      return undefined;
  }
}

function start(tabId: number): { startedAt: string } {
  const current = recordings.get(tabId);
  if (current) return { startedAt: current.startedAt };
  const canvas = document.createElement("canvas");
  canvas.width = 1;
  canvas.height = 1;
  const context = canvas.getContext("2d", { alpha: false });
  if (!context) throw new Error("browser recording canvas is unavailable");
  const startedAt = new Date().toISOString();
  recordings.set(tabId, {
    tabId,
    startedAt,
    canvas,
    context,
    recorder: null,
    mimeType: null,
    chunks: []
  });
  return { startedAt };
}

async function drawFrame(tabId: number, data: string): Promise<void> {
  const recording = recordings.get(tabId);
  if (!recording) return;
  const response = await fetch(`data:image/jpeg;base64,${data}`);
  const image = await createImageBitmap(await response.blob());
  try {
    if (!recording.recorder) {
      recording.canvas.width = Math.max(1, image.width);
      recording.canvas.height = Math.max(1, image.height);
      recording.mimeType = preferredMimeType();
      const recorder = new MediaRecorder(recording.canvas.captureStream(20), {
        mimeType: recording.mimeType
      });
      recorder.addEventListener("dataavailable", (event) => {
        if (event.data.size > 0) recording.chunks.push(event.data);
      });
      recorder.start(1_000);
      recording.recorder = recorder;
    }
    recording.context.drawImage(
      image,
      0,
      0,
      recording.canvas.width,
      recording.canvas.height
    );
  } finally {
    image.close();
  }
}

async function stop(tabId: number): Promise<{
  startedAt: string;
  mimeType: string;
  dataBase64: string;
}> {
  const recording = recordings.get(tabId);
  if (!recording) throw new Error("browser recording is not active for this tab");
  if (!recording.recorder || !recording.mimeType) {
    recordings.delete(tabId);
    throw new Error("browser recording did not receive a frame");
  }
  await stopRecorder(recording.recorder);
  recordings.delete(tabId);
  const blob = new Blob(recording.chunks, { type: recording.mimeType });
  return {
    startedAt: recording.startedAt,
    mimeType: recording.mimeType,
    dataBase64: bytesToBase64(new Uint8Array(await blob.arrayBuffer()))
  };
}

async function discard(tabId: number): Promise<void> {
  const recording = recordings.get(tabId);
  if (!recording) return;
  recordings.delete(tabId);
  await stopRecorder(recording.recorder);
}

function preferredMimeType(): string {
  const candidates = ["video/webm;codecs=vp9", "video/webm;codecs=vp8", "video/webm"];
  return candidates.find((candidate) => MediaRecorder.isTypeSupported(candidate)) ?? "video/webm";
}

async function stopRecorder(recorder: MediaRecorder | null): Promise<void> {
  if (!recorder || recorder.state === "inactive") return;
  await new Promise<void>((resolve) => {
    recorder.addEventListener("stop", () => resolve(), { once: true });
    recorder.stop();
  });
}

function bytesToBase64(bytes: Uint8Array): string {
  const chunks: string[] = [];
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    chunks.push(String.fromCharCode(...bytes.subarray(offset, offset + 0x8000)));
  }
  return btoa(chunks.join(""));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
