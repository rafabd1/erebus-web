import type { CapturedBody, HeaderField } from "./protocol";

const MAX_BODY_BYTES = 2 * 1024 * 1024;

export function normalizeHeaders(input: unknown): HeaderField[] {
  if (Array.isArray(input)) {
    return input.flatMap((entry): HeaderField[] => {
      if (typeof entry !== "object" || entry === null) return [];
      const value = entry as Record<string, unknown>;
      if (typeof value.name !== "string") return [];
      return [{ name: value.name, value: String(value.value ?? "") }];
    });
  }
  if (typeof input !== "object" || input === null) return [];
  return Object.entries(input).map(([name, value]) => ({ name, value: String(value ?? "") }));
}

export function bodyFromCdp(data: string, base64Encoded = false): CapturedBody {
  const bytes = base64Encoded ? estimateBase64Bytes(data) : new TextEncoder().encode(data).byteLength;
  if (bytes <= MAX_BODY_BYTES) {
    return {
      encoding: base64Encoded ? "base64" : "utf8",
      data,
      state: "complete"
    };
  }

  if (base64Encoded) {
    const maxChars = Math.floor(MAX_BODY_BYTES / 3) * 4;
    return {
      encoding: "base64",
      data: data.slice(0, maxChars),
      state: "truncated",
      reason: `extension body limit (${MAX_BODY_BYTES} bytes)`,
      originalSize: bytes
    };
  }

  return {
    encoding: "utf8",
    data: truncateUtf8(data, MAX_BODY_BYTES),
    state: "truncated",
    reason: `extension body limit (${MAX_BODY_BYTES} bytes)`,
    originalSize: bytes
  };
}

export function unavailableBody(reason: string): CapturedBody {
  return { encoding: "utf8", data: "", state: "unavailable", reason };
}

function estimateBase64Bytes(value: string): number {
  const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((value.length * 3) / 4) - padding);
}

function truncateUtf8(value: string, limit: number): string {
  const encoded = new TextEncoder().encode(value);
  if (encoded.byteLength <= limit) return value;
  return new TextDecoder().decode(encoded.slice(0, limit));
}

