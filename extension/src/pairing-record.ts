export interface PairingRecord {
  serverUrl: string;
  token: string;
  sessionId: string;
}

export function parsePairingRecord(value: string): PairingRecord {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("Pairing record is not valid JSON.");
  }
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("Pairing record must be an object.");
  }
  const record = parsed as Record<string, unknown>;
  const serverUrl = typeof record.serverUrl === "string" ? record.serverUrl.trim() : "";
  const token = typeof record.token === "string" ? record.token.trim() : "";
  const sessionId = typeof record.sessionId === "string" ? record.sessionId.trim() : "";
  if (!serverUrl || !token || !sessionId) {
    throw new Error("Pairing record must include serverUrl, token, and sessionId.");
  }
  return { serverUrl, token, sessionId };
}
