import type { RuntimeConfig } from "./protocol";

export type ExtensionBrowserKind = "chromium" | "firefox";

interface BootstrapRecord {
  serverUrl: string;
  token: string;
  sessionId: string;
  proxyUrl?: string;
  caPem?: string;
}

const candidatePorts = Array.from({ length: 12 }, (_, index) => [13_773 + index, 3_773 + index]).flat();

export async function discoverErebus(
  installId: string,
  browser: ExtensionBrowserKind,
  version: string
): Promise<BootstrapRecord | undefined> {
  for (const port of candidatePorts) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 500);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/browser-extension/bootstrap`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ installId, browser, version }),
        signal: controller.signal
      });
      if (!response.ok) continue;
      const value = (await response.json()) as Partial<BootstrapRecord>;
      if (
        typeof value.serverUrl === "string" &&
        typeof value.token === "string" &&
        typeof value.sessionId === "string"
      ) {
        return {
          serverUrl: value.serverUrl,
          token: value.token,
          sessionId: value.sessionId,
          ...(typeof value.proxyUrl === "string" ? { proxyUrl: value.proxyUrl } : {}),
          ...(typeof value.caPem === "string" ? { caPem: value.caPem } : {})
        };
      }
    } catch {
      // Erebus may be stopped or this port may belong to another local service.
    } finally {
      clearTimeout(timeout);
    }
  }
  return undefined;
}

export function configFromBootstrap(
  record: BootstrapRecord,
  installId: string,
  previous?: RuntimeConfig
): RuntimeConfig {
  const sameSession = previous?.sessionId === record.sessionId;
  return {
    serverUrl: record.serverUrl,
    token: record.token,
    sessionId: record.sessionId,
    producerId: installId,
    nextSequence: sameSession ? previous.nextSequence : 1,
    ...(record.proxyUrl ? { proxyUrl: record.proxyUrl } : {}),
    ...(record.caPem ? { caPem: record.caPem } : {})
  };
}
