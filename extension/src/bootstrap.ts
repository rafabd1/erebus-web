import type { RuntimeConfig } from "./protocol";

export type ExtensionBrowserKind = "chromium" | "firefox";

export interface BootstrapRecord {
  serverUrl: string;
  token: string;
  sessionId: string;
  proxyUrl?: string;
  caPem?: string;
}

interface BootstrapUnchangedRecord {
  serverUrl: string;
  sessionId: string;
  unchanged: true;
}

const candidatePorts = Array.from({ length: 12 }, (_, index) => [13_773 + index, 3_773 + index]).flat();

export async function discoverErebuses(
  installId: string,
  browser: ExtensionBrowserKind,
  version: string,
  known: readonly RuntimeConfig[] = []
): Promise<BootstrapRecord[]> {
  const discovered = await Promise.all(candidatePorts.map(async (port) => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 500);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/browser-extension/bootstrap`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          installId,
          browser,
          version,
          knownTargets: known.map(({ serverUrl, sessionId }) => ({ serverUrl, sessionId }))
        }),
        signal: controller.signal
      });
      if (!response.ok) return undefined;
      const value = (await response.json()) as Partial<BootstrapRecord> & { targets?: unknown };
      const candidates = Array.isArray(value.targets) ? value.targets : [value];
      return candidates.flatMap((candidate) => {
        const record = candidate as Partial<BootstrapRecord & BootstrapUnchangedRecord>;
        if (record.unchanged === true && typeof record.serverUrl === "string" && typeof record.sessionId === "string") {
          const retained = known.find((config) =>
            config.serverUrl === record.serverUrl && config.sessionId === record.sessionId
          );
          return retained ? [retained] : [];
        }
        return typeof record.serverUrl === "string" &&
          typeof record.token === "string" &&
          typeof record.sessionId === "string"
          ? [{
              serverUrl: record.serverUrl,
              token: record.token,
              sessionId: record.sessionId,
              ...(typeof record.proxyUrl === "string" ? { proxyUrl: record.proxyUrl } : {}),
              ...(typeof record.caPem === "string" ? { caPem: record.caPem } : {})
            } satisfies BootstrapRecord]
          : [];
      });
    } catch {
      // Erebus may be stopped or this port may belong to another local service.
    } finally {
      clearTimeout(timeout);
    }
    return undefined;
  }));
  const unique = new Map<string, BootstrapRecord>();
  for (const records of discovered) {
    for (const record of records ?? []) unique.set(`${record.serverUrl}\u0000${record.sessionId}`, record);
  }
  return [...unique.values()];
}

/** Compatibility helper for consumers that deliberately want only one endpoint. */
export async function discoverErebus(
  installId: string,
  browser: ExtensionBrowserKind,
  version: string
): Promise<BootstrapRecord | undefined> {
  return (await discoverErebuses(installId, browser, version))[0];
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
