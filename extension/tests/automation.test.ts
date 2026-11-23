import { describe, expect, it, vi } from "vitest";

import {
  executeChromiumAutomationCommand,
  type ChromiumAutomationApi
} from "../src/automation";

function command(
  operation: string,
  input: Record<string, unknown>,
  tabId?: number
) {
  return {
    id: `command-${operation}`,
    operation,
    input,
    timeoutMs: 1_000,
    createdAtMs: Date.now(),
    ...(tabId === undefined ? {} : { tabId })
  };
}

function api() {
  const state = {
    url: "about:blank",
    status: "complete" as chrome.tabs.TabStatus
  };
  const calls: string[] = [];
  const automationApi: ChromiumAutomationApi = {
    attachedTabs: vi.fn(async () => []),
    ensureAttached: vi.fn(async () => {
      calls.push("attach");
    }),
    createTab: vi.fn(async (url) => {
      calls.push(`create:${url}`);
      state.url = url ?? "about:blank";
      return { id: 17, url: state.url, active: true, status: state.status } as chrome.tabs.Tab;
    }),
    getTab: vi.fn(
      async (tabId) =>
        ({ id: tabId, url: state.url, active: true, status: state.status }) as chrome.tabs.Tab
    ),
    updateTab: vi.fn(async (tabId, update) => {
      calls.push(`update:${String(update.url ?? "active")}`);
      if (update.url) state.url = update.url;
      return { id: tabId, url: state.url, active: true, status: state.status } as chrome.tabs.Tab;
    }),
    closeTab: vi.fn(async () => undefined),
    sendCommand: vi.fn(async (_tabId, method, params) => {
      if (method === "Runtime.evaluate") {
        if (params?.expression === "document.readyState") {
          return { result: { value: "complete" } };
        }
        return {
          result: {
            value: { width: 1280, height: 720, readyState: "complete" }
          }
        };
      }
      return {};
    })
  };
  return { automationApi, calls };
}

describe("extension automation navigation", () => {
  it("attaches a new blank tab before navigating to the requested URL", async () => {
    const { automationApi, calls } = api();

    const result = await executeChromiumAutomationCommand(
      command("open", { url: "https://example.com", reuseExistingTab: false }),
      automationApi
    );

    expect(result.ok).toBe(true);
    expect(result.result).toMatchObject({
      viewportSetting: { _tag: "fill" },
      viewport: { width: 1280, height: 720 }
    });
    expect(calls.slice(0, 3)).toEqual([
      "create:about:blank",
      "attach",
      "update:https://example.com"
    ]);
  });

  it("navigates a restricted existing tab before attaching the debugger", async () => {
    const { automationApi, calls } = api();

    const result = await executeChromiumAutomationCommand(
      command("navigate", { url: "https://example.com" }, 23),
      automationApi
    );

    expect(result.ok).toBe(true);
    expect(result.result).toMatchObject({ viewportSetting: { _tag: "fill" } });
    expect(calls.slice(0, 2)).toEqual(["update:https://example.com", "attach"]);
  });

  it("returns resize settings in the shared preview contract shape", async () => {
    const { automationApi } = api();

    const result = await executeChromiumAutomationCommand(
      command("resize", { mode: "preset", preset: "iphone-15-pro" }, 23),
      automationApi
    );

    expect(result).toMatchObject({
      ok: true,
      result: {
        setting: {
          _tag: "preset",
          presetId: "iphone-15-pro",
          width: 393,
          height: 852
        }
      }
    });
  });
});
