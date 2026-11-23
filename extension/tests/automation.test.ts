import { describe, expect, it, vi } from "vitest";

import {
  executeChromiumAutomationCommand,
  implicitInputRole,
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

function api(initialUrl = "about:blank") {
  const state = {
    url: initialUrl,
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
    startRecording: vi.fn(async () => ({ startedAt: "2026-11-22T00:00:00.000Z" })),
    stopRecording: vi.fn(async () => ({
      startedAt: "2026-11-22T00:00:00.000Z",
      mimeType: "video/webm",
      dataBase64: "AA=="
    })),
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
  it("reports native input roles instead of flattening controls to textboxes", () => {
    expect(implicitInputRole("radio")).toBe("radio");
    expect(implicitInputRole("checkbox")).toBe("checkbox");
    expect(implicitInputRole("submit")).toBe("button");
    expect(implicitInputRole("search")).toBe("searchbox");
    expect(implicitInputRole("text")).toBe("textbox");
    expect(implicitInputRole("hidden")).toBeNull();
  });

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
    const { automationApi, calls } = api("chrome://newtab/");

    const result = await executeChromiumAutomationCommand(
      command("navigate", { url: "https://example.com" }, 23),
      automationApi
    );

    expect(result.ok).toBe(true);
    expect(result.result).toMatchObject({ viewportSetting: { _tag: "fill" } });
    expect(calls.slice(0, 2)).toEqual(["update:https://example.com", "attach"]);
  });

  it("navigates an attached web tab through the debugger", async () => {
    const { automationApi, calls } = api();
    await automationApi.updateTab(23, { url: "https://example.org" });
    calls.length = 0;

    const result = await executeChromiumAutomationCommand(
      command("navigate", { url: "https://example.com" }, 23),
      automationApi
    );

    expect(result.ok).toBe(true);
    expect(calls).toEqual(["attach"]);
    expect(automationApi.sendCommand).toHaveBeenCalledWith(23, "Page.navigate", {
      url: "https://example.com"
    });
  });

  it("delegates recording lifecycle to the browser runtime", async () => {
    const { automationApi } = api();

    const started = await executeChromiumAutomationCommand(
      command("recordingStart", {}, 23),
      automationApi
    );
    const stopped = await executeChromiumAutomationCommand(
      command("recordingStop", {}, 23),
      automationApi
    );

    expect(started).toMatchObject({
      ok: true,
      result: { tabId: 23, recording: true, startedAt: "2026-11-22T00:00:00.000Z" }
    });
    expect(stopped).toMatchObject({
      ok: true,
      result: { tabId: 23, mimeType: "video/webm", dataBase64: "AA==" }
    });
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
