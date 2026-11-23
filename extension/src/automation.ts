import type { ExtensionAutomationCommand, ExtensionAutomationResult } from "./protocol";

export interface ChromiumAutomationApi {
  attachedTabs(): Promise<chrome.tabs.Tab[]>;
  ensureAttached(tabId: number): Promise<void>;
  createTab(url?: string): Promise<chrome.tabs.Tab>;
  getTab(tabId: number): Promise<chrome.tabs.Tab>;
  updateTab(tabId: number, update: chrome.tabs.UpdateProperties): Promise<chrome.tabs.Tab>;
  closeTab(tabId: number): Promise<void>;
  sendCommand(
    tabId: number,
    method: string,
    params?: Record<string, unknown>
  ): Promise<Record<string, unknown>>;
}

type CommandInput = Record<string, unknown>;

const PRESET_VIEWPORTS: Record<string, { width: number; height: number }> = {
  "iphone-se": { width: 375, height: 667 },
  "iphone-14-pro-max": { width: 430, height: 932 },
  "iphone-15-pro": { width: 393, height: 852 },
  "pixel-7": { width: 412, height: 915 },
  "ipad-mini": { width: 768, height: 1024 },
  "ipad-pro-11": { width: 834, height: 1194 },
  "desktop-1280": { width: 1280, height: 720 },
  "desktop-1440": { width: 1440, height: 900 }
};

export async function executeChromiumAutomationCommand(
  command: ExtensionAutomationCommand,
  api: ChromiumAutomationApi
): Promise<ExtensionAutomationResult> {
  try {
    const result = await execute(command, api);
    return { commandId: command.id, ok: true, result };
  } catch (cause) {
    return {
      commandId: command.id,
      ok: false,
      error: automationError(cause)
    };
  }
}

async function execute(
  command: ExtensionAutomationCommand,
  api: ChromiumAutomationApi
): Promise<unknown> {
  const input = command.input ?? {};
  if (command.operation === "tabsList") {
    const tabs = await api.attachedTabs();
    return {
      tabs: tabs.map((tab) => ({
        tabId: tab.id,
        url: tab.url ?? null,
        title: tab.title ?? null,
        active: tab.active,
        loading: tab.status === "loading"
      }))
    };
  }

  if (command.operation === "open") {
    const reuse = input.reuseExistingTab !== false && command.tabId !== undefined;
    const url = optionalString(input.url);
    const tab = reuse
      ? await api.updateTab(command.tabId!, {
          ...(url ? { url } : {}),
          active: input.open !== false
        })
      : await api.createTab("about:blank");
    if (tab.id === undefined) throw new Error("browser did not return a tab id");
    await api.ensureAttached(tab.id);
    if (!reuse && url && url !== "about:blank") {
      await api.updateTab(tab.id, { url, active: input.open !== false });
    }
    await waitForReadiness(tab.id, api, "load", command.timeoutMs);
    return status(tab.id, api);
  }

  const tabId = requiredTabId(command);
  if (command.operation === "navigate") {
    const url = requiredString(input.url, "url");
    await api.updateTab(tabId, { url });
    await api.ensureAttached(tabId);
    await waitForReadiness(
      tabId,
      api,
      optionalString(input.readiness) ?? "load",
      optionalNumber(input.timeoutMs) ?? command.timeoutMs
    );
    return status(tabId, api);
  }
  await api.ensureAttached(tabId);
  switch (command.operation) {
    case "status":
      return status(tabId, api);
    case "tabClose":
      await api.closeTab(tabId);
      return { closedTabId: tabId, tabId: null };
    case "back":
      await navigateHistory(tabId, api, -1);
      await waitForReadiness(tabId, api, "load", command.timeoutMs);
      return status(tabId, api);
    case "forward":
      await navigateHistory(tabId, api, 1);
      await waitForReadiness(tabId, api, "load", command.timeoutMs);
      return status(tabId, api);
    case "reload":
      await api.sendCommand(tabId, "Page.reload");
      await waitForReadiness(tabId, api, "load", command.timeoutMs);
      return status(tabId, api);
    case "resize":
      return resize(tabId, input, api);
    case "setColorScheme":
      return setColorScheme(tabId, input, api);
    case "snapshot":
      return snapshot(tabId, api);
    case "click":
      await click(tabId, input, api);
      return undefined;
    case "type":
      await typeInto(tabId, input, api);
      return undefined;
    case "press":
      await press(tabId, input, api);
      return undefined;
    case "scroll":
      await scroll(tabId, input, api);
      return undefined;
    case "evaluate":
      return evaluate(tabId, input, api);
    case "waitFor":
      await waitForConditions(tabId, input, api, command.timeoutMs);
      return undefined;
    case "recordingStart":
    case "recordingStop":
      throw taggedError(
        "PreviewAutomationUnsupportedClientError",
        "Recording is available in the Erebus browser. The browser extension does not request tab-capture permission."
      );
    default:
      throw taggedError(
        "PreviewAutomationUnsupportedClientError",
        `The browser extension does not support ${command.operation}.`
      );
  }
}

async function status(tabId: number, api: ChromiumAutomationApi): Promise<Record<string, unknown>> {
  const tab = await api.getTab(tabId);
  const page = await runtimeValue(tabId, api, `(() => ({
    width: Math.max(1, Math.round(window.innerWidth)),
    height: Math.max(1, Math.round(window.innerHeight)),
    readyState: document.readyState
  }))()`);
  const viewport = objectValue(page);
  return {
    available: true,
    visible: tab.active,
    tabId,
    url: tab.url ?? null,
    title: tab.title ?? null,
    loading: tab.status === "loading" || viewport.readyState === "loading",
    viewportSetting: { mode: "fill" },
    viewport: {
      width: finiteInteger(viewport.width, 1),
      height: finiteInteger(viewport.height, 1)
    }
  };
}

async function navigateHistory(
  tabId: number,
  api: ChromiumAutomationApi,
  delta: number
): Promise<void> {
  const history = await api.sendCommand(tabId, "Page.getNavigationHistory");
  const currentIndex = finiteInteger(history.currentIndex, 0);
  const entries = Array.isArray(history.entries) ? history.entries : [];
  const entry = entries[currentIndex + delta] as Record<string, unknown> | undefined;
  if (!entry || typeof entry.id !== "number") return;
  await api.sendCommand(tabId, "Page.navigateToHistoryEntry", { entryId: entry.id });
}

async function resize(
  tabId: number,
  input: CommandInput,
  api: ChromiumAutomationApi
): Promise<Record<string, unknown>> {
  const mode = requiredString(input.mode, "mode");
  if (mode === "fill") {
    await api.sendCommand(tabId, "Emulation.clearDeviceMetricsOverride");
  } else {
    let width = optionalNumber(input.width);
    let height = optionalNumber(input.height);
    if (mode === "preset") {
      const preset = PRESET_VIEWPORTS[requiredString(input.preset, "preset")];
      if (!preset) throw new Error("unknown viewport preset");
      width = preset.width;
      height = preset.height;
      if (input.orientation === "landscape") [width, height] = [height, width];
    }
    if (!width || !height) throw new Error("viewport width and height are required");
    await api.sendCommand(tabId, "Emulation.setDeviceMetricsOverride", {
      width: Math.round(width),
      height: Math.round(height),
      deviceScaleFactor: 1,
      mobile: mode === "preset" && !String(input.preset).startsWith("desktop-")
    });
  }
  const current = await status(tabId, api);
  return {
    tabId,
    setting:
      mode === "fill"
        ? { mode: "fill" }
        : mode === "freeform"
          ? { mode, width: input.width, height: input.height }
          : { mode, preset: input.preset, ...(input.orientation ? { orientation: input.orientation } : {}) },
    viewport: current.viewport
  };
}

async function setColorScheme(
  tabId: number,
  input: CommandInput,
  api: ChromiumAutomationApi
): Promise<Record<string, unknown>> {
  const colorScheme = requiredString(input.colorScheme, "colorScheme");
  await api.sendCommand(tabId, "Emulation.setEmulatedMedia", {
    features:
      colorScheme === "system"
        ? []
        : [{ name: "prefers-color-scheme", value: colorScheme }]
  });
  return { tabId, colorScheme };
}

async function snapshot(tabId: number, api: ChromiumAutomationApi): Promise<Record<string, unknown>> {
  const payload = objectValue(
    await runtimeValue(
      tabId,
      api,
      `(() => {
        const visible = (el) => {
          const style = getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
        };
        const roleOf = (el) => el.getAttribute('role') || ({A:'link',BUTTON:'button',INPUT:'textbox',SELECT:'combobox',TEXTAREA:'textbox'}[el.tagName] || null);
        const nameOf = (el) => (el.getAttribute('aria-label') || el.getAttribute('title') || el.innerText || el.value || el.getAttribute('name') || '').trim().slice(0, 500);
        const selectorOf = (el) => {
          if (el.id) return '#' + CSS.escape(el.id);
          const parts = [];
          let node = el;
          while (node && node.nodeType === Node.ELEMENT_NODE && node !== document.documentElement) {
            let part = node.tagName.toLowerCase();
            const siblings = node.parentElement ? Array.from(node.parentElement.children).filter((item) => item.tagName === node.tagName) : [];
            if (siblings.length > 1) part += ':nth-of-type(' + (siblings.indexOf(node) + 1) + ')';
            parts.unshift(part);
            node = node.parentElement;
          }
          return parts.join(' > ');
        };
        const candidates = Array.from(document.querySelectorAll('a[href],button,input,select,textarea,[role],[contenteditable="true"],[tabindex]'));
        const interactiveElements = candidates.filter(visible).slice(0, 500).map((el) => {
          const rect = el.getBoundingClientRect();
          return { tag: el.tagName.toLowerCase(), role: roleOf(el), name: nameOf(el), selector: selectorOf(el), x: rect.x, y: rect.y, width: rect.width, height: rect.height };
        });
        return {
          url: location.href,
          title: document.title,
          loading: document.readyState === 'loading',
          visibleText: (document.body?.innerText || '').slice(0, 200000),
          interactiveElements,
          accessibilityTree: { role: 'document', name: document.title, children: interactiveElements.map(({role,name,selector}) => ({role,name,selector})) },
          width: Math.max(1, Math.round(innerWidth)),
          height: Math.max(1, Math.round(innerHeight))
        };
      })()`
    )
  );
  const screenshot = await api.sendCommand(tabId, "Page.captureScreenshot", {
    format: "png",
    fromSurface: true,
    captureBeyondViewport: false
  });
  return {
    url: String(payload.url ?? ""),
    title: String(payload.title ?? ""),
    loading: payload.loading === true,
    visibleText: String(payload.visibleText ?? ""),
    interactiveElements: Array.isArray(payload.interactiveElements) ? payload.interactiveElements : [],
    accessibilityTree: payload.accessibilityTree ?? null,
    consoleEntries: [],
    networkEntries: [],
    actionTimeline: [],
    ...(typeof screenshot.data === "string"
      ? {
          screenshot: {
            mimeType: "image/png",
            data: screenshot.data,
            width: finiteInteger(payload.width, 1),
            height: finiteInteger(payload.height, 1)
          }
        }
      : {})
  };
}

async function click(tabId: number, input: CommandInput, api: ChromiumAutomationApi): Promise<void> {
  let point: { x: number; y: number };
  if (typeof input.x === "number" && typeof input.y === "number") {
    point = { x: input.x, y: input.y };
  } else {
    const rect = objectValue(await runtimeValue(tabId, api, elementPointExpression(input)));
    point = { x: Number(rect.x), y: Number(rect.y) };
  }
  if (!Number.isFinite(point.x) || !Number.isFinite(point.y)) {
    throw taggedError("PreviewAutomationInvalidSelectorError", "The click target was not found.");
  }
  await api.sendCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mousePressed",
    x: point.x,
    y: point.y,
    button: "left",
    clickCount: 1
  });
  await api.sendCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mouseReleased",
    x: point.x,
    y: point.y,
    button: "left",
    clickCount: 1
  });
}

async function typeInto(
  tabId: number,
  input: CommandInput,
  api: ChromiumAutomationApi
): Promise<void> {
  const text = typeof input.text === "string" ? input.text : "";
  const target = targetExpression(input);
  const script = `(() => {
    const el = ${target};
    if (!el) return {ok:false, reason:'not-found'};
    if (!('value' in el) && !el.isContentEditable) return {ok:false, reason:'not-editable'};
    el.focus();
    const text = ${JSON.stringify(text)};
    const clear = ${input.clear === true};
    if (el.isContentEditable) {
      el.textContent = clear ? text : (el.textContent || '') + text;
    } else {
      const next = clear ? text : String(el.value || '') + text;
      const proto = Object.getPrototypeOf(el);
      const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
      setter ? setter.call(el, next) : (el.value = next);
    }
    el.dispatchEvent(new InputEvent('input', {bubbles:true, inputType:'insertText', data:text}));
    el.dispatchEvent(new Event('change', {bubbles:true}));
    return {ok:true};
  })()`;
  const result = objectValue(await runtimeValue(tabId, api, script));
  if (result.ok !== true) {
    throw taggedError(
      result.reason === "not-editable"
        ? "PreviewAutomationTargetNotEditableError"
        : "PreviewAutomationInvalidSelectorError",
      result.reason === "not-editable" ? "The target is not editable." : "The input target was not found."
    );
  }
}

async function press(tabId: number, input: CommandInput, api: ChromiumAutomationApi): Promise<void> {
  const key = requiredString(input.key, "key");
  const modifiers = Array.isArray(input.modifiers) ? input.modifiers.map(String) : [];
  const modifierMask =
    (modifiers.includes("Alt") ? 1 : 0) |
    (modifiers.includes("Control") ? 2 : 0) |
    (modifiers.includes("Meta") ? 4 : 0) |
    (modifiers.includes("Shift") ? 8 : 0);
  const params = { key, code: keyCode(key), modifiers: modifierMask };
  await api.sendCommand(tabId, "Input.dispatchKeyEvent", { type: "rawKeyDown", ...params });
  if (key.length === 1) {
    await api.sendCommand(tabId, "Input.dispatchKeyEvent", { type: "char", text: key, ...params });
  }
  await api.sendCommand(tabId, "Input.dispatchKeyEvent", { type: "keyUp", ...params });
}

async function scroll(tabId: number, input: CommandInput, api: ChromiumAutomationApi): Promise<void> {
  const target = input.selector || input.locator ? targetExpression(input) : "window";
  const script = `(() => {
    const target = ${target};
    if (!target) return false;
    target.scrollBy(${optionalNumber(input.deltaX) ?? 0}, ${optionalNumber(input.deltaY) ?? 0});
    return true;
  })()`;
  if ((await runtimeValue(tabId, api, script)) !== true) {
    throw taggedError("PreviewAutomationInvalidSelectorError", "The scroll target was not found.");
  }
}

async function evaluate(
  tabId: number,
  input: CommandInput,
  api: ChromiumAutomationApi
): Promise<unknown> {
  return runtimeValue(tabId, api, requiredString(input.expression, "expression"), {
    awaitPromise: input.awaitPromise !== false,
    returnByValue: input.returnByValue !== false
  });
}

async function waitForConditions(
  tabId: number,
  input: CommandInput,
  api: ChromiumAutomationApi,
  defaultTimeoutMs: number
): Promise<void> {
  const timeoutMs = optionalNumber(input.timeoutMs) ?? defaultTimeoutMs;
  const deadline = Date.now() + timeoutMs;
  const target = input.selector || input.locator ? targetExpression(input) : null;
  while (Date.now() <= deadline) {
    const matches = await runtimeValue(
      tabId,
      api,
      `(() => {
        const elementOk = ${target ? `Boolean(${target})` : "true"};
        const textOk = ${typeof input.text === "string" ? `(document.body?.innerText || '').includes(${JSON.stringify(input.text)})` : "true"};
        const urlOk = ${typeof input.urlIncludes === "string" ? `location.href.includes(${JSON.stringify(input.urlIncludes)})` : "true"};
        return elementOk && textOk && urlOk;
      })()`
    );
    if (matches === true) return;
    await delay(100);
  }
  throw taggedError("PreviewAutomationTimeoutError", "The browser condition did not match before timeout.");
}

async function waitForReadiness(
  tabId: number,
  api: ChromiumAutomationApi,
  readiness: string,
  timeoutMs: number
): Promise<void> {
  if (readiness === "none") return;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const tab = await api.getTab(tabId);
    const state = String(
      await runtimeValue(tabId, api, "document.readyState").catch(() => "loading")
    );
    if (
      readiness === "domContentLoaded"
        ? state === "interactive" || state === "complete"
        : tab.status !== "loading" && state === "complete"
    ) {
      return;
    }
    await delay(100);
  }
  throw taggedError("PreviewAutomationTimeoutError", "Navigation did not become ready before timeout.");
}

async function runtimeValue(
  tabId: number,
  api: ChromiumAutomationApi,
  expression: string,
  options: { awaitPromise?: boolean; returnByValue?: boolean } = {}
): Promise<unknown> {
  const response = await api.sendCommand(tabId, "Runtime.evaluate", {
    expression,
    awaitPromise: options.awaitPromise ?? true,
    returnByValue: options.returnByValue ?? true,
    userGesture: true
  });
  if (response.exceptionDetails) {
    const detail = response.exceptionDetails as Record<string, unknown>;
    throw new Error(String(detail.text ?? "page evaluation failed"));
  }
  const result = response.result as Record<string, unknown> | undefined;
  return result?.value ?? result?.description ?? null;
}

function targetExpression(input: CommandInput): string {
  if (typeof input.selector === "string") {
    return `document.querySelector(${JSON.stringify(input.selector)})`;
  }
  const locator = requiredString(input.locator, "locator");
  if (locator.startsWith("text=")) {
    const text = locator.slice(5);
    return `Array.from(document.querySelectorAll('body *')).find((el) => (el.innerText || '').trim() === ${JSON.stringify(text)} || (el.innerText || '').includes(${JSON.stringify(text)}))`;
  }
  const role = /^role=([^\[]+)(?:\[name=(?:'([^']*)'|"([^"]*)")\])?$/.exec(locator);
  if (role) {
    const roleName = role[1] ?? "";
    const name = role[2] ?? role[3];
    const selectors: Record<string, string> = {
      button: "button,[role='button'],input[type='button'],input[type='submit']",
      link: "a[href],[role='link']",
      textbox: "input:not([type]),input[type='text'],input[type='email'],input[type='search'],input[type='url'],input[type='password'],textarea,[role='textbox']",
      checkbox: "input[type='checkbox'],[role='checkbox']",
      radio: "input[type='radio'],[role='radio']",
      combobox: "select,[role='combobox']"
    };
    const selector = selectors[roleName] ?? `[role=${JSON.stringify(roleName)}]`;
    if (name === undefined) return `document.querySelector(${JSON.stringify(selector)})`;
    return `Array.from(document.querySelectorAll(${JSON.stringify(selector)})).find((el) => ((el.getAttribute('aria-label') || el.getAttribute('title') || el.innerText || el.value || '').trim() === ${JSON.stringify(name)}))`;
  }
  return `document.querySelector(${JSON.stringify(locator)})`;
}

function elementPointExpression(input: CommandInput): string {
  return `(() => {
    const el = ${targetExpression(input)};
    if (!el) return null;
    el.scrollIntoView({block:'center', inline:'center'});
    const rect = el.getBoundingClientRect();
    return {x: rect.left + rect.width / 2, y: rect.top + rect.height / 2};
  })()`;
}

function requiredTabId(command: ExtensionAutomationCommand): number {
  if (command.tabId === undefined) throw new Error("tab id is required");
  return command.tabId;
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new Error(`${name} is required`);
  return value;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function optionalNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function finiteInteger(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(1, Math.round(value)) : fallback;
}

function objectValue(value: unknown): Record<string, any> {
  return typeof value === "object" && value !== null ? (value as Record<string, any>) : {};
}

function keyCode(key: string): string {
  if (key.length === 1) return `Key${key.toUpperCase()}`;
  return key === " " ? "Space" : key;
}

function taggedError(tag: string, message: string, detail?: unknown): Error {
  const error = new Error(message) as Error & { tag?: string; detail?: unknown };
  error.tag = tag;
  if (detail !== undefined) error.detail = detail;
  return error;
}

function automationError(cause: unknown): {
  tag: string;
  message: string;
  detail?: unknown;
} {
  if (cause instanceof Error) {
    const tagged = cause as Error & { tag?: string; detail?: unknown };
    return {
      tag: tagged.tag ?? "PreviewAutomationExecutionError",
      message: cause.message,
      ...(tagged.detail === undefined ? {} : { detail: tagged.detail })
    };
  }
  return { tag: "PreviewAutomationExecutionError", message: String(cause) };
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
