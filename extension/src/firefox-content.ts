type ContentCommand = {
  type: "erebusAutomation";
  operation: string;
  input: Record<string, unknown>;
};

const api = (globalThis as typeof globalThis & { browser?: any }).browser;

api.runtime.onMessage.addListener((message: ContentCommand) => {
  if (message?.type !== "erebusAutomation") return undefined;
  return run(message.operation, message.input ?? {});
});

async function run(operation: string, input: Record<string, unknown>): Promise<unknown> {
  switch (operation) {
    case "status":
      return pageStatus();
    case "snapshot":
      return snapshot();
    case "click": {
      const element = target(input);
      if (!(element instanceof HTMLElement)) throw invalidTarget("The click target was not found.");
      element.scrollIntoView({ block: "center", inline: "center" });
      element.focus();
      element.click();
      return null;
    }
    case "type": {
      const element = target(input);
      if (!(element instanceof HTMLElement)) throw invalidTarget("The input target was not found.");
      const text = typeof input.text === "string" ? input.text : "";
      element.focus();
      if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
        const next = input.clear === true ? text : `${element.value}${text}`;
        const prototype = Object.getPrototypeOf(element) as object;
        const setter = Object.getOwnPropertyDescriptor(prototype, "value")?.set;
        if (setter) setter.call(element, next);
        else element.value = next;
      } else if (element.isContentEditable) {
        element.textContent = input.clear === true ? text : `${element.textContent ?? ""}${text}`;
      } else {
        throw taggedError("PreviewAutomationTargetNotEditableError", "The target is not editable.");
      }
      element.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: text }));
      element.dispatchEvent(new Event("change", { bubbles: true }));
      return null;
    }
    case "press": {
      const key = requiredString(input.key, "key");
      const destination = target(input) ?? document.activeElement ?? document.body;
      if (!destination) throw invalidTarget("The key target was not found.");
      const options = {
        key,
        code: key.length === 1 ? `Key${key.toUpperCase()}` : key,
        altKey: modifier(input, "Alt"),
        ctrlKey: modifier(input, "Control"),
        metaKey: modifier(input, "Meta"),
        shiftKey: modifier(input, "Shift"),
        bubbles: true,
        cancelable: true
      };
      destination.dispatchEvent(new KeyboardEvent("keydown", options));
      destination.dispatchEvent(new KeyboardEvent("keypress", options));
      destination.dispatchEvent(new KeyboardEvent("keyup", options));
      return null;
    }
    case "scroll": {
      const destination = input.selector || input.locator ? target(input) : window;
      if (!destination) throw invalidTarget("The scroll target was not found.");
      const x = optionalNumber(input.deltaX) ?? 0;
      const y = optionalNumber(input.deltaY) ?? 0;
      if (destination === window) window.scrollBy(x, y);
      else (destination as Element).scrollBy(x, y);
      return null;
    }
    case "waitProbe":
      return {
        element: input.selector || input.locator ? Boolean(target(input)) : true,
        text:
          typeof input.text === "string"
            ? (document.body?.innerText ?? "").includes(input.text)
            : true,
        url:
          typeof input.urlIncludes === "string" ? location.href.includes(input.urlIncludes) : true
      };
    default:
      throw taggedError(
        "PreviewAutomationUnsupportedClientError",
        `Firefox content automation does not support ${operation}.`
      );
  }
}

function pageStatus() {
  return {
    url: location.href,
    title: document.title,
    loading: document.readyState === "loading",
    width: Math.max(1, Math.round(innerWidth)),
    height: Math.max(1, Math.round(innerHeight))
  };
}

function snapshot() {
  const visible = (element: Element) => {
    const style = getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
  };
  const candidates = Array.from(
    document.querySelectorAll('a[href],button,input,select,textarea,[role],[contenteditable="true"],[tabindex]')
  );
  const interactiveElements = candidates
    .filter(visible)
    .slice(0, 500)
    .map((element) => {
      const rect = element.getBoundingClientRect();
      const html = element as HTMLElement;
      return {
        tag: element.tagName.toLowerCase(),
        role: roleOf(element),
        name: nameOf(html),
        selector: selectorOf(element),
        x: rect.x,
        y: rect.y,
        width: rect.width,
        height: rect.height
      };
    });
  return {
    ...pageStatus(),
    visibleText: (document.body?.innerText ?? "").slice(0, 200_000),
    interactiveElements,
    accessibilityTree: {
      role: "document",
      name: document.title,
      children: interactiveElements.map(({ role, name, selector }) => ({ role, name, selector }))
    }
  };
}

function target(input: Record<string, unknown>): Element | null {
  if (typeof input.selector === "string") return document.querySelector(input.selector);
  if (typeof input.locator !== "string") return null;
  const locator = input.locator;
  if (locator.startsWith("text=")) {
    const text = locator.slice(5);
    return (
      Array.from(document.querySelectorAll("body *")).find(
        (element) => (element.textContent ?? "").trim() === text
      ) ?? null
    );
  }
  const role = /^role=([^\[]+)(?:\[name=['"]?(.*?)['"]?\])?$/.exec(locator);
  if (role) {
    const expectedRole = role[1]?.trim();
    const expectedName = role[2]?.trim();
    return (
      Array.from(document.querySelectorAll("body *")).find((element) => {
        if (roleOf(element) !== expectedRole) return false;
        return !expectedName || nameOf(element as HTMLElement) === expectedName;
      }) ?? null
    );
  }
  return document.querySelector(locator);
}

function roleOf(element: Element): string | null {
  return (
    element.getAttribute("role") ||
    ({ A: "link", BUTTON: "button", INPUT: "textbox", SELECT: "combobox", TEXTAREA: "textbox" } as Record<
      string,
      string
    >)[element.tagName] ||
    null
  );
}

function nameOf(element: HTMLElement): string {
  const value = element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement ? element.value : "";
  return (
    element.getAttribute("aria-label") ||
    element.getAttribute("title") ||
    element.innerText ||
    value ||
    element.getAttribute("name") ||
    ""
  )
    .trim()
    .slice(0, 500);
}

function selectorOf(element: Element): string {
  if (element.id) return `#${CSS.escape(element.id)}`;
  const parts: string[] = [];
  let node: Element | null = element;
  while (node && node !== document.documentElement) {
    let part = node.tagName.toLowerCase();
    const siblings = node.parentElement
      ? Array.from(node.parentElement.children).filter((item) => item.tagName === node!.tagName)
      : [];
    if (siblings.length > 1) part += `:nth-of-type(${siblings.indexOf(node) + 1})`;
    parts.unshift(part);
    node = node.parentElement;
  }
  return parts.join(" > ");
}

function modifier(input: Record<string, unknown>, name: string): boolean {
  return Array.isArray(input.modifiers) && input.modifiers.map(String).includes(name);
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) throw new Error(`${name} is required`);
  return value;
}

function optionalNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function invalidTarget(message: string): Error {
  return taggedError("PreviewAutomationInvalidSelectorError", message);
}

function taggedError(tag: string, message: string): Error {
  const error = new Error(message) as Error & { tag?: string };
  error.tag = tag;
  return error;
}
