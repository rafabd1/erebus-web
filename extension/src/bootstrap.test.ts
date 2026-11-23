import { describe, expect, it } from "vitest";

import { configFromBootstrap } from "./bootstrap";

describe("extension bootstrap", () => {
  it("preserves the sequence only while reconnecting to the same session", () => {
    const previous = {
      serverUrl: "http://127.0.0.1:8789",
      token: "old-token",
      sessionId: "session-a",
      producerId: "install-a",
      nextSequence: 42
    };

    expect(
      configFromBootstrap(
        {
          serverUrl: "http://127.0.0.1:8789",
          token: "new-token",
          sessionId: "session-a"
        },
        "install-a",
        previous
      ).nextSequence
    ).toBe(42);

    expect(
      configFromBootstrap(
        {
          serverUrl: "http://127.0.0.1:8790",
          token: "other-token",
          sessionId: "session-b"
        },
        "install-a",
        previous
      ).nextSequence
    ).toBe(1);
  });
});
