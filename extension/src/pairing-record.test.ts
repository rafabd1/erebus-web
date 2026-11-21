import { describe, expect, it } from "vitest";

import { parsePairingRecord } from "./pairing-record";

describe("parsePairingRecord", () => {
  it("reads the record copied by Erebus", () => {
    expect(
      parsePairingRecord(
        JSON.stringify({
          serverUrl: "http://127.0.0.1:8789",
          token: "token-1",
          sessionId: "session-1",
        }),
      ),
    ).toEqual({
      serverUrl: "http://127.0.0.1:8789",
      token: "token-1",
      sessionId: "session-1",
    });
  });

  it("rejects incomplete records", () => {
    expect(() => parsePairingRecord('{"serverUrl":"http://127.0.0.1:8789"}')).toThrow(
      "serverUrl, token, and sessionId",
    );
  });
});
