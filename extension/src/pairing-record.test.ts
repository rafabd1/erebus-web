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

  it("keeps Firefox proxy and certificate details", () => {
    expect(
      parsePairingRecord(
        JSON.stringify({
          serverUrl: "http://127.0.0.1:8789",
          token: "token-1",
          sessionId: "session-1",
          proxyUrl: "http://127.0.0.1:48123",
          caPem: "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----"
        })
      )
    ).toMatchObject({
      proxyUrl: "http://127.0.0.1:48123",
      caPem: "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----"
    });
  });

  it("rejects incomplete records", () => {
    expect(() => parsePairingRecord('{"serverUrl":"http://127.0.0.1:8789"}')).toThrow(
      "serverUrl, token, and sessionId",
    );
  });
});
