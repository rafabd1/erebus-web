import { describe, expect, it } from "vitest";

import { bodyFromCdp, normalizeHeaders, unavailableBody } from "../src/normalization";

describe("normalizeHeaders", () => {
  it("preserves duplicate ordered header entries", () => {
    expect(
      normalizeHeaders([
        { name: "Set-Cookie", value: "a=1" },
        { name: "Set-Cookie", value: "b=2" }
      ])
    ).toEqual([
      { name: "Set-Cookie", value: "a=1" },
      { name: "Set-Cookie", value: "b=2" }
    ]);
  });

  it("accepts the dictionary shape returned by CDP", () => {
    expect(normalizeHeaders({ Accept: "application/json", "X-Count": 2 })).toEqual([
      { name: "Accept", value: "application/json" },
      { name: "X-Count", value: "2" }
    ]);
  });
});

describe("bodyFromCdp", () => {
  it("keeps text and base64 encoding explicit", () => {
    expect(bodyFromCdp("hello")).toMatchObject({ encoding: "utf8", state: "complete" });
    expect(bodyFromCdp("aGVsbG8=", true)).toMatchObject({ encoding: "base64", state: "complete" });
  });

  it("records why a body is unavailable", () => {
    expect(unavailableBody("evicted by Chromium")).toEqual({
      encoding: "utf8",
      data: "",
      state: "unavailable",
      reason: "evicted by Chromium"
    });
  });
});

