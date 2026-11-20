import { describe, expect, it } from "vitest";

import { applyExtraInfo, type ExtraInfoStage } from "../src/extra-info";

describe("applyExtraInfo", () => {
  it("keeps request and response extra headers on their redirect ordinal", () => {
    const stages: ExtraInfoStage[] = [
      { expectsResponseExtra: true },
      { expectsResponseExtra: false },
      { expectsResponseExtra: true }
    ];
    const requests = [{ headers: { cookie: "first" } }, { headers: {} }, { headers: { cookie: "last" } }];
    const responses = [{ statusCode: 302 }, { statusCode: 200 }];

    applyExtraInfo(stages, requests, responses);

    expect(stages[0]?.extraRequest).toBe(requests[0]);
    expect(stages[2]?.extraRequest).toBe(requests[2]);
    expect(stages[0]?.extraResponse).toBe(responses[0]);
    expect(stages[1]?.extraResponse).toBeUndefined();
    expect(stages[2]?.extraResponse).toBe(responses[1]);
  });

  it("retains a lone response extra event when expectation metadata is absent", () => {
    const stages: ExtraInfoStage[] = [{}];
    const response = { statusCode: 200 };
    applyExtraInfo(stages, [], [response]);
    expect(stages[0]?.extraResponse).toBe(response);
  });
});
