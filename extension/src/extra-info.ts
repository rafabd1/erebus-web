export interface ExtraInfoStage {
  extraRequest?: Record<string, unknown>;
  extraResponse?: Record<string, unknown>;
  expectsResponseExtra?: boolean;
}

export function applyExtraInfo(
  stages: ExtraInfoStage[],
  requestExtras: Record<string, unknown>[],
  responseExtras: Record<string, unknown>[]
): void {
  for (let index = 0; index < requestExtras.length && index < stages.length; index += 1) {
    const extra = requestExtras[index];
    if (extra) stages[index]!.extraRequest ??= extra;
  }
  const expected = stages.filter((stage) => stage.expectsResponseExtra === true);
  for (let index = 0; index < responseExtras.length && index < expected.length; index += 1) {
    const extra = responseExtras[index];
    if (extra) expected[index]!.extraResponse ??= extra;
  }
  if (expected.length === 0 && stages.length === 1 && responseExtras[0]) {
    stages[0]!.extraResponse ??= responseExtras[0];
  }
}
