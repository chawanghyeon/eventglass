import { describe, expect, it } from "vitest";

import { formatInt64, microsFromMilliseconds, parseInt64 } from "./int64";

describe("int64 formatting", () => {
  it("keeps values beyond JavaScript's safe integer exact", () => {
    expect(parseInt64("9223372036854775807")).toBe(9223372036854775807n);
    expect(formatInt64("9007199254740993").replaceAll(/\D/g, "")).toBe("9007199254740993");
  });

  it("converts absolute wall time without a floating microsecond", () => {
    expect(microsFromMilliseconds(1_700_000_000_123)).toBe("1700000000123000");
    expect(() => parseInt64("01")).toThrow();
  });
});
