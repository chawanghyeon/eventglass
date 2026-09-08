import { describe, expect, it } from "vitest";
import { formatDecimal, formatTimestampUs } from "./decimal";

describe("decimal string formatting", () => {
  it("preserves integers beyond JavaScript's safe Number range", () => {
    expect(formatDecimal("9007199254740993")).toBe("9,007,199,254,740,993");
  });

  it("keeps out-of-range timestamps as exact microseconds", () => {
    expect(formatTimestampUs("9223372036854775807")).toBe(
      "9223372036854775807 µs",
    );
  });
});
