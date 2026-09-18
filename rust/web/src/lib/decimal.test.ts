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

it("preserves malformed values and both out-of-range timestamp signs", () => {
  expect(formatDecimal("not-an-integer")).toBe("not-an-integer");
  expect(formatTimestampUs("not-a-timestamp")).toBe("not-a-timestamp");
  expect(formatTimestampUs("-9223372036854775807")).toBe(
    "-9223372036854775807 µs",
  );
  expect(formatTimestampUs("1000000")).toBe(
    new Intl.DateTimeFormat("ko-KR", {
      dateStyle: "medium",
      timeStyle: "medium",
    }).format(new Date(1000)),
  );
});
