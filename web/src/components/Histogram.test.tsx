import { render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import { Histogram } from "./Histogram";

it("retains every bucket while bounding dense chart labels", () => {
  const { container } = render(
    <Histogram
      label="Dense chart"
      buckets={{
        dimension: { histogram: { interval_ms: 300000 } },
        has_more: false,
        buckets: Array.from({ length: 289 }, (_, i) => ({
          key: {
            type: "timestamp" as const,
            timestamp_us: String(1788825600000000n + BigInt(i) * 300000000n),
          },
          doc_count: i === 100 ? "9007199254740993" : "0",
          metrics: [],
          children: null,
        })),
      }}
    />,
  );
  expect(screen.getAllByRole("listitem")).toHaveLength(289);
  expect(screen.getByLabelText(/9007199254740993건/)).toHaveAttribute(
    "title",
    expect.stringContaining("9007199254740993건"),
  );
  expect(container.querySelectorAll("time")).toHaveLength(2);
  expect(container.querySelectorAll(".histogram__value")).toHaveLength(0);
  expect(container.querySelectorAll(".histogram__bar")[100]).toHaveStyle({
    height: "100%",
  });
});
