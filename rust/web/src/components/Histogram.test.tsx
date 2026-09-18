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

it("renders empty non-time data and a single zero-count time bucket safely", () => {
  const { container, rerender } = render(
    <Histogram
      label="Empty"
      buckets={{
        dimension: { histogram: { interval_ms: 1000 } },
        has_more: false,
        buckets: [
          {
            key: { type: "string", value: "other" },
            doc_count: "1",
            metrics: [],
            children: null,
          },
        ],
      }}
    />,
  );
  expect(screen.getByText("표시할 시간 구간이 없습니다.")).toBeInTheDocument();
  rerender(
    <Histogram
      label="Zero"
      buckets={{
        dimension: { histogram: { interval_ms: 1000 } },
        has_more: false,
        buckets: [
          {
            key: { type: "timestamp", timestamp_us: "0" },
            doc_count: "0",
            metrics: [],
            children: null,
          },
        ],
      }}
    />,
  );
  expect(screen.getAllByRole("listitem")).toHaveLength(1);
  expect(container.querySelectorAll("time")).toHaveLength(1);
  expect(container.querySelector(".histogram__bar")).toHaveStyle({
    height: "0%",
  });
  expect(container.querySelector(".histogram__value")).toHaveTextContent("0");
});
