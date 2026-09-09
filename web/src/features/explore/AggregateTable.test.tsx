import { render, screen, within } from "@testing-library/react";
import { expect, it } from "vitest";
import { AggregateTable } from "./AggregateTable";

it("shows nested bucket metrics and preserves large counts and missing data", () => {
  render(
    <AggregateTable
      buckets={{
        dimension: { group: { field: "service" } },
        has_more: false,
        buckets: [
          {
            key: { type: "string", value: "shop" },
            doc_count: "3",
            metrics: [],
            children: {
              dimension: { group: { field: "level" } },
              has_more: false,
              buckets: ["error", "info"].map((level, i) => ({
                key: { type: "string" as const, value: level },
                doc_count: i ? "2" : "9007199254740993",
                children: null,
                metrics: [
                  {
                    name: "total",
                    op: "sum" as const,
                    value: i ? null : 42.125,
                    numeric_value_count: i ? "0" : "1",
                  },
                ],
              })),
            },
          },
        ],
      }}
    />,
  );
  expect(
    screen.getByRole("columnheader", { name: "total · sum" }),
  ).toBeInTheDocument();
  expect(
    within(screen.getByRole("row", { name: /shop › error/ })).getByText(
      "9007199254740993",
    ),
  ).toBeInTheDocument();
  expect(screen.getByText("42.125")).toBeInTheDocument();
  expect(
    within(screen.getByRole("row", { name: /shop › info/ })).getByText(
      "값 없음",
    ),
  ).toBeInTheDocument();
});
