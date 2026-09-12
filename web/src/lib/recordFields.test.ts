import { expect, it } from "vitest";
import { sampleFields } from "./recordFields";

it("distinguishes literal dots, nested paths and mixed numeric/string values", () => {
  const sample = sampleFields({
    "http.status": 500,
    http: { status: "500" },
    values: [false, null],
  });
  expect(sample.rows).toEqual([
    { path: '$["http.status"]', type: "number", value: "500" },
    { path: '$["http"]["status"]', type: "string", value: "500" },
    { path: '$["values"][0]', type: "boolean", value: "false" },
    { path: '$["values"][1]', type: "null", value: "null" },
  ]);
  expect(sample.truncated).toBe(false);
});

it("bounds display work and makes omitted fields explicit", () => {
  const sample = sampleFields(Array.from({ length: 1000 }, (_, i) => i));
  expect(sample.rows).toHaveLength(200);
  expect(sample.truncated).toBe(true);
});

it("shows empty collections and truncates text without changing original values", () => {
  const value = "x".repeat(241);
  const sample = sampleFields({ empty: [], object: {}, text: value });
  expect(sample.rows.map((row) => row.value)).toEqual([
    "[]",
    "{}",
    "x".repeat(240) + "…",
  ]);
  expect(value).toHaveLength(241);
  expect(sample.truncated).toBe(false);
});
it("marks depth exhaustion and row overflow while accepting an exact full sample", () => {
  let deep: unknown = "value";
  for (let i = 0; i < 14; i++) deep = { child: deep };
  expect(sampleFields(deep)).toEqual({ rows: [], truncated: true });
  const exact = Array.from({ length: 200 }, (_, i) => i);
  expect(sampleFields(exact).truncated).toBe(false);
  expect(
    sampleFields(Object.fromEntries(exact.map((i) => [String(i), i])))
      .truncated,
  ).toBe(false);
  const oversized = Object.fromEntries(
    Array.from({ length: 201 }, (_, i) => [String(i), i]),
  );
  expect(sampleFields(oversized).rows).toHaveLength(200);
  expect(sampleFields(oversized).truncated).toBe(true);
});
