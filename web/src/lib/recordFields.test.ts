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
