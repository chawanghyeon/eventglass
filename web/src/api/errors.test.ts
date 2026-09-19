import { describe, expect, it } from "vitest";

import { ApiFailure } from "./errors";

const body = { code: "test", message: "safe", retryable: false, request_id: "00000000-0000-4000-8000-000000000000" };

describe("API status semantics", () => {
  it.each([[401, "unauthenticated"], [403, "forbidden"], [409, "conflict"], [410, "expired"]] as const)("maps %d to %s", (status, kind) => {
    expect(new ApiFailure(status, body).kind).toBe(kind);
  });
});
