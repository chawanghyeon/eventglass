import { describe, expect, it } from "vitest";

import { decodeSearchURL, defaultSearchState, encodeSearchURL } from "./url";

describe("search URL ownership", () => {
  it("canonicalizes project and kind scope without tokens or cursors", () => {
    const state = decodeSearchURL(new URLSearchParams("projects=9,2,9&kinds=log,error&start=1&end=10&q=service%3D%3D%27api%27"), defaultSearchState(1000));
    const encoded = encodeSearchURL(state);
    expect(encoded.get("projects")).toBe("2,9");
    expect(encoded.get("kinds")).toBe("error,log");
    expect(encoded.has("read_token")).toBe(false);
    expect(encoded.has("cursor")).toBe(false);
  });

  it("rejects noncanonical or relative ranges", () => {
    expect(() => decodeSearchURL(new URLSearchParams("start=01&end=2"), defaultSearchState(1000))).toThrow();
    expect(() => decodeSearchURL(new URLSearchParams("start=2&end=2"), defaultSearchState(1000))).toThrow();
  });
});
