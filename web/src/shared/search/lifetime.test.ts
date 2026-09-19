import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";

import { cancelDataset, datasetQueryPrefix } from "./lifetime";

describe("dataset query lifetime", () => {
  it("aborts and removes only the previous dataset", async () => {
    const client = new QueryClient();
    const aborted = vi.fn();
    const pending = client.fetchQuery({
      queryKey: [...datasetQueryPrefix("7", "9", "old"), "new", "rows"],
      queryFn: ({ signal }) => new Promise<never>((_, reject) => signal.addEventListener("abort", () => { aborted(); reject(signal.reason); }, { once: true })),
    });
    client.setQueryData([...datasetQueryPrefix("7", "9", "new"), "new", "rows"], { rows: [] });
    await cancelDataset(client, "7", "9", "old");
    await expect(pending).rejects.toBeDefined();
    expect(aborted).toHaveBeenCalledOnce();
    expect(client.getQueriesData({ queryKey: datasetQueryPrefix("7", "9", "old") })).toHaveLength(0);
    expect(client.getQueriesData({ queryKey: datasetQueryPrefix("7", "9", "new") })).toHaveLength(1);
  });
});
