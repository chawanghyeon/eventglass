import type { QueryClient } from "@tanstack/react-query";

export function datasetQueryPrefix(userID: string, tenantID: string, key: string) {
  return ["query", userID, tenantID, key] as const;
}

export async function cancelDataset(client: QueryClient, userID: string, tenantID: string, key: string): Promise<void> {
  const queryKey = datasetQueryPrefix(userID, tenantID, key);
  await client.cancelQueries({ queryKey });
  client.removeQueries({ queryKey });
}
