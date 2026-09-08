import { queryOptions } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";

export function usersQuery(userId: string) {
  return queryOptions({
    queryKey: ["users", userId],
    queryFn: ({ signal }) => endpoints.users(signal),
    staleTime: 15_000,
  });
}
