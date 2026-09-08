import { queryOptions } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";

export function projectsQuery(userId: string) {
  return queryOptions({
    queryKey: ["projects", userId],
    queryFn: ({ signal }) => endpoints.projects(signal),
    staleTime: 15_000,
  });
}
