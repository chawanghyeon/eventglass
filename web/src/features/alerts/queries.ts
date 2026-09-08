import { queryOptions } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";

export const alertKeys = {
  all: (userId: string) => ["alerts", userId] as const,
  deliveries: (userId: string) => ["alert-deliveries", userId] as const,
};

export function alertsQuery(userId: string) {
  return queryOptions({
    queryKey: alertKeys.all(userId),
    queryFn: ({ signal }) => endpoints.alerts(signal),
    refetchInterval: 15_000,
  });
}

export function alertDeliveriesQuery(userId: string) {
  return queryOptions({
    queryKey: alertKeys.deliveries(userId),
    queryFn: ({ signal }) => endpoints.alertDeliveries(signal),
    refetchInterval: 5_000,
  });
}
