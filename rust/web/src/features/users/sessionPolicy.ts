import type { QueryClient } from "@tanstack/react-query";
import { setCsrfToken } from "../../api/client";
import type { Session, UpdateUserInput } from "../../api/types";

export function updateRevokesCurrentSession(
  session: Session,
  userId: string,
  update: UpdateUserInput,
): boolean {
  return (
    session.id === userId &&
    ((update.role !== undefined && update.role !== session.role) ||
      update.is_active === false)
  );
}

export async function finishUserUpdate(
  queryClient: QueryClient,
  session: Session,
  userId: string,
  update: UpdateUserInput,
): Promise<boolean> {
  if (updateRevokesCurrentSession(session, userId, update)) {
    setCsrfToken(undefined);
    queryClient.clear();
    return true;
  }

  await queryClient.invalidateQueries({ queryKey: ["users", session.id] });
  return false;
}
