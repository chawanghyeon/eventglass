import { isApiFailure } from "../../api/errors";

export function StatusPanel({ error, empty, onRetry }: { error?: unknown; empty?: string; onRetry?: () => void }) {
  if (!error) return empty ? <p className="status" role="status">{empty}</p> : null;
  const kind = isApiFailure(error) ? error.kind : "unknown";
  const messages = {
    unauthenticated: "Your session ended. Sign in again.",
    forbidden: "You do not have access to this scope.",
    conflict: "This data changed. Refresh before trying again.",
    expired: "This snapshot expired. Refresh all linked panels.",
    invalid: "The request is not valid. Review its fields.",
    dependency: "A dependency is unavailable. Try again shortly.",
    unknown: "The request failed unexpectedly.",
  } as const;
  return <section className="status error" role="alert">
    <p>{messages[kind]}</p>
    {onRetry ? <button type="button" onClick={onRetry}>Retry</button> : null}
  </section>;
}
