export function feedbackMessage(value: Record<string, unknown>): string {
  const contexts = value.contexts;
  if (typeof contexts !== "object" || contexts === null) return "";
  const feedback = (contexts as Record<string, unknown>).feedback;
  if (typeof feedback !== "object" || feedback === null) return "";
  const message = (feedback as Record<string, unknown>).message;
  return typeof message === "string" ? message : "";
}
