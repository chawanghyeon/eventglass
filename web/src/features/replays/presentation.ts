export function duration(ms: number) {
  const seconds = Math.max(0, Math.floor(ms / 1000));
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;
}
export function userLabel(user: Record<string, unknown> | null) {
  for (const field of ["id", "email", "username", "name"]) {
    if (typeof user?.[field] === "string") return user[field];
  }
  return "anonymous";
}
