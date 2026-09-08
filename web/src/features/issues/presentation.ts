import type { IssueStatus } from "../../api/types";

export const issueStatusLabels: Record<IssueStatus, string> = {
  unresolved: "미해결",
  resolved: "해결됨",
  ignored: "무시됨",
};

export function isIssueStatus(value: string | null): value is IssueStatus {
  return value === "unresolved" || value === "resolved" || value === "ignored";
}
