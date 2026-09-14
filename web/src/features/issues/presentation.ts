import type { Issue, IssueStatus } from "../../api/types";

export const issueStatusLabels: Record<IssueStatus, string> = {
  unresolved: "미해결",
  resolved: "해결됨",
  ignored: "무시됨",
};

export function isIssueStatus(value: string | null): value is IssueStatus {
  return value === "unresolved" || value === "resolved" || value === "ignored";
}

export function issueSignals(issue: Issue): string[] {
  if (issue.status !== "unresolved") return [];
  const nowUs = BigInt(issue.activity_as_of_us);
  const signals: string[] = [];
  if (
    issue.last_regressed_at_us &&
    BigInt(issue.last_regressed_at_us) >= nowUs - 86_400_000_000n &&
    BigInt(issue.last_regressed_at_us) <= nowUs
  ) {
    signals.push("재발");
  }
  const recent = BigInt(issue.recent_24h_count);
  const previous = BigInt(issue.previous_24h_count);
  if (recent >= 10n && (previous === 0n || recent >= previous * 2n)) {
    signals.push("최근 급증");
  }
  if (
    recent > 0n &&
    issue.first_release &&
    issue.last_release &&
    issue.first_release !== issue.last_release
  ) {
    signals.push("릴리스 변경");
  }
  return signals;
}
