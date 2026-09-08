import type { IssueStatus } from "../../api/types";
import { issueStatusLabels } from "./presentation";

export function IssueStatusBadge({ status }: { status: IssueStatus }) {
  return (
    <span className={`issue-status issue-status--${status}`}>
      {issueStatusLabels[status]}
    </span>
  );
}
