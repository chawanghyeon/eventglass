import type { Kind } from "../../api/types";
import { SearchWorkspace } from "../search/SearchWorkspace";

const logKinds: Kind[] = ["log"];

export function LogsPage() {
  return <SearchWorkspace title="Logs" defaultKinds={logKinds} />;
}
