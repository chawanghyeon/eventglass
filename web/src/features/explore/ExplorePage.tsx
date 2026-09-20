import type { Kind } from "../../api/types";
import { SearchWorkspace } from "../search/SearchWorkspace";

const exploreKinds: Kind[] = ["error", "log", "transaction"];

export function ExplorePage() {
  return <SearchWorkspace title="Explore" defaultKinds={exploreKinds} histogram />;
}
