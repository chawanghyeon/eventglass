# Implementation gates

Follow G00–G08 in [DESIGN.md section 22](../../DESIGN.md#22-implementation-gates).
The current gate status is in [README.md](../../README.md); evidence belongs in
verified commit messages and executable tests, not per-stage Markdown diaries.

Before changing a subsystem, read its design section and the ownership map in
[ARCHITECTURE.md](../../ARCHITECTURE.md). Run the focused checks documented in
[CONTRIBUTING.md](../../CONTRIBUTING.md), then the relevant integration/crash gate.

Measure architecture-sensitive costs as soon as the path exists: G02 memory and
request/batch/PUT/transaction ratios, G03 publication lag/file sizes, G04 scan bytes
and cold/warm latency. G07 still requires complete sustained ARM64 cgroup and
multiworker validation. G08 still requires real provider and restore evidence.
