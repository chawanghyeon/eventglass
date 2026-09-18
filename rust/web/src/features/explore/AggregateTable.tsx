import type { AggregateBucketSet } from "../../api/types";

function keyLabel(key: AggregateBucketSet["buckets"][number]["key"]): string {
  if (key.type === "string") return key.value || "(값 없음)";
  return new Date(Number(BigInt(key.timestamp_us) / 1_000n)).toLocaleString(
    "ko-KR",
  );
}

interface Row {
  path: string[];
  count: string;
  metrics: AggregateBucketSet["buckets"][number]["metrics"];
}

function rows(set: AggregateBucketSet, prefix: string[] = []): Row[] {
  return set.buckets.flatMap((bucket) => {
    const path = [...prefix, keyLabel(bucket.key)];
    return bucket.children
      ? rows(bucket.children, path)
      : [{ path, count: bucket.doc_count, metrics: bucket.metrics }];
  });
}

export function AggregateTable({ buckets }: { buckets: AggregateBucketSet }) {
  const values = rows(buckets);
  const metrics = Array.from(
    new Map(
      values.flatMap((row) =>
        row.metrics
          .filter((metric) => metric.op !== "count")
          .map((metric) => [metric.name, metric] as const),
      ),
    ).values(),
  );
  return (
    <div className="aggregate-table-wrap">
      <table className="aggregate-table">
        <caption className="sr-only">집계 bucket 결과</caption>
        <thead>
          <tr>
            <th scope="col">Bucket</th>
            <th scope="col">건수</th>
            {metrics.map((metric) => (
              <th scope="col" key={metric.name}>
                {metric.name} · {metric.op}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {values.map((row, index) => (
            <tr key={`${row.path.join("\u0000")}-${index}`}>
              <th scope="row">{row.path.join(" › ")}</th>
              <td>{row.count}</td>
              {metrics.map((metric) => (
                <td key={metric.name}>
                  {row.metrics.find((value) => value.name === metric.name)
                    ?.value ?? "값 없음"}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {buckets.has_more ? <p>상위 bucket만 표시했습니다.</p> : null}
    </div>
  );
}
