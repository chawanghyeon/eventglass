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
}

function rows(set: AggregateBucketSet, prefix: string[] = []): Row[] {
  return set.buckets.flatMap((bucket) => {
    const path = [...prefix, keyLabel(bucket.key)];
    return bucket.children
      ? rows(bucket.children, path)
      : [{ path, count: bucket.doc_count }];
  });
}

export function AggregateTable({ buckets }: { buckets: AggregateBucketSet }) {
  const values = rows(buckets);
  return (
    <div className="aggregate-table-wrap">
      <table className="aggregate-table">
        <caption className="sr-only">집계 bucket 결과</caption>
        <thead>
          <tr>
            <th scope="col">Bucket</th>
            <th scope="col">건수</th>
          </tr>
        </thead>
        <tbody>
          {values.map((row, index) => (
            <tr key={`${row.path.join("\u0000")}-${index}`}>
              <th scope="row">{row.path.join(" › ")}</th>
              <td>{row.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {buckets.has_more ? <p>상위 bucket만 표시했습니다.</p> : null}
    </div>
  );
}
