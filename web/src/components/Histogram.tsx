import type { AggregateBucketSet } from "../api/types";

function percent(value: string, maximum: bigint): string {
  if (maximum === 0n) return "0%";
  return `${Number((BigInt(value) * 10_000n) / maximum) / 100}%`;
}

function timestampLabel(timestampUs: string): string {
  return new Date(Number(BigInt(timestampUs) / 1_000n)).toLocaleString(
    "ko-KR",
    {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    },
  );
}

export function Histogram({
  buckets,
  label,
}: {
  buckets: AggregateBucketSet;
  label: string;
}) {
  const timestampBuckets = buckets.buckets.filter(
    (bucket) => bucket.key.type === "timestamp",
  );
  const maximum = timestampBuckets.reduce(
    (current, bucket) =>
      BigInt(bucket.doc_count) > current ? BigInt(bucket.doc_count) : current,
    0n,
  );

  if (timestampBuckets.length === 0) {
    return <p className="histogram__empty">표시할 시간 구간이 없습니다.</p>;
  }

  return (
    <figure className="histogram" aria-label={label}>
      <ol>
        {timestampBuckets.map((bucket) => {
          if (bucket.key.type !== "timestamp") return null;
          const time = timestampLabel(bucket.key.timestamp_us);
          return (
            <li
              aria-label={`${time}, ${bucket.doc_count}건`}
              key={bucket.key.timestamp_us}
            >
              <span
                className="histogram__bar"
                style={{ height: percent(bucket.doc_count, maximum) }}
              />
              <span className="histogram__value">{bucket.doc_count}</span>
              <time>{time}</time>
            </li>
          );
        })}
      </ol>
    </figure>
  );
}
