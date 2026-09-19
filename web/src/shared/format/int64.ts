const integerPattern = /^-?(0|[1-9][0-9]*)$/;

export function parseInt64(value: string): bigint {
  if (!integerPattern.test(value)) throw new Error("invalid canonical integer");
  const parsed = BigInt(value);
  if (parsed < -(1n << 63n) || parsed > (1n << 63n) - 1n) throw new Error("integer outside int64");
  return parsed;
}

export function formatInt64(value: string): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(parseInt64(value));
}

export function microsFromMilliseconds(value: number): string {
  if (!Number.isSafeInteger(value)) throw new Error("milliseconds must be a safe integer");
  return (BigInt(value) * 1000n).toString();
}
