const dateTime = new Intl.DateTimeFormat("ko-KR", {
  dateStyle: "medium",
  timeStyle: "medium",
});
const maximumDateMilliseconds = 8_640_000_000_000_000n;

export function formatDecimal(value: string): string {
  try {
    return BigInt(value).toLocaleString("ko-KR");
  } catch {
    return value;
  }
}

export function formatTimestampUs(value: string): string {
  try {
    const microseconds = BigInt(value);
    const milliseconds = microseconds / 1_000n;
    if (
      milliseconds < -maximumDateMilliseconds ||
      milliseconds > maximumDateMilliseconds
    ) {
      return `${value} µs`;
    }
    return dateTime.format(new Date(Number(milliseconds)));
  } catch {
    return value;
  }
}
