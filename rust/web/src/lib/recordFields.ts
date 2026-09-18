const MAX_ROWS = 200;
const MAX_DEPTH = 12;

interface FieldRow {
  path: string;
  type: string;
  value: string;
}

export function sampleFields(raw: unknown): {
  rows: FieldRow[];
  truncated: boolean;
} {
  const rows: FieldRow[] = [];
  let truncated = false;
  function visit(value: unknown, path: string, depth: number) {
    if (rows.length >= MAX_ROWS || depth > MAX_DEPTH) {
      truncated = true;
      return;
    }
    const type =
      value === null ? "null" : Array.isArray(value) ? "array" : typeof value;
    if (value !== null && typeof value === "object") {
      if (Array.isArray(value)) {
        if (!value.length) rows.push({ path, type, value: "[]" });
        for (let index = 0; index < value.length; index += 1) {
          visit(value[index], `${path}[${index}]`, depth + 1);
          if (rows.length >= MAX_ROWS) {
            truncated ||= index < value.length - 1;
            break;
          }
        }
      } else {
        const entries = Object.entries(value);
        if (!entries.length) rows.push({ path, type, value: "{}" });
        for (let index = 0; index < entries.length; index += 1) {
          const [key, child] = entries[index];
          visit(child, `${path}[${JSON.stringify(key)}]`, depth + 1);
          if (rows.length >= MAX_ROWS) {
            truncated ||= index < entries.length - 1;
            break;
          }
        }
      }
      return;
    }
    const text = typeof value === "string" ? value : String(value);
    rows.push({
      path,
      type,
      value: text.length > 240 ? `${text.slice(0, 240)}…` : text,
    });
  }
  visit(raw, "$", 0);
  return { rows, truncated };
}
