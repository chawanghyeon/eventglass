import { useMemo, useState } from "react";
import { sampleFields } from "../lib/recordFields";

export function RecordFields({ raw }: { raw: unknown }) {
  const [open, setOpen] = useState(false);
  const sample = useMemo(
    () => (open ? sampleFields(raw) : { rows: [], truncated: false }),
    [raw, open],
  );
  return (
    <details
      className="record-section"
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary>선택한 기록의 필드</summary>
      <p>
        이 기록 한 건의 원문 경로·타입·값입니다. 전체 데이터의 스키마나 검색
        필드 목록은 아닙니다.
      </p>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>원문 경로</th>
              <th>타입</th>
              <th>값</th>
            </tr>
          </thead>
          <tbody>
            {sample.rows.map((row) => (
              <tr key={row.path}>
                <td>
                  <code>{row.path}</code>
                </td>
                <td>{row.type}</td>
                <td>{row.value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {sample.truncated ? (
        <p>
          표시 한도에 도달했습니다. 전체 값은 아래 원문 JSON에서 확인하세요.
        </p>
      ) : null}
    </details>
  );
}
