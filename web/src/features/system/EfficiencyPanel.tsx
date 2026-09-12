import { useQuery } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import { describeApiError } from "../../api/client";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";

const operationLabels: Record<string, string> = {
  list: "객체 목록",
  read_metadata: "메타데이터 읽기",
  conditional_create: "최초 생성",
  write_metadata: "메타데이터 저장",
  upload: "파일 업로드",
  download: "파일 다운로드",
};

export function EfficiencyPanel() {
  const user = useSession().data;
  const query = useQuery({
    queryKey: ["system-efficiency", user?.id],
    queryFn: ({ signal }) => endpoints.systemEfficiency(signal),
    enabled: user?.role === "admin",
    retry: false,
  });
  if (user?.role !== "admin") return null;
  return (
    <section className="panel system-details" aria-label="자동 읽기 효율">
      <h2>자동 읽기 효율</h2>
      <p>
        설정 없이 로컬 사본을 재사용합니다. 이번 서버 실행 이후의 관측값입니다.
      </p>
      {query.isPending ? <Spinner label="효율 확인 중" /> : null}
      {query.isError ? (
        <Notice tone="error">{describeApiError(query.error)}</Notice>
      ) : null}
      {query.data ? (
        <>
          <p>
            관측 시간 {query.data.uptime_ms} ms · 입력 body{" "}
            {query.data.observed_body_bytes} bytes
          </p>
          <p>
            로컬 경로 선택 {query.data.local_reuses} · 내려받아 설치{" "}
            {query.data.hydrated_shards} · 안전한 사본 회수{" "}
            {query.data.evicted_shards}
          </p>
          {query.data.incomplete ? (
            <Notice tone="error">일부 관측값이 상한에 도달했습니다.</Notice>
          ) : null}
          <table>
            <caption>원격 저장소 호출</caption>
            <thead>
              <tr>
                <th>작업</th>
                <th>시작</th>
                <th>성공</th>
                <th>실패</th>
                <th>취소</th>
                <th>완료된 읽기 bytes</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(query.data.remote).map(([name, counts]) => (
                <tr key={name}>
                  <td>{operationLabels[name] ?? name}</td>
                  <td>{counts.started}</td>
                  <td>{counts.succeeded}</td>
                  <td>{counts.failed}</td>
                  <td>{counts.cancelled}</td>
                  <td>{counts.completed_read_bytes}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <small>
            재시작 시 초기화됩니다. 시작 시 복구, SDK 재시도 횟수, 실패·취소된
            부분 전송량은 포함하지 않습니다. 청구 금액이나 절감액이 아닙니다.
          </small>
        </>
      ) : null}
    </section>
  );
}
