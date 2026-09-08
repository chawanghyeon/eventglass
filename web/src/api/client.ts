import type { ApiErrorEnvelope } from "./types";

let csrfToken: string | undefined;

export class ApiError extends Error {
  readonly code: string;
  readonly requestId?: string;
  readonly retryable: boolean;
  readonly status: number;

  constructor(status: number, payload?: ApiErrorEnvelope) {
    const error = payload?.error;
    super(error?.message ?? `request_failed_${status}`);
    this.name = "ApiError";
    this.code = error?.code ?? "request_failed";
    this.requestId = error?.request_id;
    this.retryable = error?.retryable ?? status >= 500;
    this.status = status;
  }
}

export function setCsrfToken(token: string | undefined): void {
  csrfToken = token;
}

interface RequestOptions extends Omit<RequestInit, "body"> {
  body?: unknown;
  csrf?: boolean;
}

export async function apiRequest<T>(
  path: string,
  options: RequestOptions = {},
): Promise<T> {
  const headers = new Headers(options.headers);
  const method = options.method?.toUpperCase() ?? "GET";
  const hasBody = options.body !== undefined;

  if (hasBody) {
    headers.set("content-type", "application/json");
  }
  if (options.csrf !== false && method !== "GET" && method !== "HEAD") {
    if (!csrfToken) {
      throw new Error(
        "CSRF token is unavailable for an authenticated mutation",
      );
    }
    headers.set("x-csrf-token", csrfToken);
  }

  const response = await fetch(path, {
    ...options,
    body: hasBody ? JSON.stringify(options.body) : undefined,
    credentials: "include",
    headers,
    method,
  });

  if (!response.ok) {
    let payload: ApiErrorEnvelope | undefined;
    try {
      payload = (await response.json()) as ApiErrorEnvelope;
    } catch {
      payload = undefined;
    }
    throw new ApiError(response.status, payload);
  }

  if (
    response.status === 204 ||
    response.headers.get("content-length") === "0"
  ) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

const errorMessages: Record<string, string> = {
  aggregation_memory_limit:
    "집계 메모리 한도를 넘었습니다. 시간 범위나 그룹 수를 줄여 주세요.",
  aggregate_unavailable:
    "집계 인덱스를 사용할 수 없습니다. 잠시 후 다시 시도해 주세요.",
  admin_required: "관리자 권한이 필요합니다.",
  auth_rate_limited: "로그인 시도가 너무 많습니다. 잠시 후 다시 시도해 주세요.",
  bucket_limit_exceeded:
    "집계 구간이 너무 많습니다. 시간 범위나 그룹 수를 줄여 주세요.",
  authentication_required: "세션이 만료되었습니다. 다시 로그인해 주세요.",
  invalid_credentials: "이메일 또는 비밀번호가 올바르지 않습니다.",
  invalid_credentials_format: "이메일과 비밀번호 형식을 확인해 주세요.",
  invalid_aggregate_request: "집계 조건을 확인해 주세요.",
  invalid_csrf: "요청 보호 토큰이 만료되었습니다. 다시 로그인해 주세요.",
  invalid_issue_id: "Issue 식별자가 올바르지 않습니다.",
  invalid_issue_query: "Issue 조회 조건을 확인해 주세요.",
  invalid_issue_revision: "Issue 변경 버전을 확인해 주세요.",
  invalid_issue_status: "Issue 상태를 확인해 주세요.",
  invalid_origin: "현재 주소에서는 이 작업을 수행할 수 없습니다.",
  invalid_project: "프로젝트 이름과 slug를 확인해 주세요.",
  invalid_record_id: "발생 기록 식별자가 올바르지 않습니다.",
  invalid_role: "사용자 역할을 확인해 주세요.",
  numeric_overflow: "숫자 집계 범위를 초과했습니다.",
  empty_user_update: "변경할 사용자 설정을 선택해 주세요.",
  issue_access_denied: "이 프로젝트의 Issue를 볼 권한이 없습니다.",
  issue_not_found: "Issue를 찾을 수 없거나 프로젝트가 중지되었습니다.",
  issue_occurrence_not_found: "이 Issue에서 발생 기록 원문을 찾을 수 없습니다.",
  issue_revision_conflict: "Issue가 다른 요청에서 먼저 변경되었습니다.",
  last_admin_required: "활성 관리자는 최소 한 명 필요합니다.",
  invalid_search_request: "검색 조건 또는 검색 문법을 확인해 주세요.",
  invalid_search_token: "검색 페이지 토큰이 올바르지 않습니다.",
  project_exists: "이미 사용 중인 프로젝트 slug입니다.",
  project_or_key_not_found: "프로젝트 또는 키를 찾을 수 없습니다.",
  query_busy: "다른 검색을 처리하고 있습니다. 잠시 후 다시 시도해 주세요.",
  query_timeout: "검색 시간이 초과되었습니다. 범위를 줄여 다시 시도해 주세요.",
  record_not_found: "이 스냅샷에서 로그 원문을 찾을 수 없습니다.",
  search_access_denied: "선택한 프로젝트의 로그를 볼 권한이 없습니다.",
  search_authorization_changed:
    "검색 중 권한이 변경되었습니다. 새 스냅샷으로 다시 검색해 주세요.",
  search_result_too_large:
    "검색 결과가 너무 큽니다. 시간 범위나 결과 수를 줄여 다시 시도해 주세요.",
  search_token_expired:
    "검색 스냅샷이 만료되었습니다. 새 스냅샷으로 다시 검색해 주세요.",
  search_unavailable:
    "검색 인덱스를 사용할 수 없습니다. 잠시 후 다시 시도해 주세요.",
  setup_not_authorized: "설정 토큰이 만료되었거나 이미 사용되었습니다.",
  storage_unavailable:
    "저장소를 사용할 수 없습니다. 잠시 후 다시 시도해 주세요.",
  storage_generation_changed:
    "저장소 세대가 변경되었습니다. 새 스냅샷으로 다시 검색해 주세요.",
  user_exists: "이미 등록된 이메일입니다.",
  user_not_found: "사용자를 찾을 수 없습니다.",
};

export function describeApiError(error: unknown): string {
  if (error instanceof ApiError) {
    return errorMessages[error.code] ?? `요청에 실패했습니다 (${error.code}).`;
  }
  return "요청을 완료하지 못했습니다. 연결 상태를 확인해 주세요.";
}
