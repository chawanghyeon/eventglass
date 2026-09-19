import type { ApiErrorBody } from "./types";

export type ApiFailureKind = "unauthenticated" | "forbidden" | "conflict" | "expired" | "invalid" | "dependency" | "unknown";

export class ApiFailure extends Error {
  constructor(
    readonly status: number,
    readonly body: ApiErrorBody,
  ) {
    super(body.message);
    this.name = "ApiFailure";
  }

  get kind(): ApiFailureKind {
    switch (this.status) {
      case 400:
      case 413:
      case 415:
      case 422:
        return "invalid";
      case 401:
        return "unauthenticated";
      case 403:
        return "forbidden";
      case 409:
        return "conflict";
      case 410:
        return "expired";
      case 429:
      case 503:
      case 504:
        return "dependency";
      default:
        return "unknown";
    }
  }
}

export function isApiFailure(value: unknown): value is ApiFailure {
  return value instanceof ApiFailure;
}
