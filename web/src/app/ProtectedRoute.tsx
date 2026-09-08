import { Navigate, Outlet, useLocation } from "react-router-dom";
import { ApiError, describeApiError } from "../api/client";
import { Button } from "../components/Button";
import { Notice } from "../components/Notice";
import { Spinner } from "../components/Spinner";
import { useSession } from "../features/auth";

export function ProtectedRoute() {
  const location = useLocation();
  const session = useSession();

  if (session.isPending) {
    return (
      <main className="center-state">
        <Spinner label="세션 확인 중" />
      </main>
    );
  }
  if (session.error instanceof ApiError && session.error.status === 401) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  if (session.isError) {
    return (
      <main className="center-state">
        <Notice tone="error">{describeApiError(session.error)}</Notice>
        <Button onClick={() => void session.refetch()} type="button">
          다시 시도
        </Button>
      </main>
    );
  }
  return <Outlet />;
}
