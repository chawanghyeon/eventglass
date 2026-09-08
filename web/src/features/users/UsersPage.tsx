import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { CreateUserInput, UpdateUserInput } from "../../api/types";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";
import { CreateUserForm } from "./CreateUserForm";
import { usersQuery } from "./queries";
import { finishUserUpdate } from "./sessionPolicy";
import { UserRow } from "./UserRow";

export function UsersPage() {
  const session = useSession();
  const currentUser = session.data;
  const admin = currentUser?.role === "admin";
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const users = useQuery({
    ...usersQuery(currentUser?.id ?? "unknown"),
    enabled: admin,
  });
  const create = useMutation({
    mutationFn: (input: CreateUserInput) => endpoints.createUser(input),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["users", currentUser?.id] }),
  });
  const update = useMutation({
    mutationFn: (input: { id: string; update: UpdateUserInput }) =>
      endpoints.updateUser(input),
    onSuccess: async (_, input) => {
      if (!currentUser) return;
      const sessionRevoked = await finishUserUpdate(
        queryClient,
        currentUser,
        input.id,
        input.update,
      );
      if (sessionRevoked) navigate("/login", { replace: true });
    },
  });

  if (!currentUser) return null;

  if (!admin) {
    return (
      <div className="page-stack">
        <header className="page-heading">
          <div>
            <p className="eyebrow">접근 권한</p>
            <h1>사용자</h1>
          </div>
        </header>
        <Notice tone="warning">
          사용자 관리는 관리자 계정에서만 열 수 있습니다.
        </Notice>
      </div>
    );
  }

  const mutationError = create.error ?? update.error;

  return (
    <div className="page-stack">
      <header className="page-heading">
        <div>
          <p className="eyebrow">계정 관리</p>
          <h1>사용자</h1>
          <p>로그인 계정을 추가하고 역할과 활성 상태를 관리합니다.</p>
        </div>
        {users.data ? (
          <span className="count-badge">{users.data.length}명</span>
        ) : null}
      </header>

      <CreateUserForm
        disabled={create.isPending}
        onCreate={(input) => create.mutate(input)}
      />

      {mutationError ? (
        <Notice tone="error">{describeApiError(mutationError)}</Notice>
      ) : null}

      {users.isPending ? <Spinner label="사용자 불러오는 중" /> : null}
      {users.isError ? (
        <Notice tone="error">
          <span>{describeApiError(users.error)}</span>
          <Button
            onClick={() => void users.refetch()}
            type="button"
            variant="quiet"
          >
            다시 시도
          </Button>
        </Notice>
      ) : null}
      {users.data?.length === 0 ? (
        <div className="empty-state">
          <span className="empty-state__mark" aria-hidden="true">
            00
          </span>
          <h2>등록된 사용자가 없습니다.</h2>
          <p>위 양식에서 첫 로그인 계정을 추가하세요.</p>
        </div>
      ) : null}
      {users.data && users.data.length > 0 ? (
        <div className="user-table-wrap">
          <table className="user-table">
            <caption className="sr-only">
              Eventglass 사용자 역할 및 활성 상태
            </caption>
            <thead>
              <tr>
                <th scope="col">계정</th>
                <th scope="col">권한과 상태</th>
              </tr>
            </thead>
            <tbody>
              {users.data.map((user) => (
                <UserRow
                  busy={update.isPending}
                  current={user.id === currentUser.id}
                  key={`${user.id}:${user.role}:${user.is_active}`}
                  onUpdate={(id, changes) =>
                    update.mutate({ id, update: changes })
                  }
                  user={user}
                />
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}
