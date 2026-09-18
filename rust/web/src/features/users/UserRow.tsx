import type { FormEvent } from "react";
import { useState } from "react";
import type { UpdateUserInput, User } from "../../api/types";
import { Button } from "../../components/Button";

export function UserRow({
  busy,
  current,
  onUpdate,
  user,
}: {
  busy: boolean;
  current: boolean;
  onUpdate: (id: string, update: UpdateUserInput) => void;
  user: User;
}) {
  const [role, setRole] = useState(user.role);
  const [active, setActive] = useState(user.is_active);

  const changed = role !== user.role || active !== user.is_active;

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const update: UpdateUserInput = {};
    if (role !== user.role) update.role = role;
    if (active !== user.is_active) update.is_active = active;
    if (Object.keys(update).length > 0) onUpdate(user.id, update);
  }

  return (
    <tr>
      <th scope="row">
        <strong>{user.email}</strong>
        {current ? <span className="self-label">현재 사용자</span> : null}
      </th>
      <td>
        <form className="user-row__form" onSubmit={submit}>
          <label className="sr-only" htmlFor={`role-${user.id}`}>
            {user.email} 역할
          </label>
          <select
            disabled={busy}
            id={`role-${user.id}`}
            onChange={(event) => setRole(event.target.value as User["role"])}
            value={role}
          >
            <option value="member">멤버</option>
            <option value="admin">관리자</option>
          </select>
          <label className="user-row__active">
            <input
              checked={active}
              disabled={busy}
              onChange={(event) => setActive(event.target.checked)}
              type="checkbox"
            />
            활성
          </label>
          <Button disabled={busy || !changed} type="submit" variant="quiet">
            {busy ? "저장 중…" : "변경 저장"}
          </Button>
        </form>
      </td>
    </tr>
  );
}
