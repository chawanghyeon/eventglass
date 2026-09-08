import type { FormEvent } from "react";
import { useState } from "react";
import type { CreateUserInput } from "../../api/types";
import { Button } from "../../components/Button";

export function CreateUserForm({
  disabled,
  onCreate,
}: {
  disabled: boolean;
  onCreate: (input: CreateUserInput) => void;
}) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<CreateUserInput["role"]>("member");

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onCreate({ email: email.trim(), password, role });
  }

  return (
    <form className="project-form" onSubmit={submit}>
      <div className="form-heading">
        <div>
          <p className="eyebrow">접근 권한</p>
          <h2>사용자 추가</h2>
        </div>
        <p>새 사용자가 로그인할 이메일, 임시 비밀번호와 역할을 지정합니다.</p>
      </div>
      <div className="user-form__fields">
        <label>
          이메일
          <input
            autoComplete="email"
            maxLength={254}
            onChange={(event) => setEmail(event.target.value)}
            placeholder="member@example.com"
            required
            type="email"
            value={email}
          />
        </label>
        <label>
          임시 비밀번호
          <input
            autoComplete="new-password"
            maxLength={128}
            minLength={12}
            onChange={(event) => setPassword(event.target.value)}
            required
            type="password"
            value={password}
          />
        </label>
        <label>
          역할
          <select
            onChange={(event) =>
              setRole(event.target.value as CreateUserInput["role"])
            }
            value={role}
          >
            <option value="member">멤버</option>
            <option value="admin">관리자</option>
          </select>
        </label>
        <Button disabled={disabled} type="submit">
          {disabled ? "추가 중…" : "사용자 추가"}
        </Button>
      </div>
    </form>
  );
}
