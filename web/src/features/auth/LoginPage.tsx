import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { FormEvent } from "react";
import { useState } from "react";
import { Link, Navigate, useNavigate } from "react-router-dom";
import { describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { Credentials } from "../../api/types";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { AuthLayout } from "./AuthLayout";
import { finishLogin } from "./api";
import { useSession } from "./useSession";

export function LoginPage() {
  const session = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const login = useMutation({
    mutationFn: (input: Credentials) => endpoints.login(input),
    onSuccess: async (response) => {
      await finishLogin(queryClient, response.csrf_token);
      navigate("/projects", { replace: true });
    },
  });

  if (session.isSuccess && session.data) {
    return <Navigate to="/projects" replace />;
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    login.mutate({ email, password });
  }

  return (
    <AuthLayout>
      <p className="eyebrow">운영자 로그인</p>
      <h1>다시 오셨군요.</h1>
      <p className="lede">
        프로젝트와 수집 키를 관리하려면 관리자 계정으로 로그인하세요.
      </p>
      <form className="stack-form" onSubmit={submit}>
        <label>
          이메일
          <input
            autoComplete="username"
            inputMode="email"
            maxLength={254}
            onChange={(event) => setEmail(event.target.value)}
            required
            type="email"
            value={email}
          />
        </label>
        <label>
          비밀번호
          <input
            autoComplete="current-password"
            maxLength={128}
            onChange={(event) => setPassword(event.target.value)}
            required
            type="password"
            value={password}
          />
        </label>
        {login.isError ? (
          <Notice tone="error">{describeApiError(login.error)}</Notice>
        ) : null}
        <Button disabled={login.isPending} type="submit">
          {login.isPending ? "로그인 중…" : "로그인"}
        </Button>
      </form>
      <p className="form-aside">
        아직 관리자를 만들지 않았나요? <Link to="/setup">초기 설정</Link>
      </p>
    </AuthLayout>
  );
}
