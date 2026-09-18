import { useMutation } from "@tanstack/react-query";
import type { FormEvent } from "react";
import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { SetupRequest } from "../../api/types";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { AuthLayout } from "./AuthLayout";

export function SetupPage() {
  const navigate = useNavigate();
  const [token, setToken] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const setup = useMutation({
    mutationFn: (input: SetupRequest) => endpoints.setup(input),
    onSuccess: () =>
      navigate("/login", { replace: true, state: { setupComplete: true } }),
  });

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setup.mutate({ token: token.trim(), email, password });
  }

  return (
    <AuthLayout>
      <p className="eyebrow">최초 1회</p>
      <h1>관리자 계정을 만듭니다.</h1>
      <p className="lede">
        서버를 시작하기 전에 <code>eventglass admin setup-token</code>을 실행해
        일회용 토큰을 발급하세요. 토큰을 받은 다음 서버를 시작하고 이 화면에서
        설정을 완료합니다.
      </p>
      <p className="setup-lock-note">
        실행 중인 서버와 setup-token 명령은 같은 데이터 디렉터리를 동시에 열 수
        없습니다.
      </p>
      <form className="stack-form" onSubmit={submit}>
        <label>
          설정 토큰
          <input
            autoCapitalize="none"
            autoComplete="off"
            onChange={(event) => setToken(event.target.value)}
            required
            spellCheck={false}
            type="password"
            value={token}
          />
        </label>
        <label>
          관리자 이메일
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
            aria-describedby="password-help"
            autoComplete="new-password"
            maxLength={128}
            minLength={12}
            onChange={(event) => setPassword(event.target.value)}
            required
            type="password"
            value={password}
          />
          <small id="password-help">12자 이상 128자 이하</small>
        </label>
        {setup.isError ? (
          <Notice tone="error">{describeApiError(setup.error)}</Notice>
        ) : null}
        <Button disabled={setup.isPending} type="submit">
          {setup.isPending ? "계정 만드는 중…" : "관리자 만들기"}
        </Button>
      </form>
      <p className="form-aside">
        설정을 마쳤나요? <Link to="/login">로그인</Link>
      </p>
    </AuthLayout>
  );
}
