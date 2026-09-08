import type { PropsWithChildren } from "react";
import { Link } from "react-router-dom";

export function AuthLayout({ children }: PropsWithChildren) {
  return (
    <main className="auth-layout">
      <div className="auth-brand" aria-label="Eventglass">
        <span className="brand-mark" aria-hidden="true">
          E
        </span>
        <span>Eventglass</span>
      </div>
      <section className="auth-panel">{children}</section>
      <p className="auth-footnote">
        로컬 운영 콘솔 · <Link to="/login">로그인</Link>
      </p>
    </main>
  );
}
