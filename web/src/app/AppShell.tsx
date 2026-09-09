import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { NavLink, Outlet, useNavigate } from "react-router-dom";
import { Button } from "../components/Button";
import { logoutAndClear, useSession } from "../features/auth";
import { SystemReadiness } from "../features/system";

export function AppShell() {
  const session = useSession();
  const [menuOpen, setMenuOpen] = useState(false);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const logout = useMutation({
    mutationFn: () => logoutAndClear(queryClient),
    onSettled: () => navigate("/login", { replace: true }),
  });
  const user = session.data;

  if (!user) {
    return null;
  }

  return (
    <div className="app-frame">
      <aside className="sidebar">
        <div className="sidebar__brand">
          <span className="brand-mark" aria-hidden="true">
            E
          </span>
          <span>Eventglass</span>
        </div>
        <button
          className="mobile-menu-toggle"
          type="button"
          aria-expanded={menuOpen}
          aria-controls="main-navigation"
          onClick={() => setMenuOpen(!menuOpen)}
        >
          {menuOpen ? "메뉴 닫기" : "메뉴 열기"}
        </button>
        <nav
          id="main-navigation"
          className={menuOpen ? "is-open" : ""}
          aria-label="주요 메뉴"
          onClick={(event) => {
            if ((event.target as Element).closest("a")) setMenuOpen(false);
          }}
        >
          <NavLink end to="/">
            <span aria-hidden="true">◫</span>
            전체 현황
          </NavLink>
          <NavLink to="/explore">
            <span aria-hidden="true">▥</span>
            수치 분석
          </NavLink>
          <NavLink to="/logs">
            <span aria-hidden="true">≋</span>
            로그 검색
          </NavLink>
          <NavLink to="/issues">
            <span aria-hidden="true">◇</span>
            오류 추적
          </NavLink>
          <NavLink to="/replays">
            <span aria-hidden="true">▷</span>방문 분석
          </NavLink>
          <NavLink to="/projects">
            <span aria-hidden="true">⌁</span>
            프로젝트
          </NavLink>
          {user.role === "admin" ? (
            <>
              <NavLink to="/alerts">
                <span aria-hidden="true">△</span>
                알림 규칙
              </NavLink>
              <NavLink to="/users">
                <span aria-hidden="true">◎</span>
                사용자
              </NavLink>
              <NavLink to="/system">
                <span aria-hidden="true">◉</span>
                시스템 상태
              </NavLink>
            </>
          ) : null}
        </nav>
        <div className="sidebar__bottom">
          <SystemReadiness admin={user.role === "admin"} userId={user.id} />
          <div className="account-card">
            <div className="avatar" aria-hidden="true">
              {user.email.slice(0, 1).toUpperCase()}
            </div>
            <div>
              <strong>{user.email}</strong>
              <span>{user.role === "admin" ? "관리자" : "멤버"}</span>
            </div>
          </div>
          <Button
            disabled={logout.isPending}
            onClick={() => logout.mutate()}
            type="button"
            variant="quiet"
          >
            {logout.isPending ? "로그아웃 중…" : "로그아웃"}
          </Button>
        </div>
      </aside>
      <main className="workspace">
        <Outlet />
      </main>
    </div>
  );
}
