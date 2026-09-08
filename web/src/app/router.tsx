import { Navigate, createBrowserRouter } from "react-router-dom";
import { LoginPage, SetupPage } from "../features/auth";
import { ExplorePage } from "../features/explore";
import { IssueDetailPage, IssuesPage } from "../features/issues";
import { LogsPage } from "../features/logs";
import { ProjectsPage } from "../features/projects";
import { UsersPage } from "../features/users";
import { AppShell } from "./AppShell";
import { ProtectedRoute } from "./ProtectedRoute";

export const router = createBrowserRouter([
  { path: "/login", element: <LoginPage /> },
  { path: "/setup", element: <SetupPage /> },
  {
    element: <ProtectedRoute />,
    children: [
      {
        element: <AppShell />,
        children: [
          { path: "/issues", element: <IssuesPage /> },
          { path: "/logs", element: <LogsPage /> },
          { path: "/explore", element: <ExplorePage /> },
          { path: "/issues/:id", element: <IssueDetailPage /> },
          { path: "/projects", element: <ProjectsPage /> },
          { path: "/users", element: <UsersPage /> },
          { path: "/", element: <Navigate to="/projects" replace /> },
        ],
      },
    ],
  },
  { path: "*", element: <Navigate to="/" replace /> },
]);
