import { createBrowserRouter, Navigate } from "react-router-dom";

import { AppShell, SessionBoundary } from "./AppShell";
import { LoginPage } from "../features/auth/LoginPage";
import { SetupPage } from "../features/auth/SetupPage";
import { ExplorePage } from "../features/explore/ExplorePage";
import { IssueDetailPage } from "../features/issues/IssueDetailPage";
import { IssuesPage } from "../features/issues/IssuesPage";
import { LogsPage } from "../features/logs/LogsPage";
import { RecordDetailPage } from "../features/logs/RecordDetailPage";
import { ProjectsPage } from "../features/projects/ProjectsPage";

export const router = createBrowserRouter([
  { path: "/setup", element: <SetupPage /> },
  { path: "/login", element: <LoginPage /> },
  {
    element: <SessionBoundary />,
    children: [{
      element: <AppShell />,
      children: [
        { index: true, element: <Navigate to="/logs" replace /> },
        { path: "/projects", element: <ProjectsPage /> },
        { path: "/issues", element: <IssuesPage /> },
        { path: "/issues/:id", element: <IssueDetailPage /> },
        { path: "/logs", element: <LogsPage /> },
        { path: "/logs/:id", element: <RecordDetailPage /> },
        { path: "/explore", element: <ExplorePage /> },
      ],
    }],
  },
]);
