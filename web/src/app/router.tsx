import { createBrowserRouter, Navigate } from "react-router-dom";

import { AppShell, SessionBoundary } from "./AppShell";
import { LoginPage } from "../features/auth/LoginPage";
import { SetupPage } from "../features/auth/SetupPage";
import { ExplorePage } from "../features/explore/ExplorePage";
import { LogsPage } from "../features/logs/LogsPage";
import { RecordDetailPage } from "../features/logs/RecordDetailPage";
import { PlannedPage } from "../shared/ui/PlannedPage";

export const router = createBrowserRouter([
  { path: "/setup", element: <SetupPage /> },
  { path: "/login", element: <LoginPage /> },
  {
    element: <SessionBoundary />,
    children: [{
      element: <AppShell />,
      children: [
        { index: true, element: <Navigate to="/logs" replace /> },
        { path: "/projects", element: <PlannedPage title="Projects" /> },
        { path: "/issues", element: <PlannedPage title="Issues" /> },
        { path: "/issues/:id", element: <PlannedPage title="Issue detail" /> },
        { path: "/logs", element: <LogsPage /> },
        { path: "/logs/:id", element: <RecordDetailPage /> },
        { path: "/explore", element: <ExplorePage /> },
      ],
    }],
  },
]);
