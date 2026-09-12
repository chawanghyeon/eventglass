import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { expect, it } from "vitest";
import type { ReplayPageActivity } from "../../api/types";
import { PageMaps } from "./PageMaps";
const empty: ReplayPageActivity = {
  examples: [],
  visits: 0,
  sampled_replays: 0,
  observed_time_ms: 0,
  timed_visits: 0,
  last_observed_replays: 0,
  narrow_replays: 0,
  wide_replays: 0,
  next_pages: {},
  previous_pages: {},
  depth_clicks: {},
  depth_elements: {},
  replay_ids: [],
  frustration: { rage: 0, dead: 0, slow: 0, multi: 0 },
  clicks: {},
  movement: {},
  elements: {},
  max_scroll_viewports: 0,
  scroll_samples: 0,
  scroll_reach_replays: {},
};
const page: ReplayPageActivity = {
  ...empty,
  visits: 3,
  sampled_replays: 2,
  observed_time_ms: 120000,
  timed_visits: 2,
  next_pages: { "/thanks": 2, "/other": 1 },
  previous_pages: { "/home": 1 },
  clicks: { "1,2": 3, "3,4": 1 },
  movement: { "2,3": 2 },
  elements: { "button.buy": 3, "button.more": 1 },
  depth_clicks: { "0": 2, "1": 1 },
  depth_elements: {
    "1": { "button.buy": 3, "button.more": 1 },
    "2": { footer: 1 },
  },
  replay_ids: ["session"],
  max_scroll_viewports: 2,
  scroll_samples: 2,
  scroll_reach_replays: { "1": 2 },
  frustration: { rage: 1, dead: 0, slow: 1, multi: 0 },
  examples: [
    { kind: "clicks", key: "1,2", timestamp_ms: 1000, replay_id: "session" },
    {
      kind: "frustration",
      key: "rage",
      timestamp_ms: 2000,
      replay_id: "session",
    },
    { kind: "frustration", key: "slow", timestamp_ms: 3000, replay_id: "" },
  ],
};
function Destination() {
  const location = useLocation();
  return (
    <div data-testid="destination">
      {location.pathname + location.search} return{" "}
      {location.state?.replaysReturnTo}
    </div>
  );
}
function show(
  pages: Record<string, ReplayPageActivity>,
  project?: string,
  detail = false,
) {
  render(
    <MemoryRouter
      initialEntries={[
        {
          pathname: detail ? "/detail" : "/replays",
          search: "?project=1&view=pages",
          state: detail ? { replaysReturnTo: "/replays?project=1" } : undefined,
        },
      ]}
    >
      <Routes>
        <Route
          path={detail ? "/detail" : "/replays"}
          element={<PageMaps pages={pages} project={project} />}
        />
        <Route path="/replays/:project/:id" element={<Destination />} />
      </Routes>
    </MemoryRouter>,
  );
}
it("explains the absence of usable page coordinates", () => {
  show({});
  expect(
    screen.getByText("좌표·viewport가 함께 확인된 페이지 데이터가 없습니다."),
  ).toBeVisible();
});
it("selects pages and map modes while distinguishing measured and unknown dwell time", async () => {
  const user = userEvent.setup();
  show({ "/checkout": page, "/thanks": empty }, "1");
  expect(
    screen.getByRole("img", { name: "Click heatmap: /checkout" }),
  ).toBeVisible();
  expect(screen.getAllByText("1:00")).toHaveLength(2);
  await user.selectOptions(screen.getByLabelText("Map"), "movement");
  expect(
    screen.getByRole("img", { name: "Movement heatmap: /checkout" }),
  ).toBeVisible();
  await user.selectOptions(screen.getByLabelText("페이지"), "/thanks");
  expect(
    screen.getByRole("img", { name: "Movement heatmap: /thanks" }),
  ).toBeVisible();
  expect(screen.getAllByText("불명")).toHaveLength(2);
  await user.click(screen.getByRole("button", { name: "/checkout" }));
  await user.click(screen.getByText("이동 경로와 관련 세션"));
  expect(screen.getByRole("link", { name: "관련 Replay 1" })).toHaveAttribute(
    "href",
    "/replays/1/session",
  );
  await user.click(screen.getAllByRole("button", { name: "/thanks" })[1]);
  expect(screen.getByLabelText("페이지")).toHaveValue("/thanks");
});
it("opens representative evidence at its actual time and retains the list URL", async () => {
  const user = userEvent.setup();
  show({ "/checkout": page }, "1");
  await user.click(
    screen.getByRole("link", { name: "3 samples · 1,2 · Replay 시점 보기" }),
  );
  expect(screen.getByTestId("destination")).toHaveTextContent(
    "/replays/1/session?t=1000 return /replays?project=1&view=pages",
  );
});
it("preserves an existing return URL from detail views", async () => {
  const user = userEvent.setup();
  show({ "/checkout": page }, "1", true);
  await user.click(screen.getByRole("link", { name: "Rage 1" }));
  expect(screen.getByTestId("destination")).toHaveTextContent(
    "/replays/1/session?t=2000 return /replays?project=1",
  );
});
it("does not invent replay links without a project and shows depth evidence without guessing missing counts", async () => {
  const user = userEvent.setup();
  show({ "/checkout": page });
  expect(screen.queryByRole("link")).not.toBeInTheDocument();
  await user.click(screen.getByText("스크롤 분석"));
  const row = screen.getByText("2–3 화면 높이").closest("tr");
  if (!row) throw new Error("missing depth row");
  expect(within(row).getByText("0")).toBeVisible();
  expect(within(row).getByText("footer")).toBeVisible();
});
