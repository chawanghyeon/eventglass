import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ReplayPlayer, PLAYER_DOCUMENT } from "./ReplayPlayer";

const constructor = vi.hoisted(() => vi.fn());
vi.mock("@sentry/rrweb", () => ({
  Replayer: vi.fn(function (...args: unknown[]) {
    return constructor(...args);
  }),
}));
const handlers = new Map<string, () => void>();
const instance = {
  iframe: { width: "1000", height: "600" },
  pause: vi.fn(),
  play: vi.fn(),
  destroy: vi.fn(),
  setConfig: vi.fn(),
  getCurrentTime: vi.fn(),
  on: vi.fn((name: string, handler: () => void) => handlers.set(name, handler)),
};
const observe = vi.fn();
const disconnect = vi.fn();
const events = [
  { type: 4, timestamp: 1000, data: {} },
  { type: 2, timestamp: 1100, data: {} },
  { type: 3, timestamp: 5000, data: {} },
];
beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  handlers.clear();
  constructor.mockReset().mockReturnValue(instance);
  instance.pause.mockReset();
  instance.play.mockReset();
  instance.getCurrentTime.mockReset().mockReturnValue(200);
  instance.iframe = { width: "1000", height: "600" };
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe = observe;
      disconnect = disconnect;
    },
  );
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});
function loadFrame() {
  const frame = screen.getByTitle<HTMLIFrameElement>("Session replay 화면");
  const document = frame.contentDocument;
  if (!document) throw new Error("test iframe has no document");
  document.body.innerHTML = '<div id="player"></div>';
  Object.defineProperty(frame, "clientWidth", {
    configurable: true,
    value: 500,
  });
  fireEvent.load(frame);
  return frame;
}
it.each([
  { input: [] },
  { input: [{ type: 2 }] },
  { input: [{ type: 0 }, { type: 1 }] },
])("does not construct a player without replayable snapshots", ({ input }) => {
  render(<ReplayPlayer events={input} seekTo={null} onTime={vi.fn()} />);
  expect(screen.getByText(/재생 가능한 DOM snapshot이 없습니다/)).toBeVisible();
  expect(constructor).not.toHaveBeenCalled();
});
it("isolates replay DOM and drives play, pause, speed, timer, and bounded seeks", () => {
  const onTime = vi.fn();
  const view = render(
    <ReplayPlayer events={events} seekTo={null} onTime={onTime} />,
  );
  expect(screen.getByRole("button", { name: "Play" })).toBeDisabled();
  const frame = loadFrame();
  expect(frame).toHaveAttribute("sandbox", "allow-same-origin");
  expect(frame).toHaveAttribute("referrerpolicy", "no-referrer");
  expect(frame).toHaveAttribute("srcdoc", PLAYER_DOCUMENT);
  expect(PLAYER_DOCUMENT).toContain("default-src 'none'");
  expect(constructor).toHaveBeenCalledWith(
    events,
    expect.objectContaining({
      UNSAFE_replayCanvas: false,
      mouseTail: false,
      skipInactive: false,
    }),
  );
  expect(instance.pause).toHaveBeenCalledWith(101);
  expect(frame.style.height).toBe("300px");
  expect(observe).toHaveBeenCalledWith(frame);
  fireEvent.click(screen.getByRole("button", { name: "Play" }));
  expect(instance.play).toHaveBeenLastCalledWith(101);
  fireEvent.click(screen.getByRole("button", { name: "Pause" }));
  expect(instance.pause).toHaveBeenLastCalledWith();
  fireEvent.change(screen.getByLabelText("Speed"), { target: { value: "4" } });
  expect(instance.setConfig).toHaveBeenLastCalledWith({ speed: 4 });
  fireEvent.change(screen.getByLabelText("Replay seek"), {
    target: { value: "1000" },
  });
  expect(instance.pause).toHaveBeenLastCalledWith(1000);
  expect(onTime).toHaveBeenLastCalledWith(2000);
  act(() => vi.advanceTimersByTime(200));
  expect(onTime).toHaveBeenLastCalledWith(1200);
  instance.getCurrentTime.mockReturnValue(9999);
  act(() => vi.advanceTimersByTime(200));
  fireEvent.click(screen.getByRole("button", { name: "Play" }));
  expect(instance.play).toHaveBeenLastCalledWith(101);
  act(() => handlers.get("finish")?.());
  expect(screen.getByRole("button", { name: "Play" })).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "Play" }));
  act(() => handlers.get("pause")?.());
  expect(screen.getByRole("button", { name: "Play" })).toBeVisible();
  view.rerender(
    <ReplayPlayer
      events={events}
      seekTo={{ time: -100, request: "before" }}
      onTime={onTime}
    />,
  );
  expect(instance.pause).toHaveBeenLastCalledWith(101);
  view.rerender(
    <ReplayPlayer
      events={events}
      seekTo={{ time: 99999, request: "after" }}
      onTime={onTime}
    />,
  );
  expect(instance.pause).toHaveBeenLastCalledWith(4000);
  instance.iframe = { width: "", height: "" };
  act(() => handlers.get("resize")?.());
  expect(frame.style.height).toBe("300px");
  view.unmount();
  expect(instance.destroy).toHaveBeenCalledOnce();
  expect(disconnect).toHaveBeenCalledOnce();
  const calls = onTime.mock.calls.length;
  act(() => vi.advanceTimersByTime(1000));
  expect(onTime).toHaveBeenCalledTimes(calls);
});
it("reports construction errors and never inserts recording markup in the admin DOM", () => {
  constructor.mockImplementation(() => {
    throw new Error("corrupt snapshot");
  });
  render(<ReplayPlayer events={events} seekTo={null} onTime={vi.fn()} />);
  loadFrame();
  expect(screen.getByRole("alert")).toHaveTextContent(
    "DOM snapshot을 재생할 수 없습니다",
  );
  expect(document.querySelector("#player")).toBeNull();
});
it("reports external play and seek failures instead of silently ignoring corruption", () => {
  const onTime = vi.fn();
  const view = render(
    <ReplayPlayer events={events} seekTo={null} onTime={onTime} />,
  );
  loadFrame();
  instance.play.mockImplementation(() => {
    throw new Error("broken play");
  });
  fireEvent.click(screen.getByRole("button", { name: "Play" }));
  expect(screen.getByRole("alert")).toHaveTextContent(
    "이 구간을 재생할 수 없습니다",
  );
  instance.pause.mockImplementation(() => {
    throw new Error("broken seek");
  });
  fireEvent.change(screen.getByLabelText("Replay seek"), {
    target: { value: "500" },
  });
  expect(screen.getByRole("alert")).toHaveTextContent("누락되거나 손상된 구간");
  view.rerender(
    <ReplayPlayer
      events={events}
      seekTo={{ time: 2500, request: "seek" }}
      onTime={onTime}
    />,
  );
  expect(instance.pause).toHaveBeenLastCalledWith(1500);
  expect(screen.getByRole("alert")).toHaveTextContent("누락되거나 손상된 구간");
});
it("waits for a real player root and supports controls before initialization", () => {
  const onTime = vi.fn();
  render(
    <ReplayPlayer
      events={[{ type: 2 }, { type: 3 }]}
      seekTo={{ time: 0, request: "initial" }}
      onTime={onTime}
    />,
  );
  fireEvent.change(screen.getByLabelText("Replay seek"), {
    target: { value: "1" },
  });
  expect(onTime).toHaveBeenLastCalledWith(1);
  fireEvent.load(screen.getByTitle("Session replay 화면"));
  expect(constructor).not.toHaveBeenCalled();
});
