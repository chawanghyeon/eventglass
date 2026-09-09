import { useEffect, useRef, useState } from "react";
import { Replayer, type eventWithTime } from "@sentry/rrweb";
import "@sentry/rrweb/dist/style.css";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { duration } from "./presentation";

// Static trusted document only. Recording HTML never enters the administration DOM.
// No allow-scripts, forms, popups or top navigation. CSP is inherited by rrweb's child iframe.
export const PLAYER_DOCUMENT = `<!doctype html><html><head><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src data: blob:; font-src data:; frame-src 'self'; form-action 'none'; base-uri 'none'"><style>html,body{margin:0;overflow:hidden}#player{transform-origin:top left}</style></head><body><div id="player"></div></body></html>`;

export function ReplayPlayer({
  events,
  seekTo,
  onTime,
}: {
  events: Record<string, unknown>[];
  seekTo: { time: number; request: number } | null;
  onTime: (time: number) => void;
}) {
  const frame = useRef<HTMLIFrameElement>(null);
  const player = useRef<Replayer | null>(null);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [playing, setPlaying] = useState(false);
  const [position, setPosition] = useState(0);
  const [speed, setSpeed] = useState(1);
  const start =
    typeof events[0]?.timestamp === "number" ? events[0].timestamp : 0;
  const last = events.at(-1);
  const end = typeof last?.timestamp === "number" ? last.timestamp : start;
  const total = Math.max(0, end - start);
  useEffect(() => {
    const root = frame.current?.contentDocument?.getElementById("player");
    if (!ready || !root || events.length < 2) return;
    let instance: Replayer;
    try {
      instance = new Replayer(events as unknown as eventWithTime[], {
        root,
        showWarning: false,
        showDebug: false,
        UNSAFE_replayCanvas: false,
        mouseTail: false,
        skipInactive: false,
      });
      player.current = instance;
      instance.pause(0);
    } catch {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- Report an external replayer construction failure.
      setError("이 구간의 DOM snapshot을 재생할 수 없습니다.");
      return;
    }
    const resize = () => {
      const width = instance.iframe.width
        ? Number(instance.iframe.width)
        : 1000;
      const scale = Math.min(
        1,
        (frame.current?.clientWidth ?? width) / Math.max(1, width),
      );
      root.style.transform = `scale(${scale})`;
      if (frame.current)
        frame.current.style.height = `${Math.min(800, Math.max(240, (Number(instance.iframe.height) || 600) * scale))}px`;
    };
    const observer = new ResizeObserver(resize);
    if (frame.current) observer.observe(frame.current);
    instance.on("resize", resize);
    instance.on("finish", () => setPlaying(false));
    instance.on("pause", () => setPlaying(false));
    resize();
    const timer = window.setInterval(() => {
      const offset = Math.min(total, Math.max(0, instance.getCurrentTime()));
      setPosition(offset);
      onTime(start + offset);
    }, 200);
    return () => {
      clearInterval(timer);
      observer.disconnect();
      instance.destroy();
      player.current = null;
    };
  }, [events, ready, total, start, onTime]);
  useEffect(() => {
    player.current?.setConfig({ speed });
  }, [speed, events, ready]);
  useEffect(() => {
    if (seekTo !== null && player.current) {
      try {
        const offset = Math.max(0, Math.min(total, seekTo.time - start));
        player.current.pause(offset);
      } catch {
        // eslint-disable-next-line react-hooks/set-state-in-effect -- Report failure of the external replayer seek.
        setError("누락되거나 손상된 구간입니다. 다른 시점을 선택해 주세요.");
      }
    }
  }, [seekTo, start, total, onTime]);
  function seek(offset: number) {
    try {
      player.current?.pause(offset);
      setPosition(offset);
      onTime(start + offset);
      setPlaying(false);
    } catch {
      setError("누락되거나 손상된 구간입니다. 다른 시점을 선택해 주세요.");
    }
  }
  if (events.length < 2 || !events.some((event) => event.type === 2))
    return (
      <Notice tone="warning">
        이 구간에 재생 가능한 DOM snapshot이 없습니다. Timeline과 관측 데이터는
        아래에서 확인할 수 있습니다.
      </Notice>
    );
  return (
    <div className="replay-player">
      {error && <Notice tone="error">{error}</Notice>}
      <iframe
        ref={frame}
        title="Session replay 화면"
        sandbox="allow-same-origin"
        srcDoc={PLAYER_DOCUMENT}
        onLoad={() => setReady(true)}
        referrerPolicy="no-referrer"
        className="replay-frame"
      />
      <div className="replay-controls">
        <Button
          disabled={!ready || events.length < 2}
          onClick={() => {
            try {
              if (playing) {
                player.current?.pause();
                setPlaying(false);
              } else {
                player.current?.play(position >= total ? 0 : position);
                setPlaying(true);
              }
            } catch {
              setError("이 구간을 재생할 수 없습니다.");
            }
          }}
        >
          {playing ? "Pause" : "Play"}
        </Button>
        <label>
          Seek
          <input
            aria-label="Replay seek"
            type="range"
            min={0}
            max={Math.max(1, total)}
            value={position}
            onChange={(e) => seek(Number(e.target.value))}
          />
        </label>
        <span>
          {duration(position)} / {duration(total)}
        </span>
        <label>
          Speed
          <select
            aria-label="Speed"
            value={speed}
            onChange={(e) => setSpeed(Number(e.target.value))}
          >
            {[0.5, 1, 2, 4, 8].map((n) => (
              <option key={n} value={n}>
                {n}×
              </option>
            ))}
          </select>
        </label>
      </div>
      <p className="muted">
        외부 이미지·폰트·프레임 요청과 스크립트 실행을 차단하며 canvas 재생은
        지원하지 않습니다. 원본 사이트와 시각적 차이가 있을 수 있습니다.
      </p>
    </div>
  );
}
