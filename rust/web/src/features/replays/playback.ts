// rrweb's canvas snapshot path can load images in the parent document even with
// UNSAFE_replayCanvas=false. Remove that optional rendering hint from the private
// playback buffer. The stored SDK recording and privacy masking remain unchanged.
export function preparePlaybackEvent<T extends Record<string, unknown>>(
  event: T,
): T {
  function strip(value: unknown): void {
    if (!value || typeof value !== "object") return;
    if (Array.isArray(value)) {
      value.forEach(strip);
      return;
    }
    const object = value as Record<string, unknown>;
    delete object.rr_dataURL;
    Object.values(object).forEach(strip);
  }
  // SDK performanceSpan custom events use seconds; rrweb playback expects ms.
  const data = event.data as Record<string, unknown> | undefined;
  if (event.type === 5 && data?.tag === "performanceSpan") {
    const payload = data.payload as Record<string, unknown> | undefined;
    if (typeof payload?.startTimestamp === "number")
      (event as Record<string, unknown>).timestamp = Math.trunc(
        payload.startTimestamp * 1000,
      );
  }
  strip(event);
  return event;
}
