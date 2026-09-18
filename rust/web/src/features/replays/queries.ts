import { queryOptions } from "@tanstack/react-query";
import { preparePlaybackEvent } from "./playback";
import { endpoints } from "../../api/endpoints";

export function replaysQuery(user: string, query: string) {
  return queryOptions({
    queryKey: ["replays", user, "list", query],
    queryFn: ({ signal }) => endpoints.replays(query, signal),
    staleTime: 15000,
  });
}
export function replayQuery(user: string, project: string, id: string) {
  return queryOptions({
    queryKey: ["replays", user, project, id],
    queryFn: ({ signal }) => endpoints.replay(project, id, signal),
    staleTime: 15000,
  });
}
export function analysisQuery(user: string, project: string, id: string) {
  return queryOptions({
    queryKey: ["replays", user, project, id, "analysis"],
    queryFn: ({ signal }) => endpoints.replayAnalysis(project, id, signal),
    staleTime: 30000,
  });
}

// One bounded query owns the playback buffer; segment bodies are not separately cached.
export function recordingQuery(
  user: string,
  project: string,
  id: string,
  segments: readonly { segment_id: number }[],
) {
  return queryOptions({
    queryKey: [
      "replays",
      user,
      project,
      id,
      "recording",
      segments.map((s) => s.segment_id),
    ],
    gcTime: 0,
    staleTime: Infinity,
    queryFn: async ({ signal }) => {
      const events: Record<string, unknown>[] = [];
      let bytes = 0;
      let truncated = false;
      const gaps: number[] = [];
      let expected = 0;
      for (const segment of segments) {
        if (segment.segment_id !== expected) gaps.push(segment.segment_id);
        expected = segment.segment_id + 1;
        const result = await endpoints.replayRecording(
          project,
          id,
          segment.segment_id,
          signal,
        );
        bytes += new TextEncoder().encode(
          JSON.stringify(result.events),
        ).byteLength;
        if (
          bytes > 64 * 1024 * 1024 ||
          events.length + result.events.length > 100000
        ) {
          truncated = true;
          break;
        }
        for (const event of result.events) {
          if (
            typeof event.type === "number" &&
            event.type >= 0 &&
            event.type <= 5 &&
            typeof event.timestamp === "number" &&
            Number.isFinite(event.timestamp) &&
            typeof event.data === "object" &&
            event.data !== null
          )
            events.push(preparePlaybackEvent(event));
        }
      }
      events.sort((a, b) => (a.timestamp as number) - (b.timestamp as number));
      return { events, truncated, gaps };
    },
  });
}

export function mapsQuery(user: string, query: string) {
  return queryOptions({
    queryKey: ["replays", user, "maps", query],
    queryFn: ({ signal }) => endpoints.replayMaps(query, signal),
    staleTime: 60000,
  });
}
