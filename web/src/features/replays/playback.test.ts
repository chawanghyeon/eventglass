import { describe, expect, it } from "vitest";
import { preparePlaybackEvent } from "./playback";

describe("private Replay playback buffer", () => {
  it("keeps custom timeline events on the rrweb millisecond clock", () => {
    const result = preparePlaybackEvent({
      type: 5,
      timestamp: 1700000000,
      data: {
        tag: "performanceSpan",
        payload: { startTimestamp: 1700000000.25 },
      },
    });
    expect(result.timestamp).toBe(1700000000250);
  });
  it("removes canvas image hints from snapshots and mutations while preserving masked DOM", () => {
    const event = {
      data: {
        node: {
          attributes: {
            rr_dataURL: "https://untrusted.invalid/canvas",
            value: "****",
          },
          childNodes: [{ textContent: "****" }],
        },
        attributes: [
          { attributes: { rr_dataURL: "https://untrusted.invalid/mutation" } },
        ],
      },
    };
    const result = preparePlaybackEvent(event);
    expect(JSON.stringify(result)).not.toContain("untrusted.invalid");
    expect(result.data.node.attributes.value).toBe("****");
    expect(result.data.node.childNodes[0].textContent).toBe("****");
  });
});
