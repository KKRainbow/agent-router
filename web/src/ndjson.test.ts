import { describe, expect, it } from "vitest";

import { readNdjsonStream } from "./ndjson";
import type { WebStreamEvent } from "./types";

describe("readNdjsonStream", () => {
  it("handles split lines and error events", async () => {
    const encoder = new TextEncoder();
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode('{"type":"reply_delta","text":"he'));
        controller.enqueue(encoder.encode('llo"}\n{"type":"error","message":"bad"}'));
        controller.close();
      },
    });
    const response = new Response(stream, { status: 200 });
    const events: WebStreamEvent[] = [];

    await readNdjsonStream(response, (event) => events.push(event));

    expect(events).toEqual([
      { type: "reply_delta", text: "hello" },
      { type: "error", message: "bad" },
    ]);
  });
});
