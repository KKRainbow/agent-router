import type { WebStreamEvent } from "./types";

export async function readNdjsonStream(
  response: Response,
  onEvent: (event: WebStreamEvent) => void,
): Promise<void> {
  if (!response.ok) {
    throw new Error(`Request failed with HTTP ${response.status}`);
  }
  if (!response.body) {
    throw new Error("Streaming response body is not available");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    buffer = drainLines(buffer, onEvent);
  }

  buffer += decoder.decode();
  parseLine(buffer.trim(), onEvent);
}

function drainLines(
  buffer: string,
  onEvent: (event: WebStreamEvent) => void,
): string {
  let newline = buffer.indexOf("\n");
  while (newline !== -1) {
    const line = buffer.slice(0, newline).trim();
    parseLine(line, onEvent);
    buffer = buffer.slice(newline + 1);
    newline = buffer.indexOf("\n");
  }
  return buffer;
}

function parseLine(line: string, onEvent: (event: WebStreamEvent) => void) {
  if (!line) return;
  onEvent(JSON.parse(line) as WebStreamEvent);
}
