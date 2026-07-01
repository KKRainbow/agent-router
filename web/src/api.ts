import { readNdjsonStream } from "./ndjson";
import type { WebStreamEvent, WebTranscriptResponse } from "./types";

type FetchOptions = {
  authToken?: string;
  signal?: AbortSignal;
};

export async function fetchBootstrap(authToken?: string) {
  const response = await fetch("/api/web/bootstrap", {
    headers: authHeaders(authToken),
  });
  if (!response.ok) {
    throw new Error(`Bootstrap failed with HTTP ${response.status}`);
  }
  return response.json() as Promise<{
    user_id: string;
    channel_events: "off" | "compact" | "verbose";
  }>;
}

export async function fetchTranscript(sessionId: string, authToken?: string) {
  const response = await fetch(`/api/web/sessions/${sessionId}/transcript`, {
    headers: authHeaders(authToken),
  });
  if (!response.ok) {
    throw new Error(`Transcript failed with HTTP ${response.status}`);
  }
  return response.json() as Promise<WebTranscriptResponse>;
}

export async function streamMessage(
  sessionId: string,
  text: string,
  clientMessageId: string,
  onEvent: (event: WebStreamEvent) => void,
  options: FetchOptions = {},
) {
  const response = await fetch(`/api/web/sessions/${sessionId}/messages`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...authHeaders(options.authToken),
    },
    body: JSON.stringify({ text, client_message_id: clientMessageId }),
    signal: options.signal,
  });
  await readNdjsonStream(response, onEvent);
}

export async function stopSession(sessionId: string, authToken?: string) {
  const response = await fetch(`/api/web/sessions/${sessionId}/stop`, {
    method: "POST",
    headers: authHeaders(authToken),
  });
  if (!response.ok) {
    throw new Error(`Stop failed with HTTP ${response.status}`);
  }
  return response.json() as Promise<{ stopped: boolean; text: string }>;
}

function authHeaders(authToken?: string): HeadersInit {
  return authToken ? { authorization: `Bearer ${authToken}` } : {};
}
