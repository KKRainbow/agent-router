import type {
  ChatActivity,
  ChatMessage,
  SessionSummary,
  WebStreamEvent,
  WebTranscriptResponse,
} from "./types";

export const STORAGE_KEYS = {
  sessions: "agent-router:web:sessions",
  currentSession: "agent-router:web:current-session",
  authToken: "agent-router:web:auth-token",
  messages: (sessionId: string) => `agent-router:web:messages:${sessionId}`,
};

export type MessageCache = Record<string, ChatMessage[]>;

export function createSessionId(): string {
  const cryptoApi = globalThis.crypto;
  if (cryptoApi?.randomUUID) {
    return cryptoApi.randomUUID().replace(/-/g, "").slice(0, 24);
  }
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 12)}`;
}

export function createSessionSummary(id = createSessionId()): SessionSummary {
  return {
    id,
    title: "New chat",
    updatedAt: Date.now(),
  };
}

export function appendUserAndDraft(
  messages: ChatMessage[],
  text: string,
  clientMessageId: string,
  assistantMessageId: string,
): ChatMessage[] {
  const now = Date.now();
  const localOnly = isLocalOnlyInput(text);
  return [
    ...messages,
    {
      id: clientMessageId,
      role: "user",
      text,
      createdAt: now,
      status: "complete",
      localOnly,
    },
    {
      id: assistantMessageId,
      role: "assistant",
      text: "",
      createdAt: now,
      status: "streaming",
      localOnly,
      activities: [],
    },
  ];
}

export function applyStreamEvent(
  messages: ChatMessage[],
  assistantMessageId: string,
  event: WebStreamEvent,
): ChatMessage[] {
  if (event.type === "accepted") return messages;
  if (event.type === "done") {
    return updateAssistant(messages, assistantMessageId, (message) => ({
      ...message,
      status: message.status === "error" ? "error" : "complete",
    }));
  }
  if (event.type === "reply_delta") {
    return updateAssistant(messages, assistantMessageId, (message) => ({
      ...message,
      text: message.text + event.text,
    }));
  }
  if (event.type === "reply_break") {
    return updateAssistant(messages, assistantMessageId, (message) => ({
      ...message,
      text: message.text.trimEnd() ? `${message.text.trimEnd()}\n\n` : message.text,
    }));
  }
  if (event.type === "final_reply") {
    return updateAssistant(messages, assistantMessageId, (message) => ({
      ...message,
      text: event.text,
      status: "complete",
    }));
  }
  if (event.type === "activity") {
    const activity: ChatActivity = {
      id: `${assistantMessageId}:activity:${messages.length}:${Date.now()}`,
      kind: event.kind,
      executor: event.executor,
      title: event.title,
      text: event.text,
    };
    return updateAssistant(messages, assistantMessageId, (message) => ({
      ...message,
      activities: [...(message.activities ?? []), activity],
    }));
  }
  return updateAssistantAndPreviousUser(messages, assistantMessageId, (message) => ({
    ...message,
    text: message.text || event.message,
    status: "error",
    localOnly: true,
  }));
}

export function applyCancelReply(
  messages: ChatMessage[],
  assistantMessageId: string | null,
  text: string,
): ChatMessage[] {
  if (!assistantMessageId) return messages;
  return updateAssistantAndPreviousUser(messages, assistantMessageId, (message) => ({
    ...message,
    text: text || message.text,
    status: "complete",
    localOnly: true,
  }));
}

export function applyTransientAssistantNotice(
  messages: ChatMessage[],
  assistantMessageId: string | null,
  text: string,
): ChatMessage[] {
  if (!assistantMessageId) return messages;
  return updateAssistant(messages, assistantMessageId, (message) => ({
    ...message,
    text: message.text ? `${message.text}\n\n${text}` : text,
  }));
}

function updateAssistantAndPreviousUser(
  messages: ChatMessage[],
  assistantMessageId: string,
  update: (message: ChatMessage) => ChatMessage,
  previousUserPatch: Partial<ChatMessage> = { localOnly: true },
): ChatMessage[] {
  const assistantIndex = messages.findIndex((message) => message.id === assistantMessageId);
  return messages.map((message, index) => {
    if (index === assistantIndex) {
      return update(message);
    }
    if (index === assistantIndex - 1 && message.role === "user") {
      return {
        ...message,
        ...previousUserPatch,
      };
    }
    return message;
  });
}

export function ensureCachedSessionMessages(
  cache: MessageCache,
  sessionId: string,
  messages: ChatMessage[],
): MessageCache {
  if (hasSessionMessages(cache, sessionId)) {
    return cache;
  }
  return {
    ...cache,
    [sessionId]: messages,
  };
}

export function updateCachedSessionMessages(
  cache: MessageCache,
  sessionId: string,
  update: (messages: ChatMessage[]) => ChatMessage[],
): MessageCache {
  return {
    ...cache,
    [sessionId]: update(cache[sessionId] ?? []),
  };
}

function hasSessionMessages(cache: MessageCache, sessionId: string): boolean {
  return Object.prototype.hasOwnProperty.call(cache, sessionId);
}

export function transcriptToMessages(
  transcript: WebTranscriptResponse,
): ChatMessage[] {
  return transcript.messages
    .filter((message) => message.role === "user" || message.role === "assistant")
    .map((message) => ({
      id: message.id,
      role: message.role as "user" | "assistant",
      text: message.content.find((part) => part.type === "text")?.text ?? "",
      createdAt: message.created_at_ms,
      status: "complete" as const,
      committed: true,
      activities: [],
    }));
}

export function reconcileCommittedMessages(
  current: ChatMessage[],
  committed: ChatMessage[],
): ChatMessage[] {
  if (!committed.length) return current;
  if (!current.length) return committed;

  const represented = new Map<string, ChatMessage[]>();
  for (const message of current.filter(representsCommittedMessage)) {
    const fingerprint = messageFingerprint(message);
    represented.set(fingerprint, [...(represented.get(fingerprint) ?? []), message]);
  }
  let previousOptimisticUserWasRepresented = false;
  const missing = committed.filter((message) => {
    if (message.role === "assistant" && previousOptimisticUserWasRepresented) {
      previousOptimisticUserWasRepresented = false;
      return false;
    }
    const fingerprint = messageFingerprint(message);
    const candidates = represented.get(fingerprint) ?? [];
    const representedBy = candidates.shift();
    if (representedBy) {
      represented.set(fingerprint, candidates);
      previousOptimisticUserWasRepresented =
        message.role === "user" && !representedBy.committed;
      return false;
    }
    previousOptimisticUserWasRepresented = false;
    return true;
  });

  return missing.length ? [...current, ...missing] : current;
}

export function titleForMessages(messages: ChatMessage[]): string {
  const firstUser = messages.find((message) => message.role === "user");
  if (!firstUser) return "New chat";
  const title = firstUser.text.trim().replace(/\s+/g, " ");
  return title.length > 42 ? `${title.slice(0, 39)}...` : title || "New chat";
}

function messageFingerprint(message: ChatMessage): string {
  return `${message.role}\0${message.text}`;
}

function representsCommittedMessage(message: ChatMessage): boolean {
  return !message.localOnly;
}

function isLocalOnlyInput(text: string): boolean {
  return text.trimStart().startsWith("/");
}

function updateAssistant(
  messages: ChatMessage[],
  assistantMessageId: string,
  update: (message: ChatMessage) => ChatMessage,
): ChatMessage[] {
  return messages.map((message) =>
    message.id === assistantMessageId ? update(message) : message,
  );
}

export function readJsonStorage<T>(key: string, fallback: T): T {
  try {
    const value = window.localStorage.getItem(key);
    return value ? (JSON.parse(value) as T) : fallback;
  } catch {
    return fallback;
  }
}

export function writeJsonStorage(key: string, value: unknown) {
  window.localStorage.setItem(key, JSON.stringify(value));
}
