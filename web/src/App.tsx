import {
  AssistantRuntimeProvider,
  type AppendMessage,
  type ThreadMessageLike,
  useExternalStoreRuntime,
} from "@assistant-ui/react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { fetchBootstrap, fetchTranscript, stopSession, streamMessage } from "./api";
import { ChatContext } from "./ChatContext";
import { ChatShell } from "./ChatThread";
import {
  STORAGE_KEYS,
  appendUserAndDraft,
  applyCancelReply,
  applyStreamEvent,
  applyTransientAssistantNotice,
  createSessionSummary,
  ensureCachedSessionMessages,
  readJsonStorage,
  reconcileCommittedMessages,
  titleForMessages,
  transcriptToMessages,
  updateCachedSessionMessages,
  writeJsonStorage,
} from "./store";
import type { ChatMessage, SessionSummary } from "./types";

type ActiveRun = {
  sessionId: string;
  assistantMessageId: string;
  controller: AbortController;
};

function initialSessions(): SessionSummary[] {
  const stored = readJsonStorage<SessionSummary[]>(STORAGE_KEYS.sessions, []);
  return stored.length ? stored : [createSessionSummary()];
}

export default function App() {
  const [sessions, setSessions] = useState<SessionSummary[]>(initialSessions);
  const [currentSessionId, setCurrentSessionId] = useState(() => {
    const stored = window.localStorage.getItem(STORAGE_KEYS.currentSession);
    return stored && sessions.some((session) => session.id === stored)
      ? stored
      : sessions[0].id;
  });
  const [messagesBySession, setMessagesBySession] = useState<
    Record<string, ChatMessage[]>
  >(() => ({
    [currentSessionId]: readJsonStorage<ChatMessage[]>(
      STORAGE_KEYS.messages(currentSessionId),
      [],
    ),
  }));
  const messages = messagesBySession[currentSessionId] ?? [];
  const [isRunning, setIsRunning] = useState(false);
  const [authToken, setAuthToken] = useState(
    () => window.localStorage.getItem(STORAGE_KEYS.authToken) ?? "",
  );
  const [authRequired, setAuthRequired] = useState(false);
  const [connectionLabel, setConnectionLabel] = useState("Connecting");
  const activeRunRef = useRef<ActiveRun | null>(null);

  useEffect(() => {
    writeJsonStorage(STORAGE_KEYS.sessions, sessions);
  }, [sessions]);

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEYS.currentSession, currentSessionId);
  }, [currentSessionId]);

  useEffect(() => {
    if (authToken) {
      window.localStorage.setItem(STORAGE_KEYS.authToken, authToken);
    } else {
      window.localStorage.removeItem(STORAGE_KEYS.authToken);
    }
  }, [authToken]);

  useEffect(() => {
    for (const [sessionId, sessionMessages] of Object.entries(messagesBySession)) {
      writeJsonStorage(STORAGE_KEYS.messages(sessionId), sessionMessages);
    }
    const now = Date.now();
    setSessions((previous) =>
      previous.map((session) => {
        const sessionMessages = messagesBySession[session.id];
        if (!sessionMessages) return session;
        return {
          ...session,
          title: titleForMessages(sessionMessages),
          updatedAt: now,
        };
      }),
    );
  }, [messagesBySession]);

  useEffect(() => {
    let cancelled = false;
    fetchBootstrap(authToken || undefined)
      .then((bootstrap) => {
        if (cancelled) return;
        setAuthRequired(false);
        setConnectionLabel(bootstrap.channel_events);
      })
      .catch((error) => {
        if (cancelled) return;
        setConnectionLabel(errorMessage(error).includes("401") ? "Auth" : "Offline");
        setAuthRequired(errorMessage(error).includes("401"));
      });
    return () => {
      cancelled = true;
    };
  }, [authToken]);

  useEffect(() => {
    const localMessages = readJsonStorage<ChatMessage[]>(
      STORAGE_KEYS.messages(currentSessionId),
      [],
    );
    setMessagesBySession((previous) =>
      ensureCachedSessionMessages(previous, currentSessionId, localMessages),
    );
    if (activeRunRef.current?.sessionId === currentSessionId) return;

    let cancelled = false;
    fetchTranscript(currentSessionId, authToken || undefined)
      .then((transcript) => {
        if (cancelled) return;
        const committed = transcriptToMessages(transcript);
        if (committed.length) {
          setMessagesBySession((previous) =>
            updateCachedSessionMessages(previous, currentSessionId, (current) =>
              reconcileCommittedMessages(current, committed),
            ),
          );
        }
      })
      .catch((error) => {
        if (cancelled) return;
        if (errorMessage(error).includes("401")) {
          setAuthRequired(true);
          setConnectionLabel("Auth");
        }
      });
    return () => {
      cancelled = true;
    };
  }, [authToken, currentSessionId, isRunning]);

  const onNew = useCallback(
    async (message: AppendMessage) => {
      if (activeRunRef.current) return;
      const text = appendMessageText(message);
      if (!text.trim()) return;

      const sessionId = currentSessionId;
      const clientMessageId = `user-${Date.now()}`;
      const assistantMessageId = `assistant-${Date.now()}`;
      const controller = new AbortController();
      activeRunRef.current = {
        sessionId,
        assistantMessageId,
        controller,
      };
      setMessagesBySession((previous) =>
        updateCachedSessionMessages(previous, sessionId, (sessionMessages) =>
          appendUserAndDraft(
            sessionMessages,
            text,
            clientMessageId,
            assistantMessageId,
          ),
        ),
      );
      setIsRunning(true);

      try {
        await streamMessage(
          sessionId,
          text,
          clientMessageId,
          (event) => {
            setMessagesBySession((previous) =>
              updateCachedSessionMessages(previous, sessionId, (sessionMessages) =>
                applyStreamEvent(sessionMessages, assistantMessageId, event),
              ),
            );
          },
          {
            authToken: authToken || undefined,
            signal: controller.signal,
          },
        );
      } catch (error) {
        if (!controller.signal.aborted) {
          setMessagesBySession((previous) =>
            updateCachedSessionMessages(previous, sessionId, (sessionMessages) =>
              applyStreamEvent(sessionMessages, assistantMessageId, {
                type: "error",
                message: errorMessage(error),
              }),
            ),
          );
        }
      } finally {
        if (activeRunRef.current?.assistantMessageId === assistantMessageId) {
          activeRunRef.current = null;
          setIsRunning(false);
        }
      }
    },
    [authToken, currentSessionId, isRunning],
  );

  const onCancel = useCallback(async () => {
    const activeRun = activeRunRef.current;
    if (!activeRun) return;
    try {
      const reply = await stopSession(activeRun.sessionId, authToken || undefined);
      if (activeRunRef.current?.assistantMessageId !== activeRun.assistantMessageId) {
        return;
      }
      if (!reply.stopped) {
        return;
      }
      activeRun.controller.abort();
      setMessagesBySession((previous) =>
        updateCachedSessionMessages(
          previous,
          activeRun.sessionId,
          (sessionMessages) =>
            applyCancelReply(
              sessionMessages,
              activeRun.assistantMessageId,
              reply.text,
            ),
        ),
      );
    } catch (error) {
      if (activeRunRef.current?.assistantMessageId !== activeRun.assistantMessageId) {
        return;
      }
      setMessagesBySession((previous) =>
        updateCachedSessionMessages(
          previous,
          activeRun.sessionId,
          (sessionMessages) =>
            applyTransientAssistantNotice(
              sessionMessages,
              activeRun.assistantMessageId,
              `Stop failed: ${errorMessage(error)}`,
            ),
        ),
      );
      return;
    } finally {
      if (
        activeRun.controller.signal.aborted &&
        activeRunRef.current?.assistantMessageId === activeRun.assistantMessageId
      ) {
        activeRunRef.current = null;
        setIsRunning(false);
      }
    }
  }, [authToken]);

  const runtime = useExternalStoreRuntime({
    isRunning,
    messages,
    convertMessage,
    onNew,
    onCancel,
  });

  const contextValue = useMemo(
    () => ({
      messages,
      sessions,
      currentSessionId,
      authToken,
      authRequired,
      connectionLabel,
      isRunning,
      createSession: () => {
        if (isRunning) return;
        const session = createSessionSummary();
        setSessions((previous) => [session, ...previous]);
        setMessagesBySession((previous) => ({
          ...previous,
          [session.id]: [],
        }));
        setCurrentSessionId(session.id);
      },
      selectSession: (sessionId: string) => {
        if (isRunning || sessionId === currentSessionId) return;
        const localMessages = readJsonStorage<ChatMessage[]>(
          STORAGE_KEYS.messages(sessionId),
          [],
        );
        setMessagesBySession((previous) =>
          ensureCachedSessionMessages(previous, sessionId, localMessages),
        );
        setCurrentSessionId(sessionId);
      },
      updateAuthToken: setAuthToken,
    }),
    [
      authRequired,
      authToken,
      connectionLabel,
      currentSessionId,
      isRunning,
      messages,
      sessions,
    ],
  );

  return (
    <AssistantRuntimeProvider runtime={runtime}>
      <ChatContext.Provider value={contextValue}>
        <ChatShell />
      </ChatContext.Provider>
    </AssistantRuntimeProvider>
  );
}

function convertMessage(message: ChatMessage): ThreadMessageLike {
  return {
    id: message.id,
    role: message.role,
    content: [{ type: "text", text: message.text }],
    createdAt: new Date(message.createdAt),
  };
}

function appendMessageText(message: AppendMessage): string {
  return message.content
    .map((part) => (part.type === "text" ? part.text : ""))
    .join("")
    .trim();
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
