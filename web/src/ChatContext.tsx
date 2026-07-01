import { createContext, useContext } from "react";

import type { ChatMessage, SessionSummary } from "./types";

export type ChatContextValue = {
  messages: ChatMessage[];
  sessions: SessionSummary[];
  currentSessionId: string;
  authToken: string;
  authRequired: boolean;
  connectionLabel: string;
  isRunning: boolean;
  createSession: () => void;
  selectSession: (sessionId: string) => void;
  updateAuthToken: (token: string) => void;
};

export const ChatContext = createContext<ChatContextValue | null>(null);

export function useChatContext() {
  const value = useContext(ChatContext);
  if (!value) {
    throw new Error("ChatContext is missing");
  }
  return value;
}
