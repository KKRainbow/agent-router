export type ActivityKind = "agent_progress" | "reasoning_summary" | "tool_call";

export type WebStreamEvent =
  | { type: "accepted"; turn_id: string }
  | {
      type: "activity";
      kind: ActivityKind;
      executor: string;
      title: string;
      text: string;
    }
  | { type: "reply_delta"; text: string }
  | { type: "reply_break" }
  | { type: "final_reply"; text: string }
  | { type: "error"; message: string }
  | { type: "done" };

export type ChatRole = "user" | "assistant";

export type ChatActivity = {
  id: string;
  kind: ActivityKind;
  executor: string;
  title: string;
  text: string;
};

export type ChatMessage = {
  id: string;
  role: ChatRole;
  text: string;
  createdAt: number;
  status?: "streaming" | "complete" | "error";
  committed?: boolean;
  localOnly?: boolean;
  activities?: ChatActivity[];
};

export type WebTranscriptMessage = {
  id: string;
  role: string;
  content: Array<{ type: "text"; text: string }>;
  created_at_ms: number;
  executor?: string;
};

export type WebTranscriptResponse = {
  messages: WebTranscriptMessage[];
};

export type SessionSummary = {
  id: string;
  title: string;
  updatedAt: number;
};
