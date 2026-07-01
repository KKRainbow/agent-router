import { describe, expect, it } from "vitest";

import {
  appendUserAndDraft,
  applyCancelReply,
  applyStreamEvent,
  applyTransientAssistantNotice,
  ensureCachedSessionMessages,
  reconcileCommittedMessages,
  transcriptToMessages,
  updateCachedSessionMessages,
} from "./store";

const COMPACT_SNAPSHOT = {
  executor: "kimi",
  latest_reasoning: null,
  commands: [{ label: "cargo test", count: 1 }],
  command_remaining: 0,
  tools: [],
  tool_remaining: 0,
  attention: [],
  attention_remaining: 0,
  progress: [],
  progress_omitted: 0,
};

describe("chat store", () => {
  it("appends a user message and streaming assistant draft", () => {
    const messages = appendUserAndDraft([], "hello", "user-1", "assistant-1");

    expect(messages).toMatchObject([
      { id: "user-1", role: "user", text: "hello", status: "complete" },
      {
        id: "assistant-1",
        role: "assistant",
        text: "",
        status: "streaming",
        activities: [],
      },
    ]);
  });

  it("applies streaming deltas, activity, final reply, and done", () => {
    let messages = appendUserAndDraft([], "run", "user-1", "assistant-1");

    messages = applyStreamEvent(messages, "assistant-1", {
      type: "reply_delta",
      text: "hel",
    });
    messages = applyStreamEvent(messages, "assistant-1", {
      type: "reply_delta",
      text: "lo",
    });
    messages = applyStreamEvent(messages, "assistant-1", {
      type: "activity",
      kind: "tool_call",
      executor: "kimi",
      title: "Bash",
      text: "cargo test",
    });
    messages = applyStreamEvent(messages, "assistant-1", {
      type: "activity_snapshot",
      snapshot: COMPACT_SNAPSHOT,
    });

    expect(messages[1]).toMatchObject({
      activitySnapshot: COMPACT_SNAPSHOT,
    });

    messages = applyStreamEvent(messages, "assistant-1", {
      type: "final_reply",
      text: "done",
    });
    messages = applyStreamEvent(messages, "assistant-1", { type: "done" });

    expect(messages[1]).toMatchObject({
      text: "done",
      status: "complete",
      localOnly: false,
      activities: [
        {
          kind: "tool_call",
          executor: "kimi",
          title: "Bash",
          text: "cargo test",
        },
      ],
    });
    expect(messages[1].activitySnapshot).toBeUndefined();
    expect(messages[0]).toMatchObject({ localOnly: false });
  });

  it("clears compact activity summary on stream errors", () => {
    let messages = appendUserAndDraft([], "run", "user-1", "assistant-1");

    messages = applyStreamEvent(messages, "assistant-1", {
      type: "activity_snapshot",
      snapshot: COMPACT_SNAPSHOT,
    });
    messages = applyStreamEvent(messages, "assistant-1", {
      type: "error",
      message: "failed",
    });

    expect(messages[1]).toMatchObject({
      text: "failed",
      status: "error",
      localOnly: true,
    });
    expect(messages[1].activitySnapshot).toBeUndefined();
  });

  it("clears compact activity summary when the stream finishes without final reply", () => {
    let messages = appendUserAndDraft([], "run", "user-1", "assistant-1");

    messages = applyStreamEvent(messages, "assistant-1", {
      type: "activity_snapshot",
      snapshot: COMPACT_SNAPSHOT,
    });
    messages = applyStreamEvent(messages, "assistant-1", { type: "done" });

    expect(messages[1]).toMatchObject({ status: "complete" });
    expect(messages[1].activitySnapshot).toBeUndefined();
  });

  it("keeps slash command final replies local-only", () => {
    const messages = applyStreamEvent(
      appendUserAndDraft([], "/agent status", "user-1", "assistant-1"),
      "assistant-1",
      {
        type: "final_reply",
        text: "Default executor: kimi",
      },
    );

    expect(messages[0]).toMatchObject({ localOnly: true });
    expect(messages[1]).toMatchObject({
      text: "Default executor: kimi",
      localOnly: true,
    });
  });

  it("cancel reply clears the running draft", () => {
    let messages = appendUserAndDraft([], "run", "user-1", "assistant-1");
    messages = applyStreamEvent(messages, "assistant-1", {
      type: "activity_snapshot",
      snapshot: COMPACT_SNAPSHOT,
    });
    messages = applyCancelReply(messages, "assistant-1", "Stopped the active turn.");

    expect(messages[1]).toMatchObject({
      text: "Stopped the active turn.",
      status: "complete",
      localOnly: true,
    });
    expect(messages[1].activitySnapshot).toBeUndefined();
    expect(messages[0]).toMatchObject({ localOnly: true });
  });

  it("transient assistant notices do not change turn metadata", () => {
    let messages = appendUserAndDraft([], "run", "user-1", "assistant-1");
    messages = applyTransientAssistantNotice(
      messages,
      "assistant-1",
      "Stop failed: offline",
    );

    expect(messages[1]).toMatchObject({
      text: "Stop failed: offline",
      localOnly: false,
      status: "streaming",
    });
    expect(messages[0]).toMatchObject({ localOnly: false });
  });

  it("keeps message updates scoped to their session", () => {
    const first = appendUserAndDraft([], "first", "u1", "a1");
    const second = appendUserAndDraft([], "second", "u2", "a2");
    let cache = ensureCachedSessionMessages({}, "first", first);

    cache = ensureCachedSessionMessages(cache, "second", second);
    cache = updateCachedSessionMessages(cache, "first", (messages) =>
      applyStreamEvent(messages, "a1", {
        type: "final_reply",
        text: "first done",
      }),
    );

    expect(cache.first[1]).toMatchObject({
      id: "a1",
      text: "first done",
      status: "complete",
    });
    expect(cache.second).toEqual(second);
  });

  it("does not replace an existing session cache when switching sessions", () => {
    const existing = appendUserAndDraft([], "first", "u1", "a1");
    const staleFallback = appendUserAndDraft([], "wrong", "u2", "a2");

    const cache = ensureCachedSessionMessages(
      { first: existing },
      "first",
      staleFallback,
    );

    expect(cache.first).toEqual(existing);
  });

  it("loads assistant-ui-compatible transcript messages", () => {
    const messages = transcriptToMessages({
      messages: [
        {
          id: "m1",
          role: "user",
          content: [{ type: "text", text: "hi" }],
          created_at_ms: 1,
        },
        {
          id: "m2",
          role: "assistant",
          content: [{ type: "text", text: "hello" }],
          created_at_ms: 2,
        },
        {
          id: "m3",
          role: "system",
          content: [{ type: "text", text: "ignored" }],
          created_at_ms: 3,
        },
      ],
    });

    expect(messages).toEqual([
      {
        id: "m1",
        role: "user",
        text: "hi",
        createdAt: 1,
        status: "complete",
        committed: true,
        activities: [],
      },
      {
        id: "m2",
        role: "assistant",
        text: "hello",
        createdAt: 2,
        status: "complete",
        committed: true,
        activities: [],
      },
    ]);
  });

  it("keeps local-only command replies when committed transcript refreshes", () => {
    const committed = [
      {
        id: "c1",
        role: "user" as const,
        text: "prior",
        createdAt: 1,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
      {
        id: "c2",
        role: "assistant" as const,
        text: "prior reply",
        createdAt: 2,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
    ];
    const local = [
      ...committed,
      ...appendUserAndDraft([], "/agent status", "u3", "a3"),
    ];
    const withReply = applyStreamEvent(local, "a3", {
      type: "final_reply",
      text: "Default executor: kimi",
    });

    expect(reconcileCommittedMessages(withReply, committed)).toEqual(withReply);
  });

  it("appends newly committed transcript entries when they extend local cache", () => {
    const current = [
      {
        id: "c1",
        role: "user" as const,
        text: "prior",
        createdAt: 1,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
    ];
    const committed = [
      current[0],
      {
        id: "c2",
        role: "assistant" as const,
        text: "prior reply",
        createdAt: 2,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
    ];

    expect(reconcileCommittedMessages(current, committed)).toEqual(committed);
  });

  it("appends committed transcript entries after local-only command replies", () => {
    const prior = [
      {
        id: "c1",
        role: "user" as const,
        text: "prior",
        createdAt: 1,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
      {
        id: "c2",
        role: "assistant" as const,
        text: "prior reply",
        createdAt: 2,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
    ];
    const current = [
      ...prior,
      {
        id: "u3",
        role: "user" as const,
        text: "/agent status",
        createdAt: 3,
        status: "complete" as const,
        activities: [],
      },
      {
        id: "a3",
        role: "assistant" as const,
        text: "Default executor: kimi",
        createdAt: 4,
        status: "complete" as const,
        activities: [],
      },
    ];
    const nextCommitted = {
      id: "c3",
      role: "user" as const,
      text: "later task",
      createdAt: 5,
      status: "complete" as const,
      committed: true,
      activities: [],
    };

    const reconciled = reconcileCommittedMessages(current, [
      ...prior,
      nextCommitted,
    ]);

    expect(reconciled).toEqual([...current, nextCommitted]);
  });

  it("does not let local-only duplicate text hide a committed message", () => {
    const local = applyStreamEvent(
      appendUserAndDraft([], "/agent status", "u1", "a1"),
      "a1",
      {
        type: "final_reply",
        text: "Done.",
      },
    );
    const committed = [
      {
        id: "c1",
        role: "assistant" as const,
        text: "Done.",
        createdAt: 5,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
    ];

    expect(reconcileCommittedMessages(local, committed)).toEqual([
      ...local,
      committed[0],
    ]);
  });

  it("keeps visible final reply instead of appending projected assistant transcript", () => {
    let current = appendUserAndDraft([], "normal task", "u1", "a1");
    current = applyStreamEvent(current, "a1", {
      type: "final_reply",
      text: "Done.",
    });
    const committed = [
      {
        id: "c1",
        role: "user" as const,
        text: "normal task",
        createdAt: 10,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
      {
        id: "c2",
        role: "assistant" as const,
        text: "[Executor: kimi]\nVisible reply:\nDone.",
        createdAt: 11,
        status: "complete" as const,
        committed: true,
        activities: [],
      },
    ];

    expect(reconcileCommittedMessages(current, committed)).toEqual(current);
  });
});
