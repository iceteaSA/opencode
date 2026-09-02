import { describe, expect, test } from "bun:test"
import type { Part } from "@opencode-ai/sdk/v2"
import { activeSubagents, deriveSubagents, latestSubagentTPS } from "../../src/feature-plugins/sidebar/subagents"

const messages = [{ id: "message-1" }]
const noStatus = () => undefined
const busyStatus = () => ({ type: "busy" as const })

describe("sidebar subagents", () => {
  test("returns no entries when the session has no task parts", () => {
    expect(activeSubagents(messages, () => [], noStatus)).toEqual([])
  })

  test("derives running and completed subagents", () => {
    expect(
      activeSubagents(messages, () => [
        {
          type: "tool",
          tool: "task",
          state: {
            status: "running",
            input: { description: "Research APIs" },
            metadata: { sessionId: "child-running" },
          },
        },
        {
          type: "tool",
          tool: "task",
          state: {
            status: "completed",
            input: { description: "Review changes" },
            title: "Review changes",
            metadata: { sessionId: "child-completed" },
          },
        },
      ] as unknown as Part[], (sessionID) => (sessionID === "child-running" ? busyStatus() : undefined)),
    ).toEqual([{ description: "Research APIs", status: "active", session_id: "child-running" }])
  })

  test("marks errored subagents as failed", () => {
    expect(
      activeSubagents(messages, () => [
        {
          type: "tool",
          tool: "task",
          state: {
            status: "error",
            input: { description: "Run checks" },
            error: "failed",
            metadata: { sessionId: "child-error" },
          },
        },
      ] as unknown as Part[], noStatus),
    ).toEqual([])
  })

  test("keeps pending subagents without a session id non-navigable", () => {
    expect(
      activeSubagents(messages, () => [
        {
          type: "tool",
          tool: "task",
          state: {
            status: "pending",
            input: { description: "Start worker" },
            raw: "{}",
          },
        },
      ] as unknown as Part[], noStatus),
    ).toEqual([{ description: "Start worker", status: "pending", session_id: undefined }])
  })

  test("deduplicates one child session across messages", () => {
    expect(
      activeSubagents(
        [{ id: "message-1" }, { id: "message-2" }],
        () =>
          [
            {
              type: "tool",
              tool: "task",
              state: {
                status: "running",
                input: { description: "Research APIs" },
                metadata: { sessionId: "child-1" },
              },
            },
          ] as unknown as Part[],
          busyStatus,
      ),
    ).toEqual([{ description: "Research APIs", status: "active", session_id: "child-1" }])
  })

  test("keeps the latest status when a child session is resumed", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "running",
          input: { description: "Research APIs" },
          metadata: { sessionId: "child-1" },
        },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Research APIs" },
          title: "Research APIs",
          metadata: { sessionId: "child-1" },
        },
      },
    ] as unknown as Part[]

    expect(
      deriveSubagents(
        [{ id: "message-1" }, { id: "message-2" }],
        (messageID) => [parts[messageID === "message-1" ? 0 : 1]!],
      ),
    ).toEqual([{ description: "Research APIs", status: "done", session_id: "child-1" }])
    expect(
      activeSubagents(
        [{ id: "message-1" }, { id: "message-2" }],
        (messageID) => [parts[messageID === "message-1" ? 0 : 1]!],
        noStatus,
      ),
    ).toEqual([])
  })

  test("keeps multiple pending entries without session ids", () => {
    expect(
      activeSubagents(messages, () => [
        {
          type: "tool",
          tool: "task",
          state: { status: "pending", input: { description: "Start worker one" }, raw: "{}" },
        },
        {
          type: "tool",
          tool: "task",
          state: { status: "pending", input: { description: "Start worker two" }, raw: "{}" },
        },
      ] as unknown as Part[], noStatus),
    ).toEqual([
      { description: "Start worker one", status: "pending", session_id: undefined },
      { description: "Start worker two", status: "pending", session_id: undefined },
    ])
  })

  test("renders only in-flight entries from a mixed session", () => {
    expect(
      activeSubagents(messages, () => [
        {
          type: "tool",
          tool: "task",
          state: {
            status: "completed",
            input: { description: "Finished one" },
            title: "Finished one",
            metadata: { sessionId: "child-done-1" },
          },
        },
        {
          type: "tool",
          tool: "task",
          state: {
            status: "completed",
            input: { description: "Finished two" },
            title: "Finished two",
            metadata: { sessionId: "child-done-2" },
          },
        },
        {
          type: "tool",
          tool: "task",
          state: { status: "running", input: { description: "Working" }, metadata: { sessionId: "child-running" } },
        },
        {
          type: "tool",
          tool: "task",
          state: { status: "pending", input: { description: "Waiting" }, raw: "{}" },
        },
      ] as unknown as Part[], (sessionID) => (sessionID === "child-running" ? { type: "busy" as const } : undefined)),
    ).toEqual([
      { description: "Working", status: "active", session_id: "child-running" },
      { description: "Waiting", status: "pending", session_id: undefined },
    ])
  })

  test("includes a completed background task while its child session is busy", () => {
    expect(
      activeSubagents(messages, () => [
        {
          type: "tool",
          tool: "task",
          state: {
            status: "completed",
            input: { description: "Background research" },
            title: "Background research",
            metadata: { sessionId: "child-busy" },
          },
        },
      ] as unknown as Part[], busyStatus),
    ).toEqual([{ description: "Background research", status: "active", session_id: "child-busy" }])
  })
})

describe("latestSubagentTPS", () => {
  const user = (created: number) => ({ role: "user" as const, time: { created } })
  const assistant = (input: {
    finish?: string
    output: number
    reasoning?: number
    created: number
    firstToken?: number
    completed?: number
  }) => ({
    role: "assistant" as const,
    finish: input.finish,
    tokens: { output: input.output, reasoning: input.reasoning ?? 0 },
    time: { created: input.created, firstToken: input.firstToken, completed: input.completed },
  })

  test("returns undefined for no messages", () => {
    expect(latestSubagentTPS([])).toBeUndefined()
  })

  test("returns undefined when only user messages exist", () => {
    expect(latestSubagentTPS([user(1), user(2)])).toBeUndefined()
  })

  test("falls back to the newest completed turn while the newest is in flight", () => {
    const messages = [
      user(0),
      assistant({ finish: "stop", output: 500, reasoning: 100, created: 100, firstToken: 1000, completed: 2000 }),
      assistant({ output: 10, created: 3000, firstToken: 3100 }),
    ]
    expect(latestSubagentTPS(messages)?.rate).toBe(600)
  })

  test("uses the newest completed turn, not the highest rate", () => {
    const messages = [
      assistant({ finish: "stop", output: 1000, created: 100, firstToken: 200, completed: 1200 }),
      assistant({ finish: "stop", output: 150, created: 5000, firstToken: 5100, completed: 6100 }),
    ]
    expect(latestSubagentTPS(messages)?.rate).toBe(150)
  })

  test("counts tool-call turns", () => {
    const messages = [assistant({ finish: "tool-calls", output: 300, created: 100, firstToken: 200, completed: 700 })]
    expect(latestSubagentTPS(messages)?.rate).toBe(600)
  })

  test("skips errored turns and falls back to the previous completed one", () => {
    const messages = [
      assistant({ finish: "stop", output: 200, created: 100, firstToken: 200, completed: 1200 }),
      assistant({ finish: "error", output: 999, created: 5000, firstToken: 5100, completed: 6100 }),
    ]
    expect(latestSubagentTPS(messages)?.rate).toBe(200)
  })
})
