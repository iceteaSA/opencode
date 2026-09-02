import { describe, expect, test } from "bun:test"
import type { Message, Part } from "@opencode-ai/sdk/v2"
import {
  activeSubagents,
  deriveSubagents,
  latestSubagentTPS,
  recentSubagents,
} from "../../src/feature-plugins/sidebar/subagents"

const messages = [{ id: "message-1" }]
const noStatus = () => undefined
const busyStatus = () => ({ type: "busy" as const })

describe("sidebar subagents", () => {
  test("returns no entries when the session has no task parts", () => {
    expect(activeSubagents(messages, () => [], noStatus)).toEqual([])
  })

  test("derives running and completed subagents", () => {
    expect(
      activeSubagents(
        messages,
        () =>
          [
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
          ] as unknown as Part[],
        (sessionID) => (sessionID === "child-running" ? busyStatus() : undefined),
      ),
    ).toEqual([{ description: "Research APIs", status: "active", session_id: "child-running" }])
  })

  test("marks errored subagents as failed", () => {
    expect(
      activeSubagents(
        messages,
        () =>
          [
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
          ] as unknown as Part[],
        noStatus,
      ),
    ).toEqual([])
  })

  test("keeps pending subagents without a session id non-navigable", () => {
    expect(
      activeSubagents(
        messages,
        () =>
          [
            {
              type: "tool",
              tool: "task",
              state: {
                status: "pending",
                input: { description: "Start worker" },
                raw: "{}",
              },
            },
          ] as unknown as Part[],
        noStatus,
      ),
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
      deriveSubagents([{ id: "message-1" }, { id: "message-2" }], (messageID) => [
        parts[messageID === "message-1" ? 0 : 1]!,
      ]),
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
      activeSubagents(
        messages,
        () =>
          [
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
          ] as unknown as Part[],
        noStatus,
      ),
    ).toEqual([
      { description: "Start worker one", status: "pending", session_id: undefined },
      { description: "Start worker two", status: "pending", session_id: undefined },
    ])
  })

  test("renders only in-flight entries from a mixed session", () => {
    expect(
      activeSubagents(
        messages,
        () =>
          [
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
          ] as unknown as Part[],
        (sessionID) => (sessionID === "child-running" ? { type: "busy" as const } : undefined),
      ),
    ).toEqual([
      { description: "Working", status: "active", session_id: "child-running" },
      { description: "Waiting", status: "pending", session_id: undefined },
    ])
  })

  test("includes a completed background task while its child session is busy", () => {
    expect(
      activeSubagents(
        messages,
        () =>
          [
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
          ] as unknown as Part[],
        busyStatus,
      ),
    ).toEqual([{ description: "Background research", status: "active", session_id: "child-busy" }])
  })
})

describe("sidebar subagent history", () => {
  const noChildSession = () => undefined
  const noChildMessages = () => []

  test("excludes children that are still active", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Background research" },
          output: "",
          title: "Background research",
          metadata: { sessionId: "child-busy" },
          time: { start: 10, end: 20 },
        },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Finished work" },
          output: "",
          title: "Finished work",
          metadata: { sessionId: "child-idle" },
          time: { start: 30, end: 40 },
        },
      },
    ] as unknown as Part[]
    const getStatus = (sessionID: string) => (sessionID === "child-busy" ? busyStatus() : undefined)

    expect(
      recentSubagents(messages, () => parts, getStatus, { session: noChildSession, messages: noChildMessages }),
    ).toEqual([{ description: "Finished work", status: "done", session_id: "child-idle", activity: 40 }])
    expect(activeSubagents(messages, () => parts, getStatus)).toEqual([
      { description: "Background research", status: "active", session_id: "child-busy" },
    ])
  })

  test("caps history at the ten most recent subagents", () => {
    const parts = Array.from({ length: 12 }, (_, index) => ({
      type: "tool",
      tool: "task",
      state: {
        status: "completed",
        input: { description: `Child ${index}` },
        output: "",
        title: `Child ${index}`,
        metadata: { sessionId: `child-${index}` },
        time: { start: index, end: index + 1 },
      },
    })) as unknown as Part[]
    const sessions = new Map(
      Array.from({ length: 12 }, (_, index): [string, { time: { updated: number } }] => [
        `child-${index}`,
        { time: { updated: index * 10 } },
      ]),
    )

    const history = recentSubagents(messages, () => parts, noStatus, {
      session: (sessionID) => sessions.get(sessionID),
      messages: noChildMessages,
    })

    expect(history).toHaveLength(10)
    expect(history[0]).toEqual({ description: "Child 11", status: "done", session_id: "child-11", activity: 110 })
    expect(history[9]).toEqual({ description: "Child 2", status: "done", session_id: "child-2", activity: 20 })
    expect(history.some((entry) => entry.session_id === "child-1")).toBe(false)
    expect(history.some((entry) => entry.session_id === "child-0")).toBe(false)
  })

  test("sorts by child activity rather than parent part time", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Early dispatch" },
          output: "",
          title: "Early dispatch",
          metadata: { sessionId: "child-early" },
          time: { start: 100, end: 200 },
        },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Late dispatch" },
          output: "",
          title: "Late dispatch",
          metadata: { sessionId: "child-late" },
          time: { start: 300, end: 400 },
        },
      },
    ] as unknown as Part[]
    const sessions = new Map<string, { time: { updated: number } }>([
      ["child-early", { time: { updated: 5000 } }],
      ["child-late", { time: { updated: 300 } }],
    ])

    expect(
      recentSubagents(messages, () => parts, noStatus, {
        session: (sessionID) => sessions.get(sessionID),
        messages: noChildMessages,
      }),
    ).toEqual([
      { description: "Early dispatch", status: "done", session_id: "child-early", activity: 5000 },
      { description: "Late dispatch", status: "done", session_id: "child-late", activity: 400 },
    ])
  })

  test("ranks by message completion when session updated time is older", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Long runner" },
          output: "",
          title: "Long runner",
          metadata: { sessionId: "child-long" },
          time: { start: 10, end: 50 },
        },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Quick runner" },
          output: "",
          title: "Quick runner",
          metadata: { sessionId: "child-quick" },
          time: { start: 60, end: 100 },
        },
      },
    ] as unknown as Part[]
    const sessions = new Map<string, { time: { updated: number } }>([
      ["child-long", { time: { updated: 100 } }],
      ["child-quick", { time: { updated: 800 } }],
    ])
    const childMessages = new Map<string, Message[]>([
      ["child-long", [{ role: "assistant", time: { created: 400, completed: 900 } } as unknown as Message]],
    ])

    expect(
      recentSubagents(messages, () => parts, noStatus, {
        session: (sessionID) => sessions.get(sessionID),
        messages: (sessionID) => childMessages.get(sessionID) ?? [],
      }),
    ).toEqual([
      { description: "Long runner", status: "done", session_id: "child-long", activity: 900 },
      { description: "Quick runner", status: "done", session_id: "child-quick", activity: 800 },
    ])
  })

  test("shows a resumed child once, in history when idle and in active when busy", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "running",
          input: { description: "Research APIs" },
          metadata: { sessionId: "child-1" },
          time: { start: 10 },
        },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Research APIs" },
          output: "",
          title: "Research APIs",
          metadata: { sessionId: "child-1" },
          time: { start: 20, end: 30 },
        },
      },
    ] as unknown as Part[]
    const sessionMessages = [{ id: "message-1" }, { id: "message-2" }]
    const getParts = (messageID: string) => [parts[messageID === "message-1" ? 0 : 1]!]

    expect(activeSubagents(sessionMessages, getParts, busyStatus)).toEqual([
      { description: "Research APIs", status: "active", session_id: "child-1" },
    ])
    expect(
      recentSubagents(sessionMessages, getParts, busyStatus, { session: noChildSession, messages: noChildMessages }),
    ).toEqual([])
    expect(activeSubagents(sessionMessages, getParts, noStatus)).toEqual([])
    expect(
      recentSubagents(sessionMessages, getParts, noStatus, { session: noChildSession, messages: noChildMessages }),
    ).toEqual([{ description: "Research APIs", status: "done", session_id: "child-1", activity: 30 }])
  })

  test("marks history failed from the child's errored newest assistant message", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Run checks" },
          output: "",
          title: "Run checks",
          metadata: { sessionId: "child-1" },
          time: { start: 1, end: 2 },
        },
      },
    ] as unknown as Part[]
    const erroredLast = [
      { role: "assistant", time: { created: 5, completed: 6 } },
      { role: "assistant", time: { created: 7, completed: 8 }, error: { name: "RunError", message: "boom" } },
    ] as unknown as Message[]
    const erroredEarlier = [
      { role: "assistant", time: { created: 5, completed: 6 }, error: { name: "RunError", message: "boom" } },
      { role: "assistant", time: { created: 7, completed: 8 } },
    ] as unknown as Message[]

    expect(
      recentSubagents(messages, () => parts, noStatus, { session: noChildSession, messages: () => erroredLast }),
    ).toEqual([{ description: "Run checks", status: "failed", session_id: "child-1", activity: 8 }])
    expect(
      recentSubagents(messages, () => parts, noStatus, { session: noChildSession, messages: () => erroredEarlier }),
    ).toEqual([{ description: "Run checks", status: "done", session_id: "child-1", activity: 8 }])
  })

  test("marks history failed from an errored parent part", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "error",
          input: { description: "Run checks" },
          error: "failed",
          metadata: { sessionId: "child-1" },
          time: { start: 1, end: 2 },
        },
      },
    ] as unknown as Part[]

    expect(
      recentSubagents(messages, () => parts, noStatus, { session: noChildSession, messages: noChildMessages }),
    ).toEqual([{ description: "Run checks", status: "failed", session_id: "child-1", activity: 2 }])
  })

  test("keeps entries without a session id out of history", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: { status: "pending", input: { description: "Start worker" }, raw: "{}" },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "running",
          input: { description: "Working" },
          time: { start: 5 },
        },
      },
    ] as unknown as Part[]

    expect(
      recentSubagents(messages, () => parts, noStatus, { session: noChildSession, messages: noChildMessages }),
    ).toEqual([])
  })

  test("falls back to child message times when session info is unavailable", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "First" },
          output: "",
          title: "First",
          metadata: { sessionId: "child-a" },
          time: { start: 1, end: 2 },
        },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Second" },
          output: "",
          title: "Second",
          metadata: { sessionId: "child-b" },
          time: { start: 3, end: 4 },
        },
      },
    ] as unknown as Part[]
    const childMessages = new Map<string, Message[]>([
      ["child-a", [{ role: "assistant", time: { created: 100, completed: 200 } } as unknown as Message]],
      ["child-b", [{ role: "user", time: { created: 50 } } as unknown as Message]],
    ])

    expect(
      recentSubagents(messages, () => parts, noStatus, {
        session: noChildSession,
        messages: (sessionID) => childMessages.get(sessionID) ?? [],
      }),
    ).toEqual([
      { description: "First", status: "done", session_id: "child-a", activity: 200 },
      { description: "Second", status: "done", session_id: "child-b", activity: 50 },
    ])
  })

  test("breaks activity ties by later dispatch first", () => {
    const parts = [
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "First" },
          output: "",
          title: "First",
          metadata: { sessionId: "child-first" },
          time: { start: 1, end: 10 },
        },
      },
      {
        type: "tool",
        tool: "task",
        state: {
          status: "completed",
          input: { description: "Second" },
          output: "",
          title: "Second",
          metadata: { sessionId: "child-second" },
          time: { start: 2, end: 20 },
        },
      },
    ] as unknown as Part[]
    const sessions = new Map<string, { time: { updated: number } }>([
      ["child-first", { time: { updated: 100 } }],
      ["child-second", { time: { updated: 100 } }],
    ])

    expect(
      recentSubagents(messages, () => parts, noStatus, {
        session: (sessionID) => sessions.get(sessionID),
        messages: noChildMessages,
      }),
    ).toEqual([
      { description: "Second", status: "done", session_id: "child-second", activity: 100 },
      { description: "First", status: "done", session_id: "child-first", activity: 100 },
    ])
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
