/** @jsxImportSource @opentui/solid */
import { describe, expect, test } from "bun:test"
import { testRender } from "@opentui/solid"
import type { JSX } from "solid-js"
import { createStore } from "solid-js/store"
import type { Message, Part } from "@opencode-ai/sdk/v2"
import { createTuiPluginApi } from "../fixture/tui-plugin"
import { wait } from "../fixture/tui-sync"
import plugin from "../../src/feature-plugins/sidebar/subagents"

const parentID = "ses_parent"
const childWithRate = "ses_child_rate"
const childWithout = "ses_child_idle"

const parentMessages = [{ id: "message-1" }] as unknown as Message[]

const taskPart = (sessionID: string, description: string) =>
  ({
    type: "tool",
    tool: "task",
    state: { status: "running", input: { description }, metadata: { sessionId: sessionID } },
  }) as unknown as Part

const completedAssistant = (output: number, reasoning: number) =>
  ({
    id: "assistant-1",
    role: "assistant",
    finish: "stop",
    tokens: { output, reasoning },
    time: { created: 100, firstToken: 200, completed: 1200 },
  }) as unknown as Message

async function renderSidebar() {
  const [childMessages, setChildMessages] = createStore<Record<string, Message[]>>({})
  setChildMessages(childWithRate, [completedAssistant(500, 100)])

  let handler: ((ctx: unknown, props: { session_id: string }) => JSX.Element) | undefined
  const api = {
    ...createTuiPluginApi(),
    state: {
      session: {
        messages: (sessionID: string) =>
          sessionID === parentID ? parentMessages : (childMessages[sessionID] ?? []),
        status: (sessionID: string) =>
          sessionID === childWithRate || sessionID === childWithout ? { type: "busy" as const } : undefined,
      },
      part: (messageID: string) =>
        messageID === "message-1"
          ? [taskPart(childWithRate, "Research APIs"), taskPart(childWithout, "Review changes")]
          : [],
    },
    slots: {
      register: (input: { slots: Record<string, (ctx: unknown, props: { session_id: string }) => JSX.Element> }) => {
        handler = input.slots.sidebar_content
      },
    },
    route: { navigate: () => {} },
  } as unknown as Parameters<typeof plugin.tui>[0]

  await plugin.tui(api, undefined, {
    id: plugin.id,
    source: "internal",
    spec: plugin.id,
    target: plugin.id,
    version: "0.0.0",
    first_time: 0,
    last_time: 0,
    time_changed: 0,
    load_count: 1,
    fingerprint: "test",
    state: "first",
  })
  if (!handler) throw new Error("sidebar_content slot was not registered")

  const app = await testRender(() => handler!({}, { session_id: parentID }))
  return { app, setChildMessages }
}

const tokPerSecCount = (frame: string) => frame.split("tok/s").length - 1

async function settle(app: Awaited<ReturnType<typeof testRender>>) {
  await app.renderOnce()
  await Bun.sleep(25)
  await app.renderOnce()
}

describe("sidebar subagents render", () => {
  test("shows the live rate for a child with a completed turn", async () => {
    const { app } = await renderSidebar()
    try {
      await settle(app)
      const frame = app.captureCharFrame()
      expect(frame).toContain("Research APIs")
      expect(frame).toContain("600 tok/s")
    } finally {
      app.renderer.destroy()
    }
  })

  test("omits the rate for a child without a completed turn", async () => {
    const { app } = await renderSidebar()
    try {
      await settle(app)
      const frame = app.captureCharFrame()
      expect(frame).toContain("Review changes")
      expect(frame).not.toContain("Review changes Active ·")
      expect(tokPerSecCount(frame)).toBe(1)
    } finally {
      app.renderer.destroy()
    }
  })

  test("updates the rate when the child completes a turn", async () => {
    const { app, setChildMessages } = await renderSidebar()
    try {
      await settle(app)
      expect(tokPerSecCount(app.captureCharFrame())).toBe(1)
      setChildMessages(childWithout, [completedAssistant(150, 0)])
      await wait(async () => {
        await app.renderOnce()
        return app.captureCharFrame().includes("150 tok/s")
      })
    } finally {
      app.renderer.destroy()
    }
  })
})
