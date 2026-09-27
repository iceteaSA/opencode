import { afterEach, describe, expect } from "bun:test"
import { Effect } from "effect"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionStatus } from "@/session/status"
import { Messaging } from "@/messaging"
import { Interrupt } from "@/session/interrupt"
import { TaskOutcomes } from "@/tool/task-outcomes"
import { TaskAbortTool, TaskCancelTool, TaskSteerTool } from "@/tool/task-interrupt"
import { Tool } from "@/tool/tool"
import { MessageID, SessionID } from "@/session/schema"
import { disposeAllInstances } from "../fixture/fixture"
import { pollWithTimeout } from "../lib/effect"
import { TestLLMServer, httpError, reply } from "../lib/llm-server"
import { caseFor, providerCfgFor, useServerConfig } from "./task-outcomes-fixture"

function matchesUser(text: string) {
  return (hit: { body: Record<string, unknown> }) =>
    Array.isArray(hit.body.messages) &&
    hit.body.messages.some(
      (message) =>
        message &&
        typeof message === "object" &&
        message.role === "user" &&
        JSON.stringify(message.content).includes(text),
    )
}

function expectOnlyReason(text: string, reason: string) {
  expect(text).toContain(`reason: ${reason}`)
  expect(text.match(/reason: /g)).toHaveLength(1)
}

function toolContext(sessionID: SessionID): Tool.Context {
  return {
    sessionID,
    messageID: MessageID.ascending(),
    agent: "build",
    abort: new AbortController().signal,
    messages: [],
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

const startInterruptibleTask = Effect.fn("TaskOutcomesTest.startInterruptibleTask")(function* (
  slug: string,
  options?: { timeout?: number; fallback_model?: string; parentVariant?: string },
) {
  const { llm } = yield* useServerConfig(providerCfgFor)
  const prompt = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const status = yield* SessionStatus.Service
  const parent = yield* sessions.create({
    title: "coordinator",
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  })
  const sibling = yield* sessions.create({ parentID: parent.id, title: "sibling" })
  yield* prompt.prompt({
    sessionID: parent.id,
    agent: "build",
    ...(options?.parentVariant ? { variant: options.parentVariant } : {}),
    noReply: true,
    parts: [{ type: "text", text: "start" }],
  })
  yield* llm.tool("task", {
    description: "inspect outcome",
    prompt: "child assignment",
    subagent_type: "build",
    background: true,
    wake_on_message: true,
    task_id: slug,
    ...(options?.timeout ? { timeout: options.timeout } : {}),
    ...(options?.fallback_model ? { fallback_model: options.fallback_model } : {}),
  })
  yield* llm.textMatch(matchesUser("child assignment"), "first answer")
  yield* llm.textMatch(matchesUser("Background task completed: inspect outcome"), "initial notice handled")
  yield* prompt.loop({ sessionID: parent.id })
  const child = yield* pollWithTimeout(
    Effect.map(sessions.children(parent.id), (children) => children.find((item) => item.slug === slug)),
    "dispatched child was not created",
    "5 seconds",
  )
  const notices = () =>
    Effect.map(sessions.messages({ sessionID: parent.id }), (messages) =>
      messages
        .filter((message) => message.info.role === "user")
        .flatMap((message) => message.parts)
        .filter((part) => part.type === "text" && part.synthetic && part.text.includes(`<task id="${child.id}"`)),
    )
  yield* pollWithTimeout(
    Effect.gen(function* () {
      const count = (yield* notices()).length
      const parentStatus = yield* status.get(parent.id)
      const childStatus = yield* status.get(child.id)
      return count === 1 && parentStatus.type === "idle" && childStatus.type === "idle" ? count : undefined
    }),
    "initial task completion did not finish",
    "5 seconds",
  )
  return { llm, parent, child, sibling, sessions, status, notices }
})

const emptyCompletion = Effect.fn("TaskOutcomesTest.emptyCompletion")(function* (input: {
  taskID: string
  completion?: "full" | "terse"
  timeout?: number
  script: (llm: ReturnType<typeof TestLLMServer.of>) => Effect.Effect<void>
}) {
  const { llm } = yield* useServerConfig(providerCfgFor)
  const prompt = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const status = yield* SessionStatus.Service
  const parent = yield* sessions.create({
    title: "coordinator",
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  })
  yield* prompt.prompt({
    sessionID: parent.id,
    agent: "build",
    noReply: true,
    parts: [{ type: "text", text: "start" }],
  })
  yield* llm.tool("task", {
    description: "inspect empty completion",
    prompt: "child assignment",
    subagent_type: "build",
    background: true,
    wake_on_message: true,
    task_id: input.taskID,
    ...(input.completion ? { completion: input.completion } : {}),
    ...(input.timeout ? { timeout: input.timeout } : {}),
  })
  yield* input.script(llm)
  yield* llm.textMatch(matchesUser("<task id="), "notice handled")
  yield* prompt.loop({ sessionID: parent.id })
  const child = yield* pollWithTimeout(
    Effect.map(sessions.children(parent.id), (children) => children.find((item) => item.slug === input.taskID)),
    "task child was not created",
    "5 seconds",
  )
  const notices = () =>
    Effect.map(sessions.messages({ sessionID: parent.id }), (messages) =>
      messages
        .filter((message) => message.info.role === "user")
        .flatMap((message) => message.parts)
        .filter(
          (part) =>
            part.type === "text" &&
            part.synthetic &&
            (part.text.includes(`<task id="${child.id}"`) ||
              part.text.includes(`full result: task session ${child.id}`)),
        ),
    )
  const first = yield* pollWithTimeout(
    Effect.gen(function* () {
      const items = yield* notices()
      const parentStatus = yield* status.get(parent.id)
      const childStatus = yield* status.get(child.id)
      return items.length === 1 && parentStatus.type === "idle" && childStatus.type === "idle" ? items[0] : undefined
    }),
    "empty completion did not reach parent",
    "5 seconds",
  )
  expect(first.type).toBe("text")
  if (first.type !== "text") throw new Error("Expected a synthetic text notice")
  return { llm, parent, child, sessions, status, notices, text: first.text }
})

afterEach(async () => {
  await disposeAllInstances()
})

describe("task outcomes: real SessionPrompt and task dispatch", () => {
  {
    const it = caseFor()
    it.instance(
      "empty structured-only completion states structured_result_only",
      () =>
        Effect.gen(function* () {
          const result = yield* emptyCompletion({
            taskID: "structured-empty",
            script: (llm) =>
              Effect.gen(function* () {
                yield* llm.toolMatch(matchesUser("child assignment"), "task_return", { result: { status: "ok" } })
                yield* llm.textMatch(matchesUser("child assignment"), "")
              }),
          })
          expectOnlyReason(result.text, "structured_result_only")
          expect(result.text.match(/<task_return>/g)).toHaveLength(1)
          expect(result.text.match(/<task_return>/g)).toHaveLength(1)
          expect(result.text).toContain('"status": "ok"')
          yield* result.llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty output-length completion states output_limit",
      () =>
        Effect.gen(function* () {
          const result = yield* emptyCompletion({
            taskID: "length-empty",
            script: (llm) => llm.pushMatch(matchesUser("child assignment"), reply().length().item()),
          })
          expectOnlyReason(result.text, "output_limit")
          expect(result.text).not.toContain("structured_result_only")
          yield* result.llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty provider or tool failure includes error message",
      () =>
        Effect.gen(function* () {
          const result = yield* emptyCompletion({
            taskID: "provider-empty",
            script: (llm) =>
              llm.pushMatch(
                matchesUser("child assignment"),
                httpError(401, { error: { message: "provider exploded", type: "authentication_error" } }),
              ),
          })
          expectOnlyReason(result.text, "provider_or_tool_error")
          expect(result.text).toContain("provider exploded")
          yield* result.llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty tool failure includes error message",
      () =>
        Effect.gen(function* () {
          const result = yield* emptyCompletion({
            taskID: "tool-empty",
            script: (llm) =>
              Effect.gen(function* () {
                yield* llm.toolMatch(matchesUser("child assignment"), "bash", { command: "echo hi", timeout: -1 })
                yield* llm.textMatch(matchesUser("child assignment"), "")
              }),
          })
          expectOnlyReason(result.text, "provider_or_tool_error")
          expect(result.text).toContain("Expected a value greater than 0")
          yield* result.llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty provider or tool failure includes error message after a wake",
      () =>
        Effect.gen(function* () {
          const { llm, parent, child, sibling, status, notices } = yield* startInterruptibleTask("wake-tool-error")
          const messaging = yield* Messaging.Service
          yield* llm.toolMatch(matchesUser("wake tool failure"), "bash", { command: "echo hi", timeout: -1 })
          yield* llm.textMatch(matchesUser("wake tool failure"), "")
          yield* llm.textMatch(matchesUser("run #2"), "notice handled")
          const before = yield* llm.calls
          yield* messaging.enqueue({
            target: child.id,
            from: sibling.id,
            fromSlug: "sibling",
            body: "wake tool failure",
          })

          const followup = yield* pollWithTimeout(
            Effect.gen(function* () {
              const messages = yield* notices()
              const parentStatus = yield* status.get(parent.id)
              const childStatus = yield* status.get(child.id)
              return messages.length === 2 && parentStatus.type === "idle" && childStatus.type === "idle"
                ? messages[1]
                : undefined
            }),
            "wake tool failure did not notify the parent",
          )
          if (followup.type !== "text") return yield* Effect.fail(new Error("wake tool notice was not text"))
          expectOnlyReason(followup.text, "provider_or_tool_error")
          expect(followup.text).toContain("Expected a value greater than 0")
          expect(followup.text).not.toContain("first answer")
          const wakeRequests = (yield* llm.inputs)
            .slice(before)
            .filter((body) => matchesUser("wake tool failure")({ body }))
          expect(wakeRequests).toHaveLength(2)
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty cancelled completion states cancelled",
      () =>
        Effect.gen(function* () {
          const { llm, parent, child, sibling, status, notices } = yield* startInterruptibleTask("cancelled-empty")
          const messaging = yield* Messaging.Service
          const abort = yield* (yield* TaskAbortTool).init()
          const gate = { resolve: () => {} }
          const held = new Promise<void>((resolve) => {
            gate.resolve = resolve
          })
          const before = yield* llm.calls
          yield* llm.pushMatch(
            matchesUser("cancel empty wake"),
            reply().wait(held).text("should not arrive").stop().item(),
          )
          yield* llm.textMatch(matchesUser("run #2"), "aborted notice handled")
          yield* messaging.enqueue({
            target: child.id,
            from: sibling.id,
            fromSlug: "sibling",
            body: "cancel empty wake",
          })
          yield* llm.wait(before + 1)
          yield* Effect.gen(function* () {
            expect((yield* status.get(child.id)).type).toBe("busy")
            expect(
              (yield* abort.execute({ task_id: child.id, reason: "stop now" }, toolContext(parent.id))).metadata.state,
            ).toBe("aborted")
            yield* pollWithTimeout(
              Effect.map(status.get(child.id), (current) => (current.type === "idle" ? current : undefined)),
              "cancelled wake did not stop",
              "3 seconds",
            )
          }).pipe(Effect.ensuring(Effect.sync(() => gate.resolve())))
          const second = yield* pollWithTimeout(
            Effect.map(notices(), (items) => (items.length === 2 ? items[1] : undefined)),
            "cancelled completion notice missing",
            "5 seconds",
          )
          expect(second.type).toBe("text")
          if (second.type === "text") {
            expectOnlyReason(second.text, "cancelled")
            expect(second.text).not.toContain("first answer")
          }
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty first-run timeout states timed_out",
      () =>
        Effect.gen(function* () {
          const result = yield* emptyCompletion({
            taskID: "timeout-empty",
            timeout: 150,
            script: (llm) => llm.pushMatch(matchesUser("child assignment"), reply().hang().item()),
          })
          expectOnlyReason(result.text, "timed_out")
          yield* result.llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty stop completion states no_final_text",
      () =>
        Effect.gen(function* () {
          const result = yield* emptyCompletion({
            taskID: "stop-empty",
            completion: "terse",
            script: (llm) => llm.textMatch(matchesUser("child assignment"), ""),
          })
          expectOnlyReason(result.text, "no_final_text")
          expect(result.text).not.toContain("structured_result_only")
          yield* result.llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "empty wake after prior task_return is not structured_result_only",
      () =>
        Effect.gen(function* () {
          const result = yield* emptyCompletion({
            taskID: "structured-then-empty",
            script: (llm) =>
              Effect.gen(function* () {
                yield* llm.toolMatch(matchesUser("child assignment"), "task_return", { result: { status: "earlier" } })
                yield* llm.textMatch(matchesUser("child assignment"), "")
              }),
          })
          const messaging = yield* Messaging.Service
          const sibling = yield* result.sessions.create({ parentID: result.parent.id, title: "sibling" })
          yield* result.llm.textMatch(matchesUser("empty wake"), "")
          yield* result.llm.textMatch(matchesUser("run #2"), "follow-up handled")
          yield* messaging.enqueue({
            target: result.child.id,
            from: sibling.id,
            fromSlug: "sibling",
            body: "empty wake",
          })
          const second = yield* pollWithTimeout(
            Effect.map(result.notices(), (items) => (items.length === 2 ? items[1] : undefined)),
            "empty follow-up did not reach parent",
            "5 seconds",
          )
          expect(second.type).toBe("text")
          if (second.type === "text") {
            expectOnlyReason(second.text, "no_final_text")
            expect(second.text).not.toContain("structured_result_only")
            expect(second.text).not.toContain('"status": "earlier"')
            expect(second.text).not.toContain("<task_return>")
          }
          yield* result.llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "wake beyond task timeout reports timed_out and stops runner without fallback",
      () =>
        Effect.gen(function* () {
          const { llm, parent, child, sibling, status, notices } = yield* startInterruptibleTask("timed-wake-child", {
            timeout: 1500,
            fallback_model: "test/test-model",
          })
          const messaging = yield* Messaging.Service
          const outcomes = yield* TaskOutcomes.Service
          const gate = { resolve: () => {} }
          const held = new Promise<void>((resolve) => {
            gate.resolve = resolve
          })
          const before = yield* llm.calls
          yield* llm.pushMatch(matchesUser("timed wake"), reply().wait(held).text("late answer").stop().item())
          yield* llm.textMatch(matchesUser("run #2"), "notice handled")
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "timed wake" })
          yield* llm.wait(before + 1)
          expect((yield* status.get(child.id)).type).toBe("busy")
          const second = yield* pollWithTimeout(
            Effect.gen(function* () {
              const items = yield* notices()
              const current = yield* status.get(child.id)
              return items.length === 2 && current.type === "idle" ? items[1] : undefined
            }),
            "timed-out wake did not settle or stop its runner",
            "8 seconds",
          ).pipe(Effect.ensuring(Effect.sync(() => gate.resolve())))
          expect(second?.type).toBe("text")
          if (second?.type === "text") {
            expect(second.text).toContain('state="timed_out"')
            expectOnlyReason(second.text, "timed_out")
            expect(second.text).not.toContain("late answer")
          }
          expect((yield* outcomes.current(child.id))?.last).toMatchObject({ sequence: 2, state: "timed_out" })
          expect((yield* notices()).length).toBe(2)
          expect(
            (yield* llm.inputs).slice(before).filter((input) => matchesUser("timed wake")({ body: input })),
          ).toHaveLength(1)
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "cancel pending when a wake times out wins over the timer",
      () =>
        Effect.gen(function* () {
          const { llm, parent, child, sibling, status, notices } = yield* startInterruptibleTask(
            "cancel-timeout-child",
            {
              timeout: 1500,
            },
          )
          const messaging = yield* Messaging.Service
          const outcomes = yield* TaskOutcomes.Service
          const cancel = yield* (yield* TaskCancelTool).init()
          const gate = { resolve: () => {} }
          const held = new Promise<void>((resolve) => {
            gate.resolve = resolve
          })
          yield* llm.pushMatch(matchesUser("cancel timeout wake"), reply().wait(held).text("late answer").stop().item())
          yield* llm.textMatch(matchesUser("run #2"), "notice handled")
          yield* messaging.enqueue({
            target: child.id,
            from: sibling.id,
            fromSlug: "sibling",
            body: "cancel timeout wake",
          })
          yield* pollWithTimeout(
            Effect.map(status.get(child.id), (current) => (current.type === "busy" ? true : undefined)),
            "wake did not become busy",
          )
          const request = yield* cancel.execute(
            { task_id: child.id, reason: "stop at timeout" },
            toolContext(parent.id),
          )
          expect(request.metadata.state).toBe("delivered")
          const second = yield* pollWithTimeout(
            Effect.gen(function* () {
              const items = yield* notices()
              return items.length === 2 && (yield* status.get(child.id)).type === "idle" ? items[1] : undefined
            }),
            "cancel did not win the overlapping wake timeout",
            "8 seconds",
          ).pipe(Effect.ensuring(Effect.sync(() => gate.resolve())))
          expect(second?.type).toBe("text")
          if (second?.type === "text") {
            expect(second.text).toContain('state="aborted"')
            expectOnlyReason(second.text, "cancelled")
          }
          expect((yield* outcomes.current(child.id))?.last).toMatchObject({ sequence: 2, state: "aborted" })
          expect((yield* notices()).length).toBe(2)
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "abort during real wake stops runner and notifies aborted; idle abort reports last settlement",
      () =>
        Effect.gen(function* () {
          const { llm, parent, child, sibling, status, notices } = yield* startInterruptibleTask("abort-wake-child")
          const messaging = yield* Messaging.Service
          const outcomes = yield* TaskOutcomes.Service
          const abort = yield* (yield* TaskAbortTool).init()
          const gate = { resolve: () => {} }
          const held = new Promise<void>((resolve) => {
            gate.resolve = resolve
          })
          const before = yield* llm.calls
          yield* llm.pushMatch(
            matchesUser("abort wake ping"),
            reply().wait(held).text("should not arrive").stop().item(),
          )
          yield* llm.textMatch(matchesUser("run #2"), "aborted notice handled")
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "abort wake ping" })
          yield* llm.wait(before + 1)
          expect((yield* status.get(child.id)).type).toBe("busy")
          const first = yield* Effect.gen(function* () {
            const result = yield* abort.execute(
              { task_id: child.slug ?? child.id, reason: "stop this wake" },
              toolContext(parent.id),
            )
            expect(result.metadata.state).toBe("aborted")
            yield* pollWithTimeout(
              Effect.map(status.get(child.id), (current) => (current.type === "idle" ? current : undefined)),
              "abort did not stop the held wake provider turn",
              "3 seconds",
            )
            return result
          }).pipe(Effect.ensuring(Effect.sync(() => gate.resolve())))
          expect(first.metadata.state).toBe("aborted")
          const second = yield* pollWithTimeout(
            Effect.map(notices(), (items) => (items.length === 2 ? items : undefined)),
            "aborted wake did not notify parent once",
            "5 seconds",
          )
          expect(second[1]?.type).toBe("text")
          if (second[1]?.type === "text") {
            expect(second[1].text).toContain('state="aborted"')
            expect(second[1].text).not.toContain("should not arrive")
          }
          const last = (yield* outcomes.current(child.id))?.last
          expect(last).toMatchObject({ trigger: "wake", state: "aborted", sequence: 2 })
          if (!last) return
          const idle = yield* abort.execute(
            { task_id: child.slug ?? child.id, reason: "try again" },
            toolContext(parent.id),
          )
          expect(idle.metadata.state).toBe("already_finished")
          expect(idle.output).toContain("aborted")
          expect(idle.output).toContain(new Date(last.settledAt).toISOString())
          expect((yield* notices()).length).toBe(2)
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "steer and cancel reach running wake",
      () =>
        Effect.gen(function* () {
          const { llm, parent, child, sibling, status, notices } =
            yield* startInterruptibleTask("controlled-wake-child")
          const messaging = yield* Messaging.Service
          const steer = yield* (yield* TaskSteerTool).init()
          const cancel = yield* (yield* TaskCancelTool).init()
          const gate = { resolve: () => {} }
          const held = new Promise<void>((resolve) => {
            gate.resolve = resolve
          })
          const before = yield* llm.calls
          yield* llm.pushMatch(matchesUser("control wake ping"), reply().wait(held).text("working").stop().item())
          yield* llm.textMatch(matchesUser("wrap up"), "wrapped up")
          yield* llm.textMatch(matchesUser("run #2"), "control notice handled")
          yield* messaging.enqueue({
            target: child.id,
            from: sibling.id,
            fromSlug: "sibling",
            body: "control wake ping",
          })
          yield* llm.wait(before + 1)
          expect((yield* status.get(child.id)).type).toBe("busy")
          const delivered = yield* Effect.gen(function* () {
            const steered = yield* steer.execute(
              { task_id: child.slug ?? child.id, reason: "adjust focus" },
              toolContext(parent.id),
            )
            const cancelled = yield* cancel.execute(
              { task_id: child.slug ?? child.id, reason: "wrap up" },
              toolContext(parent.id),
            )
            expect(steered.metadata.state).toBe("delivered")
            expect(cancelled.metadata.state).toBe("delivered")
            return { steered, cancelled }
          }).pipe(Effect.ensuring(Effect.sync(() => gate.resolve())))
          expect(delivered.cancelled.metadata.state).toBe("delivered")
          const second = yield* pollWithTimeout(
            Effect.map(notices(), (items) => (items.length === 2 ? items : undefined)),
            "steered and cancelled wake did not notify parent once",
            "5 seconds",
          )
          expect(second[1]?.type).toBe("text")
          if (second[1]?.type === "text") expect(second[1].text).toContain("wrapped up")
          yield* pollWithTimeout(
            Effect.map(status.get(child.id), (current) => (current.type === "idle" ? current : undefined)),
            "controlled wake remained busy",
            "5 seconds",
          )
          expect((yield* notices()).length).toBe(2)
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    // Each case needs a fresh layer graph so a prior test's provider config cannot leak into the next.
    const it = caseFor()
    it.instance(
      "wake after its inbox was drained does not replay the previous assistant",
      () =>
        Effect.gen(function* () {
          const { llm } = yield* useServerConfig(providerCfgFor)
          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const messaging = yield* Messaging.Service
          const outcomes = yield* TaskOutcomes.Service
          const status = yield* SessionStatus.Service
          const parent = yield* sessions.create({
            title: "coordinator",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          const sibling = yield* sessions.create({ parentID: parent.id, title: "sibling" })
          yield* prompt.prompt({
            sessionID: parent.id,
            agent: "build",
            noReply: true,
            parts: [{ type: "text", text: "start" }],
          })
          yield* llm.tool("task", {
            description: "inspect outcome",
            prompt: "child assignment",
            subagent_type: "build",
            background: true,
            wake_on_message: true,
            task_id: "drained-child",
          })
          yield* llm.textMatch(matchesUser("child assignment"), "previous run answer")
          yield* llm.textMatch(matchesUser("Background task completed: inspect outcome"), "initial notice handled")
          yield* prompt.loop({ sessionID: parent.id })
          const child = (yield* sessions.children(parent.id)).find((item) => item.slug === "drained-child")
          expect(child).toBeDefined()
          if (!child) return
          yield* pollWithTimeout(
            Effect.gen(function* () {
              const messages = yield* sessions.messages({ sessionID: parent.id })
              const notice = messages
                .flatMap((message) => message.parts)
                .find((part) => part.type === "text" && part.synthetic && part.text.includes(`<task id="${child.id}"`))
              if (!notice) return undefined
              const parentStatus = yield* status.get(parent.id)
              const childStatus = yield* status.get(child.id)
              return parentStatus.type === "idle" && childStatus.type === "idle" ? notice : undefined
            }),
            "initial completion and parent continuation did not finish",
            "5 seconds",
          )
          yield* sessions.setResult({ sessionID: child.id, result: { stale: "prior task_return" } })
          yield* messaging.setWakePolicy({ sessionID: child.id, budget: 0 })
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "already drained" })
          expect((yield* messaging.drain(child.id)).map((item) => item.body)).toEqual(["already drained"])
          const before = yield* llm.calls
          yield* llm.textMatch(matchesUser("run #2"), "empty notice handled")
          yield* prompt.loop({ sessionID: child.id })
          const notice = yield* pollWithTimeout(
            Effect.map(sessions.messages({ sessionID: parent.id }), (messages) =>
              messages
                .flatMap((message) => message.parts)
                .find((part) => part.type === "text" && part.synthetic && part.text.includes("run #2")),
            ),
            "idle no-answer wake did not notify parent",
            "5 seconds",
          )
          expect(notice.type).toBe("text")
          if (notice.type === "text") {
            expect(notice.text).not.toContain("previous run answer")
            expect(notice.text).not.toContain("prior task_return")
          }
          expect(
            (yield* llm.inputs).slice(before).filter((input) => matchesUser("already drained")({ body: input })),
          ).toHaveLength(0)
          yield* pollWithTimeout(
            Effect.map(status.get(parent.id), (current) => (current.type === "idle" ? current : undefined)),
            "parent no-answer follow-up remained busy",
            "5 seconds",
          )
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "steer inside active wake does not create a fourth notice",
      () =>
        Effect.gen(function* () {
          const { llm } = yield* useServerConfig(providerCfgFor)
          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const messaging = yield* Messaging.Service
          const interrupt = yield* Interrupt.Service
          const outcomes = yield* TaskOutcomes.Service
          const status = yield* SessionStatus.Service
          const parent = yield* sessions.create({
            title: "coordinator",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          const sibling = yield* sessions.create({ parentID: parent.id, title: "sibling" })
          yield* prompt.prompt({
            sessionID: parent.id,
            agent: "build",
            noReply: true,
            parts: [{ type: "text", text: "start" }],
          })
          yield* llm.tool("task", {
            description: "inspect outcome",
            prompt: "child assignment",
            subagent_type: "build",
            background: true,
            wake_on_message: true,
            task_id: "steered-child",
          })
          yield* llm.textMatch(matchesUser("child assignment"), "first answer")
          yield* llm.textMatch(matchesUser("Background task completed: inspect outcome"), "first notice handled")
          yield* prompt.loop({ sessionID: parent.id })
          const child = (yield* sessions.children(parent.id)).find((item) => item.slug === "steered-child")
          expect(child).toBeDefined()
          if (!child) return
          const notices = () =>
            Effect.map(sessions.messages({ sessionID: parent.id }), (messages) =>
              messages
                .filter((message) => message.info.role === "user")
                .flatMap((message) => message.parts)
                .filter(
                  (part) => part.type === "text" && part.synthetic && part.text.includes(`<task id="${child.id}"`),
                ),
            )
          yield* pollWithTimeout(
            Effect.gen(function* () {
              const count = (yield* notices()).length
              const parentStatus = yield* status.get(parent.id)
              return count === 1 && parentStatus.type === "idle" ? count : undefined
            }),
            "initial notice did not finish",
            "5 seconds",
          )

          yield* llm.textMatch(matchesUser("first ping"), "intermediate")
          yield* llm.textMatch(matchesUser("run #2"), "second notice handled")
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "first ping" })
          yield* pollWithTimeout(
            Effect.gen(function* () {
              const count = (yield* notices()).length
              const parentStatus = yield* status.get(parent.id)
              const childStatus = yield* status.get(child.id)
              return count === 2 && parentStatus.type === "idle" && childStatus.type === "idle" ? count : undefined
            }),
            "first idle wake did not finish",
            "5 seconds",
          )
          const gate = { resolve: () => {} }
          const held = new Promise<void>((resolve) => {
            gate.resolve = resolve
          })
          const before = yield* llm.calls
          yield* llm.pushMatch(matchesUser("second ping"), reply().wait(held).text("working").stop().item())
          yield* llm.textMatch(matchesUser("adjust focus"), "final answer")
          yield* llm.textMatch(matchesUser("run #3"), "third notice handled")
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "second ping" })
          yield* pollWithTimeout(
            Effect.map(llm.inputs, (inputs) => inputs.slice(before).some((body) => matchesUser("second ping")({ body })) || undefined),
            "held second ping did not reach the provider",
            "15 seconds",
          )
          expect((yield* status.get(child.id)).type).toBe("busy")
          yield* interrupt.request({ sessionID: child.id, intent: "steer", reason: "adjust focus", origin: "parent" })
          gate.resolve()
          const third = yield* pollWithTimeout(
            Effect.map(notices(), (items) => (items.length === 3 ? items : undefined)),
            "active wake did not settle after steer",
            "5 seconds",
          )
          expect(third[2]?.type).toBe("text")
          if (third[2]?.type === "text") {
            expect(third[2].text).toContain("final answer")
            expect(third[2].text).not.toContain("working")
          }
          const wakeRequests = (yield* llm.inputs)
            .slice(before)
            .map((input) => {
              const users = Array.isArray(input.messages)
                ? input.messages.filter((message) => message && typeof message === "object" && message.role === "user")
                : []
              return JSON.stringify(users.at(-1)?.content ?? "")
            })
            .filter((text) => text.includes("second ping") || text.includes("adjust focus"))
          expect(wakeRequests).toHaveLength(2)
          expect(wakeRequests[0]).toContain("second ping")
          expect(wakeRequests[1]).toContain("adjust focus")
          expect((yield* outcomes.current(child.id))?.last).toMatchObject({ sequence: 3, trigger: "wake" })
          yield* pollWithTimeout(
            Effect.map(status.get(parent.id), (current) => (current.type === "idle" ? current : undefined)),
            "parent third notice remained busy",
            "5 seconds",
          )
          yield* Effect.sleep("30 millis")
          expect((yield* notices()).length).toBe(3)
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "direct prompt.loop sibling wake uses the task outcome wrapper",
      () =>
        Effect.gen(function* () {
          const { llm, parent, child, sibling, sessions, status, notices } = yield* startInterruptibleTask(
            "direct-loop-child",
            { parentVariant: "fast" },
          )
          const messaging = yield* Messaging.Service
          const outcomes = yield* TaskOutcomes.Service
          const prompt = yield* SessionPrompt.Service
          const parentMessages = yield* sessions.messages({ sessionID: parent.id })
          const firstUser = parentMessages.find(
            (message) =>
              message.info.role === "user" &&
              message.parts.some((part) => part.type === "text" && part.text === "start"),
          )
          expect(firstUser?.info.role).toBe("user")
          if (firstUser?.info.role !== "user") return yield* Effect.fail(new Error("initial parent prompt missing"))
          expect(firstUser.info.model).toEqual({
            providerID: ProviderV2.ID.make("test"),
            modelID: ModelV2.ID.make("test-model"),
            variant: "fast",
          })

          yield* messaging.setWakePolicy({ sessionID: child.id, budget: 0 })
          yield* llm.textMatch(matchesUser("direct ping"), "direct reply")
          yield* llm.textMatch(matchesUser("run #2"), "direct notice handled")
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "direct ping" })
          expect((yield* notices()).length).toBe(1)
          expect((yield* status.get(child.id)).type).toBe("idle")
          yield* prompt.loop({ sessionID: child.id })
          expect((yield* notices()).length).toBe(2)

          const second = yield* pollWithTimeout(
            Effect.gen(function* () {
              const items = yield* notices()
              return items.length === 2 && (yield* status.get(parent.id)).type === "idle" ? items[1] : undefined
            }),
            "direct prompt.loop did not notify the parent",
            "5 seconds",
          )
          expect(second?.type).toBe("text")
          if (second?.type === "text") {
            expect(second.text).toContain(`<task id="${child.id}"`)
            expect(second.text).toContain("Follow-up run #2")
            expect(second.text).toContain("direct reply")
          }
          const followup = (yield* sessions.messages({ sessionID: parent.id })).find(
            (message) =>
              message.info.role === "user" &&
              message.parts.some(
                (part) => part.type === "text" && part.synthetic && part.text.includes("Follow-up run #2"),
              ),
          )
          expect(followup?.info.role).toBe("user")
          if (followup?.info.role !== "user") return yield* Effect.fail(new Error("follow-up prompt missing"))
          expect(followup.info.model).toEqual(firstUser.info.model)
          expect((yield* outcomes.current(child.id))?.last).toMatchObject({
            sequence: 2,
            trigger: "wake",
            state: "completed",
          })
          expect((yield* notices()).length).toBe(2)
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance("direct prompt.loop timeout interrupts the wake runner without cancelRun", () =>
      Effect.gen(function* () {
        const { llm, parent, child, sibling, sessions, status, notices } = yield* startInterruptibleTask(
          "direct-timeout-child",
          { timeout: 1500, fallback_model: "test/test-model" },
        )
        const prompt = yield* SessionPrompt.Service
        const messaging = yield* Messaging.Service
        const outcomes = yield* TaskOutcomes.Service
        const gate = { resolve: () => {} }
        const held = new Promise<void>((resolve) => {
          gate.resolve = resolve
        })
        yield* messaging.setWakePolicy({ sessionID: child.id, budget: 0 })
        yield* llm.pushMatch(matchesUser("direct timed ping"), reply().wait(held).text("late answer").stop().item())
        yield* llm.textMatch(matchesUser("run #2"), "notice handled")
        const before = yield* llm.calls
        yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "direct timed ping" })
        expect((yield* notices()).length).toBe(1)

        const second = yield* Effect.gen(function* () {
          yield* prompt.loop({ sessionID: child.id }).pipe(
            Effect.timeoutOrElse({
              duration: "8 seconds",
              orElse: () => Effect.fail(new Error("direct wake did not time out")),
            }),
          )
          expect((yield* status.get(child.id)).type).toBe("idle")
          return yield* pollWithTimeout(
            Effect.gen(function* () {
              const items = yield* notices()
              if (items.length !== 2 || (yield* status.get(parent.id)).type !== "idle") return undefined
              return items
            }),
            "direct wake timeout did not notify parent",
          )
        }).pipe(Effect.ensuring(Effect.sync(() => gate.resolve())))
        expect(second).toHaveLength(2)
        expect(second[1]?.type).toBe("text")
        if (second[1]?.type === "text") {
          expect(second[1].text).toContain('state="timed_out"')
          expectOnlyReason(second[1].text, "timed_out")
        }
        expect((yield* outcomes.current(child.id))?.last).toMatchObject({ sequence: 2, state: "timed_out" })
        const wakeRequests = (yield* llm.inputs)
          .slice(before)
          .filter((input) => matchesUser("direct timed ping")({ body: input }))
        expect(wakeRequests).toHaveLength(1)
        yield* llm.reset
      }),
    )

    it.instance(
      "dispatch then two idle sibling wakes emits three ordered notices",
      () =>
        Effect.gen(function* () {
          const { llm } = yield* useServerConfig(providerCfgFor)
          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const messaging = yield* Messaging.Service
          const outcomes = yield* TaskOutcomes.Service
          const status = yield* SessionStatus.Service
          const parent = yield* sessions.create({
            title: "coordinator",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          const sibling = yield* sessions.create({ parentID: parent.id, title: "sibling" })
          yield* prompt.prompt({
            sessionID: parent.id,
            agent: "build",
            noReply: true,
            parts: [{ type: "text", text: "start" }],
          })
          yield* llm.tool("task", {
            description: "inspect outcome",
            prompt: "child assignment",
            subagent_type: "build",
            background: true,
            wake_on_message: true,
            task_id: "outcome-child",
          })
          yield* llm.textMatch(matchesUser("child assignment"), "first answer")
          yield* llm.textMatch(matchesUser("Background task completed: inspect outcome"), "first notice handled")
          yield* prompt.loop({ sessionID: parent.id })
          const child = (yield* sessions.children(parent.id)).find((item) => item.slug === "outcome-child")
          expect(child).toBeDefined()
          if (!child) return

          const notices = () =>
            Effect.map(sessions.messages({ sessionID: parent.id }), (messages) =>
              messages
                .filter((message) => message.info.role === "user")
                .flatMap((message) => message.parts)
                .filter(
                  (part) => part.type === "text" && part.synthetic && part.text.includes(`<task id="${child.id}"`),
                ),
            )
          yield* pollWithTimeout(
            Effect.map(notices(), (items) => (items.length === 1 ? items : undefined)),
            "initial notice missing",
            "5 seconds",
          )
          const first = yield* notices()
          expect(first[0]?.type).toBe("text")
          if (first[0]?.type === "text") expect(first[0].text).not.toContain("Follow-up run")
          yield* pollWithTimeout(
            Effect.map(status.get(parent.id), (current) => (current.type === "idle" ? current : undefined)),
            "parent initial notice remained busy",
            "5 seconds",
          )
          expect((yield* outcomes.current(child.id))?.last?.sequence).toBe(1)
          yield* sessions.setResult({ sessionID: child.id, result: { stale: "do not replay" } })

          yield* llm.textMatch(matchesUser("first ping"), "intermediate")
          yield* llm.textMatch(matchesUser("run #2"), "second notice handled")
          const beforeFirstWake = yield* llm.calls
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "first ping" })
          const second = yield* pollWithTimeout(
            Effect.gen(function* () {
              const items = yield* notices()
              const requested = (yield* llm.inputs)
                .slice(beforeFirstWake)
                .some((input) => matchesUser("first ping")({ body: input }))
              if (!requested || (yield* status.get(child.id)).type !== "idle") return undefined
              if ((yield* status.get(parent.id)).type !== "idle") return undefined
              return items
            }),
            "first idle wake did not finish",
            "5 seconds",
          )
          expect(second[1]?.type).toBe("text")
          if (second[1]?.type === "text") {
            expect(second[1].text).toContain("run #2")
            expect(second[1].text).toContain("intermediate")
            expect(second[1].text).not.toContain("first answer")
            expect(second[1].text).not.toContain("do not replay")
            expect(second[1].text).not.toContain("<task_return>")
          }
          yield* pollWithTimeout(
            Effect.map(status.get(parent.id), (current) => (current.type === "idle" ? current : undefined)),
            "parent first follow-up remained busy",
            "5 seconds",
          )
          yield* pollWithTimeout(
            Effect.map(status.get(child.id), (current) => (current.type === "idle" ? current : undefined)),
            "child first wake remained busy",
            "5 seconds",
          )

          yield* llm.textMatch(matchesUser("second ping"), "final answer")
          yield* llm.textMatch(matchesUser("run #3"), "third notice handled")
          yield* messaging.enqueue({ target: child.id, from: sibling.id, fromSlug: "sibling", body: "second ping" })
          const third = yield* pollWithTimeout(
            Effect.map(notices(), (items) => (items.length === 3 ? items : undefined)),
            "second idle wake did not notify parent",
            "5 seconds",
          )
          expect(third[2]?.type).toBe("text")
          if (third[2]?.type === "text") {
            expect(third[2].text).toContain("run #3")
            expect(third[2].text).toContain("final answer")
            expect(third[2].text).not.toContain("intermediate")
            expect(third[2].text).toContain(`id="${child.id}"`)
          }
          expect((yield* outcomes.current(child.id))?.last).toMatchObject({ sequence: 3, trigger: "wake" })
          yield* pollWithTimeout(
            Effect.map(status.get(parent.id), (current) => (current.type === "idle" ? current : undefined)),
            "parent final follow-up remained busy",
            "5 seconds",
          )
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "actual background task dispatch admits one first-run parent notice",
      () =>
        Effect.gen(function* () {
          const { llm } = yield* useServerConfig(providerCfgFor)
          const prompt = yield* SessionPrompt.Service
          const sessions = yield* Session.Service
          const outcomes = yield* TaskOutcomes.Service
          const parent = yield* sessions.create({
            title: "coordinator",
            permission: [{ permission: "*", pattern: "*", action: "allow" }],
          })
          yield* prompt.prompt({
            sessionID: parent.id,
            agent: "build",
            noReply: true,
            parts: [{ type: "text", text: "start" }],
          })
          yield* llm.tool("task", {
            description: "inspect outcome",
            prompt: "child assignment",
            subagent_type: "build",
            background: true,
            wake_on_message: true,
            task_id: "outcome-child",
          })
          yield* llm.textMatch(matchesUser("child assignment"), "first answer")
          yield* llm.textMatch(matchesUser("Background task completed: inspect outcome"), "notice received")
          yield* prompt.loop({ sessionID: parent.id })
          const child = (yield* sessions.children(parent.id)).find((item) => item.slug === "outcome-child")
          expect(child).toBeDefined()
          if (!child) return
          const notices = yield* pollWithTimeout(
            Effect.map(sessions.messages({ sessionID: parent.id }), (messages) => {
              const notices = messages
                .flatMap((message) => message.parts)
                .filter((part) => part.type === "text" && part.synthetic && part.text.includes("first answer"))
              return notices.length ? notices : undefined
            }),
            "first-run task outcome did not reach parent",
            "5 seconds",
          )
          expect(notices).toHaveLength(1)
          expect((yield* outcomes.current(child.id))?.last).toMatchObject({
            sequence: 1,
            trigger: "dispatch",
            state: "completed",
          })
          yield* pollWithTimeout(
            Effect.map(sessions.messages({ sessionID: parent.id }), (messages) => {
              const notice = messages.find(
                (message) =>
                  message.info.role === "user" &&
                  message.parts.some(
                    (part) => part.type === "text" && part.synthetic && part.text.includes("first answer"),
                  ),
              )
              return messages.find(
                (message) => message.info.role === "assistant" && message.info.parentID === notice?.info.id,
              )
            }),
            "parent did not finish the admitted completion turn",
            "5 seconds",
          )
          const status = yield* SessionStatus.Service
          yield* pollWithTimeout(
            Effect.map(status.get(parent.id), (current) => (current.type === "idle" ? current : undefined)),
            "parent continuation remained busy after notice",
            "5 seconds",
          )
          yield* llm.reset
        }),
      30000,
    )
  }
  {
    const it = caseFor()
    it.instance(
      "task outcome service is visible in a real prompt run and assigns one sequence per run",
      () =>
        Effect.gen(function* () {
          const { llm } = yield* useServerConfig(providerCfgFor)
          const sessions = yield* Session.Service
          const prompt = yield* SessionPrompt.Service
          const outcomes = yield* TaskOutcomes.Service
          const parent = yield* sessions.create({ title: "parent" })
          const child = yield* sessions.create({ parentID: parent.id, title: "child" })
          yield* prompt.prompt({
            sessionID: child.id,
            agent: "build",
            noReply: true,
            parts: [{ type: "text", text: "start" }],
          })
          yield* llm.text("first")
          yield* prompt.loop({ sessionID: child.id })
          const observed: number[] = []
          yield* outcomes.register({
            childID: child.id,
            description: "child",
            notify: (outcome) =>
              Effect.gen(function* () {
                expect((yield* outcomes.current(child.id))?.active).toBeUndefined()
                expect((yield* outcomes.current(child.id))?.last).toMatchObject({
                  sequence: outcome.sequence,
                  state: outcome.state,
                })
                observed.push(outcome.sequence)
              }),
          })
          yield* outcomes.beginInitial(child.id)
          const first = yield* outcomes.settleInitial(child.id, { state: "completed", text: "first" })
          const second = yield* outcomes.runWake(
            child.id,
            Effect.succeed({ state: "completed" as const, text: "second" }),
          )
          expect([first?.sequence, second?.sequence]).toEqual([1, 2])
          expect(observed).toEqual([1, 2])
          expect((yield* outcomes.current(child.id))?.last).toMatchObject({
            sequence: 2,
            trigger: "wake",
            state: "completed",
          })
          expect((yield* outcomes.current(child.id))?.last?.settledAt).toBeNumber()
          yield* llm.reset
        }),
      30000,
    )
  }
})
