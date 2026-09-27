import { afterEach, beforeAll, describe, expect } from "bun:test"
import { randomUUID } from "crypto"
import { mkdirSync } from "fs"
import { rm } from "fs/promises"
import { tmpdir } from "os"
import path from "path"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { sql } from "drizzle-orm"
import { Cause, Effect, Exit, Layer, Option } from "effect"
import { Agent } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Interrupt } from "../../src/session/interrupt"
import { Messaging } from "../../src/messaging"
import { Session } from "../../src/session/session"
import { SessionRunState } from "@/session/run-state"
import { MessageID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "@/session/status"
import { TaskTool, type TaskPromptOps } from "../../src/tool/task"
import { Tool } from "../../src/tool/tool"
import { TaskOutcomes } from "../../src/tool/task-outcomes"
import { ScheduledTaskStore } from "../../src/tool/scheduled-task-store"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffectIsolatedShared } from "../lib/effect"

const dbFile = path.join(tmpdir(), "opencode", "s2s-durable-impl", `task-schedule-${randomUUID()}.sqlite`)
beforeAll(() => mkdirSync(path.dirname(dbFile), { recursive: true }))

afterEach(async () => {
  await disposeAllInstances()
  await Promise.all([dbFile, `${dbFile}-wal`, `${dbFile}-shm`].map((file) => rm(file, { force: true })))
})

const flags = RuntimeFlags.layer({ experimentalBackgroundSubagents: true })
const services = LayerNode.group([
  Agent.node,
  BackgroundJob.node,
  Config.node,
  CrossSpawnSpawner.node,
  FSUtil.node,
  Database.node,
  EventV2Bridge.node,
  Interrupt.node,
  Messaging.node,
  Ripgrep.node,
  RuntimeFlags.node,
  ScheduledTaskStore.node,
  Session.node,
  SessionProjector.node,
  SessionRunState.node,
  SessionStatus.node,
  TaskOutcomes.node,
  ToolRegistry.node,
  Truncate.node,
])

const isolated = testEffectIsolatedShared(LayerNode.compile(services, [[RuntimeFlags.node, flags]]))
const persisted = testEffectIsolatedShared(LayerNode.compile(services, [
  [RuntimeFlags.node, flags],
  [Database.node, Database.layerFromPath(dbFile)],
]))

const ref = { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }

const ops: TaskPromptOps = {
  cancel: () => Effect.void,
  cancelRun: () => Effect.void,
  resolvePromptParts: (text) => Effect.succeed([{ type: "text", text }]),
  prompt: () => Effect.die(new Error("deferred task started before due")),
  loop: () => Effect.die(new Error("deferred task entered provider loop before due")),
}

const seed = Effect.fn("TaskScheduleTest.seed")(function* () {
  const sessions = yield* Session.Service
  const parent = yield* sessions.create({ title: "Scheduled parent" })
  const user = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    sessionID: parent.id,
    role: "user",
    origin: "operator",
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    sessionID: parent.id,
    role: "assistant",
    parentID: user.id,
    agent: "build",
    mode: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
  }
  yield* sessions.updateMessage(assistant)
  return { parent, assistant }
})

function context(parent: Session.Info, assistant: SessionV1.Assistant): Tool.Context {
  return {
    sessionID: parent.id,
    messageID: assistant.id,
    agent: "build",
    callID: "call-scheduled-task",
    abort: new AbortController().signal,
    messages: [],
    extra: { promptOps: ops },
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

describe("durable task scheduling admission", () => {
  persisted.instance("reclaims a prelaunch crash at 30 seconds but never replays launch intent", () =>
    Effect.gen(function* () {
      const store = yield* ScheduledTaskStore.Service
      const now = Date.now()
      const input = {
        id: "task_claim_recovery",
        parentSessionID: SessionID.make("ses_claim_parent"),
        childSessionID: SessionID.make("ses_claim_child"),
        dueAt: now - 10_000,
        admittedAt: now - 20_000,
        params: { requested: { prompt: "check work" } },
      }
      yield* store.admit(input)
      expect((yield* store.dueForParent(input.parentSessionID, now)).map((row) => row.id)).toEqual([input.id])
      expect(yield* store.claimStart(input.id, "owner-one", now)).toBe(true)
      expect(yield* store.claimStart(input.id, "owner-two", now + 29_999)).toBe(false)
      expect(yield* store.claimStart(input.id, "owner-two", now + 30_000)).toBe(true)
      expect(yield* store.launchIntent(input.id, "owner-two", now + 30_001)).toBe(true)
      expect(yield* store.claimStart(input.id, "owner-three", now + 91_000)).toBe(false)
      expect(yield* store.markAmbiguous(input.id, now + 89_999)).toBe(false)
      expect(yield* store.markAmbiguous(input.id, now + 90_001)).toBe(true)
      const row = yield* store.get(input.id)
      expect(row._tag === "Some" ? row.value.state : undefined).toBe("ambiguous")
    }),
  )

  persisted.instance("a live launch heartbeat prevents false owner-loss ambiguity", () =>
    Effect.gen(function* () {
      const store = yield* ScheduledTaskStore.Service
      const now = Date.now()
      const input = {
        id: "task_heartbeat_recovery",
        parentSessionID: SessionID.make("ses_heartbeat_parent"),
        childSessionID: SessionID.make("ses_heartbeat_child"),
        dueAt: now - 1000,
        admittedAt: now - 2000,
        params: { requested: { prompt: "slow work" } },
      }
      yield* store.admit(input)
      expect(yield* store.claimStart(input.id, "owner-one", now)).toBe(true)
      expect(yield* store.launchIntent(input.id, "owner-one", now + 1)).toBe(true)
      expect(yield* store.heartbeat(input.id, "owner-one", now + 50_000)).toBe(true)
      expect(yield* store.markAmbiguous(input.id, now + 70_000)).toBe(false)
      expect(yield* store.markStarted(input.id, "owner-one", now + 2)).toBe(true)
      expect(yield* store.heartbeat(input.id, "owner-one", now + 120_000)).toBe(true)
      expect(yield* store.markAmbiguous(input.id, now + 130_000)).toBe(false)
    }),
  )

  persisted.instance("transactional admission is idempotent and refuses conflicting dispatch inputs", () =>
    Effect.gen(function* () {
      const store = yield* ScheduledTaskStore.Service
      const { db } = yield* Database.Service
      const input = {
        id: "task_dispatch_once",
        parentSessionID: SessionID.make("ses_schedule_parent"),
        childSessionID: SessionID.make("ses_schedule_child"),
        slug: "dispatch-once",
        dueAt: Date.now() + 120_000,
        admittedAt: Date.now(),
        params: { prompt: "Inspect the index", model: ref },
      }
      const rows = yield* Effect.all([store.admit(input), store.admit(input)], { concurrency: "unbounded" })
      expect(rows.map((row) => row.duplicate).toSorted()).toEqual([false, true])
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE id = ${input.id}`)).toEqual({ n: 1 })
      const conflict = yield* Effect.exit(store.admit({ ...input, params: { ...input.params, prompt: "Different work" } }))
      expect(Exit.isFailure(conflict)).toBe(true)
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE id = ${input.id}`)).toEqual({ n: 1 })
    }),
  )

  persisted.instance("malformed scheduled rows fail through the declared error channel", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const store = yield* ScheduledTaskStore.Service
      yield* db.run(sql`INSERT INTO scheduled_task (id, parent_session_id, dispatch_inputs, due_at, admitted_at, state)
        VALUES ('task_missing_child', 'ses_parent', '{}', 1, 1, 'queued')`)
      const result = yield* Effect.exit(store.get("task_missing_child"))
      expect(Exit.isFailure(result)).toBe(true)
      if (Exit.isFailure(result)) {
        const error = Cause.findErrorOption(result.cause)
        expect(Option.isSome(error)).toBe(true)
        if (Option.isSome(error)) expect(error.value).toBeInstanceOf(ScheduledTaskStore.ScheduledTaskError)
      }
    }),
  )

  persisted.instance("corrupt stored dispatch inputs fail as ScheduledTaskError without a defect", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const store = yield* ScheduledTaskStore.Service
      yield* db.run(sql`INSERT INTO scheduled_task (id, parent_session_id, child_session_id, dispatch_inputs, due_at, admitted_at, state)
        VALUES ('task_corrupt_json', 'ses_parent', 'ses_child', '{invalid', 1, 1, 'queued')`)
      const assertTypedFailure = <A>(result: Exit.Exit<A, ScheduledTaskStore.ScheduledTaskError>) => {
        expect(Exit.isFailure(result)).toBe(true)
        if (!Exit.isFailure(result)) return
        const error = Cause.findErrorOption(result.cause)
        expect(Option.isSome(error)).toBe(true)
        if (Option.isSome(error)) expect(error.value).toBeInstanceOf(ScheduledTaskStore.ScheduledTaskError)
      }
      assertTypedFailure(yield* Effect.exit(store.get("task_corrupt_json")))
      assertTypedFailure(yield* Effect.exit(store.dueForParent(SessionID.make("ses_parent"), Date.now())))
    }),
  )

  persisted.instance("admits a deferred task once and a fresh database layer finds the same row", () =>
    Effect.gen(function* () {
      const { parent, assistant } = yield* seed()
      const sessions = yield* Session.Service
      const background = yield* BackgroundJob.Service
      const store = yield* ScheduledTaskStore.Service
      const def = yield* (yield* TaskTool).init()
      const due = Date.now() + 120_000
      const input = {
        description: "inspect cache path",
        prompt: "Check the cache key",
        subagent_type: "general",
        task_id: "scheduled-cache",
        background: true,
        start_at: new Date(due).toISOString(),
        model: "test/test-model",
        fallback_model: "test/fallback-model",
        variant: "high",
        context: "sparse" as const,
        completion: "terse" as const,
        timeout: 9000,
        message_allow: ["analyst"],
        wake_on_message: true,
        metadata: { ticket: "TASK-11" },
      }
      const result = yield* def.execute(input, context(parent, assistant))
      expect(result.metadata).toMatchObject({ scheduled: true, background: true, due_at: new Date(due).toISOString() })
      expect(result.output).toContain(new Date(due).toISOString())
      const jobID = result.metadata.jobId
      if (!jobID) throw new Error("Scheduled task returned no durable job id")
      const child = yield* sessions.get(result.metadata.sessionId)
      expect((yield* sessions.messages({ sessionID: child.id })).filter((message) => message.info.role === "user")).toEqual([])
      expect(yield* background.list()).toEqual([])
      const first = yield* store.get(jobID)
      expect(first._tag).toBe("Some")
      if (first._tag === "Some") {
        expect(first.value).toMatchObject({ parentSessionID: parent.id, childSessionID: child.id, dueAt: due, state: "queued" })
        expect(first.value.params).toMatchObject({
          prompt: input.prompt,
          metadata: input.metadata,
          model: ref,
          variant: "high",
          context: "sparse",
          completion: "terse",
          fallbackModel: { providerID: "test", modelID: "fallback-model" },
          timeout: 9000,
          messageAllow: ["analyst"],
          wakeOnMessage: true,
          permission: expect.any(Array),
        })
      }
      const reopened = yield* Effect.scoped(
        Effect.gen(function* () {
          const fresh = yield* ScheduledTaskStore.Service
          return yield* fresh.get(jobID)
        }).pipe(Effect.provide(ScheduledTaskStore.layer.pipe(Layer.provideMerge(Database.layerFromPath(dbFile))))),
      )
      expect(reopened).toEqual(first)
    }),
  )

  isolated.instance("a named task_id exact retry reuses its queued dispatch and conflicting reuse fails", () =>
    Effect.gen(function* () {
      const { parent, assistant } = yield* seed()
      const def = yield* (yield* TaskTool).init()
      const { db } = yield* Database.Service
      const input = {
        description: "investigate database",
        prompt: "Inspect this query",
        subagent_type: "general",
        task_id: "scheduled-query",
        background: true,
        start_at: new Date(Date.now() + 120_000).toISOString(),
      }
      const first = yield* def.execute(input, context(parent, assistant))
      const retry = yield* def.execute(input, context(parent, assistant))
      expect(retry.metadata.jobId).toBe(first.metadata.jobId)
      expect(retry.metadata.sessionId).toBe(first.metadata.sessionId)
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ n: 1 })
      const conflict = yield* Effect.exit(def.execute({ ...input, prompt: "Do something else" }, context(parent, assistant)))
      expect(Exit.isFailure(conflict)).toBe(true)
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ n: 1 })
    }),
  )

  isolated.instance("an unnamed dispatch reuses the same child when its callID is retried", () =>
    Effect.gen(function* () {
      const { parent, assistant } = yield* seed()
      const def = yield* (yield* TaskTool).init()
      const { db } = yield* Database.Service
      const input = { description: "check cache key", prompt: "Inspect cache lookup", subagent_type: "general", background: true, start_at: new Date(Date.now() + 120_000).toISOString() }
      const first = yield* def.execute(input, context(parent, assistant))
      const retry = yield* def.execute(input, context(parent, assistant))
      expect(retry.metadata.jobId).toBe(first.metadata.jobId)
      expect(retry.metadata.sessionId).toBe(first.metadata.sessionId)
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ n: 1 })
    }),
  )

  isolated.instance("retries admission after the child placeholder exists but the queued row does not", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { db } = yield* Database.Service
      const def = yield* (yield* TaskTool).init()
      const { parent, assistant } = yield* seed()
      const childID = SessionID.make("ses_scheduled_placeholder")
      yield* sessions.create({ id: childID, parentID: parent.id, title: "Scheduled placeholder" })

      const result = yield* def.execute({ description: "Recover admission", prompt: "Continue later", subagent_type: "general", background: true, task_id: childID, start_at: new Date(Date.now() + 90_000).toISOString() }, context(parent, assistant))
      expect(result.metadata.sessionId).toBe(childID)
      expect(yield* db.get(sql`SELECT id FROM scheduled_task WHERE child_session_id = ${childID}`)).toBeDefined()
      expect((yield* sessions.messages({ sessionID: childID })).filter((entry) => entry.info.role === "user")).toHaveLength(0)
    }),
  )

  isolated.instance("the depth limit prevents scheduling before any durable admission", () =>
    Effect.gen(function* () {
      const { parent, assistant } = yield* seed()
      const { db } = yield* Database.Service
      const def = yield* (yield* TaskTool).init()
      const refused = yield* Effect.exit(def.execute({ description: "too deep", prompt: "Do this later", subagent_type: "general", background: true, start_at: new Date(Date.now() + 120_000).toISOString() }, context(parent, assistant)))
      expect(Exit.isFailure(refused)).toBe(true)
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ n: 0 })
    }),
    { config: { subagent_depth: 0 } },
  )

  isolated.instance("validates permission and depth before admission, normalizes offsets, and runs past starts immediately", () =>
    Effect.gen(function* () {
      const { parent, assistant } = yield* seed()
      const { db } = yield* Database.Service
      const def = yield* (yield* TaskTool).init()
      const future = Date.now() + 120_000
      const offset = `${new Date(future + 2 * 60 * 60_000).toISOString().slice(0, -1)}+02:00`
      const input = { description: "inspect index", prompt: "Check index", subagent_type: "general", background: true, start_at: offset }
      const denied = yield* Effect.exit(def.execute(input, { ...context(parent, assistant), ask: () => Effect.die(new Error("denied")) }))
      expect(Exit.isFailure(denied)).toBe(true)
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ n: 0 })
      const invalid = yield* Effect.exit(def.execute({ ...input, start_at: "2026-02-30T11:00:00Z" }, context(parent, assistant)))
      expect(Exit.isFailure(invalid)).toBe(true)
      const naive = yield* Effect.exit(def.execute({ ...input, start_at: "2026-09-28T11:00:00" }, context(parent, assistant)))
      expect(Exit.isFailure(naive)).toBe(true)
      const foreground = yield* Effect.exit(def.execute({ ...input, background: false }, context(parent, assistant)))
      expect(Exit.isFailure(foreground)).toBe(true)
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ n: 0 })
      const accepted = yield* def.execute(input, context(parent, assistant))
      expect(accepted.metadata.due_at).toBe(new Date(future).toISOString())
      expect(yield* db.get(sql`SELECT due_at AS due FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ due: future })

      const immediateOps: TaskPromptOps = {
        ...ops,
        prompt: (prompt) => Effect.succeed({
          info: {
            id: MessageID.ascending(), sessionID: prompt.sessionID, role: "assistant", parentID: MessageID.ascending(),
            mode: "general", agent: "general", cost: 0, path: { cwd: "/tmp", root: "/tmp" },
            tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
            modelID: ref.modelID, providerID: ref.providerID, time: { created: Date.now() }, finish: "stop",
          },
          parts: [],
        }),
      }
      const immediate = yield* def.execute(
        { ...input, start_at: new Date(Date.now() - 1000).toISOString(), background: false },
        { ...context(parent, assistant), callID: "call-past-immediate", extra: { promptOps: immediateOps } },
      )
      expect(immediate.metadata.scheduled).toBeUndefined()
      expect(yield* db.get(sql`SELECT count(*) AS n FROM scheduled_task WHERE parent_session_id = ${parent.id}`)).toEqual({ n: 1 })
    }),
  )
})
