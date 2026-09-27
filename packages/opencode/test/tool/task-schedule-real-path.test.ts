import { afterEach, describe, expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { Session } from "../../src/session/session"
import { SessionPrompt } from "../../src/session/prompt"
import { SessionID } from "../../src/session/schema"
import { disposeAllInstances } from "../fixture/fixture"
import { pollWithTimeout } from "../lib/effect"
import { reply } from "../lib/llm-server"
import { caseFor, providerCfgFor, useServerConfig } from "../messaging/task-outcomes-fixture"

afterEach(disposeAllInstances)

const it = caseFor()
const ref = { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }

const seedDue = Effect.fn("ScheduledTaskTest.seedDue")(function* (options?: {
  state?: "queued" | "prelaunch_claim" | "launch_intent"
  claimedAt?: number
}) {
  const sessions = yield* Session.Service
  const { db } = yield* Database.Service
  const parent = yield* sessions.create({ title: "Scheduled parent", permission: [{ permission: "*", pattern: "*", action: "allow" }] })
  const child = yield* sessions.create({ id: SessionID.make("ses_sched_parent_next_run"), parentID: parent.id, title: "Scheduled child", agent: "build", permission: [{ permission: "read", pattern: "*", action: "allow" }] })
  const due = Date.now() - 5_000
  const id = `task_${child.id}`
  yield* db.run(sql`INSERT INTO scheduled_task (id, parent_session_id, child_session_id, dispatch_inputs, due_at, admitted_at, state, claimed_at, claim_owner)
    VALUES (${id}, ${parent.id}, ${child.id}, ${JSON.stringify({ requested: { description: "scheduled child", prompt: "inspect due work", subagent_type: "build", background: true, start_at: new Date(due).toISOString() }, description: "scheduled child", prompt: "inspect due work", agent: "build", model: ref, permission: [], completion: "full" })}, ${due}, ${due - 5000}, ${options?.state ?? "queued"}, ${options?.claimedAt ?? null}, ${options?.state ? "dead-owner" : null})`)
  return { sessions, db, parent, child, id }
})

const childHit = (hit: { body: Record<string, unknown> }) =>
  JSON.stringify(hit.body.messages).includes("inspect due work")

describe("scheduled tasks on the parent run path", () => {
  it.instance("starts a due child when the parent next owns a run", () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfgFor)
      const prompt = yield* SessionPrompt.Service
      const { parent, child, id, db, sessions } = yield* seedDue()
      expect(yield* db.get(sql`SELECT started_at FROM scheduled_task WHERE id = ${id}`)).toEqual({ started_at: null })
      yield* prompt.prompt({ sessionID: parent.id, agent: "build", model: ref, noReply: true, parts: [{ type: "text", text: "parent next turn" }] })
      yield* llm.text("parent handled next turn")
      yield* llm.textMatch(childHit, "child completed")
      yield* prompt.loop({ sessionID: parent.id })
      yield* pollWithTimeout(Effect.gen(function* () {
        const row = yield* db.get<{ state: string; started_at: number | null }>(sql`SELECT state, started_at FROM scheduled_task WHERE id = ${id}`)
        return row?.state === "completed" ? row.started_at : undefined
      }), "due task was not started by the parent loop", "2 seconds")
      const notice = yield* pollWithTimeout(sessions.messages({ sessionID: parent.id }).pipe(Effect.map((messages) => messages.find((message) =>
        message.info.role === "user" && message.info.origin === "wake" && message.parts.some((part) => part.type === "text" && part.text.includes("started late")),
      ))), "overdue start did not notify the parent", "2 seconds")
      expect(notice.info.role).toBe("user")
      expect((yield* llm.hits).filter(childHit)).toHaveLength(1)
      expect((yield* sessions.messages({ sessionID: child.id })).some((message) => message.info.role === "assistant")).toBe(true)
    }),
  )

  it.instance("reclaims a prelaunch crash but never repeats the child dispatch", () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfgFor)
      const prompt = yield* SessionPrompt.Service
      const { parent, db, id } = yield* seedDue({ state: "prelaunch_claim", claimedAt: Date.now() - 30_001 })
      yield* prompt.prompt({ sessionID: parent.id, agent: "build", model: ref, noReply: true, parts: [{ type: "text", text: "parent next turn" }] })
      yield* llm.text("parent handled next turn")
      yield* llm.textMatch(childHit, "child completed")
      yield* prompt.loop({ sessionID: parent.id })
      yield* pollWithTimeout(Effect.gen(function* () {
        const row = yield* db.get<{ state: string }>(sql`SELECT state FROM scheduled_task WHERE id = ${id}`)
        return row?.state === "completed" ? true : undefined
      }), "reclaimed prelaunch task did not finish", "2 seconds")
      yield* prompt.prompt({ sessionID: parent.id, agent: "build", model: ref, noReply: true, parts: [{ type: "text", text: "another parent run" }] })
      yield* llm.text("another parent answer")
      yield* prompt.loop({ sessionID: parent.id })
      expect((yield* llm.hits).filter(childHit)).toHaveLength(1)
    }),
  )

  it.instance("reports owner loss after launch without replaying the child", () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfgFor)
      const prompt = yield* SessionPrompt.Service
      const { parent, db, id } = yield* seedDue({ state: "launch_intent", claimedAt: Date.now() - 60_001 })
      yield* prompt.prompt({ sessionID: parent.id, agent: "build", model: ref, noReply: true, parts: [{ type: "text", text: "parent next turn" }] })
      yield* llm.text("parent handled next turn")
      yield* prompt.loop({ sessionID: parent.id })
      yield* pollWithTimeout(Effect.gen(function* () {
        const row = yield* db.get<{ state: string }>(sql`SELECT state FROM scheduled_task WHERE id = ${id}`)
        return row?.state === "ambiguous" ? true : undefined
      }), "postlaunch owner loss was not reported", "2 seconds")
      expect((yield* llm.hits).filter(childHit)).toHaveLength(0)
    }),
  )

  it.instance("recomputes child permissions from the parent at start instead of the admission snapshot", () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfgFor)
      const prompt = yield* SessionPrompt.Service
      const { parent, child, db, id, sessions } = yield* seedDue()
      yield* sessions.setPermission({ sessionID: parent.id, permission: [
        { permission: "*", pattern: "*", action: "allow" },
        { permission: "read", pattern: "*", action: "deny" },
      ] })
      yield* prompt.prompt({ sessionID: parent.id, agent: "build", model: ref, noReply: true, parts: [{ type: "text", text: "parent next turn" }] })
      yield* llm.text("parent handled next turn")
      yield* llm.textMatch(childHit, "child completed")
      yield* prompt.loop({ sessionID: parent.id })
      yield* pollWithTimeout(Effect.gen(function* () {
        const row = yield* db.get<{ state: string }>(sql`SELECT state FROM scheduled_task WHERE id = ${id}`)
        return row?.state === "completed" ? true : undefined
      }), "scheduled child did not finish after permission refresh", "2 seconds")
      expect((yield* sessions.get(child.id)).permission?.findLast((rule) => rule.permission === "read")?.action).toBe("deny")
    }),
  )

  it.instance("refuses a queued dispatch when the parent now denies task permission", () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfgFor)
      const prompt = yield* SessionPrompt.Service
      const { parent, db, id, sessions } = yield* seedDue()
      yield* sessions.setPermission({ sessionID: parent.id, permission: [
        { permission: "*", pattern: "*", action: "allow" },
        { permission: "task", pattern: "build", action: "deny" },
      ] })
      yield* prompt.prompt({ sessionID: parent.id, agent: "build", model: ref, noReply: true, parts: [{ type: "text", text: "parent next turn" }] })
      yield* llm.text("parent handled next turn")
      yield* prompt.loop({ sessionID: parent.id })
      yield* pollWithTimeout(Effect.gen(function* () {
        const row = yield* db.get<{ state: string; failure_reason: string | null }>(sql`SELECT state, failure_reason FROM scheduled_task WHERE id = ${id}`)
        return row?.state === "failed" ? row.failure_reason : undefined
      }), "now-denied dispatch was not rejected", "2 seconds")
      expect((yield* llm.hits).filter(childHit)).toHaveLength(0)
    }),
  )

  it.instance("does not wait for the due child's provider work before returning the parent turn", () =>
    Effect.gen(function* () {
      const { llm } = yield* useServerConfig(providerCfgFor)
      const prompt = yield* SessionPrompt.Service
      const { parent, db, id } = yield* seedDue()
      let release!: () => void
      const gate = new Promise<void>((resolve) => { release = resolve })
      yield* prompt.prompt({ sessionID: parent.id, agent: "build", model: ref, noReply: true, parts: [{ type: "text", text: "parent next turn" }] })
      yield* llm.text("parent handled next turn")
      yield* llm.pushMatch(childHit, reply().text("child completed").wait(gate))
      yield* prompt.loop({ sessionID: parent.id }).pipe(Effect.timeout("2 seconds"))
      yield* pollWithTimeout(Effect.gen(function* () {
        const row = yield* db.get<{ started_at: number | null }>(sql`SELECT started_at FROM scheduled_task WHERE id = ${id}`)
        return row?.started_at ?? undefined
      }), "held child was not started", "2 seconds")
      expect((yield* llm.hits).filter(childHit)).toHaveLength(1)
      release()
      yield* pollWithTimeout(Effect.gen(function* () {
        const row = yield* db.get<{ state: string }>(sql`SELECT state FROM scheduled_task WHERE id = ${id}`)
        return row?.state === "completed" ? true : undefined
      }), "held child did not complete after release", "2 seconds")
    }),
  )
})
