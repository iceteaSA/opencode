import { describe, expect, test } from "bun:test"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import path from "node:path"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { Layer } from "effect"
import { Effect } from "effect"
import { SessionID, MessageID } from "../../src/session/schema"
import { testEffectIsolatedShared } from "../lib/effect"
import { S2SStore } from "../../src/s2s/store"
import { Session } from "../../src/session/session"
import { S2STool } from "../../src/tool/s2s"
import { receiptLayer } from "./fixtures/receipt-layer"

const it = testEffectIsolatedShared(receiptLayer(":memory:") as unknown as Layer.Layer<any, any, never>)

const context = (sessionID: SessionID) => ({
  sessionID,
  messageID: MessageID.ascending(),
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

test("a scheduled canonical row survives a fresh database layer before its due instant", async () => {
  const root = "/tmp/opencode/s2s-durable-impl"
  await mkdir(root, { recursive: true })
  const directory = await mkdtemp(path.join(root, "schedule-reopen-"))
  const filename = path.join(directory, "mail.sqlite")
  const sender = SessionID.make("ses_scheduled_sender")
  const recipient = SessionID.make("ses_scheduled_recipient")
  const due = Date.now() + 120_000
  const layer = () => S2SStore.layer.pipe(Layer.provideMerge(Database.layerFromPath(filename)))
  try {
    await Effect.runPromise(Effect.gen(function* () {
      const { db } = yield* Database.Service
      const store = yield* S2SStore.Service
      yield* db.run(sql`PRAGMA foreign_keys = OFF`)
      yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (${sender}, 'prj_schedule', 'sender', '/tmp', 'Sender', '1', 1, 1)`)
      yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (${recipient}, 'prj_schedule', 'recipient', '/tmp', 'Recipient', '1', 1, 1)`)
      yield* db.run(sql`PRAGMA foreign_keys = ON`)
      const outcome = yield* store.tryEnqueueWithDedup({ dedupeKey: "schedule-restart-key", sender, target: recipient, fromSlug: "sender", capsule: "{}", capsuleId: "caps_schedule_restart", timeCreated: Date.now(), deliverAt: due, windowMs: 600_000, inboxCap: 50 })
      expect(outcome._tag).toBe("inserted")
    }).pipe(Effect.provide(layer()), Effect.scoped))

    await Effect.runPromise(Effect.gen(function* () {
      const store = yield* S2SStore.Service
      expect(yield* store.pendingTargets([recipient], due - 1)).toEqual([])
      expect(yield* store.pendingForSession(recipient, due - 1)).toEqual([])
      expect(yield* store.pendingTargets([recipient], due)).toEqual([recipient])
      expect((yield* store.pendingForSession(recipient, due)).map((row) => row.id)).toEqual(["caps_schedule_restart"])
    }).pipe(Effect.provide(layer()), Effect.scoped))
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

describe("scheduled durable S2S delivery", () => {
  it.instance("pendingTargets and pendingForSession agree on the exact due instant", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      const due = Date.now() + 120_000
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, deliver_at)
        VALUES ('caps_due_future', ${recipient.id}, ${sender.id}, 'sender', '{}', ${due - 10_000}, ${due})`)
      expect(yield* store.pendingTargets([recipient.id], due - 1)).not.toContain(recipient.id)
      expect(yield* store.pendingForSession(recipient.id, due - 1)).toEqual([])
      expect(yield* store.pendingTargets([recipient.id], due)).toContain(recipient.id)
      expect((yield* store.pendingForSession(recipient.id, due)).map((row) => row.id)).toEqual(["caps_due_future"])
    }),
  )

  it.instance("one call fans out three peers with stable staggered due times and dedupes an exact retry", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const def = yield* (yield* S2STool).init()
      const sender = yield* sessions.create({ title: "sender" })
      const peers = yield* Effect.forEach(["one", "two", "three"], (title) => sessions.create({ title }))
      for (const peer of peers) yield* store.insertAllow(sender.id, peer.id)
      const due = Date.now() + 120_000
      const input = { command: "msg" as const, targets: peers.map((peer) => peer.id), body: "review this", deliver_at: new Date(due).toISOString(), stagger_ms: 2500 }
      const result = yield* def.execute(input, context(sender.id))
      expect(result.metadata.results?.map((entry) => entry.status)).toEqual(["sent", "sent", "sent"])
      expect(result.metadata.results?.map((entry) => entry.target)).toEqual(peers.map((peer) => peer.id))
      const rows = yield* db.all<{ id: string; target: string; due: number }>(sql`SELECT id, target_session_id AS target, deliver_at AS due FROM s2s_message WHERE from_session_id = ${sender.id} ORDER BY deliver_at`)
      expect(rows.map((row) => row.due)).toEqual([due, due + 2500, due + 5000])
      expect(rows.map((row) => row.target)).toEqual(peers.map((peer) => peer.id))
      const retry = yield* def.execute(input, context(sender.id))
      expect(retry.metadata.results?.map((entry) => entry.status)).toEqual(["duplicate", "duplicate", "duplicate"])
      expect((yield* db.all(sql`SELECT id FROM s2s_message WHERE from_session_id = ${sender.id}`)).length).toBe(3)
    }),
  )

  it.instance("fan-out identifies denied and capped recipients without charging them to the sender", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const def = yield* (yield* S2STool).init()
      const sender = yield* sessions.create({ title: "sender" })
      const allowed = yield* sessions.create({ title: "allowed" })
      const denied = yield* sessions.create({ title: "denied" })
      const full = yield* sessions.create({ title: "full" })
      yield* store.insertAllow(sender.id, allowed.id)
      yield* store.insertAllow(sender.id, full.id)
      for (const index of Array.from({ length: 50 }, (_, index) => index)) {
        yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at)
          VALUES (${`caps_full_${index}`}, ${full.id}, ${sender.id}, 'sender', '{}', ${Date.now()})`)
      }
      const result = yield* def.execute({ command: "msg", targets: [allowed.id, denied.id, full.id], body: "partial admission" }, context(sender.id))
      expect(result.metadata.results?.map((entry) => entry.status)).toEqual(["sent", "failed", "failed"])
      expect(result.metadata.results?.map((entry) => entry.target)).toEqual([allowed.id, denied.id, full.id])
      expect(result.output).toContain(String(denied.id))
      expect(result.output).toContain(String(full.id))
      expect(result.metadata.allowance?.used).toBe(1)
      expect((yield* db.all(sql`SELECT id FROM s2s_message WHERE target_session_id = ${allowed.id}`)).length).toBe(1)
      expect((yield* db.all(sql`SELECT id FROM s2s_message WHERE target_session_id = ${full.id}`)).length).toBe(50)
    }),
  )

  it.instance("a storage fault for one peer does not undo the other accepted peers", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const def = yield* (yield* S2STool).init()
      const sender = yield* sessions.create({ title: "sender" })
      const peers = yield* Effect.forEach(["first", "broken", "third"], (title) => sessions.create({ title }))
      for (const peer of peers) yield* store.insertAllow(sender.id, peer.id)
      yield* db.run(sql`CREATE TRIGGER fail_middle BEFORE INSERT ON s2s_message WHEN NEW.target_session_id = (SELECT id FROM session WHERE title = 'broken' LIMIT 1)
        BEGIN SELECT RAISE(ABORT, 'per-peer storage fault'); END`)
      const result = yield* def.execute({ command: "msg", targets: peers.map((peer) => peer.id), body: "partial storage" }, context(sender.id))
      expect(result.metadata.results?.map((entry) => entry.status)).toEqual(["sent", "failed", "sent"])
      expect(result.metadata.results?.[1]).toMatchObject({ target: peers[1]!.id, reason: expect.stringContaining("retry") })
      expect(result.metadata.allowance?.used).toBe(2)
      expect((yield* db.all(sql`SELECT target_session_id FROM s2s_message WHERE from_session_id = ${sender.id} ORDER BY target_session_id`)).length).toBe(2)
    }),
  )

  it.instance("rejects invalid shape and never schedules a target after its expiry", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const def = yield* (yield* S2STool).init()
      const sender = yield* sessions.create({ title: "sender" })
      const first = yield* sessions.create({ title: "first" })
      const second = yield* sessions.create({ title: "second" })
      yield* store.insertAllow(sender.id, first.id)
      yield* store.insertAllow(sender.id, second.id)
      const base = Date.now() + 60_000
      const result = yield* def.execute({ command: "msg", targets: [first.id, second.id], body: "bounded", deliver_at: new Date(base).toISOString(), stagger_ms: 1000, expires_at: new Date(base + 500).toISOString() }, context(sender.id))
      expect(result.metadata.results?.map((entry) => entry.status)).toEqual(["sent", "failed"])
      expect(result.output).toContain("expires_at")
      for (const params of [
        { command: "msg" as const, target: first.id, targets: [second.id], body: "ambiguous" },
        { command: "msg" as const, targets: [], body: "empty" },
        { command: "msg" as const, target: first.id, body: "bad stagger", stagger_ms: 1 },
        { command: "msg" as const, targets: [first.id], body: "bad stagger", stagger_ms: -1 },
        { command: "msg" as const, targets: Array.from({ length: 21 }, () => first.id), body: "too many" },
      ]) {
        const exit = yield* def.execute(params, context(sender.id)).pipe(Effect.exit)
        expect(exit._tag).toBe("Failure")
      }
      expect(yield* db.get(sql`SELECT count(*) AS n FROM s2s_message WHERE from_session_id = ${sender.id}`)).toEqual({ n: 1 })
    }),
  )
})
