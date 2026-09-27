import { describe, expect } from "bun:test"
import { sql } from "drizzle-orm"
import { Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Session } from "../../src/session/session"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { encodeCapsule } from "../../src/s2s/capsule"
import { S2SDelivery } from "../../src/s2s/delivery"
import { S2SStore } from "../../src/s2s/store"
import { S2STool } from "../../src/tool/s2s"
import { testEffectIsolatedShared } from "../lib/effect"
import { receiptLayer } from "./fixtures/receipt-layer"

const it = testEffectIsolatedShared(receiptLayer(":memory:") as unknown as Layer.Layer<any, any, never>)
const ctxFor = (sessionID: SessionID) => ({
  sessionID,
  messageID: MessageID.ascending(),
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

const operator = (sessionID: SessionID): SessionV1.User => ({
  id: MessageID.ascending(),
  sessionID,
  role: "user",
  origin: "operator",
  time: { created: Date.now() },
  agent: "build",
  model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
})

describe("pending S2S expiry and supersession", () => {
  it.instance("requires a real calendar instant with an explicit timezone for expires_at", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      yield* store.insertAllow(sender.id, recipient.id)
      const def = yield* (yield* S2STool).init()
      for (const expires_at of ["2026-02-30T10:00:00Z", "2026-10-01T10:00:00"]) {
        const outcome = yield* def.execute({ command: "msg", target: recipient.id, body: "invalid clock", expires_at }, ctxFor(sender.id)).pipe(Effect.exit)
        expect(Exit.isFailure(outcome)).toBe(true)
      }
      expect(yield* store.sentHistory(sender.id, recipient.id)).toHaveLength(0)
    }),
  )

  it.instance("expires a dormant ask at delivery without publishing its original instruction", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      const lastUser = operator(recipient.id)
      yield* sessions.updateMessage(lastUser)
      const sentAt = Date.now() - 60_000
      const expiresAt = sentAt + 10_000
      const id = "caps_dormant_expired"
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, expires_at)
        VALUES (${id}, ${recipient.id}, ${sender.id}, 'sender', ${encodeCapsule({ version: 1, id, sender_slug: "sender", sender_session_id: sender.id, timestamp: sentAt, body: "obsolete instruction" })}, ${sentAt}, ${expiresAt})`)
      const row = (yield* store.pendingForSession(recipient.id, Date.now()))[0]!
      expect(yield* S2SDelivery.admit(row, lastUser, sessions)).toBe(false)
      expect(yield* db.get(sql`SELECT expired_at FROM s2s_message WHERE id = ${id}`)).toMatchObject({ expired_at: expect.any(Number) })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 0 })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${recipient.id} AND type LIKE 'message.%'`)).toEqual({ count: 1 })
    }),
  )

  it.instance("collapses a same-thread A→B→C chain and rejects another sender's retraction", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const other = yield* sessions.create({ title: "other" })
      const recipient = yield* sessions.create({ title: "recipient" })
      yield* store.insertAllow(sender.id, recipient.id)
      yield* store.insertAllow(other.id, recipient.id)
      const def = yield* (yield* S2STool).init()
      yield* def.execute({ command: "msg", target: recipient.id, body: "instruction A" }, ctxFor(sender.id))
      const first = (yield* store.sentHistory(sender.id, recipient.id))[0]!.id
      const second = yield* def.execute({ command: "msg", target: recipient.id, body: "instruction B", supersedes: first }, ctxFor(sender.id))
      expect(second.metadata.supersession).toBe("superseded")
      const middle = (yield* store.sentHistory(sender.id, recipient.id))[0]!.id
      const denial = yield* def.execute({ command: "msg", target: recipient.id, body: "unauthorized", supersedes: middle }, ctxFor(other.id)).pipe(Effect.exit)
      expect(Exit.isFailure(denial)).toBe(true)
      const third = yield* def.execute({ command: "msg", target: recipient.id, body: "instruction C", supersedes: middle }, ctxFor(sender.id))
      expect(third.metadata.supersession).toBe("superseded")
      expect((yield* store.sentHistory(sender.id, recipient.id)).map((row) => row.state)).toEqual(["pending", "superseded", "superseded"])
      expect((yield* store.pendingForSession(recipient.id, Date.now())).map((row) => row.id)).toEqual([(yield* store.sentHistory(sender.id, recipient.id))[0]!.id])
      expect((yield* store.sentHistory(other.id, recipient.id))).toHaveLength(0)
      const lastUser = operator(recipient.id)
      yield* sessions.updateMessage(lastUser)
      const latest = (yield* store.pendingForSession(recipient.id, Date.now()))[0]!
      expect(yield* S2SDelivery.admit(latest, lastUser, sessions)).toBe(true)
      const { db } = yield* Database.Service
      const part = yield* db.get<{ text: string }>(sql`SELECT json_extract(data, '$.text') AS text FROM part WHERE id = ${PartID.make(`prt_${latest.id}_frame`)}`)
      expect(part?.text).toContain(`retracts="${middle}"`)
      expect(part?.text).toContain("instruction C")
      expect(part?.text).not.toContain("instruction A")
      expect(part?.text).not.toContain("instruction B")
    }),
  )

  it.instance("renders one visible summary for two expired asks in a recipient drain batch", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      const lastUser = operator(recipient.id)
      yield* sessions.updateMessage(lastUser)
      for (const id of ["caps_expired_a", "caps_expired_b"]) {
        yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, expires_at)
          VALUES (${id}, ${recipient.id}, ${sender.id}, 'sender', ${encodeCapsule({ version: 1, id, sender_slug: "sender", sender_session_id: sender.id, timestamp: Date.now() - 60_000, body: `DO NOT SHOW ${id}` })}, ${Date.now() - 60_000}, ${Date.now() - 1})`)
      }
      const expired = yield* store.resolvePendingForSession(recipient.id, Date.now())
      expect(expired).toEqual(["caps_expired_a", "caps_expired_b"])
      yield* S2SDelivery.summarizeExpired(recipient.id, expired, lastUser, sessions)
      const messages = yield* sessions.messages({ sessionID: recipient.id })
      expect(messages.filter((message) => message.info.role === "user" && message.info.origin === "s2s")).toHaveLength(1)
      const notice = messages.find((message) => message.info.role === "user" && message.info.origin === "s2s")
      expect(notice?.parts.map((part) => part.type === "text" ? part.text : "").join(" ")).toContain("2 expired")
      expect(JSON.stringify(notice)).not.toContain("DO NOT SHOW")
    }),
  )

  it.instance("collects expiries discovered after the batch scan into one notice", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      const lastUser = operator(recipient.id)
      yield* sessions.updateMessage(lastUser)
      for (const id of ["caps_late_expired_a", "caps_late_expired_b"]) {
        yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, expires_at)
          VALUES (${id}, ${recipient.id}, ${sender.id}, 'sender', ${encodeCapsule({ version: 1, id, sender_slug: "sender", sender_session_id: sender.id, timestamp: Date.now() - 60_000, body: `HIDDEN-${id}` })}, ${Date.now() - 60_000}, ${Date.now() - 1})`)
      }
      const rows = yield* store.pendingForSession(recipient.id, Date.now())
      const expired: string[] = []
      for (const row of rows) expect(yield* S2SDelivery.admit(row, lastUser, sessions, expired)).toBe(false)
      expect(expired.toSorted()).toEqual(["caps_late_expired_a", "caps_late_expired_b"])
      yield* S2SDelivery.summarizeExpired(recipient.id, expired, lastUser, sessions)
      expect((yield* sessions.messages({ sessionID: recipient.id })).filter((message) => message.info.role === "user" && message.info.origin === "s2s")).toHaveLength(1)
    }),
  )

  it.instance("finishes an already-claimed receipt when expiry passes mid-protocol", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      const lastUser = operator(recipient.id)
      yield* sessions.updateMessage(lastUser)
      const id = "caps_claim_before_expiry"
      const expiresAt = Date.now() + 5_000
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, expires_at)
        VALUES (${id}, ${recipient.id}, ${sender.id}, 'sender', ${encodeCapsule({ version: 1, id, sender_slug: "sender", sender_session_id: sender.id, timestamp: Date.now(), body: "committed instruction" })}, ${Date.now()}, ${expiresAt})`)
      const row = (yield* store.pendingForSession(recipient.id, Date.now()))[0]!
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const delayed = {
        ...sessions,
        updateMessage: <T extends SessionV1.Info>(message: T) => Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return yield* sessions.updateMessage(message)
        }),
      }
      const receipt = yield* Effect.forkChild(store.receipt({
        id, target: recipient.id, deliveredAt: Date.now(), sessions: delayed,
        buildTranscript: (created) => ({
          message: { ...lastUser, id: MessageID.make(`msg_${id}`), origin: "s2s", time: { created } },
          parts: [{ id: PartID.make(`prt_${id}_frame`), messageID: MessageID.make(`msg_${id}`), sessionID: recipient.id, type: "text", text: "committed instruction" }],
        }),
      }))
      yield* Deferred.await(entered)
      const resolution = yield* Effect.forkChild(store.resolvePending(id, recipient.id, expiresAt + 1))
      yield* Deferred.succeed(release, undefined)
      expect(yield* Fiber.join(receipt)).toBe(true)
      expect(yield* Fiber.join(resolution)).not.toBe("expired")
      expect(yield* db.get(sql`SELECT delivered_at, expired_at FROM s2s_message WHERE id = ${id}`)).toMatchObject({ delivered_at: expect.any(Number), expired_at: null })
    }),
  )

  it.instance("reports an older message as in delivery or delivered instead of claiming to retract it", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      yield* store.insertAllow(sender.id, recipient.id)
      const lastUser = operator(recipient.id)
      yield* sessions.updateMessage(lastUser)
      const def = yield* (yield* S2STool).init()
      yield* def.execute({ command: "msg", target: recipient.id, body: "original" }, ctxFor(sender.id))
      const original = (yield* store.sentHistory(sender.id, recipient.id))[0]!.id
      yield* db.run(sql`CREATE TRIGGER fail_first_part BEFORE INSERT ON event WHEN new.type LIKE 'message.part.updated%' BEGIN SELECT RAISE(ABORT, 'pause after message'); END`)
      const pending = (yield* store.pendingForSession(recipient.id, Date.now()))[0]!
      expect(Exit.isFailure(yield* S2SDelivery.admit(pending, lastUser, sessions).pipe(Effect.exit))).toBe(true)
      const followup = yield* def.execute({ command: "msg", target: recipient.id, body: "replacement", supersedes: original }, ctxFor(sender.id))
      expect(followup.metadata.supersession).toBe("in_delivery")
      const replacement = (yield* store.sentHistory(sender.id, recipient.id))[0]!.id
      expect((yield* store.pendingForSession(recipient.id, Date.now())).find((row) => row.id === replacement)?.supersedes).toBeNull()
      expect((yield* db.get(sql`SELECT superseded_at FROM s2s_message WHERE id = ${original}`))).toEqual({ superseded_at: null })
      yield* db.run(sql`DROP TRIGGER fail_first_part`)
      expect(yield* S2SDelivery.admit(pending, lastUser, sessions)).toBe(true)
      const late = yield* def.execute({ command: "msg", target: recipient.id, body: "later", supersedes: original }, ctxFor(sender.id))
      expect(late.metadata.supersession).toBe("already_delivered")
      expect((yield* store.sentHistory(sender.id, recipient.id)).find((row) => row.id === original)?.state).toBe("delivered")
    }),
  )

  it.instance("uses the durable sender clock for sent= and the recipient clock for transcript order", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const recipient = yield* sessions.create({ title: "recipient" })
      const lastUser = operator(recipient.id)
      yield* sessions.updateMessage(lastUser)
      const id = "caps_sender_clock"
      const sentAt = Date.now() - 60_000
      const forgedCapsuleTime = sentAt - 3_600_000
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at)
        VALUES (${id}, ${recipient.id}, ${sender.id}, 'sender', ${encodeCapsule({ version: 1, id, sender_slug: "sender", sender_session_id: sender.id, timestamp: forgedCapsuleTime, body: "arriving now" })}, ${sentAt})`)
      const receiptStart = Date.now()
      expect(yield* S2SDelivery.admit((yield* store.pendingForSession(recipient.id, Date.now()))[0]!, lastUser, sessions)).toBe(true)
      const message = yield* db.get<{ time_created: number }>(sql`SELECT time_created FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)
      const frame = yield* db.get<{ text: string }>(sql`SELECT json_extract(data, '$.text') AS text FROM part WHERE id = ${PartID.make(`prt_${id}_frame`)}`)
      expect(message?.time_created).toBeGreaterThanOrEqual(receiptStart)
      expect(frame?.text).toContain(`time="${message?.time_created}"`)
      expect(frame?.text).toContain(`sent="${new Date(sentAt).toISOString()}"`)
      expect(frame?.text).not.toContain(new Date(forgedCapsuleTime).toISOString())
    }),
  )
})
