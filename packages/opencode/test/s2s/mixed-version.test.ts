import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { mkdir, rm } from "node:fs/promises"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { sql } from "drizzle-orm"
import { Cause, Effect, Exit, Layer, Schema } from "effect"
import { Session } from "../../src/session/session"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { S2SStore } from "../../src/s2s/store"
import { S2STool } from "../../src/tool/s2s"
import { claimLegacy } from "./fixtures/legacy-claim"
import { receiptLayer } from "./fixtures/receipt-layer"
import { pollWithTimeout, testEffectIsolatedShared } from "../lib/effect"

const it = testEffectIsolatedShared(receiptLayer(":memory:") as unknown as Layer.Layer<any, any, never>)
const root = "/tmp/opencode/s2s-durable-impl-2"
const dbFile = path.join(root, `mixed-${process.pid}-${crypto.randomUUID()}.sqlite`)
const shared = testEffectIsolatedShared(receiptLayer(dbFile) as unknown as Layer.Layer<any, any, never>)
const ref = { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }

beforeAll(() => mkdir(root, { recursive: true }))
afterAll(async () => { await Promise.all([dbFile, `${dbFile}-wal`, `${dbFile}-shm`].map((file) => rm(file, { force: true }))) })

const ctxFor = (sessionID: SessionID) => ({
  sessionID,
  messageID: MessageID.ascending(),
  agent: "build",
  abort: new AbortController().signal,
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

const send = Effect.fn("S2SMixedVersionTest.send")(function* (sender: SessionID, target: SessionID, body: string) {
  const tool = yield* S2STool
  const def = yield* tool.init()
  return yield* def.execute({ command: "msg", target, body }, ctxFor(sender))
})

describe("mixed S2S builds", () => {
  it.instance("isolates new receipts from the legacy delete path", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const peer = yield* sessions.create({ title: "recipient" })
      yield* store.insertAllow(sender.id, peer.id)
      const sent = yield* send(sender.id, peer.id, "canonical send")
      expect(sent.output).toContain("Persisted to s2s_message")
      const canonical = (yield* store.pendingForSession(peer.id, Date.now()))[0]!
      yield* store.insertInbox({ id: "inb_legacy_mixed", targetSessionID: peer.id, fromSessionID: sender.id, fromSlug: "sender", capsule: "{}", timeCreated: Date.now() })
      expect((yield* claimLegacy([peer.id])).map((row) => row.id)).toEqual(["inb_legacy_mixed"])
      yield* db.run(sql`DELETE FROM s2s_inbox WHERE id = ${canonical.id}`)
      expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message WHERE id = ${canonical.id} AND delivered_at IS NULL`)).toEqual({ count: 1 })
      expect(yield* store.adoptLegacy("inb_legacy_mixed")).toBeUndefined()
      yield* store.reapStale(Date.now() + 1_000)
      expect((yield* store.adoptLegacy("inb_legacy_mixed"))?.id).toBe("inb_legacy_mixed")
      expect(yield* store.receipt({
        id: "inb_legacy_mixed",
        target: peer.id,
        deliveredAt: Date.now(),
        sessions,
        buildTranscript: (created) => ({
          message: { id: MessageID.make("msg_inb_legacy_mixed"), sessionID: peer.id, role: "user", origin: "s2s", agent: "build", model: ref, time: { created } },
          parts: [{ id: PartID.make("prt_inb_legacy_mixed"), messageID: MessageID.make("msg_inb_legacy_mixed"), sessionID: peer.id, type: "text", text: "legacy inbound", synthetic: true }],
        }),
      })).toBe(true)
      expect((yield* store.pendingForSession(peer.id, Date.now())).map((row) => row.id)).toContain(canonical.id)
      const events = yield* db.all<{ type: string }>(sql`SELECT type FROM event WHERE aggregate_id = ${peer.id} AND type LIKE 'message.%' ORDER BY seq`)
      expect(events.map((row) => row.type)).toEqual([
        expect.stringContaining(SessionV1.Event.MessageUpdated.type),
        expect.stringContaining(SessionV1.Event.PartUpdated.type),
      ])
      const oldUser = yield* Schema.decodeUnknownEffect(SessionV1.User)({ id: MessageID.ascending(), sessionID: peer.id, role: "user", agent: "build", model: ref, time: { created: Date.now() } })
      expect(oldUser.origin).toBeUndefined()
    }),
  )

  it.instance("accepts an unknown recipient as pending without configuration", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const peer = yield* sessions.create({ title: "recipient" })
      yield* store.insertAllow(sender.id, peer.id)
      expect(yield* store.peerCapability(peer.id, Date.now())).toEqual({ state: "unknown" })
      const result = yield* send(sender.id, peer.id, "waiting for upgrade")
      expect(result.output).toContain("No current presence record; stored pending and will deliver when the recipient runs a current build.")
      expect((yield* store.pendingForSession(peer.id, Date.now())).map((row) => row.fromSessionID)).toEqual([sender.id])
    }),
  )

  it.instance("refuses fresh lower capability but treats an old heartbeat as unknown", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const peer = yield* sessions.create({ title: "recipient" })
      yield* store.insertAllow(sender.id, peer.id)
      yield* db.run(sql`INSERT INTO s2s_presence (session_id, owner_id, capability_version, heartbeat_at) VALUES (${peer.id}, 'old-binary', 0, ${Date.now()})`)
      const rejected = yield* send(sender.id, peer.id, "refused").pipe(Effect.exit)
      expect(Exit.isFailure(rejected)).toBe(true)
      if (Exit.isFailure(rejected)) expect(Cause.pretty(rejected.cause)).toContain("Recipient is running an incompatible S2S build (capability 0); upgrade it and retry.")
      expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message WHERE target_session_id = ${peer.id}`)).toEqual({ count: 0 })
      yield* db.run(sql`UPDATE s2s_presence SET heartbeat_at = ${Date.now() - 60_000} WHERE session_id = ${peer.id}`)
      expect(yield* store.peerCapability(peer.id, Date.now())).toEqual({ state: "unknown" })
      expect((yield* send(sender.id, peer.id, "accepted after stale presence")).output).toContain("No current presence record")
    }),
  )

  shared.instance("current processes deliver with no readiness variable", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const sender = yield* sessions.create({ title: "sender" })
      const peer = yield* sessions.create({ title: "recipient" })
      yield* store.insertAllow(sender.id, peer.id)
      const ready = path.join(root, `mixed-ready-${process.pid}`)
      const barrier = path.join(root, `mixed-go-${process.pid}`)
      const worker = Bun.spawn({ cmd: [process.execPath, new URL("./fixtures/adopt-worker.ts", import.meta.url).pathname, dbFile, barrier, ready, "auto", "canonical-presence", peer.id], cwd: import.meta.dir, stdout: "pipe", stderr: "pipe" })
      yield* pollWithTimeout(Effect.promise(() => Bun.file(ready).exists()).pipe(Effect.map((exists) => exists ? true : undefined)), "recipient worker did not announce its presence", "5 seconds")
      const capability = yield* store.peerCapability(peer.id, Date.now())
      const result = yield* send(sender.id, peer.id, "cross process")
      const id = (yield* store.pendingForSession(peer.id, Date.now()))[0]?.id
      if (!id) return yield* Effect.fail(new Error("Canonical message missing after accepted send"))
      yield* Effect.promise(() => Bun.write(barrier, id))
      const status = yield* Effect.promise(() => worker.exited)
      const output = yield* Effect.promise(() => new Response(worker.stdout).text())
      const errors = yield* Effect.promise(() => new Response(worker.stderr).text())
      expect(capability).toEqual({ state: "current", version: 1 })
      expect(result.output).toContain("recipient process will poll and wake")
      expect(status, errors).toBe(0)
      expect(output.trim()).toBe("won")
      expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message WHERE id = ${id} AND delivered_at IS NOT NULL AND transcript_message_id = ${`msg_${id}`}`)).toEqual({ count: 1 })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${`msg_${id}`}`)).toEqual({ count: 1 })
      yield* Effect.promise(() => Promise.all([ready, barrier].map((file) => rm(file, { force: true }))))
    }),
  )
})
