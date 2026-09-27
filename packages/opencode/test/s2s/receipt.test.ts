import { expect, test } from "bun:test"
import { mkdir, mkdtemp, rm } from "node:fs/promises"
import path from "node:path"
import { sql } from "drizzle-orm"
import { Cause, Effect, Exit, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { Session } from "../../src/session/session"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { S2SDelivery } from "../../src/s2s/delivery"
import { S2SStore } from "../../src/s2s/store"
import { testEffectIsolatedShared } from "../lib/effect"
import { claimLegacy } from './fixtures/legacy-claim';
import { receiptLayer } from "./fixtures/receipt-layer"

const it = testEffectIsolatedShared(receiptLayer(":memory:") as unknown as Layer.Layer<any, any, never>)
const target = SessionID.make("ses_atomic_receipt_target")

function observeSession(events: EventV2.Interface, id: SessionID, received: string[]) {
  return events.listen((event) => Effect.sync(() => {
    if (typeof event.data === "object" && event.data !== null && "sessionID" in event.data && event.data.sessionID === id) {
      received.push(event.type)
    }
  }))
}

const frame = (id: string, created = Date.now()) => {
  const message: SessionV1.User = {
    id: MessageID.make(`msg_${id}`),
    sessionID: target,
    role: "user",
    origin: "s2s",
    time: { created },
    agent: "build",
    model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
  }
  const parts: SessionV1.TextPart[] = [
    { id: PartID.make(`prt_${id}_frame`), messageID: message.id, sessionID: target, type: "text", text: "<external-context>body</external-context>", synthetic: true },
    { id: PartID.make(`prt_${id}_marker`), messageID: message.id, sessionID: target, type: "text", text: "✉ body", synthetic: false },
  ]
  return { message, parts }
}

function receipt(store: S2SStore.Interface, input: { id: string; target: SessionID; deliveredAt: number; sessions: Session.Interface }) {
  return store.receipt({
    ...input,
    buildTranscript: (created) => {
      const built = frame(input.id, created)
      return {
        message: { ...built.message, sessionID: input.target },
        parts: built.parts.map((part) => ({ ...part, sessionID: input.target })),
      }
    },
  })
}

it.instance("delivered frames publish durable transcript events to live subscribers", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const events = yield* EventV2Bridge.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "recipient" })
    const id = "inb_event_receipt"
    const received: string[] = []
    const unsubscribe = yield* observeSession(events, chat.id, received)
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${chat.id}, 'ses_sender', 'sender', '{}', 1)`)
    const input = frame(id)
    const message = { ...input.message, sessionID: chat.id }
    const parts = input.parts.map((part) => ({ ...part, sessionID: chat.id }))
    expect(yield* receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions })).toBe(true)
    const types = [SessionV1.Event.MessageUpdated.type, SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.type]
    expect(received).toEqual(types)
    const rows = yield* db.all<{ type: string }>(sql`SELECT type FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%' ORDER BY seq`)
    expect(rows.map((row) => row.type)).toEqual(types.map((type) => expect.stringContaining(type)))
  }),
)

it.instance("nested publish rolls back the database but not the live stream", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2Bridge.Service
    const chat = yield* sessions.create({ title: "recipient" })
    const id = "inb_nested_receipt"
    const received: string[] = []
    const unsubscribe = yield* observeSession(events, chat.id, received)
    yield* Effect.addFinalizer(() => unsubscribe)
    const input = frame(id)
    const message = { ...input.message, sessionID: chat.id }
    const parts = input.parts.map((part) => ({ ...part, sessionID: chat.id }))
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${chat.id}, 'ses_sender', 'sender', '{}', 1)`)
    yield* db.run(sql`CREATE TRIGGER fail_receipt BEFORE UPDATE OF delivered_at ON s2s_message BEGIN SELECT RAISE(ABORT, 'injected crash between event publish and receipt'); END`)
    const attempt = yield* db.transaction((tx) => Effect.gen(function* () {
      yield* sessions.updateMessage(message)
      for (const part of parts) yield* sessions.updatePart(part)
      yield* tx.run(sql`UPDATE s2s_message SET delivered_at = ${Date.now()} WHERE id = ${id}`)
    }), { behavior: "immediate" }).pipe(Effect.exit)
    expect(Exit.isFailure(attempt)).toBe(true)
    if (Exit.isFailure(attempt)) expect(Cause.pretty(attempt.cause)).toContain("injected crash between event publish and receipt")
    expect(yield* db.get(sql`SELECT delivered_at FROM s2s_message WHERE id = ${id}`)).toEqual({ delivered_at: null })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 0 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${message.id}`)).toEqual({ count: 0 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${message.id}`)).toEqual({ count: 0 })
    expect(received).toEqual([SessionV1.Event.MessageUpdated.type, SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.type])
  }),
)

it.instance("a failed delivered mark retains one visible frame and retry marks it without duplicate events", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const events = yield* EventV2Bridge.Service
    const sessions = yield* Session.Service
    const id = "inb_atomic_receipt"
    yield* db.run(sql`PRAGMA foreign_keys = OFF`)
    yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (${target}, 'prj_atomic', 'atomic', '/tmp', 'Atomic', '1', 1, 1)`)
    yield* db.run(sql`PRAGMA foreign_keys = ON`)
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${target}, 'ses_sender', 'sender', '{}', 1)`)
    const received: string[] = []
    const unsubscribe = yield* observeSession(events, target, received)
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* db.run(sql`CREATE TRIGGER fail_receipt BEFORE UPDATE OF delivered_at ON s2s_message BEGIN SELECT RAISE(ABORT, 'injected crash between publish and delivered mark'); END`)
    const first = yield* receipt(store, { id, target, deliveredAt: Date.now(), sessions }).pipe(Effect.exit)
    expect(Exit.isFailure(first)).toBe(true)
    if (Exit.isFailure(first)) expect(Cause.pretty(first.cause)).toContain("injected crash between publish and delivered mark")
    expect(yield* db.get(sql`SELECT delivered_at FROM s2s_message WHERE id = ${id}`)).toEqual({ delivered_at: null })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 2 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${target} AND type LIKE 'message.%'`)).toEqual({ count: 3 })
    expect(received).toHaveLength(3)
    yield* db.run(sql`DROP TRIGGER fail_receipt`)
    expect(yield* receipt(store, { id, target, deliveredAt: Date.now(), sessions })).toBe(true)
    expect(yield* receipt(store, { id, target, deliveredAt: Date.now(), sessions })).toBe(false)
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 2 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${target} AND type LIKE 'message.%'`)).toEqual({ count: 3 })
    expect(received).toHaveLength(3)
    expect(yield* db.get(sql`SELECT transcript_message_id FROM s2s_message WHERE id = ${id} AND delivered_at IS NOT NULL`)).toEqual({ transcript_message_id: `msg_${id}` })
  }),
)

it.instance("a failed transcript publish leaves no event, frame or receipt and retries cleanly", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const events = yield* EventV2Bridge.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "recipient" })
    const id = "inb_failed_publish"
    const input = frame(id)
    const message = { ...input.message, sessionID: chat.id }
    const parts = input.parts.map((part) => ({ ...part, sessionID: chat.id }))
    const received: string[] = []
    const unsubscribe = yield* observeSession(events, chat.id, received)
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${chat.id}, 'ses_sender', 'sender', '{}', 1)`)
    yield* db.run(sql`CREATE TRIGGER fail_event BEFORE INSERT ON event WHEN new.type LIKE 'message.%' BEGIN SELECT RAISE(ABORT, 'injected crash during transcript publish'); END`)
    const first = yield* receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions }).pipe(Effect.exit)
    expect(Exit.isFailure(first)).toBe(true)
    expect(yield* db.get(sql`SELECT delivered_at FROM s2s_message WHERE id = ${id}`)).toEqual({ delivered_at: null })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 0 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${message.id}`)).toEqual({ count: 0 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${message.id}`)).toEqual({ count: 0 })
    expect(received).toEqual([])
    yield* db.run(sql`DROP TRIGGER fail_event`)
    expect(yield* receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions })).toBe(true)
    expect(received).toEqual([SessionV1.Event.MessageUpdated.type, SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.type])
  }),
)

it.instance("a failure at the first part preserves its committed message event and retry finishes the frame", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2Bridge.Service
    const chat = yield* sessions.create({ title: "recipient" })
    const id = "inb_failed_second_event"
    const input = frame(id)
    const message = { ...input.message, sessionID: chat.id }
    const parts = input.parts.map((part) => ({ ...part, sessionID: chat.id }))
    const received: string[] = []
    const unsubscribe = yield* observeSession(events, chat.id, received)
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${chat.id}, 'ses_sender', 'sender', '{}', 1)`)
    yield* db.run(sql`CREATE TRIGGER fail_part_event BEFORE INSERT ON event WHEN new.type LIKE 'message.part.updated%' BEGIN SELECT RAISE(ABORT, 'injected failure after message publish'); END`)
    const attempt = yield* receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions }).pipe(Effect.exit)
    expect(Exit.isFailure(attempt)).toBe(true)
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${message.id}`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${message.id}`)).toEqual({ count: 0 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT delivered_at FROM s2s_message WHERE id = ${id}`)).toEqual({ delivered_at: null })
    expect(received).toEqual([SessionV1.Event.MessageUpdated.type])
    yield* db.run(sql`DROP TRIGGER fail_part_event`)
    expect(yield* receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions })).toBe(true)
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${message.id}`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${message.id}`)).toEqual({ count: 2 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 3 })
    expect(received).toEqual([SessionV1.Event.MessageUpdated.type, SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.type])
  }),
)

it.instance("a retried delivery renders its missing frame with the committed message's receive time", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "recipient" })
    const id = "inb_consistent_receive_time"
    const lastUser: SessionV1.User = {
      id: MessageID.ascending(), sessionID: chat.id, role: "user", origin: "operator", time: { created: Date.now() }, agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
    }
    yield* sessions.updateMessage(lastUser)
    const sentAt = Date.now() - 60_000
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${chat.id}, 'ses_sender', 'sender', ${JSON.stringify({ version: 1, id, sender_slug: "sender", sender_session_id: "ses_sender", timestamp: sentAt, body: "retry" })}, ${sentAt})`)
    const row = (yield* store.pendingForSession(chat.id, Date.now()))[0]!
    yield* db.run(sql`CREATE TRIGGER fail_part_event BEFORE INSERT ON event WHEN new.type LIKE 'message.part.updated%' BEGIN SELECT RAISE(ABORT, 'part failure after message'); END`)
    const first = yield* S2SDelivery.admit(row, lastUser, sessions).pipe(Effect.exit)
    expect(Exit.isFailure(first)).toBe(true)
    const stored = yield* db.get<{ time_created: number }>(sql`SELECT time_created FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)
    expect(stored?.time_created).toBeGreaterThan(sentAt)
    yield* Effect.sleep("30 millis")
    yield* db.run(sql`DROP TRIGGER fail_part_event`)
    expect(yield* S2SDelivery.admit(row, lastUser, sessions)).toBe(true)
    const persisted = yield* db.get<{ time_created: number }>(sql`SELECT time_created FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)
    const frame = yield* db.get<{ text: string }>(sql`SELECT json_extract(data, '$.text') AS text FROM part WHERE id = ${PartID.make(`prt_${id}_frame`)}`)
    expect(persisted?.time_created).toBe(stored?.time_created)
    expect(frame?.text).toContain(`time="${stored?.time_created}"`)
    expect(frame?.text).toContain(new Date(sentAt).toISOString())
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 4 })
  }),
)

it.instance("a failure at the second part preserves the first part event and retry only publishes the missing one", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const sessions = yield* Session.Service
    const events = yield* EventV2Bridge.Service
    const chat = yield* sessions.create({ title: "recipient" })
    const id = "inb_failed_third_event"
    const input = frame(id)
    const message = { ...input.message, sessionID: chat.id }
    const parts = input.parts.map((part) => ({ ...part, sessionID: chat.id }))
    const received: string[] = []
    const unsubscribe = yield* observeSession(events, chat.id, received)
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${chat.id}, 'ses_sender', 'sender', '{}', 1)`)
    yield* db.run(sql`CREATE TRIGGER fail_second_part BEFORE INSERT ON event WHEN new.type LIKE 'message.part.updated%' AND (SELECT count(*) FROM event WHERE type LIKE 'message.part.updated%') >= 1 BEGIN SELECT RAISE(ABORT, 'injected failure after first part'); END`)
    const attempt = yield* receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions }).pipe(Effect.exit)
    expect(Exit.isFailure(attempt)).toBe(true)
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${message.id}`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${message.id}`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 2 })
    expect(received).toEqual([SessionV1.Event.MessageUpdated.type, SessionV1.Event.PartUpdated.type])
    yield* db.run(sql`DROP TRIGGER fail_second_part`)
    expect(yield* receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions })).toBe(true)
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${message.id}`)).toEqual({ count: 2 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 3 })
    expect(received).toEqual([SessionV1.Event.MessageUpdated.type, SessionV1.Event.PartUpdated.type, SessionV1.Event.PartUpdated.type])
  }),
)

it.instance("in-process competing receipts publish one set of events", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const sessions = yield* Session.Service
    const chat = yield* sessions.create({ title: "recipient" })
    const id = "inb_competing_local_receipts"
    const input = frame(id)
    const message = { ...input.message, sessionID: chat.id }
    const parts = input.parts.map((part) => ({ ...part, sessionID: chat.id }))
    const delayed = {
      ...sessions,
      updateMessage: <T extends SessionV1.Info>(message: T) => sessions.updateMessage(message).pipe(Effect.delay("25 millis")),
    }
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${chat.id}, 'ses_sender', 'sender', '{}', 1)`)
    const results = yield* Effect.all([
      receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions: delayed }),
      receipt(store, { id, target: chat.id, deliveredAt: Date.now(), sessions: delayed }),
    ], { concurrency: "unbounded" })
    expect(results.sort()).toEqual([false, true])
    expect(yield* db.get(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${chat.id} AND type LIKE 'message.%'`)).toEqual({ count: 3 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${message.id}`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${message.id}`)).toEqual({ count: 2 })
  }),
)

it.instance("adopts an unclaimed legacy id once without deleting another worker's claim", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    yield* store.insertInbox({ id: "inb_legacy_atomic", targetSessionID: target, fromSessionID: target, fromSlug: "sender", capsule: "{}", timeCreated: 1 })
    const first = yield* store.adoptLegacy("inb_legacy_atomic")
    expect(first?.id).toBe("inb_legacy_atomic")
    expect(yield* store.adoptLegacy("inb_legacy_atomic")).toBeUndefined()
    expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_inbox WHERE id = 'inb_legacy_atomic'`)).toEqual({ count: 0 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message WHERE id = 'inb_legacy_atomic'`)).toEqual({ count: 1 })
    yield* store.insertInbox({ id: "inb_claimed_elsewhere", targetSessionID: target, fromSessionID: target, fromSlug: "sender", capsule: "{}", timeCreated: 1 })
    yield* claimLegacy([target])
    expect(yield* store.adoptLegacy("inb_claimed_elsewhere")).toBeUndefined()
    expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_inbox WHERE id = 'inb_claimed_elsewhere'`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message WHERE id = 'inb_claimed_elsewhere'`)).toEqual({ count: 0 })
    yield* store.reapStale(Date.now() + 1_000)
    expect((yield* store.adoptLegacy("inb_claimed_elsewhere"))?.id).toBe("inb_claimed_elsewhere")
  }),
)

it.instance("distinct canonical ids retain two frames even when sender and body match", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const sessions = yield* Session.Service
    yield* db.run(sql`PRAGMA foreign_keys = OFF`)
    yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (${target}, 'prj_distinct', 'distinct', '/tmp', 'Distinct', '1', 1, 1)`)
    yield* db.run(sql`PRAGMA foreign_keys = ON`)
    for (const id of ["inb_distinct_first", "inb_distinct_second"]) {
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${target}, 'ses_sender', 'sender', '{}', 1)`)
    }
    const rows = yield* store.pendingForSession(target, Date.now())
    expect(rows.filter((row) => row.id.startsWith("inb_distinct_"))).toHaveLength(2)
    for (const id of ["inb_distinct_first", "inb_distinct_second"]) {
      expect(yield* receipt(store, { id, target, deliveredAt: Date.now(), sessions })).toBe(true)
    }
    expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message WHERE id LIKE 'inb_distinct_%' AND delivered_at IS NOT NULL`)).toEqual({ count: 2 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id LIKE 'msg_inb_distinct_%'`)).toEqual({ count: 4 })
  }),
)

test("two processes adopt one legacy inbox id and commit one receipt", async () => {
  const root = "/tmp/opencode/s2s-durable-impl"
  await mkdir(root, { recursive: true })
  const directory = await mkdtemp(path.join(root, "adopt-"))
  const filename = path.join(directory, "shared.sqlite")
  const id = "inb_competing_adopters"
  try {
    await Effect.runPromise(Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db.run(sql`PRAGMA foreign_keys = OFF`)
      yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (${target}, 'prj_atomic', 'atomic', '/tmp', 'Atomic', '1', 1, 1)`)
      yield* db.run(sql`PRAGMA foreign_keys = ON`)
      yield* db.run(sql`INSERT INTO s2s_inbox (id, target_session_id, from_session_id, from_slug, capsule, time_created) VALUES (${id}, ${target}, 'ses_sender', 'sender', '{}', 1)`)
    }).pipe(Effect.provide(Database.layerFromPath(filename)), Effect.scoped))

    const barrier = path.join(directory, "start")
    const workers: Array<{ exited: Promise<number>; stdout: ReadableStream<Uint8Array>; stderr: ReadableStream<Uint8Array> }> = []
    for (const index of [0, 1]) {
      workers.push(Bun.spawn({
        cmd: [process.execPath, new URL("./fixtures/adopt-worker.ts", import.meta.url).pathname, filename, barrier, path.join(directory, `ready-${index}`), id],
        cwd: import.meta.dir,
        stdout: "pipe",
        stderr: "pipe",
      }))
      for (let attempt = 0; !(await Bun.file(path.join(directory, `ready-${index}`)).exists()); attempt++) {
        if (attempt === 499) throw new Error("adoption worker did not reach the start barrier")
        await Bun.sleep(10)
      }
    }
    await Bun.write(barrier, "go")
    const outcomes = await Promise.all(workers.map(async (worker) => ({
      exit: await worker.exited,
      stdout: await new Response(worker.stdout).text(),
      stderr: await new Response(worker.stderr).text(),
    })))
    for (const outcome of outcomes) expect(outcome.exit, outcome.stderr).toBe(0)
    expect(outcomes.map((outcome) => outcome.stdout.trim()).sort()).toEqual(["lost", "won"])
    await Effect.runPromise(Effect.gen(function* () {
      const { db } = yield* Database.Service
      const store = yield* S2SStore.Service
      expect(yield* store.adoptLegacy(id)).toBeUndefined()
      expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_inbox WHERE id = ${id}`)).toEqual({ count: 0 })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message WHERE id = ${id} AND delivered_at IS NOT NULL`)).toEqual({ count: 1 })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 1 })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 2 })
    }).pipe(Effect.provide(S2SStore.layer.pipe(Layer.provideMerge(Database.layerFromPath(filename)))), Effect.scoped))
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
}, 30000)

test("two processes race one canonical receipt with unique transcript rows and one delivered mark", async () => {
  const root = "/tmp/opencode/s2s-durable-impl"
  await mkdir(root, { recursive: true })
  const directory = await mkdtemp(path.join(root, "receipt-race-"))
  const filename = path.join(directory, "shared.sqlite")
  const id = "inb_competing_receipts"
  try {
    await Effect.runPromise(Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db.run(sql`PRAGMA foreign_keys = OFF`)
      yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (${target}, 'prj_atomic', 'atomic', '/tmp', 'Atomic', '1', 1, 1)`)
      yield* db.run(sql`PRAGMA foreign_keys = ON`)
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES (${id}, ${target}, 'ses_sender', 'sender', '{}', 1)`)
    }).pipe(Effect.provide(Database.layerFromPath(filename)), Effect.scoped))

    const barrier = path.join(directory, "start")
    const workers: Array<{ exited: Promise<number>; stdout: ReadableStream<Uint8Array>; stderr: ReadableStream<Uint8Array> }> = []
    for (const index of [0, 1]) {
      workers.push(Bun.spawn({
        cmd: [process.execPath, new URL("./fixtures/adopt-worker.ts", import.meta.url).pathname, filename, barrier, path.join(directory, `ready-${index}`), id, "canonical"],
        cwd: import.meta.dir,
        stdout: "pipe",
        stderr: "pipe",
      }))
      for (let attempt = 0; !(await Bun.file(path.join(directory, `ready-${index}`)).exists()); attempt++) {
        if (attempt === 499) throw new Error("receipt worker did not reach the start barrier")
        await Bun.sleep(10)
      }
    }
    await Bun.write(barrier, "go")
    const outcomes = await Promise.all(workers.map(async (worker) => ({
      exit: await worker.exited,
      stdout: await new Response(worker.stdout).text(),
      stderr: await new Response(worker.stderr).text(),
    })))
    for (const outcome of outcomes) expect(outcome.exit, outcome.stderr).toBe(0)
    expect(outcomes.map((outcome) => outcome.stdout.trim()).sort()).toEqual(["lost", "won"])
    await Effect.runPromise(Effect.gen(function* () {
      const { db } = yield* Database.Service
      const events = yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM event WHERE aggregate_id = ${target} AND type LIKE 'message.%'`)
      expect(events?.count).toBeGreaterThanOrEqual(3)
      expect(yield* db.get(sql`SELECT count(*) AS count FROM message WHERE id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 1 })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM part WHERE message_id = ${MessageID.make(`msg_${id}`)}`)).toEqual({ count: 2 })
      expect(yield* db.get(sql`SELECT transcript_message_id FROM s2s_message WHERE id = ${id} AND delivered_at IS NOT NULL`)).toEqual({ transcript_message_id: `msg_${id}` })
    }).pipe(Effect.provide(Database.layerFromPath(filename)), Effect.scoped))
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
}, 30000)
