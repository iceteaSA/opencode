// Session-to-Session — insert-time dedupe for cross-process sends.
//
// Problem: the cross-process `s2s msg` path persists a row to
// s2s_inbox BEFORE returning, then hard-deletes the row on drain.
// If the tool call is interrupted (or its return is lost after the
// row was written), the caller cannot distinguish "written, return
// lost" from "nothing written" and will retry — the recipient sees
// the same body twice.
//
// Fix: every cross-process send computes a content-addressed dedup
// key (sha256(sender + NUL + recipient + NUL + body)) and the store
// inserts an s2s_sent row in the SAME transaction as the s2s_inbox
// row. A second send with the same key inside a 10-minute window is
// rejected at insert without writing a new inbox row, returning the
// original inbox id so the caller can recognize the retry.
//
// The s2s_sent table outlives s2s_inbox (delivered inbox rows are
// hard-deleted), so a duplicate retry that arrives AFTER the original
// was drained still resolves to the original id.

import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { Effect, Fiber, Layer } from "effect"
import { mkdtempSync, rmSync } from "fs"
import { tmpdir } from "os"
import { join } from "path"
import { sql } from "drizzle-orm"
import { Agent } from "../../src/agent/agent"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Messaging } from "../../src/messaging"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { S2SCapsule, encodeCapsule } from "../../src/s2s/capsule"
import { DEDUPE_WINDOW_MS, S2SStore } from "../../src/s2s/store"
import { Session } from "@/session/session"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Truncate } from "@/tool/truncate"
import { S2STool } from "../../src/tool/s2s"
import { MessageID, SessionID } from "../../src/session/schema"
import { testEffectShared } from "../lib/effect"

const database = Database.layerFromPath(":memory:")
const s2sFlags = RuntimeFlags.layer({
  experimentalEventSystem: true,
  experimentalAgentMessaging: true,
  experimentalS2S: true,
})

const baseLayer = LayerNode.compile(
  LayerNode.group([
    Database.node,
    Session.node,
    SessionProjector.node,
    EventV2Bridge.node,
    Config.node,
    S2SStore.node,
    Messaging.node,
    Agent.node,
    CrossSpawnSpawner.node,
    Truncate.node,
  ]),
  [
    [Database.node, database],
    [RuntimeFlags.node, s2sFlags],
  ],
)

const it = testEffectShared(baseLayer as unknown as Layer.Layer<any, any, never>)

const seedSession = Effect.fn("S2SDedupeTest.seedSession")(function* (slug: string) {
  const sessions = yield* Session.Service
  const info = yield* sessions.create({ title: slug, agent: "build" })
  return { id: info.id, slug }
})

const ctxFor = (sessionID: SessionID) => ({
  sessionID,
  messageID: MessageID.ascending(),
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

// sha256(sender + NUL + recipient + NUL + body). Mirror of the helper
// in `src/tool/s2s.ts` so tests can compare keys deterministically
// without going through crypto.subtle inside an Effect generator.
const dedupeKeyFor = async (sender: SessionID, recipient: SessionID, body: string): Promise<string> => {
  const data = new TextEncoder().encode(`${sender}\u0000${recipient}\u0000${body}`)
  const digest = await crypto.subtle.digest("SHA-256", data)
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
}

// Direct store entry point. Bypasses the tool layer's clock so the
// dedupe-window tests can pin `timeCreated` to a known epoch and then
// re-call past the window without sleeping for ten real minutes.
const tryEnqueue = Effect.fn("S2SDedupeTest.tryEnqueue")(function* (input: {
  store: S2SStore.Interface
  sender: SessionID
  target: SessionID
  fromSlug: string
  capsuleId: string
  body: string
  timeCreated: number
}) {
  const key = yield* Effect.promise(() => dedupeKeyFor(input.sender, input.target, input.body))
  const capsule: S2SCapsule = {
    version: 1,
    id: input.capsuleId,
    sender_slug: input.fromSlug,
    sender_session_id: String(input.sender),
    timestamp: input.timeCreated,
    body: input.body,
  }
  return yield* input.store.tryEnqueueWithDedup({
    dedupeKey: key,
    sender: input.sender,
    target: input.target,
    fromSlug: input.fromSlug,
    capsule: encodeCapsule(capsule),
    capsuleId: input.capsuleId,
    timeCreated: input.timeCreated,
    windowMs: DEDUPE_WINDOW_MS,
    inboxCap: 50,
  })
})

describe("S2S cross-process insert-time dedupe", () => {
  it.instance("an identical second send within the window is rejected and returns the original inbox id", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-1")
      const target = yield* seedSession("dedupe-target-1")
      yield* store.insertAllow(sender.id, target.id)
      const tool = yield* S2STool
      const def = yield* tool.init()

      const first = yield* def.execute({ command: "msg", target: target.id, body: "same-body" }, ctxFor(sender.id))
      expect(first.output).toContain("Persisted to s2s_inbox")
      const firstClaimed = yield* store.claimForSessions([target.id])
      const firstInboxId = firstClaimed[0]!.id

      const second = yield* def.execute({ command: "msg", target: target.id, body: "same-body" }, ctxFor(sender.id))

      // Second send is reported as already sent, NOT as a fresh persist.
      expect(second.output).toContain("Already sent within the last 10 minutes")
      expect(second.output).toContain("not re-queued")
      expect(second.output).toContain(firstInboxId)
      // Exactly one inbox row exists (counted directly so we don't
      // accidentally re-claim and lose the visibility).
      const { db } = yield* Database.Service
      const inboxCount = yield* db.all<{ n: number }>(sql`
        SELECT COUNT(*) AS n FROM s2s_inbox WHERE target_session_id = ${target.id}
      `)
      expect(inboxCount[0]?.n).toBe(1)
      // And the dedupe ledger has exactly one row for this key.
      const dedupeKey = yield* Effect.promise(() => dedupeKeyFor(sender.id, target.id, "same-body"))
      const sentCount = yield* db.all<{ n: number }>(sql`
        SELECT COUNT(*) AS n FROM s2s_sent WHERE dedupe_key = ${dedupeKey}
      `)
      expect(sentCount[0]?.n).toBe(1)
    }),
  )

  it.instance("duplicate after the original was delivered and deleted is still rejected via the s2s_sent ledger", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-2")
      const target = yield* seedSession("dedupe-target-2")
      yield* store.insertAllow(sender.id, target.id)
      const tool = yield* S2STool
      const def = yield* tool.init()

      const first = yield* def.execute(
        { command: "msg", target: target.id, body: "delivered-then-retry" },
        ctxFor(sender.id),
      )

      // Simulate the recipient process draining + hard-deleting the row
      // (the poller's success path). One claim, one delete, then the
      // inbox is empty again.
      const claimed = yield* store.claimForSessions([target.id])
      expect(claimed).toHaveLength(1)
      const originalId = claimed[0]!.id
      expect(first.output).toContain(originalId)
      yield* store.deleteInbox(originalId)
      expect(yield* store.claimForSessions([target.id])).toEqual([])

      // The retry still resolves as a duplicate because the s2s_sent
      // row outlives the s2s_inbox row.
      const retry = yield* def.execute(
        { command: "msg", target: target.id, body: "delivered-then-retry" },
        ctxFor(sender.id),
      )
      expect(retry.output).toContain("Already sent within the last 10 minutes")
      expect(retry.output).toContain(originalId)
      expect(yield* store.claimForSessions([target.id])).toEqual([])
    }),
  )

  it.instance("after the dedupe window the same body is sent normally", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-3")
      const target = yield* seedSession("dedupe-target-3")

      // Pin `timeCreated` to a fixed epoch, then jump past the window
      // for the second send. No `Effect.sleep(10 minutes)` — the store
      // derives the cutoff from `timeCreated`, which the caller fully
      // controls, so two calls with straddle values are sufficient.
      const t1 = 1_700_000_000_000
      const t2 = t1 + DEDUPE_WINDOW_MS + 1

      const first = yield* tryEnqueue({
        store,
        sender: sender.id,
        target: target.id,
        fromSlug: sender.slug,
        capsuleId: "caps_first_window",
        body: "after-window",
        timeCreated: t1,
      })
      expect(first._tag).toBe("inserted")

      const second = yield* tryEnqueue({
        store,
        sender: sender.id,
        target: target.id,
        fromSlug: sender.slug,
        capsuleId: "caps_second_window",
        body: "after-window",
        timeCreated: t2,
      })
      expect(second._tag).toBe("inserted")

      // Two inbox rows total.
      const rows = yield* store.claimForSessions([target.id])
      expect(rows.map((r) => r.id).sort()).toEqual(["caps_first_window", "caps_second_window"])
    }),
  )

  it.instance("a different body to the same recipient is sent normally", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-4")
      const target = yield* seedSession("dedupe-target-4")
      yield* store.insertAllow(sender.id, target.id)
      const tool = yield* S2STool
      const def = yield* tool.init()

      yield* def.execute({ command: "msg", target: target.id, body: "body-a" }, ctxFor(sender.id))
      const second = yield* def.execute({ command: "msg", target: target.id, body: "body-b" }, ctxFor(sender.id))
      expect(second.output).toContain("Persisted to s2s_inbox")
      const rows = yield* store.claimForSessions([target.id])
      expect(rows).toHaveLength(2)
    }),
  )

  it.instance("the same body to a different recipient is sent normally", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-5")
      const target1 = yield* seedSession("dedupe-target-5a")
      const target2 = yield* seedSession("dedupe-target-5b")
      yield* store.insertAllow(sender.id, target1.id)
      yield* store.insertAllow(sender.id, target2.id)
      const tool = yield* S2STool
      const def = yield* tool.init()

      yield* def.execute({ command: "msg", target: target1.id, body: "shared" }, ctxFor(sender.id))
      const second = yield* def.execute({ command: "msg", target: target2.id, body: "shared" }, ctxFor(sender.id))
      expect(second.output).toContain("Persisted to s2s_inbox")
      expect(yield* store.claimForSessions([target1.id])).toHaveLength(1)
      expect(yield* store.claimForSessions([target2.id])).toHaveLength(1)
    }),
  )

  it.instance("two concurrent duplicate sends (different fibers) produce exactly one inbox row", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-6")
      const target = yield* seedSession("dedupe-target-6")
      yield* store.insertAllow(sender.id, target.id)

      // Two fibers race on the same dedupe key with DIFFERENT capsule
      // ids. The store transaction's write lock serializes them, so
      // exactly one returns `inserted` and the other returns `duplicate`.
      const senderID = sender.id
      const targetID = target.id
      const key = yield* Effect.promise(() => dedupeKeyFor(senderID, targetID, "race"))
      const capsuleA = encodeCapsule({
        version: 1,
        id: "capsule-aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        sender_slug: sender.slug,
        sender_session_id: String(senderID),
        timestamp: 1_700_000_000_000,
        body: "race",
      } as S2SCapsule)
      const capsuleB = encodeCapsule({
        version: 1,
        id: "capsule-bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
        sender_slug: sender.slug,
        sender_session_id: String(senderID),
        timestamp: 1_700_000_000_001,
        body: "race",
      } as S2SCapsule)
      const now = Date.now()
      const fiberA = yield* store
        .tryEnqueueWithDedup({
          dedupeKey: key,
          sender: senderID,
          target: targetID,
          fromSlug: sender.slug,
          capsule: capsuleA,
          capsuleId: "capsule-aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
          timeCreated: now,
          windowMs: DEDUPE_WINDOW_MS,
          inboxCap: 50,
        })
        .pipe(Effect.forkScoped)
      const fiberB = yield* store
        .tryEnqueueWithDedup({
          dedupeKey: key,
          sender: senderID,
          target: targetID,
          fromSlug: sender.slug,
          capsule: capsuleB,
          capsuleId: "capsule-bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
          timeCreated: now,
          windowMs: DEDUPE_WINDOW_MS,
          inboxCap: 50,
        })
        .pipe(Effect.forkScoped)
      const [a, b] = yield* Effect.all([Fiber.join(fiberA), Fiber.join(fiberB)])
      const tags = [a._tag, b._tag].sort()
      expect(tags).toEqual(["duplicate", "inserted"])
      const inboxRows = yield* store.claimForSessions([target.id])
      expect(inboxRows).toHaveLength(1)
    }),
  )

  it.instance("the duplicate path does NOT consume the recipient's INBOX_CAP", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-7")
      const target = yield* seedSession("dedupe-target-7")
      yield* store.insertAllow(sender.id, target.id)
      const tool = yield* S2STool
      const def = yield* tool.init()

      yield* def.execute({ command: "msg", target: target.id, body: "fills-cap" }, ctxFor(sender.id))
      // 49 retries with the same key produce 0 additional inbox rows.
      for (let i = 0; i < 49; i++) {
        const retry = yield* def.execute({ command: "msg", target: target.id, body: "fills-cap" }, ctxFor(sender.id))
        expect(retry.output).toContain("Already sent within the last 10 minutes")
      }
      const rows = yield* store.claimForSessions([target.id])
      expect(rows).toHaveLength(1)
      yield* store.deleteInbox(rows[0]!.id)
    }),
  )

  it.instance("an expired dedupe key is pruned opportunistically during a fresh send", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sender = yield* seedSession("dedupe-sender-8")
      const target = yield* seedSession("dedupe-target-8")

      // Two sends straddling the window: first at t1, second at
      // t1 + window + 1. The opportunistic prune inside the second
      // transaction must delete the first s2s_sent row, leaving no
      // stale entries for the same key.
      const t1 = 1_700_000_000_000
      const t2 = t1 + DEDUPE_WINDOW_MS + 1
      yield* tryEnqueue({
        store,
        sender: sender.id,
        target: target.id,
        fromSlug: sender.slug,
        capsuleId: "prune_old",
        body: "pruneable",
        timeCreated: t1,
      })
      yield* tryEnqueue({
        store,
        sender: sender.id,
        target: target.id,
        fromSlug: sender.slug,
        capsuleId: "prune_new",
        body: "pruneable",
        timeCreated: t2,
      })
      // Two inbox rows, two distinct ids.
      const inboxRows = yield* store.claimForSessions([target.id])
      expect(inboxRows.map((r) => r.id).sort()).toEqual(["prune_new", "prune_old"])
    }),
  )
})

// The single-DB tests above all run through one bun:sqlite handle, so
// the Effect runtime serializes them on a single writer. They cannot
// reproduce the cross-process snapshot race the WAL fix protects
// against. The probe below does: two raw bun:sqlite connections to
// the same WAL file racing the same dedupe key. With BEGIN IMMEDIATE
// on both connections, one acquires the write lock first; the second
// blocks (busy_timeout) until the first commits, then unblocks,
// reads the just-committed s2s_sent row, and returns "duplicate"
// instead of failing with SQLITE_BUSY. Without IMMEDIATE (control)
// the same race trips BUSY_SNAPSHOT on the second connection's first
// write — the exact failure mode the store-layer
// `{ behavior: "immediate" }` upgrade closes.
//
// The Effect-layer harness cannot reproduce this: every s2s test
// shares one Database.layerFromPath handle, so concurrent store calls
// serialize on a single writer and never reach the cross-connection
// snapshot branch. Driving bun:sqlite here proves the SQL-level fix
// the `{ behavior: "immediate" }` configuration issues.
import { Database as BunSqlite } from "bun:sqlite"

describe("S2S dedupe BEGIN-IMMEDIATE race across two WAL connections", () => {
  let walDir: string
  let walFile: string
  let conn1: BunSqlite
  let conn2: BunSqlite

  const SCHEMA = `
    CREATE TABLE IF NOT EXISTS s2s_inbox (
      id TEXT PRIMARY KEY,
      target_session_id TEXT NOT NULL,
      from_session_id TEXT,
      from_slug TEXT,
      capsule TEXT NOT NULL,
      drained_at INTEGER,
      time_created INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS s2s_inbox_target ON s2s_inbox (target_session_id, drained_at);
    CREATE TABLE IF NOT EXISTS s2s_sent (
      dedupe_key TEXT PRIMARY KEY,
      recipient_session_id TEXT NOT NULL,
      inbox_id TEXT,
      time_created INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS s2s_sent_recipient ON s2s_sent (recipient_session_id, time_created);
  `

  beforeAll(() => {
    walDir = mkdtempSync(join(tmpdir(), "s2s-dedupe-wal-"))
    walFile = join(walDir, "test.db")
    // Apply schema on the first connection (it owns the migrations).
    const setup = new BunSqlite(walFile)
    setup.exec("PRAGMA journal_mode = WAL")
    setup.exec(SCHEMA)
    setup.close()
    conn1 = new BunSqlite(walFile)
    conn2 = new BunSqlite(walFile)
    conn1.exec("PRAGMA busy_timeout = 5000")
    conn2.exec("PRAGMA busy_timeout = 5000")
  })

  // Each test gets its own freshly truncated inbox + sent tables —
  // the previous test's state must not leak into the control race.
  const resetTables = () => {
    conn1.exec("DELETE FROM s2s_inbox")
    conn1.exec("DELETE FROM s2s_sent")
  }

  afterAll(() => {
    conn1.close()
    conn2.close()
    rmSync(walDir, { recursive: true, force: true })
  })

  // Mirrors S2SStore.tryEnqueueWithDedup's SELECT + INSERT sequence
  // but parameterised on the BEGIN flavor so the same scenario can be
  // re-driven under DEFERRED to prove the failure mode the IMMEDIATE
  // upgrade closes.
  const runRace = (begin: "begin" | "begin immediate", salt: string): { tags: string[]; threw: string | null } => {
    const dedupeKey = `wal-race-${salt}-${Date.now()}-${Math.random()}`
    const now = Date.now()

    const tryInsert = (
      conn: BunSqlite,
      capsuleId: string,
    ): { result: string; inboxId?: string; originalInboxId?: string } => {
      conn.exec(begin)
      try {
        const existing = conn
          .query("SELECT inbox_id FROM s2s_sent WHERE dedupe_key = ? AND time_created > ? LIMIT 1")
          .get(dedupeKey, now - 600_000) as { inbox_id: string | null } | undefined
        if (existing) {
          conn.exec("rollback")
          return { result: "duplicate", originalInboxId: existing.inbox_id ?? capsuleId }
        }
        const count = conn
          .query("SELECT COUNT(*) AS n FROM s2s_inbox WHERE target_session_id = ? AND drained_at IS NULL")
          .get("ses_wal_race_target") as { n: number }
        if (count.n >= 50) {
          conn.exec("rollback")
          return { result: "inbox_full" }
        }
        ;(conn.run as (sql: string, ...params: unknown[]) => unknown)(
          "INSERT INTO s2s_inbox (id, target_session_id, from_session_id, from_slug, capsule, time_created) VALUES (?, ?, ?, ?, ?, ?)",
          capsuleId,
          "ses_wal_race_target",
          "ses_wal_race_sender",
          "wal-race",
          "{}",
          now,
        )
        ;(conn.run as (sql: string, ...params: unknown[]) => unknown)(
          "INSERT INTO s2s_sent (dedupe_key, recipient_session_id, inbox_id, time_created) VALUES (?, ?, ?, ?)",
          dedupeKey,
          "ses_wal_race_target",
          capsuleId,
          now,
        )
        conn.exec("commit")
        return { result: "inserted", inboxId: capsuleId }
      } catch (err) {
        try {
          conn.exec("rollback")
        } catch {}
        throw err
      }
    }

    // Drive conn1 first (acquires the write lock immediately under
    // IMMEDIATE). Hold the lock until conn1 commits. Then conn2's
    // BEGIN returns, the dedupe-check SELECT reads the committed row,
    // and we exit as `duplicate`.
    let threw: string | null = null
    let resultA: ReturnType<typeof tryInsert> | null = null
    let resultB: ReturnType<typeof tryInsert> | null = null
    try {
      resultA = tryInsert(conn1, "capsule-aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa")
      resultB = tryInsert(conn2, "capsule-bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb")
    } catch (err) {
      threw = err instanceof Error ? err.message : String(err)
    }
    const tags: string[] = []
    if (resultA) tags.push(resultA.result)
    if (resultB) tags.push(resultB.result)
    return { tags, threw }
  }

  test("BEGIN IMMEDIATE on both connections serializes the race: one inserted, one duplicate", () => {
    resetTables()
    const { tags, threw } = runRace("begin immediate", "happy")
    expect(threw).toBeNull()
    expect(tags.sort()).toEqual(["duplicate", "inserted"])
  })

  test("control: BEGIN DEFERRED on conn2 surfaces the SQLITE_BUSY / BUSY_SNAPSHOT the store-layer IMMEDIATE upgrade closes", () => {
    resetTables()
    // The reviewer race that motivated the IMMEDIATE upgrade: conn2
    // starts BEGIN DEFERRED first and reads a pre-insert state, then
    // conn1 commits a matching s2s_sent row, then conn2's first
    // write hits SQLITE_BUSY / BUSY_SNAPSHOT — the failure mode
    // `{ behavior: "immediate" }` prevents by acquiring the lock
    // up front so the dedupe check sees the committed row.
    const dedupeKey = `wal-race-control-${Date.now()}`
    const now = Date.now()
    const capsuleId = "capsule-cccccccc-cccc-7ccc-8ccc-cccccccccccc"

    // T2 starts first under DEFERRED, reads (empty), then T1 commits.
    conn2.exec("begin")
    const r2 = conn2
      .query("SELECT inbox_id FROM s2s_sent WHERE dedupe_key = ? AND time_created > ? LIMIT 1")
      .get(dedupeKey, now - 600_000)
    expect(r2).toBeNull() // stale snapshot pre-dates T1's commit

    // T1 wins.
    conn1.exec("begin immediate")
    ;(conn1.run as (sql: string, ...params: unknown[]) => unknown)(
      "INSERT INTO s2s_inbox (id, target_session_id, from_session_id, from_slug, capsule, time_created) VALUES (?, ?, ?, ?, ?, ?)",
      capsuleId,
      "ses_wal_race_target",
      "ses_wal_race_sender",
      "wal-race",
      "{}",
      now,
    )
    ;(conn1.run as (sql: string, ...params: unknown[]) => unknown)(
      "INSERT INTO s2s_sent (dedupe_key, recipient_session_id, inbox_id, time_created) VALUES (?, ?, ?, ?)",
      dedupeKey,
      "ses_wal_race_target",
      capsuleId,
      now,
    )
    conn1.exec("commit")

    // T2 tries to write under its stale snapshot — must fail.
    let busy: unknown = null
    try {
      ;(conn2.run as (sql: string, ...params: unknown[]) => unknown)(
        "DELETE FROM s2s_sent WHERE dedupe_key = ?",
        dedupeKey,
      )
    } catch (e) {
      busy = e
    } finally {
      try {
        conn2.exec("rollback")
      } catch {}
    }
    expect(busy).not.toBeNull()
    expect((busy as Error).message).toMatch(/database is locked|busy/i)
  })
})
