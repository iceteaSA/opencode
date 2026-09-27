// Session-to-Session — Task 2 (store CRUD test).
//
// The store is a thin SQL layer over the s2s_* tables added in
// `packages/core/src/database/migration/20260616101412_s2s_tables.ts`.
// This test exercises every public method against a real in-memory
// `Database.Service` so the multi-statement claim+accept transactions
// (the cross-process safety boundary) run on actual SQLite, not mocks.
//
// Mirrors the shared-`:memory:` + `Database.layerFromPath` pattern used by
// `packages/core/test/move-session.test.ts` and `credential.test.ts`:
// the database layer is a module-level constant so every test inside
// this file shares one in-memory instance (Bun's `Database(":memory:")`
// creates a new in-memory DB per native handle — sharing the layer
// guarantees all `Database.Service` consumers see the same handle and
// therefore the same set of migrations).

import { describe, expect } from "bun:test"
import { sql } from "drizzle-orm"
import { Effect, Layer, Option } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { S2SStore } from "../../src/s2s/store"
import { SessionID } from "../../src/session/schema"
import { testEffect } from "../lib/effect"
import { claimLegacy } from './fixtures/legacy-claim';
import { countUndelivered } from './fixtures/undelivered-count';

const database = Database.layerFromPath(":memory:")
const it = testEffect(S2SStore.layer.pipe(Layer.provideMerge(database)))

// Two valid arbitrary session ids for table-row targets.
const S1 = SessionID.make("ses_target_alpha")
const S2 = SessionID.make("ses_target_beta")
const S3 = SessionID.make("ses_target_gamma")
const S4 = SessionID.make("ses_target_delta")
const INVITER = SessionID.make("ses_inviter_one")
const JOINER = SessionID.make("ses_joiner_one")

describe("S2SStore", () => {
  it.effect("pendingTargets returns distinct unclaimed local targets only", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      yield* Effect.forEach(
        [
          { id: "inb_pending_first", targetSessionID: S1 },
          { id: "inb_pending_second", targetSessionID: S1 },
          { id: "inb_claimed", targetSessionID: S2 },
          { id: "inb_other", targetSessionID: S3 },
        ],
        (row) => store.insertInbox({ ...row, fromSessionID: INVITER, fromSlug: "peer", capsule: "x", timeCreated: 1 }),
      )
      yield* claimLegacy([S2])

      expect(yield* store.pendingTargets([S1, S2])).toEqual([S1])
      expect(yield* store.pendingTargets([])).toEqual([])
    }),
  )
  it.effect("adoptLegacy moves one pending id into the canonical queue", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertInbox({
        id: "inb_1",
        targetSessionID: S1,
        fromSessionID: INVITER,
        fromSlug: "inviter",
        capsule: '{"body":"hi"}',
        timeCreated: 1_700_000_000_000,
      })

      expect((yield* store.pendingLegacyForSession(S1)).map((row) => row.id)).toContain("inb_1")
      const first = yield* store.adoptLegacy("inb_1")
      expect(first).toMatchObject({ id: "inb_1", targetSessionID: S1, fromSessionID: INVITER, fromSlug: "inviter", capsule: '{"body":"hi"}' })
      expect(yield* store.adoptLegacy("inb_1")).toBeUndefined()
      expect((yield* store.pendingForSession(S1, Date.now())).map((row) => row.id)).toContain("inb_1")
    }),
  )

  it.effect("pendingLegacyForSession scopes legacy rows to the recipient", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertInbox({
        id: "inb_scoped",
        targetSessionID: S1,
        fromSessionID: INVITER,
        fromSlug: "inviter",
        capsule: "x",
        timeCreated: 1,
      })

      expect((yield* store.pendingLegacyForSession(S2)).map((row) => row.id)).not.toContain("inb_scoped")
      expect((yield* store.pendingLegacyForSession(S1)).map((row) => row.id)).toContain("inb_scoped")
    }),
  )

  it.effect("reapStale resets a stale claim so a follow-up claim succeeds", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertInbox({
        id: "inb_stale",
        targetSessionID: S1,
        fromSessionID: INVITER,
        fromSlug: "inviter",
        capsule: "x",
        timeCreated: 1,
      })

      const claimed = yield* claimLegacy([S1])
      expect(claimed).toHaveLength(1)

      // Immediately after, the claim is held — nothing to drain.
      const stillHeld = yield* claimLegacy([S1])
      expect(stillHeld).toEqual([])

      // Reap everything older than now+1s. The previous claim's drained_at
      // is approximately Date.now() (very small), so reaping at now+1s
      // captures it and reopens the row.
      yield* store.reapStale(Date.now() + 1_000)

      const reclaimed = yield* claimLegacy([S1])
      expect(reclaimed.map((r) => r.id)).toEqual(["inb_stale"])
    }),
  )

  it.effect("a deleted legacy row is NOT redelivered by reapStale", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service

      yield* store.insertInbox({
        id: "inb_delivered",
        targetSessionID: S1,
        fromSessionID: INVITER,
        fromSlug: "inviter",
        capsule: "x",
        timeCreated: 1,
      })

      // An older process could delete its own completed legacy claim before the new owner adopts it.
      const claimed = yield* claimLegacy([S1])
      expect(claimed.map((r) => r.id)).toEqual(["inb_delivered"])
      yield* db.run(sql`DELETE FROM s2s_inbox WHERE id = 'inb_delivered'`)

      // The reaper must not resurrect a completed legacy claim.
      yield* store.reapStale(Date.now() + 1_000_000)
      const afterReap = yield* claimLegacy([S1])
      expect(afterReap).toEqual([])

      // The shared sender cap also reflects the legacy deletion.
      expect(yield* countUndelivered(S1)).toBe(0)
    }),
  )

  it.effect("reapStale STILL redelivers a crashed claim (claimed, never deleted)", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertInbox({
        id: "inb_crashed",
        targetSessionID: S2,
        fromSessionID: INVITER,
        fromSlug: "inviter",
        capsule: "x",
        timeCreated: 1,
      })

      // An old process that crashes after claiming leaves a row for the new owner to adopt.
      yield* claimLegacy([S2])
      yield* store.reapStale(Date.now() + 1_000_000)
      const reclaimed = yield* claimLegacy([S2])
      expect(reclaimed.map((r) => r.id)).toEqual(["inb_crashed"])
    }),
  )

  it.effect("claimToken accepts a single use and rejects a second claim", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertToken({
        token: "tok_abc",
        inviterSessionID: INVITER,
        inviterSlug: "inviter",
        createdAt: Date.now(),
      })

      const first = yield* store.claimToken("tok_abc", JOINER)
      expect(Option.isSome(first)).toBe(true)
      if (Option.isSome(first)) {
        expect(first.value.token).toBe("tok_abc")
        expect(first.value.inviterSessionID).toBe(INVITER)
        expect(first.value.inviterSlug).toBe("inviter")
      }

      const second = yield* store.claimToken("tok_abc", JOINER)
      expect(Option.isNone(second)).toBe(true)
    }),
  )

  it.effect("allow list is directional: insertAllow(a,b) does not imply isAllowed(b,a)", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertAllow(S1, S2)

      expect(yield* store.isAllowed(S1, S2)).toBe(true)
      expect(yield* store.isAllowed(S2, S1)).toBe(false)
    }),
  )

  it.effect("listAllows returns every inbound and outbound row for a session", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertAllow(S3, S4)
      yield* store.insertAllow(S4, S3)
      yield* store.insertAllow(S2, S3)

      const rows = yield* store.listAllows(S3)
      expect(rows).toHaveLength(3)
      expect(rows).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ sessionID: S3, allowedSessionID: S4 }),
          expect.objectContaining({ sessionID: S4, allowedSessionID: S3 }),
          expect.objectContaining({ sessionID: S2, allowedSessionID: S3 }),
        ]),
      )
    }),
  )

  it.effect("deleteAllow removes a previously-allowed pair", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service

      yield* store.insertAllow(S1, S2)
      expect(yield* store.isAllowed(S1, S2)).toBe(true)

      yield* store.deleteAllow(S1, S2)
      expect(yield* store.isAllowed(S1, S2)).toBe(false)
    }),
  )

  it.effect("the cap and visible count include the same pending canonical and undrained legacy rows", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const recipient = SessionID.make("ses_cap_parity_target")
      const sender = SessionID.make("ses_cap_parity_sender")
      yield* db.run(sql`PRAGMA foreign_keys = OFF`)
      yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (${recipient}, 'prj_cap', 'cap-target', '/tmp', 'Recipient', '1', 1, 1), (${sender}, 'prj_cap', 'cap-sender', '/tmp', 'Sender', '1', 1, 1)`)
      yield* db.run(sql`PRAGMA foreign_keys = ON`)
      yield* store.insertInbox({ id: "inb_cap_legacy", targetSessionID: recipient, fromSessionID: sender, fromSlug: "sender", capsule: "{}", timeCreated: 1 })
      const send = (id: string) => store.tryEnqueueWithDedup({
        dedupeKey: `key_${id}`, sender, target: recipient, fromSlug: "sender", capsule: "{}",
        capsuleId: id, timeCreated: Date.now(), windowMs: 600_000, inboxCap: 2,
      })
      expect(yield* countUndelivered(recipient)).toBe(1)
      expect((yield* send("caps_cap_first"))._tag).toBe("inserted")
      expect(yield* countUndelivered(recipient)).toBe(2)
      expect((yield* send("caps_cap_second"))._tag).toBe("inbox_full")
      yield* claimLegacy([recipient])
      expect(yield* countUndelivered(recipient)).toBe(1)
      expect((yield* send("caps_cap_second"))._tag).toBe("inserted")
      expect(yield* countUndelivered(recipient)).toBe(2)
    }),
  )
})
