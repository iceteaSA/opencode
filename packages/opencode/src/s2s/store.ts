// Session-to-Session — Task 2 (store CRUD).
//
// Thin SQL layer over the s2s_inbox / s2s_token / s2s_allow tables added
// in `packages/core/src/database/migration/20260616101412_s2s_tables.ts`
// (with the Drizzle schema mirror in
// `packages/core/src/database/s2s.sql.ts` so the codegen pipeline keeps
// `schema.gen.ts` in sync for fresh-DB setups).
//
// The store is deliberately small: every method is a single SQL statement
// (or a single UPDATE…RETURNING) so the cross-process safety contract
// reduces to the database's own atomicity + WAL. No in-process cache, no
// fan-out — when a future Task wires the store into a wakeup loop the
// only thing that matters is that draining/accepting from a row is atomic
// under multi-process contention.
//
// Design notes:
//   * `claimForSessions` uses a single `UPDATE…RETURNING` with a
//     `drained_at IS NULL` guard so two concurrent claimers racing on
//     the same row see exactly one claim succeed. `db.all` on the bun
//     stack surfaces the RETURNING rows (verified at runtime in Task 2
//     setup; see `database-migration.test.ts` and the smoke probe we
//     ran before writing the implementation).
//   * `claimToken` does the same trick on `s2s_token` using
//     `accepted_by IS NULL` as the guard. A second call on an already-
//     accepted token returns None (the RETURNING array is empty).
//   * `s2s_allow` is directional: the composite PK is
//     (session_id, allowed_session_id), so "is X allowed to talk to Y?"
//     is a one-row `SELECT 1 … LIMIT 1`; the reverse direction is its
//     own row (or non-existent).

import { sql } from "drizzle-orm"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Context, Duration, Effect, Layer, Option, Schedule, Schema } from "effect"
import { Cause } from "effect"
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError"
import { Database } from "@opencode-ai/core/database/database"
import { SessionID } from "@/session/schema"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

// 10 minutes. Token TTL is enforced at the store layer so an expired
// token is never atomically consumed (the UPDATE WHERE clause includes
// the TTL guard). The caller-facing error message references this
// constant for display.
export const TOKEN_TTL_MS = 600_000

// 10 minutes. Insert-time dedupe window for cross-process `s2s msg`.
// The same body to the same recipient within this window is rejected
// as a duplicate, making the tool safe to retry after a lost tool-
// result return without redelivering to the recipient.
export const DEDUPE_WINDOW_MS = 600_000

// Per-call cap on rows pruned from s2s_sent by the dedupe transaction.
// Bounded so a backlog cannot stall a single send; full-table cleanup
// is a separate concern (the table is also pruned opportunistically
// at every send, so growth is bounded by send rate × window).
const DEDUPE_PRUNE_BATCH = 100

// ──────────────────────────────────────────────────────────────────
// Retry helpers — exported for unit-testing so concurrency behavior
// can be verified without real database locking.
// ──────────────────────────────────────────────────────────────────

/**
 * Walks the Error → EffectDrizzleQueryError → Cause → SqlError chain
 * to extract `sqlError.reason.isRetryable`. Returns `false` on any
 * value it cannot classify.
 */
export function isRetryableSqlError(err: unknown): boolean {
  if (!(err instanceof EffectDrizzleQueryError)) return false
  const inner = Cause.findErrorOption(err.cause as Cause.Cause<unknown>)
  if (Option.isNone(inner)) return false
  if (!isSqlError(inner.value)) return false
  return inner.value.reason.isRetryable === true
}

const RETRY_MAX = 4
const RETRY_BASE_MS = 20

/**
 * Applies a bounded exponential-backoff retry that stops on errors that
 * are not cross-process SQLite lock-timeout / deadlock / serialization
 * failures.
 *
 * The retry runs the RAW database effect — BEFORE the `query` helper
 * remaps it to `S2SStoreError` — so the predicate sees the original
 * drizzle + SqlError chain.
 */
export function retryOnBusy<A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> {
  return Effect.retry(effect, {
    while: (err) => isRetryableSqlError(err),
    times: RETRY_MAX,
    schedule: Schedule.jittered(Schedule.exponential(Duration.millis(RETRY_BASE_MS))),
  }) as unknown as Effect.Effect<A, E>
}

export class S2SStoreError extends Schema.TaggedErrorClass<S2SStoreError>()("S2SStore.Error", {
  message: Schema.String,
  cause: Schema.Unknown,
}) {}

// Outcome of `tryEnqueueWithDedup`. The store returns a tagged result
// rather than throwing so the tool layer can branch on the duplicate
// path (which is a normal success result) without an error-channel
// detour. `originalInboxId` is the inbox id the FIRST send produced; on
// a successful insert it equals the freshly minted `inboxId`.
export type EnqueueResult =
  | { _tag: "inserted"; inboxId: string }
  | { _tag: "duplicate"; originalInboxId: string }
  | { _tag: "inbox_full" }

export interface InboxRow {
  id: string
  targetSessionID: SessionID
  fromSessionID: SessionID | null
  fromSlug: string | null
  capsule: string
  timeCreated: number
}

export interface NewInboxRow {
  id: string
  targetSessionID: SessionID
  fromSessionID: SessionID | null
  fromSlug: string | null
  capsule: string
  timeCreated: number
}

export interface TokenRow {
  token: string
  inviterSessionID: SessionID
  inviterSlug: string
  createdAt: number
}

export interface NewTokenRow {
  token: string
  inviterSessionID: SessionID
  inviterSlug: string
  createdAt: number
}

export interface AllowRow {
  sessionID: SessionID
  allowedSessionID: SessionID
  establishedAt: number
}

export interface Interface {
  readonly insertInbox: (row: NewInboxRow) => Effect.Effect<void, S2SStoreError>
  // Atomic cross-process send: dedup check + INBOX_CAP check + inbox
  // insert + dedup-record insert in ONE transaction (Bun's SQLite WAL
  // serializes writers, so two concurrent sends racing on the same key
  // see exactly one `inserted` and the rest `duplicate`).
  readonly tryEnqueueWithDedup: (input: {
    dedupeKey: string
    sender: SessionID
    target: SessionID
    fromSlug: string | null
    capsule: string
    capsuleId: string
    timeCreated: number
    windowMs: number
    inboxCap: number
  }) => Effect.Effect<EnqueueResult, S2SStoreError>
  readonly claimForSessions: (ids: ReadonlyArray<SessionID>) => Effect.Effect<InboxRow[], S2SStoreError>
  readonly pendingTargets: (ids: ReadonlyArray<SessionID>) => Effect.Effect<SessionID[], S2SStoreError>
  readonly deleteInbox: (id: string) => Effect.Effect<void, S2SStoreError>
  readonly reapStale: (olderThan: number) => Effect.Effect<void, S2SStoreError>
  readonly countUndelivered: (target: SessionID) => Effect.Effect<number, S2SStoreError>
  readonly insertToken: (row: NewTokenRow) => Effect.Effect<void, S2SStoreError>
  readonly claimToken: (token: string, by: SessionID) => Effect.Effect<Option.Option<TokenRow>, S2SStoreError>
  readonly insertAllow: (from: SessionID, to: SessionID) => Effect.Effect<void, S2SStoreError>
  readonly listAllows: (me: SessionID) => Effect.Effect<AllowRow[], S2SStoreError>
  readonly isAllowed: (from: SessionID, to: SessionID) => Effect.Effect<boolean, S2SStoreError>
  readonly deleteAllow: (from: SessionID, to: SessionID) => Effect.Effect<void, S2SStoreError>
  readonly deleteOrphaned: () => Effect.Effect<void, S2SStoreError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/S2SStore") {}

interface InboxDbRow {
  id: string
  target_session_id: string
  from_session_id: string | null
  from_slug: string | null
  capsule: string
  time_created: number
}

interface TokenDbRow {
  token: string
  inviter_session_id: string
  inviter_slug: string
  created_at: number
}

interface AllowDbRow {
  session_id: string
  allowed_session_id: string
  established_at: number
}

function toInboxRow(row: InboxDbRow): InboxRow {
  return {
    id: row.id,
    targetSessionID: SessionID.make(row.target_session_id),
    fromSessionID: row.from_session_id === null ? null : SessionID.make(row.from_session_id),
    fromSlug: row.from_slug,
    capsule: row.capsule,
    timeCreated: row.time_created,
  }
}

function toTokenRow(row: TokenDbRow): TokenRow {
  return {
    token: row.token,
    inviterSessionID: SessionID.make(row.inviter_session_id),
    inviterSlug: row.inviter_slug,
    createdAt: row.created_at,
  }
}

function toAllowRow(row: AllowDbRow): AllowRow {
  return {
    sessionID: SessionID.make(row.session_id),
    allowedSessionID: SessionID.make(row.allowed_session_id),
    establishedAt: row.established_at,
  }
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service

    // Wrap each SQL call so the public Interface exposes a single
    // S2SStoreError rather than the raw EffectDrizzleQueryError union.
    // Mirrors the `query` helper in `account/repo.ts`.
    const query = <A, E>(effect: Effect.Effect<A, E>) =>
      effect.pipe(Effect.mapError((cause) => new S2SStoreError({ message: "Database operation failed", cause })))

    const insertInbox: Interface["insertInbox"] = Effect.fn("S2SStore.insertInbox")(function* (row) {
      yield* query(
        db.run(sql`
          INSERT INTO s2s_inbox (id, target_session_id, from_session_id, from_slug, capsule, time_created)
          VALUES (${row.id}, ${row.targetSessionID}, ${row.fromSessionID}, ${row.fromSlug}, ${row.capsule}, ${row.timeCreated})
        `),
      )
    })

    // Atomic dedupe + insert. The transaction holds a write lock on
    // s2s_inbox and s2s_sent for its full duration, so concurrent
    // fibers racing on the same dedupe_key see one `inserted` and the
    // rest `duplicate` — even when the original row has already been
    // delivered and deleted from s2s_inbox, the s2s_sent row outlives
    // it. The inbox_cap check is inside the transaction so two
    // concurrent sends at the cap cannot both pass the check.
    const tryEnqueueWithDedup: Interface["tryEnqueueWithDedup"] = Effect.fn("S2SStore.tryEnqueueWithDedup")(
      function* (input) {
        const cutoff = input.timeCreated - input.windowMs
        const result = yield* query(
          db.transaction(
            (tx) =>
              Effect.gen(function* () {
                const existing = yield* tx.get<{ inbox_id: string | null }>(sql`
                  SELECT inbox_id FROM s2s_sent
                  WHERE dedupe_key = ${input.dedupeKey}
                    AND time_created > ${cutoff}
                  LIMIT 1
                `)
                if (existing) {
                  return {
                    _tag: "duplicate" as const,
                    originalInboxId: existing.inbox_id ?? input.capsuleId,
                  }
                }
                const row = yield* tx.get<{ n: number }>(sql`
                  SELECT COUNT(*) AS n FROM s2s_inbox
                  WHERE target_session_id = ${input.target} AND drained_at IS NULL
                `)
                if ((row?.n ?? 0) >= input.inboxCap) {
                  return { _tag: "inbox_full" as const }
                }
                // Opportunistically prune expired rows. Bounded by
                // DEDUPE_PRUNE_BATCH so a backlog cannot stall a send.
                yield* tx.run(sql`
                  DELETE FROM s2s_sent
                  WHERE time_created <= ${cutoff}
                  ORDER BY time_created ASC
                  LIMIT ${DEDUPE_PRUNE_BATCH}
                `)
                // If the dedupe key existed but was expired (the SELECT
                // above filtered it out), its row is now gone — but a
                // concurrent send at the same key may have inserted one
                // between our SELECT and DELETE; remove any stale match
                // explicitly so the PK INSERT below cannot collide.
                yield* tx.run(sql`
                  DELETE FROM s2s_sent WHERE dedupe_key = ${input.dedupeKey}
                `)
                yield* tx.run(sql`
                  INSERT INTO s2s_inbox (id, target_session_id, from_session_id, from_slug, capsule, time_created)
                  VALUES (${input.capsuleId}, ${input.target}, ${input.sender}, ${input.fromSlug}, ${input.capsule}, ${input.timeCreated})
                `)
                yield* tx.run(sql`
                  INSERT INTO s2s_sent (dedupe_key, recipient_session_id, inbox_id, time_created)
                  VALUES (${input.dedupeKey}, ${input.target}, ${input.capsuleId}, ${input.timeCreated})
                `)
                return { _tag: "inserted" as const, inboxId: input.capsuleId }
              }),
            { behavior: "immediate" },
          ),
        )
        return result
      },
    )

    const claimForSessions: Interface["claimForSessions"] = Effect.fn("S2SStore.claimForSessions")(function* (ids) {
      if (ids.length === 0) return []
      const claimed = yield* query(
        db.all<InboxDbRow>(sql`
            UPDATE s2s_inbox
            SET drained_at = ${Date.now()}
            WHERE target_session_id IN (${sql.join(
              ids.map((id) => sql`${id}`),
              sql`, `,
            )})
              AND drained_at IS NULL
            RETURNING id, target_session_id, from_session_id, from_slug, capsule, time_created
          `),
      )
      return claimed.map(toInboxRow)
    })

    const pendingTargets: Interface["pendingTargets"] = Effect.fn("S2SStore.pendingTargets")(function* (ids) {
      if (ids.length === 0) return []
      const rows = yield* query(
        db.all<{ target: string }>(sql`
          SELECT DISTINCT target_session_id AS target FROM s2s_inbox
          WHERE target_session_id IN (${sql.join(
            ids.map((id) => sql`${id}`),
            sql`, `,
          )})
            AND drained_at IS NULL
        `),
      )
      return rows.map((row) => SessionID.make(row.target))
    })

    // Hard-delete a row once it has been successfully delivered into the
    // recipient's in-process inbox. This is what makes a *claimed* row
    // (drained_at set) distinct from a *delivered* row (gone): the reaper
    // only ever sees rows that were claimed but whose delivering fiber died
    // before deleting them, so it correctly redelivers only crashed claims —
    // never a row that already reached the recipient.
    const deleteInbox: Interface["deleteInbox"] = Effect.fn("S2SStore.deleteInbox")(function* (id) {
      yield* query(db.run(sql`DELETE FROM s2s_inbox WHERE id = ${id}`))
    })

    // Resets the claim on rows that were claimed (drained_at set) but never
    // deleted — i.e. the delivering fiber crashed between claim and
    // deleteInbox. A successfully delivered row is hard-deleted, so it is
    // NOT visible here and is never redelivered. (Before deleteInbox existed
    // this reset every delivered row, causing endless redelivery.)
    const reapStale: Interface["reapStale"] = Effect.fn("S2SStore.reapStale")(function* (olderThan) {
      yield* query(
        db.run(sql`
          UPDATE s2s_inbox SET drained_at = NULL
          WHERE drained_at IS NOT NULL AND drained_at < ${olderThan}
        `),
      )
    })

    const countUndelivered: Interface["countUndelivered"] = Effect.fn("S2SStore.countUndelivered")(function* (target) {
      const row = yield* query(
        db.get<{ n: number }>(sql`
            SELECT COUNT(*) AS n FROM s2s_inbox
            WHERE target_session_id = ${target} AND drained_at IS NULL
          `),
      )
      return row?.n ?? 0
    })

    const insertToken: Interface["insertToken"] = Effect.fn("S2SStore.insertToken")(function* (row) {
      yield* query(
        db.run(sql`
          INSERT INTO s2s_token (token, inviter_session_id, inviter_slug, created_at)
          VALUES (${row.token}, ${row.inviterSessionID}, ${row.inviterSlug}, ${row.createdAt})
        `),
      )
    })

    const claimToken: Interface["claimToken"] = Effect.fn("S2SStore.claimToken")(function* (token, by) {
      const minCreatedAt = Date.now() - TOKEN_TTL_MS
      const rows = yield* query(
        db.all<TokenDbRow>(sql`
          UPDATE s2s_token SET accepted_by = ${by}, accepted_at = ${Date.now()}
          WHERE token = ${token} AND accepted_by IS NULL AND created_at > ${minCreatedAt}
          RETURNING token, inviter_session_id, inviter_slug, created_at
        `),
      )
      return rows.length === 0 ? Option.none<TokenRow>() : Option.some(toTokenRow(rows[0]!))
    })

    const insertAllow: Interface["insertAllow"] = Effect.fn("S2SStore.insertAllow")(function* (from, to) {
      yield* query(
        db.run(sql`
          INSERT OR IGNORE INTO s2s_allow (session_id, allowed_session_id, established_at)
          VALUES (${from}, ${to}, ${Date.now()})
        `),
      )
    })

    const listAllows: Interface["listAllows"] = Effect.fn("S2SStore.listAllows")(function* (me) {
      const rows = yield* query(
        db.all<AllowDbRow>(sql`
          SELECT session_id, allowed_session_id, established_at FROM s2s_allow
          WHERE session_id = ${me} OR allowed_session_id = ${me}
        `),
      )
      return rows.map(toAllowRow)
    })

    const isAllowed: Interface["isAllowed"] = Effect.fn("S2SStore.isAllowed")(function* (from, to) {
      const row = yield* query(
        db.get<{ present: number }>(sql`
          SELECT 1 AS present FROM s2s_allow
          WHERE session_id = ${from} AND allowed_session_id = ${to}
          LIMIT 1
        `),
      )
      return row !== undefined
    })

    const deleteAllow: Interface["deleteAllow"] = Effect.fn("S2SStore.deleteAllow")(function* (from, to) {
      yield* query(
        db.run(sql`
          DELETE FROM s2s_allow
          WHERE session_id = ${from} AND allowed_session_id = ${to}
        `),
      )
    })

    // Removes s2s rows whose target / owner / inviter no longer exists in
    // the session table. A session row is always created BEFORE any s2s row
    // can reference it (synchronous at session creation), so NOT IN cannot
    // race-delete rows that belong to a live, newly created session.
    const deleteOrphaned: Interface["deleteOrphaned"] = Effect.fn("S2SStore.deleteOrphaned")(function* () {
      yield* query(
        db.run(sql`
            DELETE FROM s2s_inbox
            WHERE target_session_id NOT IN (SELECT id FROM session)
          `),
      )
      yield* query(
        db.run(sql`
            DELETE FROM s2s_allow
            WHERE session_id NOT IN (SELECT id FROM session)
               OR allowed_session_id NOT IN (SELECT id FROM session)
          `),
      )
      yield* query(
        db.run(sql`
            DELETE FROM s2s_token
            WHERE inviter_session_id NOT IN (SELECT id FROM session)
          `),
      )
    })

    return {
      insertInbox,
      tryEnqueueWithDedup,
      claimForSessions,
      pendingTargets,
      deleteInbox,
      reapStale,
      countUndelivered,
      insertToken,
      claimToken,
      insertAllow,
      listAllows,
      isAllowed,
      deleteAllow,
      deleteOrphaned,
    } satisfies Interface
  }),
)

// The store has no upstream dependencies beyond `Database.Service`, which
// AppLayer already provides. `node` is exported so the S2S wiring step
// (later task) can splice it into the graph without re-deriving the
// dependency list.
export const node = LayerNode.make({ service: Service, layer, deps: [Database.node] })
export const defaultLayer = layer

export * as S2SStore from "./store"
