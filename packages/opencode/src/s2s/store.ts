// The canonical S2S row survives delivery. Legacy inbox rows are adopted by
// their owner under a guarded transaction; transcript events precede a retryable receipt mark.

import { sql } from "drizzle-orm"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { Context, Duration, Effect, Layer, Option, Schedule, Schema } from "effect"
import { Cause } from "effect"
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError"
import { Database } from "@opencode-ai/core/database/database"
import { KeyedMutex } from "@opencode-ai/core/effect/keyed-mutex"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Session } from "@/session/session"
import { MessageID, SessionID } from "@/session/schema"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

// 10 minutes. Token TTL is enforced at the store layer so an expired
// token is never atomically consumed (the UPDATE WHERE clause includes
// the TTL guard). The caller-facing error message references this
// constant for display.
export const TOKEN_TTL_MS = 600_000

// 10 minutes. Insert-time dedupe window for local and cross-process `s2s msg`.
// The same body to the same recipient within this window is rejected
// as a duplicate, making the tool safe to retry after a lost tool-
// result return without redelivering to the recipient.
export const DEDUPE_WINDOW_MS = 600_000

export const PRESENCE_TTL_MS = 15_000
export const PROCESS_OWNER_ID = crypto.randomUUID()

// Per-call cap on rows pruned from s2s_sent by the dedupe transaction.
// Bounded so a backlog cannot stall a single send; full-table cleanup
// is a separate concern (the table is also pruned opportunistically
// at every send, so growth is bounded by send rate × window).
const DEDUPE_PRUNE_BATCH = 100
const receiptLocks = KeyedMutex.makeUnsafe<string>()

export const undeliveredCount = (target: SessionID) => sql`
  (SELECT COUNT(*) FROM s2s_message WHERE target_session_id = ${target}
    AND delivered_at IS NULL AND expired_at IS NULL AND superseded_at IS NULL) +
  (SELECT COUNT(*) FROM s2s_inbox WHERE target_session_id = ${target} AND drained_at IS NULL)
`

const dueBy = (now: number) => sql`(deliver_at IS NULL OR deliver_at <= ${now})`

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
  | {
      _tag: "inserted"
      inboxId: string
      sentAt: number
      supersession?: "superseded" | "already_delivered" | "in_delivery"
    }
  | { _tag: "duplicate"; originalInboxId: string; originalDueAt: number }
  | { _tag: "inbox_full" }
  | { _tag: "invalid_supersedes" }

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

export interface MessageRow {
  id: string
  targetSessionID: SessionID
  fromSessionID: SessionID
  fromSlug: string
  capsule: string
  sentAt: number
  expiresAt: number | null
  supersedes: string | null
}

export type PeerCapability = { state: "unknown" } | { state: "incompatible" | "current"; version: number }

export interface SentRow {
  id: string
  target: SessionID
  sentAt: number
  deliveredAt: number | null
  state: "pending" | "delivered" | "expired" | "superseded"
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
  // Atomic send: dedup check + INBOX_CAP check + canonical message
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
    expiresAt?: number
    deliverAt?: number
    supersedes?: string
    windowMs: number
    inboxCap: number
  }) => Effect.Effect<EnqueueResult, S2SStoreError>
  readonly pendingForSession: (target: SessionID, now: number) => Effect.Effect<MessageRow[], S2SStoreError>
  readonly resolvePending: (
    id: string,
    target: SessionID,
    now: number,
  ) => Effect.Effect<"pending" | "expired" | "superseded" | "delivered" | "missing", S2SStoreError>
  readonly resolvePendingForSession: (target: SessionID, now: number) => Effect.Effect<string[], S2SStoreError>
  readonly pendingLegacyForSession: (target: SessionID) => Effect.Effect<InboxRow[], S2SStoreError>
  readonly adoptLegacy: (id: string) => Effect.Effect<MessageRow | undefined, S2SStoreError>
  readonly receipt: (input: {
    id: string
    target: SessionID
    buildTranscript: (created: number) => { message: SessionV1.User; parts: SessionV1.TextPart[] }
    deliveredAt: number
    sessions: Session.Interface
  }) => Effect.Effect<boolean, S2SStoreError>
  readonly peerCapability: (target: SessionID, now: number) => Effect.Effect<PeerCapability, S2SStoreError>
  readonly sentHistory: (
    sender: SessionID,
    target?: SessionID,
    limit?: number,
  ) => Effect.Effect<SentRow[], S2SStoreError>
  readonly peerActivity: (
    ids: ReadonlyArray<SessionID>,
    now: number,
  ) => Effect.Effect<Map<SessionID, "running" | "unknown">, S2SStoreError>
  readonly heartbeat: (session: SessionID, owner: string, now: number) => Effect.Effect<void, S2SStoreError>
  readonly claimPresence: (session: SessionID, owner: string, now: number) => Effect.Effect<boolean, S2SStoreError>
  readonly clearPresence: (session: SessionID, owner: string) => Effect.Effect<void, S2SStoreError>
  readonly pendingTargets: (ids: ReadonlyArray<SessionID>, now?: number) => Effect.Effect<SessionID[], S2SStoreError>
  readonly reapStale: (olderThan: number) => Effect.Effect<void, S2SStoreError>
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

interface MessageDbRow {
  id: string
  target_session_id: string
  from_session_id: string
  from_slug: string
  capsule: string
  sent_at: number
  expires_at: number | null
  supersedes: string | null
}

function toMessageRow(row: MessageDbRow): MessageRow {
  return {
    id: row.id,
    targetSessionID: SessionID.make(row.target_session_id),
    fromSessionID: SessionID.make(row.from_session_id),
    fromSlug: row.from_slug,
    capsule: row.capsule,
    sentAt: row.sent_at,
    expiresAt: row.expires_at,
    supersedes: row.supersedes,
  }
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
    // s2s_message and s2s_sent for its full duration, so concurrent
    // fibers racing on the same dedupe_key see one `inserted` and the
    // rest `duplicate` — even when the original row has already been
    // delivered, the s2s_sent row still supports lost-result retries
    // within the window. The inbox_cap check is inside the transaction so two
    // concurrent sends at the cap cannot both pass the check.
    const tryEnqueueWithDedup: Interface["tryEnqueueWithDedup"] = Effect.fn("S2SStore.tryEnqueueWithDedup")(
      function* (input) {
        const cutoff = input.timeCreated - input.windowMs
        const result = yield* query(
          db.transaction(
            (tx) =>
              Effect.gen(function* () {
                const existing = yield* tx.get<{
                  inbox_id: string | null
                  sent_at: number
                  deliver_at: number | null
                }>(sql`
                  SELECT s.inbox_id, COALESCE(m.sent_at, s.time_created) AS sent_at, m.deliver_at
                  FROM s2s_sent s LEFT JOIN s2s_message m ON m.id = s.inbox_id
                  WHERE s.dedupe_key = ${input.dedupeKey}
                    AND s.time_created > ${cutoff}
                  LIMIT 1
                `)
                if (existing) {
                  return {
                    _tag: "duplicate" as const,
                    originalInboxId: existing.inbox_id ?? input.capsuleId,
                    originalDueAt: existing.deliver_at ?? existing.sent_at,
                  }
                }
                const previous = input.supersedes
                  ? yield* tx.get<{
                      from_session_id: string
                      target_session_id: string
                      delivered_at: number | null
                      expired_at: number | null
                      superseded_at: number | null
                    }>(sql`
                  SELECT from_session_id, target_session_id, delivered_at, expired_at, superseded_at
                  FROM s2s_message WHERE id = ${input.supersedes}
                `)
                  : undefined
                if (
                  input.supersedes &&
                  (!previous ||
                    previous.from_session_id !== input.sender ||
                    previous.target_session_id !== input.target ||
                    previous.expired_at !== null ||
                    previous.superseded_at !== null ||
                    input.supersedes === input.capsuleId)
                ) {
                  return { _tag: "invalid_supersedes" as const }
                }
                const row = yield* tx.get<{ n: number }>(sql`SELECT ${undeliveredCount(input.target)} AS n`)
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
                const inDelivery =
                  previous?.delivered_at === null && input.supersedes
                    ? yield* tx.get<{ id: string }>(
                        sql`SELECT id FROM message WHERE id = ${MessageID.make(`msg_${input.supersedes}`)}`,
                      )
                    : undefined
                const supersession =
                  previous?.delivered_at !== null && previous
                    ? ("already_delivered" as const)
                    : inDelivery
                      ? ("in_delivery" as const)
                      : input.supersedes
                        ? ("superseded" as const)
                        : undefined
                if (supersession === "superseded") {
                  yield* tx.run(sql`
                    UPDATE s2s_message SET superseded_at = ${input.timeCreated}
                    WHERE id = ${input.supersedes} AND from_session_id = ${input.sender} AND target_session_id = ${input.target}
                      AND delivered_at IS NULL AND expired_at IS NULL AND superseded_at IS NULL
                      AND NOT EXISTS (SELECT 1 FROM message WHERE id = ${MessageID.make(`msg_${input.supersedes}`)})
                  `)
                }
                yield* tx.run(sql`
                  INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, expires_at, supersedes, deliver_at)
                  VALUES (${input.capsuleId}, ${input.target}, ${input.sender}, ${input.fromSlug}, ${input.capsule}, ${input.timeCreated}, ${input.expiresAt ?? null}, ${supersession === "superseded" ? input.supersedes : null}, ${input.deliverAt ?? null})
                `)
                yield* tx.run(sql`
                  INSERT INTO s2s_sent (dedupe_key, recipient_session_id, inbox_id, time_created)
                  VALUES (${input.dedupeKey}, ${input.target}, ${input.capsuleId}, ${input.timeCreated})
                `)
                return { _tag: "inserted" as const, inboxId: input.capsuleId, sentAt: input.timeCreated, supersession }
              }),
            { behavior: "immediate" },
          ),
        )
        return result
      },
    )

    const pendingForSession: Interface["pendingForSession"] = Effect.fn("S2SStore.pendingForSession")(
      function* (target, now) {
        const rows = yield* query(
          db.all<MessageDbRow>(sql`
        SELECT id, target_session_id, from_session_id, from_slug, capsule, sent_at, expires_at, supersedes
        FROM s2s_message
        WHERE target_session_id = ${target} AND delivered_at IS NULL
          AND expired_at IS NULL AND superseded_at IS NULL
          AND ${dueBy(now)}
        ORDER BY sent_at ASC, id ASC
      `),
        )
        return rows.map(toMessageRow)
      },
    )

    const resolvePending: Interface["resolvePending"] = Effect.fn("S2SStore.resolvePending")(
      function* (id, target, now) {
        return yield* query(
          db.transaction(
            (tx) =>
              Effect.gen(function* () {
                const row = yield* tx.get<{
                  delivered_at: number | null
                  expired_at: number | null
                  superseded_at: number | null
                  expires_at: number | null
                }>(
                  sql`SELECT delivered_at, expired_at, superseded_at, expires_at FROM s2s_message WHERE id = ${id} AND target_session_id = ${target}`,
                )
                if (!row) return "missing" as const
                if (row.delivered_at !== null) return "delivered" as const
                if (row.expired_at !== null) return "expired" as const
                if (row.superseded_at !== null) return "superseded" as const
                if (row.expires_at === null || row.expires_at > now) return "pending" as const
                // A committed message is the claim: after it exists, expiry cannot retract a visible frame.
                const expired = yield* tx.all<{ id: string }>(sql`
          UPDATE s2s_message SET expired_at = ${now}
          WHERE id = ${id} AND target_session_id = ${target}
            AND delivered_at IS NULL AND expired_at IS NULL AND superseded_at IS NULL
            AND expires_at <= ${now}
            AND NOT EXISTS (SELECT 1 FROM message WHERE id = ${MessageID.make(`msg_${id}`)})
          RETURNING id
        `)
                return expired.length > 0 ? ("expired" as const) : ("pending" as const)
              }),
            { behavior: "immediate" },
          ),
        )
      },
    )

    const resolvePendingForSession: Interface["resolvePendingForSession"] = Effect.fn(
      "S2SStore.resolvePendingForSession",
    )(function* (target, now) {
      const expired = yield* query(
        db.transaction(
          (tx) =>
            tx.all<{ id: string }>(sql`
        UPDATE s2s_message SET expired_at = ${now}
        WHERE target_session_id = ${target} AND delivered_at IS NULL AND expired_at IS NULL AND superseded_at IS NULL
          AND expires_at <= ${now} AND ${dueBy(now)}
          AND NOT EXISTS (SELECT 1 FROM message WHERE id = 'msg_' || s2s_message.id)
        RETURNING id
      `),
          { behavior: "immediate" },
        ),
      )
      return expired.map((row) => row.id).toSorted()
    })

    const peerCapability: Interface["peerCapability"] = Effect.fn("S2SStore.peerCapability")(function* (target, now) {
      const row = yield* query(
        db.get<{ capability_version: number; heartbeat_at: number }>(sql`
        SELECT capability_version, heartbeat_at FROM s2s_presence WHERE session_id = ${target}
      `),
      )
      if (!row || row.heartbeat_at > now || now - row.heartbeat_at > PRESENCE_TTL_MS) return { state: "unknown" }
      return row.capability_version < 1
        ? { state: "incompatible", version: row.capability_version }
        : { state: "current", version: row.capability_version }
    })

    const sentHistory: Interface["sentHistory"] = Effect.fn("S2SStore.sentHistory")(function* (
      sender,
      target,
      limit = 20,
    ) {
      const rows = yield* query(
        db.all<{
          id: string
          target_session_id: string
          sent_at: number
          delivered_at: number | null
          expired_at: number | null
          superseded_at: number | null
        }>(sql`
        SELECT id, target_session_id, sent_at, delivered_at, expired_at, superseded_at
        FROM s2s_message WHERE from_session_id = ${sender}
          ${target ? sql`AND target_session_id = ${target}` : sql``}
        ORDER BY sent_at DESC, id DESC LIMIT ${Math.max(1, Math.min(limit, 100))}
      `),
      )
      return rows.map((row) => ({
        id: row.id,
        target: SessionID.make(row.target_session_id),
        sentAt: row.sent_at,
        deliveredAt: row.delivered_at,
        state:
          row.delivered_at !== null
            ? ("delivered" as const)
            : row.expired_at !== null
              ? ("expired" as const)
              : row.superseded_at !== null
                ? ("superseded" as const)
                : ("pending" as const),
      }))
    })

    const peerActivity: Interface["peerActivity"] = Effect.fn("S2SStore.peerActivity")(function* (ids, now) {
      const state = new Map<SessionID, "running" | "unknown">(ids.map((id) => [id, "unknown"]))
      if (ids.length === 0) return state
      const rows = yield* query(
        db.all<{ session_id: string }>(sql`
        SELECT session_id FROM s2s_presence
        WHERE session_id IN (${sql.join(
          ids.map((id) => sql`${id}`),
          sql`, `,
        )})
          AND heartbeat_at BETWEEN ${now - PRESENCE_TTL_MS} AND ${now}
          AND capability_version >= 1 AND owner_id <> ''
      `),
      )
      for (const row of rows) state.set(SessionID.make(row.session_id), "running")
      return state
    })

    const heartbeat: Interface["heartbeat"] = Effect.fn("S2SStore.heartbeat")(function* (session, owner, now) {
      // Only the process holding the latest operator registration calls this;
      // a later owner's row must survive a stale owner's delayed cleanup.
      yield* query(
        db.run(sql`
        INSERT INTO s2s_presence (session_id, owner_id, capability_version, heartbeat_at)
        VALUES (${session}, ${owner}, 1, ${now})
        ON CONFLICT(session_id) DO UPDATE SET
          owner_id = excluded.owner_id,
          capability_version = excluded.capability_version,
          heartbeat_at = excluded.heartbeat_at
        WHERE s2s_presence.heartbeat_at <= excluded.heartbeat_at
      `),
      )
    })

    const claimPresence: Interface["claimPresence"] = Effect.fn("S2SStore.claimPresence")(
      function* (session, owner, now) {
        const row = yield* query(
          db.get<{ owner_id: string }>(sql`
        INSERT INTO s2s_presence (session_id, owner_id, capability_version, heartbeat_at)
        VALUES (${session}, ${owner}, 1, ${now})
        ON CONFLICT(session_id) DO UPDATE SET
          owner_id = excluded.owner_id,
          capability_version = excluded.capability_version,
          heartbeat_at = excluded.heartbeat_at
        WHERE s2s_presence.owner_id = excluded.owner_id
          OR s2s_presence.heartbeat_at NOT BETWEEN ${now - PRESENCE_TTL_MS} AND ${now}
        RETURNING owner_id
      `),
        )
        return row?.owner_id === owner
      },
    )

    const clearPresence: Interface["clearPresence"] = Effect.fn("S2SStore.clearPresence")(function* (session, owner) {
      yield* query(db.run(sql`DELETE FROM s2s_presence WHERE session_id = ${session} AND owner_id = ${owner}`))
    })

    const pendingLegacyForSession: Interface["pendingLegacyForSession"] = Effect.fn("S2SStore.pendingLegacyForSession")(
      function* (target) {
        const rows = yield* query(
          db.all<InboxDbRow>(sql`
        SELECT id, target_session_id, from_session_id, from_slug, capsule, time_created
        FROM s2s_inbox WHERE target_session_id = ${target} AND drained_at IS NULL
        ORDER BY time_created ASC, id ASC
      `),
        )
        return rows.map(toInboxRow)
      },
    )

    const adoptLegacy: Interface["adoptLegacy"] = Effect.fn("S2SStore.adoptLegacy")(function* (id) {
      return yield* query(
        db.transaction(
          (tx) =>
            Effect.gen(function* () {
              const claimed = yield* tx.all<InboxDbRow>(sql`
          UPDATE s2s_inbox SET drained_at = ${Date.now()}
          WHERE id = ${id} AND drained_at IS NULL
          RETURNING id, target_session_id, from_session_id, from_slug, capsule, time_created
        `)
              const row = claimed[0]
              if (!row) return undefined
              yield* tx.run(sql`
          INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at)
          VALUES (${row.id}, ${row.target_session_id}, ${row.from_session_id ?? row.target_session_id}, ${row.from_slug ?? "unknown"}, ${row.capsule}, ${row.time_created})
          ON CONFLICT(id) DO NOTHING
        `)
              yield* tx.run(sql`DELETE FROM s2s_inbox WHERE id = ${id} AND drained_at IS NOT NULL`)
              const canonical = yield* tx.get<MessageDbRow>(sql`
          SELECT id, target_session_id, from_session_id, from_slug, capsule, sent_at, expires_at, supersedes
          FROM s2s_message WHERE id = ${id}
        `)
              return canonical ? toMessageRow(canonical) : undefined
            }),
          { behavior: "immediate" },
        ),
      )
    })

    const receipt: Interface["receipt"] = Effect.fn("S2SStore.receipt")(function* (input) {
      return yield* receiptLocks.withLock(input.id)(
        Effect.gen(function* () {
          const started = yield* query(
            db.transaction(
              (tx) =>
                Effect.gen(function* () {
                  const pending = yield* tx.get<{ expires_at: number | null }>(sql`
            SELECT expires_at FROM s2s_message
            WHERE id = ${input.id} AND target_session_id = ${input.target}
              AND delivered_at IS NULL AND expired_at IS NULL AND superseded_at IS NULL
               AND ${dueBy(Date.now())}
          `)
                  if (!pending) return { state: "stopped" as const }
                  const messageID = MessageID.make(`msg_${input.id}`)
                  const existing = yield* tx.get<{ session_id: string; time_created: number }>(
                    sql`SELECT session_id, time_created FROM message WHERE id = ${messageID}`,
                  )
                  if (!existing && pending.expires_at !== null && pending.expires_at <= Date.now())
                    return { state: "stopped" as const }
                  const { message, parts } = input.buildTranscript(existing?.time_created ?? input.deliveredAt)
                  if (
                    message.id !== messageID ||
                    message.time.created !== (existing?.time_created ?? input.deliveredAt)
                  )
                    return { state: "invalid" as const }
                  if (existing && existing.session_id !== input.target) return { state: "other_session" as const }
                  // The message is the persisted claim. This publish is the last operation in its transaction.
                  if (!existing) yield* input.sessions.updateMessage(message)
                  return { state: "ready" as const, message, parts }
                }),
              { behavior: "immediate" },
            ),
          )
          if (started.state === "stopped") return false
          if (started.state !== "ready")
            return yield* Effect.fail(new S2SStoreError({ message: "Invalid receipt transcript", cause: input.id }))
          for (const part of started.parts) {
            const found = yield* query(
              db.get<{ message_id: string; session_id: string }>(
                sql`SELECT message_id, session_id FROM part WHERE id = ${part.id}`,
              ),
            )
            if (found && (found.message_id !== started.message.id || found.session_id !== input.target)) {
              return yield* Effect.fail(
                new S2SStoreError({ message: "Transcript part belongs to another message", cause: part.id }),
              )
            }
            if (!found) yield* input.sessions.updatePart(part)
          }
          // A cross-process handover can publish the same ids with different receive times.
          // The projector keeps rows unique, but live events may repeat with different timestamps.
          const delivered = yield* query(
            db.all<{ id: string }>(sql`
          UPDATE s2s_message SET delivered_at = ${input.deliveredAt}, transcript_message_id = ${started.message.id}
          WHERE id = ${input.id} AND target_session_id = ${input.target} AND delivered_at IS NULL
            AND expired_at IS NULL AND superseded_at IS NULL
            AND ${dueBy(input.deliveredAt)}
          RETURNING id
        `),
          )
          return delivered.length > 0
        }),
      )
    })

    const pendingTargets: Interface["pendingTargets"] = Effect.fn("S2SStore.pendingTargets")(function* (
      ids,
      now = Date.now(),
    ) {
      if (ids.length === 0) return []
      const rows = yield* query(
        db.all<{ target: string }>(sql`
          SELECT DISTINCT target_session_id AS target FROM (
            SELECT target_session_id FROM s2s_inbox WHERE drained_at IS NULL
            UNION ALL
            SELECT target_session_id FROM s2s_message
            WHERE delivered_at IS NULL AND expired_at IS NULL AND superseded_at IS NULL
              AND ${dueBy(now)}
          )
          WHERE target_session_id IN (${sql.join(
            ids.map((id) => sql`${id}`),
            sql`, `,
          )})
        `),
      )
      return rows.map((row) => SessionID.make(row.target))
    })

    // Old processes may leave claimed legacy rows behind; reopening them lets the new owner adopt them.
    const reapStale: Interface["reapStale"] = Effect.fn("S2SStore.reapStale")(function* (olderThan) {
      yield* query(
        db.run(sql`
          UPDATE s2s_inbox SET drained_at = NULL
          WHERE drained_at IS NOT NULL AND drained_at < ${olderThan}
        `),
      )
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

    // Session creation precedes writes to these tables, so the anti-joins
    // cannot remove rows for a newly admitted live session.
    const deleteOrphaned: Interface["deleteOrphaned"] = Effect.fn("S2SStore.deleteOrphaned")(function* () {
      yield* query(
        db.run(sql`
            DELETE FROM s2s_message
            WHERE target_session_id NOT IN (SELECT id FROM session)
               OR from_session_id NOT IN (SELECT id FROM session)
          `),
      )
      yield* query(
        db.run(sql`
            DELETE FROM s2s_presence
            WHERE session_id NOT IN (SELECT id FROM session)
          `),
      )
      yield* query(
        db.run(sql`
            DELETE FROM scheduled_task
            WHERE parent_session_id NOT IN (SELECT id FROM session)
          `),
      )
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
      pendingForSession,
      resolvePending,
      resolvePendingForSession,
      pendingLegacyForSession,
      adoptLegacy,
      receipt,
      peerCapability,
      sentHistory,
      peerActivity,
      heartbeat,
      claimPresence,
      clearPresence,
      pendingTargets,
      reapStale,
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
