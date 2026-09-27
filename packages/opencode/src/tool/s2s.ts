// S2S addresses globally unique session IDs, not collision-prone slugs.
// Every send, including a same-process send, first commits one canonical
// s2s_message row; waking the recipient never substitutes for durability.

import { isLocalForLatestUser } from "@/s2s/local-owner"
import { Cause, Effect, Exit, Option, Schema } from "effect"
import * as Tool from "./tool"
import { Messaging, AbuseError, INBOX_CAP, S2S_HOURLY_OUTBOUND_CAP } from "../messaging"
import { Session } from "@/session/session"
import { S2SStore, TOKEN_TTL_MS, DEDUPE_WINDOW_MS } from "@/s2s/store"
import { S2SCapsule, encodeCapsule } from "@/s2s/capsule"
import { uuidv7 } from "@/s2s/uuidv7"
import { SessionID } from "@/session/schema"
import { parseInstant } from "./iso-instant"
import DESCRIPTION from "./s2s.txt"

const MAX_BODY_LENGTH = 16000
const MAX_FANOUT = 20

export const Parameters = Schema.Struct({
  command: Schema.Literals(["invite", "accept", "msg", "sent", "list", "leave", "relay"]).annotate({
    description: "Which s2s subcommand to run",
  }),
  // For `msg` and `leave` the user supplies the peer's session_id. For
  // `msg` the body is also required. `sent` may filter on target; the
  // other args are unused. Each is optional at the schema level so a
  // partial call decodes; the tool's run function rejects shape
  // mismatches per-command with a precise error.
  target: Schema.optional(Schema.String).annotate({
    description: "For msg/leave: peer session_id; for sent: optional peer filter",
  }),
  targets: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "For msg: multiple peer session_ids, instead of target",
  }),
  token: Schema.optional(Schema.String).annotate({
    description: "For accept: the one-shot token shared by the inviter",
  }),
  body: Schema.optional(Schema.String).annotate({
    description: "For msg: the message body",
  }),
  expires_at: Schema.optional(Schema.String).annotate({
    description: "For msg: expire an undelivered message at this timezone-qualified ISO instant",
  }),
  deliver_at: Schema.optional(Schema.String).annotate({
    description: "For msg: start delivery at this timezone-qualified ISO instant",
  }),
  stagger_ms: Schema.optional(Schema.Number).annotate({
    description: "For msg with targets: offset each target by this many milliseconds",
  }),
  supersedes: Schema.optional(Schema.String).annotate({
    description: "For msg: retract an undelivered earlier message to the same peer from this sender",
  }),
})

type Peer = {
  peer_id: SessionID
  title: string
  established_at: number
  outbound: boolean
  inbound: boolean
  activity: "running" | "unknown"
}

type SendResult =
  | { target: string; status: "sent"; id: string; due_at: string; peer: "current" | "unknown"; supersession?: "superseded" | "already_delivered" | "in_delivery" }
  | { target: string; status: "duplicate"; id: string; due_at: string }
  | { target: string; status: "failed"; due_at: string; reason: string }

type Metadata = {
  command: string
  target?: string
  peers?: Peer[]
  history?: S2SStore.SentRow[]
  allowance?: ReturnType<typeof outboundAllowance>
  supersession?: "superseded" | "already_delivered" | "in_delivery"
  due_at?: string
  results?: SendResult[]
}

export const S2STool = Tool.define<typeof Parameters, Metadata, Messaging.Service | Session.Service | S2SStore.Service>(
  "s2s",
  Effect.gen(function* () {
    const messaging = yield* Messaging.Service
    const sessions = yield* Session.Service
    // S2SStore is a REAL dependency captured at init (like messaging/
    // sessions), NOT resolved via serviceOption at execute time. A tool's
    // execute runs in the processor's fiber, whose context does NOT carry
    // S2SStore (it lives in AppLayer above the tool-exec scope), so an
    // execute-time serviceOption(S2SStore) returns None in production even
    // though AppLayer provides it — making the tool unusable. ToolRegistry
    // provides S2SStore so this init-time yield resolves it once, captured
    // in the closure for every command.
    const store = yield* S2SStore.Service

    const run = Effect.fn("S2STool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context<Metadata>,
    ) {
      // Pre-flight: every command needs a known source session so we
      // can pull the sender's slug (used as fromSlug on rows/capsules
      // and in in-process enqueue calls). Reject early if the session
      // is gone — the tool's caller side has no useful action.
      const me = yield* sessions.get(ctx.sessionID)

      switch (params.command) {
        case "invite": {
          // Single-use v4 UUID. The token is the join credential; its
          // shape is intentionally opaque so a user can share it over
          // a chat channel without leaking session metadata.
          const token = crypto.randomUUID()
          yield* store.insertToken({
            token,
            inviterSessionID: ctx.sessionID,
            inviterSlug: me.slug,
            createdAt: Date.now(),
          })
          return {
            title: "Minted invite token",
            metadata: { command: "invite" },
            output: `Invite token: ${token}\nShare it with the peer session; they accept it with s2s(command:"accept", token:"...") within ${TOKEN_TTL_MS / 60_000} minutes.`,
          }
        }

        case "accept": {
          if (!params.token) return yield* Effect.fail(new Error('s2s(command:"accept") requires a token'))
          const claimed = yield* store.claimToken(params.token, ctx.sessionID)
          if (Option.isNone(claimed))
            return yield* Effect.fail(
              new Error(`s2s accept: token "${params.token}" is invalid, expired, or already used`),
            )
          const row = claimed.value
          // Two-direction allow: the joiner can now send to the inviter
          // AND the inviter can now send to the joiner. The DB rows
          // are directional (s2s_allow.session_id is the sender), so
          // both sides need a row.
          yield* store.insertAllow(ctx.sessionID, row.inviterSessionID)
          yield* store.insertAllow(row.inviterSessionID, ctx.sessionID)
          // Consent is durable in s2s_allow (session_id based) — that is
          // the authority `msg` checks via store.isAllowed. We do NOT seed
          // the in-process Messaging allow list here: that list is for the
          // subagent `message` tool's slug allow-list, kept separate so s2s
          // (session_id) and coordinator-messaging (slug) never mix keys.
          return {
            title: `Accepted invite from ${row.inviterSessionID}`,
            metadata: { command: "accept" },
            output: `Now allow-listed with peer ${row.inviterSessionID}. Use s2s(command:"msg", target:"${row.inviterSessionID}", body:"...") to send.`,
          }
        }

        case "msg": {
          const batched = params.targets !== undefined
          if (Boolean(params.target) === batched)
            return yield* Effect.fail(new Error('s2s(command:"msg") requires exactly one of target or nonempty targets'))
          if (batched && (params.targets.length === 0 || params.targets.length > MAX_FANOUT))
            return yield* Effect.fail(new Error(`s2s targets must contain 1–${MAX_FANOUT} peers`))
          if (params.stagger_ms !== undefined && (!batched || !Number.isSafeInteger(params.stagger_ms) || params.stagger_ms < 0))
            return yield* Effect.fail(new Error("s2s stagger_ms requires targets and a nonnegative integer"))
          const body = params.body
          if (!body) return yield* Effect.fail(new Error('s2s(command:"msg") requires body="..."'))
          if (body.length > MAX_BODY_LENGTH)
            return yield* Effect.fail(
              new Error(`s2s body exceeds maximum length of ${MAX_BODY_LENGTH} characters (got ${body.length})`),
            )
          const expiresAt = params.expires_at ? parseInstant(params.expires_at) : undefined
          if (params.expires_at && expiresAt === undefined)
            return yield* Effect.fail(new Error("s2s expires_at must be a timezone-qualified ISO-8601 instant"))
          const deliverAt = params.deliver_at ? parseInstant(params.deliver_at) : undefined
          if (params.deliver_at && deliverAt === undefined)
            return yield* Effect.fail(new Error("s2s deliver_at must be a timezone-qualified ISO-8601 instant"))
          if (params.supersedes === "") return yield* Effect.fail(new Error("s2s supersedes requires a message id"))
          const baseDue = deliverAt ?? Date.now()
          if (batched && !Number.isFinite(new Date(baseDue + (params.targets.length - 1) * (params.stagger_ms ?? 0)).getTime()))
            return yield* Effect.fail(new Error("s2s staggered deliver_at exceeds the supported time range"))

          const capsuleFor = (due?: number): S2SCapsule => ({
            version: 1,
            id: uuidv7(),
            sender_slug: me.slug,
            sender_name: me.title,
            sender_session_id: String(ctx.sessionID),
            timestamp: Date.now(),
            expires_at: params.expires_at,
            deliver_at: due === undefined ? undefined : new Date(due).toISOString(),
            supersedes: params.supersedes,
            body,
          })

          if (batched) {
            const results = yield* Effect.forEach(params.targets, (target, index) => Effect.gen(function* () {
              const due = baseDue + index * (params.stagger_ms ?? 0)
              const due_at = new Date(due).toISOString()
              if (target === ctx.sessionID) return { target, due_at, status: "failed" as const, reason: "cannot send to self" }
              if (expiresAt !== undefined && expiresAt <= due) return { target, due_at, status: "failed" as const, reason: "expires_at must be after this target's deliver_at" }
              const peer = SessionID.make(target)
              if (!(yield* store.isAllowed(ctx.sessionID, peer))) return { target, due_at, status: "failed" as const, reason: "target is not in your s2s allow list" }
              const presence = yield* store.peerCapability(peer, Date.now())
              if (presence.state === "incompatible") return { target, due_at, status: "failed" as const, reason: `Recipient is running an incompatible S2S build (capability ${presence.version}); upgrade it and retry.` }
              const capsule = capsuleFor(due)
              const attempted = yield* enqueueExternal({
                store,
                target: peer,
                fromSlug: me.slug,
                capsule,
                expiresAt,
                deliverAt: due,
                supersedes: params.supersedes,
                scheduleKey: params.deliver_at ? `at:${due}` : `stagger:${params.stagger_ms ?? 0}:${index}`,
              }).pipe(Effect.exit)
              if (Exit.isFailure(attempted)) {
                const error = Cause.findErrorOption(attempted.cause)
                return { target, due_at, status: "failed" as const, reason: Option.isSome(error) && error.value instanceof AbuseError ? error.value.detail : "Store admission failed; retry this target." }
              }
              const outcome = attempted.value
              if (outcome._tag === "invalid_supersedes") return { target, due_at, status: "failed" as const, reason: "supersedes id must be a pending message from this sender to this recipient" }
              if (outcome._tag === "duplicate") return { target, due_at: new Date(outcome.originalDueAt).toISOString(), status: "duplicate" as const, id: outcome.originalInboxId }
              return { target, due_at, status: "sent" as const, id: outcome.inboxId, peer: presence.state, supersession: outcome.supersession }
            }), { concurrency: 1 })
            return {
              title: "S2S staggered fan-out",
              metadata: { command: "msg", results, allowance: outboundAllowance(ctx.sessionID) },
              output: results.map((result) => `${result.target} · ${result.status}${"id" in result ? ` id=${result.id}` : ""} · due ${result.due_at}${"reason" in result ? ` · ${result.reason}` : ""}`).join("\n"),
            }
          }
          if (!params.target) return yield* Effect.fail(new Error('s2s(command:"msg") requires target=<peer-session-id>'))
          if (expiresAt !== undefined && expiresAt <= baseDue)
            return yield* Effect.fail(new Error("s2s expires_at must be after deliver_at"))

          // Addressing is by session_id. The target string IS the peer's
          // SessionID — no slug resolution (session.slug is not unique).
          const targetID = SessionID.make(params.target)
          if (targetID === ctx.sessionID) return yield* Effect.fail(new Error("s2s msg: cannot send to self"))

          // Consent: the durable s2s_allow table (session_id based) is the
          // single authority. `isAllowed(me, target)` is true iff we
          // accepted this peer (or they accepted us — accept writes both
          // directions). This works the same in-process and cross-process,
          // and survives a process restart (no in-proc allow re-seed needed).
          const allowed = yield* store.isAllowed(ctx.sessionID, targetID)
          if (!allowed)
            return yield* Effect.fail(
              new Error(`s2s msg: target "${params.target}" is not in your s2s allow list (invite/accept first)`),
            )

          const presence = yield* store.peerCapability(targetID, Date.now())
          if (presence.state === "incompatible")
            return yield* Effect.fail(new Error(`Recipient is running an incompatible S2S build (capability ${presence.version}); upgrade it and retry.`))
          const inProcess = yield* isLocalForLatestUser(targetID, messaging, sessions)

          const capsule = capsuleFor(deliverAt)
          const outcome = yield* enqueueExternal({ store, target: targetID, fromSlug: me.slug, capsule, expiresAt, deliverAt, supersedes: params.supersedes, scheduleKey: deliverAt === undefined ? undefined : `at:${deliverAt}` }).pipe(
            Effect.catchTag("Messaging.AbuseError", (e) => Effect.fail(new Error(e.detail))),
          )
          if (outcome._tag === "duplicate") {
            return {
              title: `Already sent to ${params.target}`,
              metadata: { command: "msg", target: params.target, due_at: new Date(outcome.originalDueAt).toISOString(), allowance: outboundAllowance(ctx.sessionID) },
              output: `Already sent within the last ${DEDUPE_WINDOW_MS / 60_000} minutes (id=${outcome.originalInboxId}); not re-queued. ${allowanceText(ctx.sessionID)}`,
            }
          }
          if (outcome._tag === "invalid_supersedes")
            return yield* Effect.fail(new Error("s2s supersedes id must be a pending message from this sender to this recipient"))
          return {
            title: `Sent to ${params.target}`,
            metadata: { command: "msg", target: params.target, due_at: new Date(deliverAt ?? outcome.sentAt).toISOString(), allowance: outboundAllowance(ctx.sessionID), supersession: outcome.supersession },
              output: `Persisted to s2s_message (id=${capsule.id}); ${outcome.supersession === "superseded" ? `Retracted pending ${params.supersedes}. ` : outcome.supersession === "already_delivered" ? `Earlier ${params.supersedes} already delivered; not retracted. ` : outcome.supersession === "in_delivery" ? `Earlier ${params.supersedes} already in delivery; not retracted. ` : ""}${presence.state === "unknown" ? "No current presence record; stored pending and will deliver when the recipient runs a current build." : inProcess ? "Stored pending; local recipient will drain at the next safe turn." : "Stored pending; recipient process will poll and wake."} ${allowanceText(ctx.sessionID)}`,
          }
        }

        case "sent": {
          const rows = yield* store.sentHistory(ctx.sessionID, params.target ? SessionID.make(params.target) : undefined)
          return {
            title: "S2S sent history",
            metadata: { command: "sent", target: params.target, history: rows },
            output: rows.length === 0 ? "No sent s2s messages." : rows.map((row) =>
              `${row.id} → ${row.target} · sent ${new Date(row.sentAt).toISOString()} · ${row.state}${row.deliveredAt !== null ? ` (${new Date(row.deliveredAt).toISOString()})` : ""}`,
            ).join("\n"),
          }
        }

        case "list": {
          const rows = yield* store.listAllows(ctx.sessionID)
          const peers = rows.reduce((result, row) => {
            const peerID = row.sessionID === ctx.sessionID ? row.allowedSessionID : row.sessionID
            const entry = result.get(peerID)
            const outbound = row.sessionID === ctx.sessionID
            const inbound = row.allowedSessionID === ctx.sessionID
            if (entry) {
              result.set(peerID, {
                ...entry,
                established_at: Math.min(entry.established_at, row.establishedAt),
                outbound: entry.outbound || outbound,
                inbound: entry.inbound || inbound,
              })
              return result
            }
            result.set(peerID, {
              peer_id: peerID,
              established_at: row.establishedAt,
              outbound,
              inbound,
            })
            return result
          }, new Map<SessionID, Omit<Peer, "title" | "activity">>())
          const activity = yield* store.peerActivity([...peers.keys()], Date.now())
          const entries = yield* Effect.forEach(Array.from(peers.values()), (peer) =>
            sessions.get(peer.peer_id).pipe(
              Effect.map((session) => session.title || "(unknown)"),
              Effect.catchTag("NotFoundError", () => Effect.succeed("(unknown)")),
              Effect.map((title) => ({ ...peer, title, activity: activity.get(peer.peer_id) ?? "unknown" as const })),
            ),
          )
          const sorted = entries.toSorted((a, b) => b.established_at - a.established_at)
          if (sorted.length === 0)
            return {
              title: "S2S peers",
              metadata: { command: "list", peers: [] },
              output: "No s2s peers.",
            }
          return {
            title: "S2S peers",
            metadata: { command: "list", peers: sorted },
            output: sorted
              .map((peer) => {
                const consent =
                  peer.outbound === peer.inbound
                    ? "bidirectional"
                    : `ANOMALOUS one-way (${peer.outbound ? "outbound only" : "inbound only"})`
                return `${peer.peer_id} · ${peer.title} · established ${new Date(peer.established_at).toISOString()} · consent: ${consent} · ${peer.activity}`
              })
              .join("\n"),
          }
        }

        case "leave": {
          if (!params.target)
            return yield* Effect.fail(new Error('s2s(command:"leave") requires target=<peer-session-id>'))
          // Addressing by session_id: delete both allow directions in the
          // durable s2s_allow table. Idempotent — deleting a non-existent
          // allow is a no-op (the peer was never accepted or already left).
          const targetID = SessionID.make(params.target)
          yield* store.deleteAllow(ctx.sessionID, targetID)
          yield* store.deleteAllow(targetID, ctx.sessionID)
          return {
            title: `Left ${params.target}`,
            metadata: { command: "leave", target: params.target },
            output: `Removed s2s_allow rows in both directions for ${params.target}.`,
          }
        }

        case "relay": {
          // Zero-infra fallback: emit a capsule-shaped blob the user
          // can copy/paste to a peer on a different machine. v1 just
          // wraps the body (or an explanatory stub if no body is
          // given) in a v1 capsule. The future cross-machine wire
          // (Task 8+) will be the real consumer of this format.
          const capsule: S2SCapsule = {
            version: 1,
            id: uuidv7(),
            sender_slug: me.slug,
            sender_session_id: String(ctx.sessionID),
            timestamp: Date.now(),
            body: params.body ?? "(no body — relay stub)",
          }
          return {
            title: "Relay payload",
            metadata: { command: "relay" },
            output: JSON.stringify(capsule, null, 2),
          }
        }
      }
    })

    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context<Metadata>) =>
        run(params, ctx).pipe(Effect.orDie) as unknown as Effect.Effect<Tool.ExecuteResult<Metadata>>,
    }
  }),
)

// The soft per-process throttle cannot replace the transactionally enforced
// recipient cap: other processes may write to the same database concurrently.
const enqueueExternalBumpOutbound = new Map<SessionID, { hour: number; count: number }>()

function outboundAllowance(sender: SessionID) {
  const hour = Math.floor(Date.now() / 3_600_000)
  const current = enqueueExternalBumpOutbound.get(sender)
  const used = current?.hour === hour ? current.count : 0
  return {
    used,
    limit: S2S_HOURLY_OUTBOUND_CAP,
    remaining: S2S_HOURLY_OUTBOUND_CAP - used,
    resets_at: new Date((hour + 1) * 3_600_000).toISOString(),
  }
}

function allowanceText(sender: SessionID) {
  const value = outboundAllowance(sender)
  return `S2S sends this UTC clock hour: ${value.used}/${value.limit} used, ${value.remaining} remaining; resets at ${value.resets_at}.`
}

// Global cap on sender entries per Map to prevent unbounded growth
// over process lifetime. When a Map exceeds this limit, the oldest
// (first-inserted) sender entry is evicted before the new write.
const MAX_SENDER_ENTRIES = 500

const evictIfNeeded = <V>(map: Map<SessionID, V>, max: number) => {
  while (map.size > max) {
    const first = map.keys().next()
    if (first.done) break
    map.delete(first.value)
  }
}

// Preserve old body-only keys across upgrades. Version the metadata form
// before the recipient id so a body containing separators cannot mimic it.
const dedupeKeyFor = async (sender: SessionID, recipient: SessionID, body: string, expiresAt?: number, supersedes?: string, scheduleKey?: string): Promise<string> => {
  const material = expiresAt === undefined && supersedes === undefined && scheduleKey === undefined
    ? `${sender}\u0000${recipient}\u0000${body}`
    : `${sender}\u0000v2\u0000${recipient}\u0000${JSON.stringify({ body, expiresAt, supersedes, scheduleKey })}`
  const data = new TextEncoder().encode(material)
  const digest = await crypto.subtle.digest("SHA-256", data)
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
}

const enqueueExternal = Effect.fn("S2STool.enqueueExternal")(function* (input: {
  store: S2SStore.Interface
  target: SessionID
  fromSlug: string
  capsule: S2SCapsule
  expiresAt?: number
  deliverAt?: number
  scheduleKey?: string
  supersedes?: string
}) {
  const sender = SessionID.make(input.capsule.sender_session_id)
  const store = input.store
  // SOFT per-process outbound throttle, NOT a durable abuse bound: this
  // Map lives in process memory, so it resets on restart and is not shared
  // across processes — a determined sender can exceed it by restarting or
  // running multiple processes. It exists only to catch a runaway loop in
  // the common single-process case. The DURABLE, cross-process abuse bound
  // is the recipient's INBOX_CAP below (enforced inside the same
  // transaction as the dedupe check + insert, so two concurrent sends
  // racing at the cap cannot both pass the gate). Wall-clock hour bucket;
  // resets on the next hour boundary.
  const now = Date.now()
  const hour = Math.floor(now / 3_600_000)
  const existing = enqueueExternalBumpOutbound.get(sender)
  const current = existing && existing.hour === hour ? existing : { hour, count: 0 }
  if (current.count >= S2S_HOURLY_OUTBOUND_CAP)
    return yield* new AbuseError({
      detail: `s2s outbound cap (${S2S_HOURLY_OUTBOUND_CAP}) reached for this session in the UTC clock hour; resets at ${outboundAllowance(sender).resets_at}`,
    })
  // Durable insert-time dedupe covers the recipient cap and send record in one transaction.
  // A duplicate return takes the early-out path below and does NOT
  // consume the per-process outbound budget or the recipient's cap.
  const dedupeKey = yield* Effect.promise(() => dedupeKeyFor(sender, input.target, input.capsule.body, input.expiresAt, input.supersedes, input.scheduleKey))
  const result = yield* store.tryEnqueueWithDedup({
    dedupeKey,
    sender,
    target: input.target,
    fromSlug: input.fromSlug,
    capsule: encodeCapsule(input.capsule),
    capsuleId: input.capsule.id,
    timeCreated: now,
    expiresAt: input.expiresAt,
    deliverAt: input.deliverAt,
    supersedes: input.supersedes,
    windowMs: DEDUPE_WINDOW_MS,
    inboxCap: INBOX_CAP,
  })
  if (result._tag === "duplicate") {
    return { _tag: "duplicate" as const, originalInboxId: result.originalInboxId, originalDueAt: result.originalDueAt }
  }
  if (result._tag === "inbox_full") {
    return yield* new AbuseError({
      detail: `recipient s2s inbox cap (${INBOX_CAP}) reached`,
    })
  }
  if (result._tag === "invalid_supersedes") return result
  enqueueExternalBumpOutbound.set(sender, { hour: current.hour, count: current.count + 1 })
  evictIfNeeded(enqueueExternalBumpOutbound, MAX_SENDER_ENTRIES)
  return { _tag: "inserted" as const, inboxId: result.inboxId, sentAt: result.sentAt, supersession: result.supersession }
})
