// Local owners adopt legacy ingress under a guarded claim and receipt canonical
// mail through the same transaction as the run-loop drain. The poller wakes
// idle recipients; busy recipients admit pending mail at their next turn boundary.
// The reaper only resets legacy claims left behind by older owners.

import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import * as Scope from "effect/Scope"
import { Cause, Context, Duration, Effect, Layer, Option, Schedule, Stream } from "effect"
import { Messaging } from "@/messaging"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { S2SStore } from "@/s2s/store"
import { S2SDelivery } from "@/s2s/delivery"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionStatus } from "@/session/status"
import { SessionID } from "@/session/schema"
import { EventV2Bridge } from "@/event-v2-bridge"

import { registerWakeBody } from "@/s2s/wake-registry"
import { isLocalForLatestUser } from "@/s2s/local-owner"

const REAPER_WINDOW_MS_DEFAULT = 60_000
const MIN_REAPER_MS = 1
const MAX_ROW_FAILURES = 3
const MAX_ROW_ENTRIES = 500
const rowFailures = new Map<string, number>()
const abandonedRows = new Set<string>()

const evictIfNeeded = <T>(collection: Map<T, unknown> | Set<T>, max: number) => {
  while (collection.size > max) {
    const first = (collection instanceof Map ? collection.keys() : collection.values()).next() as IteratorResult<T>
    if (first.done) break
    collection.delete(first.value)
  }
}

export interface Interface {
  readonly pollOnce: () => Effect.Effect<void>
  readonly reapOnce: (now: number) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/S2SPoller") {}

const processRow = Effect.fn("S2SPoller.processRow")(function* (row: S2SStore.MessageRow, expired: string[]) {
  if (abandonedRows.has(row.id)) return
  const status = yield* SessionStatus.Service
  if ((yield* status.get(row.targetSessionID)).type !== "idle") return
  const sessions = yield* Session.Service
  const last = yield* sessions.findMessage(row.targetSessionID, (message) => message.info.role === "user")
  if (Option.isNone(last) || last.value.info.role !== "user") return
  if (!(yield* S2SDelivery.admit(row, last.value.info, sessions, expired))) return
  rowFailures.delete(row.id)
  return true
})

const wakeIfIdle = Effect.fn("S2SPoller.wakeIfIdle")(function* (target: SessionID) {
  const status = yield* SessionStatus.Service
  const sessions = yield* Session.Service
  const prompt = yield* SessionPrompt.Service

  // A busy / retry session will drain the inbox at its own next turn
  // boundary (the drain block sits at the top of the runLoop iteration);
  // there is nothing for us to do. A session in "idle" is either not
  // running or is between iterations and waiting.
  const st = yield* status.get(target)
  if (st.type !== "idle") return

  // A zero-message session would throw in SessionPrompt.runLoop on the
  // first iteration ("No user message found in stream"). Skip
  // the wake in that case; the user-driven path that creates the first
  // user message will loop and pick up the inbox naturally.
  const last = yield* sessions.findMessage(target, (m) => m.info.role === "user")
  if (Option.isNone(last)) return

  // Wake only after the committed receipt so the next provider turn sees the frame.
  yield* prompt.loop({ sessionID: target })
})

export const pollOnceImpl = Effect.fn("S2SPoller.pollOnce")(function* () {
  const store = yield* S2SStore.Service
  const messaging = yield* Messaging.Service

  const locals = yield* messaging.localSet()
  if (locals.length === 0) return

  const sessions = yield* Session.Service
  const owned = yield* Effect.filter(locals, (sessionID) => isLocalForLatestUser(sessionID, messaging, sessions))
  for (const sessionID of locals) {
    if (owned.includes(sessionID)) yield* store.heartbeat(sessionID, S2SStore.PROCESS_OWNER_ID, Date.now())
    else yield* store.clearPresence(sessionID, S2SStore.PROCESS_OWNER_ID)
  }
  if (owned.length === 0) return
  const pending = yield* store.pendingTargets(owned)
  const expiries = new Map<SessionID, string[]>()
  const rows = (yield* Effect.forEach(pending, (sessionID) => Effect.gen(function* () {
    // A claimed legacy id belongs to its claiming process until the reaper resets it.
    for (const legacy of yield* store.pendingLegacyForSession(sessionID)) yield* store.adoptLegacy(legacy.id)
    const last = yield* sessions.findMessage(sessionID, (message) => message.info.role === "user")
    if (Option.isSome(last) && last.value.info.role === "user") {
      expiries.set(sessionID, yield* store.resolvePendingForSession(sessionID, Date.now()))
    }
    return yield* store.pendingForSession(sessionID, Date.now())
  }))).flat()
  const delivered = new Set<SessionID>()
  for (const row of rows) {
    // processRow is per-row; an exception in one row's wake must not
    // prevent subsequent rows from being processed. Failures are caught
    // and logged so the loop always continues to the next row.
    const didDeliver = yield* processRow(row, expiries.get(row.targetSessionID) ?? []).pipe(
      Effect.catch((e) =>
        Effect.gen(function* () {
          const failures = (rowFailures.get(row.id) ?? 0) + 1
          rowFailures.set(row.id, failures)
          evictIfNeeded(rowFailures, MAX_ROW_ENTRIES)
          if (failures < MAX_ROW_FAILURES) return
          abandonedRows.add(row.id)
          evictIfNeeded(abandonedRows, MAX_ROW_ENTRIES)
          yield* Effect.logWarning("S2SPoller: giving up on row for this process lifetime", {
            rowID: row.id,
            failures,
            error: e,
          })
        }),
      ),
    )
    if (didDeliver) delivered.add(row.targetSessionID)
  }
  for (const sessionID of pending) {
    const expired = expiries.get(sessionID) ?? []
    const last = yield* sessions.findMessage(sessionID, (message) => message.info.role === "user")
    if (expired.length > 0 && Option.isSome(last) && last.value.info.role === "user") {
      yield* S2SDelivery.summarizeExpired(sessionID, [...new Set(expired)].toSorted(), last.value.info, sessions)
    }
    if (expired.length > 0 || delivered.has(sessionID)) yield* wakeIfIdle(sessionID)
  }
})

// The per-instance wake-poller needs the live fiber's InstanceRef; the
// erased requirements match the Interface wrapper in S2SPoller.layer.
// At runtime the caller's fiber has all required services in context.
export const wakePollerLoop = (pollMs: number): Effect.Effect<void> =>
  pollOnceImpl().pipe(
    // A single tick failure must NOT silently terminate Effect.schedule (which
    // would permanently disable cross-process delivery for this instance). Log
    // at warning level and let the schedule continue to the next tick.
    Effect.catchCause((cause) => Effect.logWarning("s2s wake-poller tick failed", { cause: Cause.pretty(cause) })),
    Effect.schedule(Schedule.spaced(Duration.millis(pollMs))),
    Effect.ensuring(Effect.gen(function* () {
      const messaging = yield* Messaging.Service
      const store = yield* S2SStore.Service
      for (const id of yield* messaging.localSet()) yield* store.clearPresence(id, S2SStore.PROCESS_OWNER_ID)
    }).pipe(Effect.catchCause((cause) => Effect.logWarning("s2s presence cleanup failed", { cause: Cause.pretty(cause) })))),
  ) as unknown as Effect.Effect<void>

// Register into the wake-registry so SessionPrompt.loop can fork the
// C′ poller without importing from poller.ts (which imports SessionPrompt
// → would create a module-level cycle).
registerWakeBody(wakePollerLoop)

const reapOnceImpl = Effect.fn("S2SPoller.reapOnce")(function* (now: number, windowMs = REAPER_WINDOW_MS_DEFAULT) {
  const store = yield* S2SStore.Service
  // Reopen only abandoned legacy claims; canonical receipts are never reaped.
  yield* store.reapStale(now - windowMs)

  // Garbage-collect rows that reference sessions that no longer exist.
  // Best-effort: a GC failure is logged but must not break the reaper
  // tick (the same pattern as the pollOnce error handler).
  yield* store
    .deleteOrphaned()
    .pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("S2SPoller: deleteOrphaned failed", { cause: Cause.pretty(cause) }),
      ),
    )
})

const parseMs = (raw: string | undefined, fallback: number, min: number): number => {
  if (!raw) return fallback
  const n = Number.parseInt(raw, 10)
  if (!Number.isFinite(n)) return fallback
  return Math.max(min, n)
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const flags = yield* RuntimeFlags.Service
    const scope = yield* Scope.Scope

    // The interface signatures use the bare `Effect.Effect<void>` shape
    // (which TS widens to `Effect<void, never, never>`); the underlying
    // `pollOnceImpl` / `reapOnceImpl` carry their real error and
    // requirement types. Wrap with a thin lambda so the Interface's
    // `Effect<void, never, never>` contract is preserved.
    const pollOnce: Interface["pollOnce"] = () => pollOnceImpl() as unknown as Effect.Effect<void>
    const reapOnce: Interface["reapOnce"] = (now) => reapOnceImpl(now) as unknown as Effect.Effect<void>

    // Background loops — gated on the experimentalS2S flag so the
    // service is dead code in environments where S2S is off (the test
    // harness sets experimentalS2S: false to avoid a racing loop).
    //
    if (flags.experimentalS2S) {
      const reapWindowMs = parseMs(process.env["OPENCODE_S2S_REAP_WINDOW_MS"], REAPER_WINDOW_MS_DEFAULT, MIN_REAPER_MS)
      // Reap interval matches the window by default so each tick resets any
      // claim older than one window. Overriding the window also shrinks the
      // interval, which lets tests drive the loop quickly without real sleeps.
      const reapSchedule = Schedule.spaced(Duration.millis(reapWindowMs))

      // Only the reaper can fork at layer-build: SessionPrompt.loop forks the
      // wake poller with the live InstanceRef needed for recipient ownership.
      yield* Effect.suspend(() => reapOnceImpl(Date.now(), reapWindowMs)).pipe(
        Effect.schedule(reapSchedule),
        Effect.forkIn(scope, { startImmediately: true }),
      )
    }

    return Service.of({ pollOnce, reapOnce })
  }),
)

// The poller depends on the services the AppLayer already provides
// (RuntimeFlags, S2SStore, Messaging, Session, SessionStatus, SessionPrompt,
// EventV2Bridge). `Scope` is a built-in primitive so it is consumed by the
// layer effect itself and not listed here. The `node` form is exported so
// the AppLayer wiring step can splice it into the graph without re-deriving
// the dep list.
export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [
    RuntimeFlags.node,
    S2SStore.node,
    Messaging.node,
    Session.node,
    SessionStatus.node,
    SessionPrompt.node,
    EventV2Bridge.node,
  ],
})

export * as S2SPoller from "./poller"
