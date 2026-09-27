import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionID } from "@/session/schema"
import { sql, type SQL } from "drizzle-orm"
import { Context, Effect, Layer, Option, Schema } from "effect"

export type Row = {
  id: string
  parentSessionID: SessionID
  childSessionID: SessionID
  slug: string | null
  dueAt: number
  admittedAt: number
  state: "queued" | "prelaunch_claim" | "launch_intent" | "running" | "ambiguous" | "completed" | "failed"
  claimedAt: number | null
  claimOwner: string | null
  startedAt: number | null
  failureReason: string | null
  params: Record<string, unknown>
}

export class ScheduledTaskError extends Schema.TaggedErrorClass<ScheduledTaskError>()("ScheduledTaskError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Unknown),
}) {}

export interface Interface {
  readonly get: (id: string) => Effect.Effect<Option.Option<Row>, ScheduledTaskError>
  readonly admit: (input: {
    id: string
    parentSessionID: SessionID
    childSessionID: SessionID
    slug?: string
    dueAt: number
    admittedAt: number
    params: Record<string, unknown>
  }) => Effect.Effect<{ row: Row; duplicate: boolean }, ScheduledTaskError>
  readonly dueForParent: (parentID: SessionID, now: number) => Effect.Effect<Row[], ScheduledTaskError>
  readonly claimStart: (id: string, owner: string, now: number) => Effect.Effect<boolean, ScheduledTaskError>
  readonly launchIntent: (id: string, owner: string, now: number) => Effect.Effect<boolean, ScheduledTaskError>
  readonly markStarted: (id: string, owner: string, actualAt: number) => Effect.Effect<boolean, ScheduledTaskError>
  readonly heartbeat: (id: string, owner: string, now: number) => Effect.Effect<boolean, ScheduledTaskError>
  readonly markAmbiguous: (id: string, now: number) => Effect.Effect<boolean, ScheduledTaskError>
  readonly markFailed: (id: string, reason: string, owner?: string) => Effect.Effect<boolean, ScheduledTaskError>
  readonly markCompleted: (id: string, owner: string) => Effect.Effect<boolean, ScheduledTaskError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ScheduledTaskStore") {}

type DbRow = {
  id: string
  parent_session_id: string
  child_session_id: string | null
  slug: string | null
  dispatch_inputs: string
  due_at: number
  admitted_at: number
  state: Row["state"]
  claimed_at: number | null
  claim_owner: string | null
  started_at: number | null
  failure_reason: string | null
}

function toRow(row: DbRow): Effect.Effect<Row, ScheduledTaskError> {
  if (!row.child_session_id) return Effect.fail(new ScheduledTaskError({ message: `Scheduled task ${row.id} has no child session` }))
  const parsed = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)(row.dispatch_inputs)
  const params = Option.isSome(parsed)
    ? Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Unknown))(parsed.value)
    : Option.none<Record<string, unknown>>()
  if (Option.isNone(params)) return Effect.fail(new ScheduledTaskError({ message: `Scheduled task ${row.id} has invalid dispatch inputs` }))
  return Effect.succeed({
    id: row.id,
    parentSessionID: SessionID.make(row.parent_session_id),
    childSessionID: SessionID.make(row.child_session_id),
    slug: row.slug,
    dueAt: row.due_at,
    admittedAt: row.admitted_at,
    state: row.state,
    claimedAt: row.claimed_at,
    claimOwner: row.claim_owner,
    startedAt: row.started_at,
    failureReason: row.failure_reason,
    params: params.value,
  })
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const transition = (statement: SQL) => db.all<{ id: string }>(statement).pipe(
      Effect.map((rows) => rows.length > 0),
      Effect.mapError((cause) => new ScheduledTaskError({ message: "Could not update scheduled task", cause })),
    )

    const get: Interface["get"] = Effect.fn("ScheduledTaskStore.get")(function* (id) {
      const row = yield* db.get<DbRow>(sql`SELECT * FROM scheduled_task WHERE id = ${id}`).pipe(
        Effect.mapError((cause) => new ScheduledTaskError({ message: "Could not read scheduled task", cause })),
      )
      return row ? Option.some(yield* toRow(row)) : Option.none()
    })

    const admit: Interface["admit"] = Effect.fn("ScheduledTaskStore.admit")(function* (input) {
      const encoded = yield* Effect.try({
        try: () => JSON.stringify(input.params),
        catch: () => new ScheduledTaskError({ message: "Scheduled task inputs must be serializable" }),
      })
      return yield* db.transaction((tx) =>
        Effect.gen(function* () {
          const existing = yield* tx.get<DbRow>(sql`SELECT * FROM scheduled_task WHERE id = ${input.id}`)
          if (existing) {
            if (
              existing.parent_session_id !== input.parentSessionID ||
              existing.child_session_id !== input.childSessionID ||
              existing.slug !== (input.slug ?? null) ||
              existing.due_at !== input.dueAt ||
              existing.dispatch_inputs !== encoded
            )
              return yield* Effect.fail(new ScheduledTaskError({ message: `Conflicting reuse of scheduled task ${input.id}` }))
            return { row: yield* toRow(existing), duplicate: true }
          }
          yield* tx.run(sql`
            INSERT INTO scheduled_task (id, parent_session_id, child_session_id, slug, dispatch_inputs, due_at, admitted_at, state)
            VALUES (${input.id}, ${input.parentSessionID}, ${input.childSessionID}, ${input.slug ?? null}, ${encoded}, ${input.dueAt}, ${input.admittedAt}, 'queued')
          `)
          return {
            row: {
              id: input.id,
              parentSessionID: input.parentSessionID,
              childSessionID: input.childSessionID,
              slug: input.slug ?? null,
              dueAt: input.dueAt,
              admittedAt: input.admittedAt,
              state: "queued" as const,
              claimedAt: null,
              claimOwner: null,
              startedAt: null,
              failureReason: null,
              params: input.params,
            },
            duplicate: false,
          }
        }),
        { behavior: "immediate" },
      ).pipe(
        Effect.mapError((cause) => cause instanceof ScheduledTaskError ? cause : new ScheduledTaskError({ message: "Could not admit scheduled task", cause })),
      )
    })

    const dueForParent: Interface["dueForParent"] = Effect.fn("ScheduledTaskStore.dueForParent")(function* (parentID, now) {
      const rows = yield* db.all<DbRow>(sql`
        SELECT * FROM scheduled_task WHERE parent_session_id = ${parentID} AND due_at <= ${now}
          AND (state = 'queued' OR (state = 'prelaunch_claim' AND claimed_at <= ${now - 30_000})
            OR (state IN ('launch_intent', 'running') AND claimed_at <= ${now - 60_000}))
        ORDER BY due_at, id
      `).pipe(Effect.mapError((cause) => new ScheduledTaskError({ message: "Could not read due tasks", cause })))
      return yield* Effect.forEach(rows, toRow)
    })

    const claimStart: Interface["claimStart"] = Effect.fn("ScheduledTaskStore.claimStart")((id, owner, now) =>
      transition(sql`UPDATE scheduled_task SET state = 'prelaunch_claim', claimed_at = ${now}, claim_owner = ${owner}
        WHERE id = ${id} AND due_at <= ${now}
          AND (state = 'queued' OR (state = 'prelaunch_claim' AND claimed_at <= ${now - 30_000})) RETURNING id`))

    const launchIntent: Interface["launchIntent"] = Effect.fn("ScheduledTaskStore.launchIntent")((id, owner, now) =>
      transition(sql`UPDATE scheduled_task SET state = 'launch_intent', claimed_at = ${now}
        WHERE id = ${id} AND state = 'prelaunch_claim' AND claim_owner = ${owner} RETURNING id`))

    const markStarted: Interface["markStarted"] = Effect.fn("ScheduledTaskStore.markStarted")((id, owner, actualAt) =>
      transition(sql`UPDATE scheduled_task SET state = 'running', started_at = ${actualAt}, claimed_at = ${actualAt}
        WHERE id = ${id} AND state = 'launch_intent' AND claim_owner = ${owner} RETURNING id`))

    const heartbeat: Interface["heartbeat"] = Effect.fn("ScheduledTaskStore.heartbeat")((id, owner, now) =>
      transition(sql`UPDATE scheduled_task SET claimed_at = ${now}
        WHERE id = ${id} AND state IN ('launch_intent', 'running') AND claim_owner = ${owner} RETURNING id`))

    const markAmbiguous: Interface["markAmbiguous"] = Effect.fn("ScheduledTaskStore.markAmbiguous")((id, now) =>
      transition(sql`UPDATE scheduled_task SET state = 'ambiguous', failure_reason = 'Owner lost after launch intent'
        WHERE id = ${id} AND state IN ('launch_intent', 'running') AND claimed_at <= ${now - 60_000} RETURNING id`))

    const markFailed: Interface["markFailed"] = Effect.fn("ScheduledTaskStore.markFailed")((id, reason, owner) =>
      transition(sql`UPDATE scheduled_task SET state = 'failed', failure_reason = ${reason}
        WHERE id = ${id} AND (state = 'queued' OR (state IN ('prelaunch_claim', 'launch_intent', 'running') AND claim_owner = ${owner ?? null})) RETURNING id`))

    const markCompleted: Interface["markCompleted"] = Effect.fn("ScheduledTaskStore.markCompleted")((id, owner) =>
      transition(sql`UPDATE scheduled_task SET state = 'completed'
        WHERE id = ${id} AND state = 'running' AND claim_owner = ${owner} RETURNING id`))

    return { get, admit, dueForParent, claimStart, launchIntent, markStarted, heartbeat, markAmbiguous, markFailed, markCompleted } satisfies Interface
  }),
)

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node] })
export const defaultLayer = layer

export * as ScheduledTaskStore from "./scheduled-task-store"
