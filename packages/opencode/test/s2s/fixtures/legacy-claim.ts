import { Database } from "@opencode-ai/core/database/database"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { SessionID } from "../../../src/session/schema"

// Only an old binary claims legacy ingress; new owners use adoptLegacy instead.
export const claimLegacy = Effect.fn("S2STest.claimLegacy")(function* (ids: ReadonlyArray<SessionID>) {
  if (ids.length === 0) return []
  const { db } = yield* Database.Service
  const rows = yield* db.all<{
    id: string
    target_session_id: string
    from_session_id: string
    from_slug: string | null
    capsule: string
  }>(sql`
    UPDATE s2s_inbox SET drained_at = ${Date.now()}
    WHERE target_session_id IN (${sql.join(ids.map((id) => sql`${id}`), sql`, `)}) AND drained_at IS NULL
    RETURNING id, target_session_id, from_session_id, from_slug, capsule
  `)
  return rows.map((row) => ({
    id: row.id,
    targetSessionID: SessionID.make(row.target_session_id),
    fromSessionID: SessionID.make(row.from_session_id),
    fromSlug: row.from_slug,
    capsule: row.capsule,
  }))
})
