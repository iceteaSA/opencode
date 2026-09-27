import { Database } from "@opencode-ai/core/database/database"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { undeliveredCount } from "../../../src/s2s/store"
import { SessionID } from "../../../src/session/schema"

export const countUndelivered = Effect.fn("S2STest.countUndelivered")(function* (target: SessionID) {
  const { db } = yield* Database.Service
  return (yield* db.get<{ n: number }>(sql`SELECT ${undeliveredCount(target)} AS n`))?.n ?? 0
})
