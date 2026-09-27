import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { DatabaseMigration } from "../src/database/migration"
import { migrations } from "../src/database/migration.gen"
import { Flock } from "../src/util/flock"
import { Global } from "../src/global"
import { tmpdir } from "./fixture/tmpdir"
import path from "path"

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
const run = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(effect.pipe(Effect.provide(SqliteClient.layer({ filename, disableWAL: true })), Effect.scoped))

const columns = (db: EffectDrizzleSqlite.EffectSQLiteDatabase, table: string) =>
  db.all<{ name: string }>(sql`SELECT name FROM pragma_table_info(${table})`).pipe(Effect.map((rows) => rows.map((row) => row.name)))

test("creates canonical mail, presence and scheduled tasks on fresh databases and reopens their rows", async () => {
  await using tmp = await tmpdir()
  Flock.setGlobal({ state: Global.Path.state })
  const filename = path.join(tmp.path, "fresh.sqlite")
  await run(filename, Effect.gen(function* () {
    const db = yield* makeDb
    yield* DatabaseMigration.apply(db)
    expect((yield* db.all<{ name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('s2s_message', 's2s_presence', 'scheduled_task') ORDER BY name`)).map((row) => row.name)).toEqual(["s2s_message", "s2s_presence", "scheduled_task"])
    expect(yield* columns(db, "s2s_message")).toContain("delivered_at")
    expect(yield* columns(db, "s2s_message")).toContain("transcript_message_id")
    expect(yield* columns(db, "s2s_message")).toContain("target_session_id")
    expect(yield* columns(db, "s2s_presence")).toEqual(["session_id", "owner_id", "capability_version", "heartbeat_at"])
    expect(yield* db.all<{ name: string; type: string; notnull: number }>(sql`SELECT name, type, "notnull" FROM pragma_table_info('s2s_presence') WHERE name = 'capability_version'`)).toEqual([{ name: "capability_version", type: "INTEGER", notnull: 1 }])
    expect((yield* db.all<{ name: string }>(sql`SELECT name FROM pragma_index_info('s2s_message_sender_sent')`)).map((row) => row.name)).toEqual(["from_session_id", "sent_at"])
    expect(yield* columns(db, "scheduled_task")).toContain("parent_session_id")
    expect(yield* columns(db, "scheduled_task")).toContain("child_session_id")
    expect(yield* columns(db, "scheduled_task")).toContain("due_at")
    expect(yield* columns(db, "scheduled_task")).toContain("state")
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES ('mail', 'target', 'sender', 'sender', '{}', 1)`)
    yield* db.run(sql`INSERT INTO scheduled_task (id, parent_session_id, dispatch_inputs, due_at, admitted_at, state) VALUES ('task', 'parent', '{}', 2, 1, 'pending')`)
  }))
  await run(filename, Effect.gen(function* () {
    const db = yield* makeDb
    yield* DatabaseMigration.apply(db)
    expect(yield* db.get(sql`SELECT target_session_id FROM s2s_message WHERE id = 'mail'`)).toEqual({ target_session_id: "target" })
    expect(yield* db.get(sql`SELECT state FROM scheduled_task WHERE id = 'task'`)).toEqual({ state: "pending" })
  }))
})

test("upgrades a legacy inbox without consuming mail and replays the new migration", async () => {
  const latest = migrations.find((migration) => migration.id.endsWith("_s2s_durable"))
  expect(latest).toBeDefined()
  if (!latest) return
  await run(":memory:", Effect.gen(function* () {
    const db = yield* makeDb
    yield* db.run(sql`CREATE TABLE s2s_inbox (id text PRIMARY KEY, target_session_id text NOT NULL, capsule text NOT NULL, drained_at integer)`)
    yield* db.run(sql`INSERT INTO s2s_inbox (id, target_session_id, capsule) VALUES ('old', 'target', '{}')`)
    yield* DatabaseMigration.applyOnly(db, [latest])
    expect(yield* columns(db, "s2s_message")).toContain("delivered_at")
    expect(yield* columns(db, "scheduled_task")).toContain("due_at")
    expect(yield* columns(db, "s2s_presence")).toContain("capability_version")
    expect((yield* db.all<{ name: string }>(sql`SELECT name FROM pragma_index_info('s2s_message_sender_sent')`)).map((row) => row.name)).toEqual(["from_session_id", "sent_at"])
    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at) VALUES ('mail', 'target', 'sender', 'sender', '{}', 1)`)
    yield* db.run(sql`INSERT INTO scheduled_task (id, parent_session_id, dispatch_inputs, due_at, admitted_at, state) VALUES ('task', 'parent', '{}', 2, 1, 'pending')`)
    yield* db.run(sql`DELETE FROM migration WHERE id = ${latest.id}`)
    yield* DatabaseMigration.applyOnly(db, [latest])
    expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_inbox`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM s2s_message`)).toEqual({ count: 1 })
    expect(yield* db.get(sql`SELECT count(*) AS count FROM scheduled_task`)).toEqual({ count: 1 })
  }))
})
