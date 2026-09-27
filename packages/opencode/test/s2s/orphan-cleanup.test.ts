import { expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { sql } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { S2SStore } from "../../src/s2s/store"
import { Session } from "../../src/session/session"
import { testEffectIsolatedShared } from "../lib/effect"
import { receiptLayer } from "./fixtures/receipt-layer"

const it = testEffectIsolatedShared(receiptLayer(":memory:") as unknown as Layer.Layer<any, any, never>)

it.instance("deletes orphaned canonical mail, presence, and scheduled tasks but preserves live sessions", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const store = yield* S2SStore.Service
    const sessions = yield* Session.Service
    const sender = yield* sessions.create({ title: "Live sender" })
    const target = yield* sessions.create({ title: "Live recipient" })

    yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, delivered_at)
      VALUES ('orphan_target', 'ses_deleted_target', ${sender.id}, 'sender', '{}', 1, NULL),
        ('orphan_sender', ${target.id}, 'ses_deleted_sender', 'sender', '{}', 1, NULL),
        ('orphan_terminal', 'ses_deleted_target', ${sender.id}, 'sender', '{}', 1, 2),
        ('live_mail', ${target.id}, ${sender.id}, 'sender', '{}', 1, NULL)`)
    yield* db.run(sql`INSERT INTO s2s_presence (session_id, owner_id, capability_version, heartbeat_at)
      VALUES ('ses_deleted_presence', 'old', 1, 1), (${target.id}, 'live', 1, 1)`)
    yield* db.run(sql`INSERT INTO scheduled_task (id, parent_session_id, child_session_id, dispatch_inputs, due_at, admitted_at, state)
      VALUES ('orphan_task', 'ses_deleted_parent', ${sender.id}, '{}', 1, 1, 'queued'),
        ('live_task', ${target.id}, 'ses_deleted_child', '{}', 1, 1, 'queued')`)

    yield* store.deleteOrphaned()

    expect((yield* db.all<{ id: string }>(sql`SELECT id FROM s2s_message WHERE id IN ('orphan_target', 'orphan_sender', 'orphan_terminal', 'live_mail') ORDER BY id`)).map((row) => row.id)).toEqual(["live_mail"])
    expect((yield* db.all<{ session_id: string }>(sql`SELECT session_id FROM s2s_presence WHERE owner_id IN ('old', 'live') ORDER BY owner_id`)).map((row) => row.session_id)).toEqual([target.id])
    expect((yield* db.all<{ id: string }>(sql`SELECT id FROM scheduled_task WHERE id IN ('orphan_task', 'live_task') ORDER BY id`)).map((row) => row.id)).toEqual(["live_task"])
  }),
)
