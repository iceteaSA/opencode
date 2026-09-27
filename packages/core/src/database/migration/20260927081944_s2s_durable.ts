import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260927081944_s2s_durable",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`s2s_message\` (
          \`id\` text PRIMARY KEY,
          \`target_session_id\` text NOT NULL,
          \`from_session_id\` text NOT NULL,
          \`from_slug\` text NOT NULL,
          \`capsule\` text NOT NULL,
          \`sent_at\` integer NOT NULL,
          \`delivered_at\` integer,
          \`expired_at\` integer,
          \`superseded_at\` integer,
          \`supersedes\` text,
          \`expires_at\` integer,
          \`deliver_at\` integer,
          \`transcript_message_id\` text
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`s2s_presence\` (
          \`session_id\` text PRIMARY KEY,
          \`owner_id\` text NOT NULL,
          \`capability_version\` integer NOT NULL,
          \`heartbeat_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`scheduled_task\` (
          \`id\` text PRIMARY KEY,
          \`parent_session_id\` text NOT NULL,
          \`child_session_id\` text,
          \`slug\` text,
          \`dispatch_inputs\` text NOT NULL,
          \`due_at\` integer NOT NULL,
          \`admitted_at\` integer NOT NULL,
          \`claimed_at\` integer,
          \`claim_owner\` text,
          \`started_at\` integer,
          \`state\` text NOT NULL,
          \`failure_reason\` text
        );
      `)
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS \`s2s_message_pending\` ON \`s2s_message\` (\`target_session_id\`,\`deliver_at\`,\`delivered_at\`);`,
      )
      yield* tx.run(`CREATE INDEX IF NOT EXISTS \`s2s_message_sender_sent\` ON \`s2s_message\` (\`from_session_id\`,\`sent_at\`);`)
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS \`scheduled_task_due\` ON \`scheduled_task\` (\`parent_session_id\`,\`due_at\`,\`state\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
