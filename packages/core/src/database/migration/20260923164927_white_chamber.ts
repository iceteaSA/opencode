import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260923164927_white_chamber",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`s2s_sent\` (
          \`dedupe_key\` text PRIMARY KEY,
          \`recipient_session_id\` text NOT NULL,
          \`inbox_id\` text,
          \`time_created\` integer NOT NULL
        );
      `)
      yield* tx.run(`CREATE INDEX \`s2s_sent_recipient\` ON \`s2s_sent\` (\`recipient_session_id\`,\`time_created\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
