import { index, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"

export const ScheduledTaskTable = sqliteTable(
  "scheduled_task",
  {
    id: text().primaryKey(),
    parent_session_id: text().notNull(),
    child_session_id: text(),
    slug: text(),
    dispatch_inputs: text().notNull(),
    due_at: integer().notNull(),
    admitted_at: integer().notNull(),
    claimed_at: integer(),
    claim_owner: text(),
    started_at: integer(),
    state: text().notNull(),
    failure_reason: text(),
  },
  (table) => [index("scheduled_task_due").on(table.parent_session_id, table.due_at, table.state)],
)
