import { expect, test } from "bun:test"
import { getTableColumns } from "drizzle-orm"
import { S2SInboxTable, S2SSentTable } from "../src/database/s2s.sql"

test("declares canonical receipt and presence without changing legacy ingress", async () => {
  const { S2SMessageTable, S2SPresenceTable } = await import("../src/database/s2s.sql")
  expect(S2SMessageTable).toBeDefined()
  expect(getTableColumns(S2SMessageTable).delivered_at.name).toBe("delivered_at")
  expect(getTableColumns(S2SMessageTable).transcript_message_id.name).toBe("transcript_message_id")
  const messageColumns = Object.values(getTableColumns(S2SMessageTable)).map((column) => column.name)
  for (const name of ["id", "target_session_id", "from_session_id", "from_slug", "capsule", "sent_at", "expired_at", "superseded_at", "supersedes", "expires_at", "deliver_at"]) expect(messageColumns).toContain(name)
  const presenceColumns = Object.values(getTableColumns(S2SPresenceTable)).map((column) => column.name)
  for (const name of ["session_id", "owner_id", "capability_version", "heartbeat_at"]) {
    expect(presenceColumns).toContain(name)
  }
  expect(Object.values(getTableColumns(S2SPresenceTable)).find((column) => column.name === "capability_version")?.dataType).toBe("number int53")
  expect(getTableColumns(S2SInboxTable).drained_at.name).toBe("drained_at")
  expect(getTableColumns(S2SSentTable).dedupe_key.name).toBe("dedupe_key")
})

test("declares scheduled task dispatch inputs and claim state", async () => {
  expect(await Bun.file(new URL("../src/database/scheduled-task.sql.ts", import.meta.url)).exists()).toBe(true)
  const { ScheduledTaskTable } = await import("../src/database/scheduled-task.sql")
  const columns = Object.values(getTableColumns(ScheduledTaskTable)).map((column) => column.name)
  for (const name of ["id", "parent_session_id", "child_session_id", "slug", "dispatch_inputs", "due_at", "admitted_at", "claimed_at", "claim_owner", "started_at", "state", "failure_reason"]) {
    expect(columns).toContain(name)
  }
})
