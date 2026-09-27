import { expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { MessageID, SessionID } from "../../src/session/schema"

test("decodes pre-origin user rows without attributing them to an operator", () => {
  const legacy = {
    id: MessageID.ascending(),
    sessionID: SessionID.make("ses_origin_test"),
    role: "user",
    time: { created: Date.now() },
    agent: "build",
    model: { providerID: "test", modelID: "test-model" },
  }
  const decoded = Schema.decodeUnknownSync(SessionV1.User)(legacy)
  expect(decoded.origin).toBeUndefined()
})

test("every persisted V1 user producer declares its origin", async () => {
  const root = new URL("../../src/", import.meta.url)
  const excluded = new Set([
    "session/message-v2.ts",
    "server/routes/instance/httpapi/handlers/project-copy.ts",
    "agent/agent.ts",
  ])
  const expected = new Map([
    ["session/prompt.ts", 5],
    ["session/compaction.ts", 3],
    ["s2s/delivery.ts", 2],
    ["session/interrupt.ts", 1],
    ["tool/message.ts", 1],
    ["tool/plan.ts", 1],
  ])
  const found = new Map<string, number>()
  for await (const file of new Bun.Glob("**/*.ts").scan({ cwd: root.pathname, onlyFiles: true })) {
    if (excluded.has(file)) continue
    const text = await Bun.file(new URL(file, root)).text()
    for (const match of text.matchAll(/^\s*role:\s*"user"/gm)) {
      const before = text.slice(Math.max(0, match.index - 280), match.index)
      const after = text.slice(match.index, match.index + 360)
      if (file === "session/prompt.ts" && before.includes("messages:")) continue
      if (file === "session/compaction.ts" && after.includes("content: [")) continue
      found.set(file, (found.get(file) ?? 0) + 1)
      expect(before + after).toMatch(/\borigin:\s*(?:input\.origin|"(?:operator|subagent|plugin|peer|s2s|wake|compaction|system)"|[^\n]*\?[^\n]*)/)
    }
  }
  expect(found).toEqual(expected)
  const task = await Bun.file(new URL("tool/task.ts", root)).text()
  expect(task.slice(task.indexOf("const runAttempt"), task.indexOf("evidence.finalText = result.parts"))).toContain('origin: "subagent"')
  expect(task.slice(task.indexOf("const inject ="), task.indexOf("if (Exit.isFailure(admission))"))).toContain('origin: "subagent"')
})
