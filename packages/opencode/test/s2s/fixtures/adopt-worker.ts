import { Effect } from "effect"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { MessageID, PartID, SessionID } from "../../../src/session/schema"
import { S2SStore } from "../../../src/s2s/store"
import { Session } from "../../../src/session/session"
import { receiptLayer } from "./receipt-layer"

const [filename, barrier, ready, inputID, mode, peerID] = process.argv.slice(2)
if (!filename || !barrier || !ready || !inputID) throw new Error("Missing adoption worker arguments")

const result = await Effect.runPromise(Effect.gen(function* () {
  const store = yield* S2SStore.Service
  const sessions = yield* Session.Service
  const target = SessionID.make(peerID ?? "ses_atomic_receipt_target")
  if (mode === "canonical-presence") yield* store.heartbeat(target, String(process.pid), Date.now())
  yield* Effect.promise(() => Bun.write(ready, String(process.pid)))
  for (let attempt = 0; !(yield* Effect.promise(() => Bun.file(barrier).exists())); attempt++) {
    if (attempt >= 1_000) throw new Error("Adoption barrier timed out")
    yield* Effect.promise(() => Bun.sleep(10))
  }
  const id = inputID === "auto" ? (yield* Effect.promise(() => Bun.file(barrier).text())).trim() : inputID
  const row = mode === "canonical" || mode === "canonical-presence"
    ? (yield* store.pendingForSession(target, Date.now())).find((item) => item.id === id)
    : yield* store.adoptLegacy(id)
  if (!row) return "lost"
  return (yield* store.receipt({
    id, target: row.targetSessionID, deliveredAt: Date.now(), sessions,
    buildTranscript: (created) => {
      const message = {
        id: MessageID.make(`msg_${id}`), sessionID: row.targetSessionID, role: "user" as const,
        origin: "s2s" as const, time: { created }, agent: "build",
        model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
      }
      const parts = [
        { id: PartID.make(`prt_${id}_frame`), messageID: message.id, sessionID: row.targetSessionID, type: "text" as const, text: "body", synthetic: true },
        { id: PartID.make(`prt_${id}_marker`), messageID: message.id, sessionID: row.targetSessionID, type: "text" as const, text: "✉ body", synthetic: false },
      ]
      return { message, parts }
    },
  })) ? "won" : "lost"
}).pipe(Effect.provide(receiptLayer(filename)), Effect.scoped))

console.log(result)
