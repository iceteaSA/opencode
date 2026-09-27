import { Effect, Option } from "effect"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Messaging } from "@/messaging"
import { Session } from "@/session/session"
import { MessageID, PartID } from "@/session/schema"
import { decodeCapsuleOption, type S2SCapsule } from "@/s2s/capsule"
import { S2SFrame } from "@/s2s/frame"
import { S2SStore } from "@/s2s/store"
import { SessionV1 } from "@opencode-ai/core/v1/session"

export const admit = Effect.fn("S2SDelivery.admit")(function* (row: S2SStore.MessageRow, lastUser: SessionV1.User, sessions: Session.Interface, expired?: string[]) {
  const capsule = decodeCapsuleOption(row.capsule)
  if (Option.isNone(capsule)) {
    yield* Effect.logWarning("S2SDelivery: malformed capsule row skipped", { rowID: row.id })
    return false
  }
  const store = yield* S2SStore.Service
  const events = yield* EventV2Bridge.Service
  const state = yield* store.resolvePending(row.id, row.targetSessionID, Date.now())
  if (state === "expired") expired?.push(row.id)
  if (state !== "pending") return false
  const name = capsule.value.sender_name ?? row.fromSlug
  const delivered = yield* store.receipt({
    id: row.id,
    target: row.targetSessionID,
    deliveredAt: Date.now(),
    sessions,
    buildTranscript: (created) => buildTranscript(row, lastUser, capsule.value, name, created),
  })
  if (!delivered) {
    if (row.expiresAt !== null && row.expiresAt <= Date.now() && (yield* store.resolvePending(row.id, row.targetSessionID, Date.now())) === "expired") expired?.push(row.id)
    return false
  }
  // UI notification follows the committed receipt; it never projects transcript rows again.
  yield* events.publish(Messaging.S2sDelivered, {
    target: row.targetSessionID,
    from: row.fromSessionID,
    fromName: name,
    body: capsule.value.body,
  }).pipe(Effect.catchCause((cause) => Effect.logWarning("s2s receipt notification failed", { cause })))
  return true
})

export const summarizeExpired = Effect.fn("S2SDelivery.summarizeExpired")(function* (
  target: S2SStore.MessageRow["targetSessionID"],
  ids: string[],
  lastUser: SessionV1.User,
  sessions: Session.Interface,
) {
  if (ids.length === 0) return false
  const created = Date.now()
  const message: SessionV1.User = {
    id: MessageID.ascending(),
    sessionID: target,
    role: "user",
    origin: "s2s",
    time: { created },
    agent: lastUser.agent,
    model: lastUser.model,
  }
  yield* sessions.updateMessage(message)
  yield* sessions.updatePart({
    id: PartID.ascending(),
    messageID: message.id,
    sessionID: target,
    type: "text",
    text: `${ids.length} expired s2s message${ids.length === 1 ? "" : "s"} (${ids.join(", ")}) were not delivered; their original instructions were not shown.`,
  })
  return true
})

function buildTranscript(row: S2SStore.MessageRow, lastUser: SessionV1.User, capsule: S2SCapsule, name: string, created: number) {
  const message: SessionV1.User = {
    id: MessageID.make(`msg_${row.id}`),
    sessionID: row.targetSessionID,
    role: "user",
    origin: "s2s",
    time: { created },
    agent: lastUser.agent,
    model: lastUser.model,
  }
  const rendered = S2SFrame.render({ name, sessionID: String(row.fromSessionID), time: created, sent: row.sentAt, body: capsule.body, retracts: row.supersedes })
  const parts: SessionV1.TextPart[] = [
    {
      id: PartID.make(`prt_${row.id}_frame`),
      messageID: message.id,
      sessionID: message.sessionID,
      type: "text",
      text: rendered.frame,
      synthetic: true,
    },
    {
      id: PartID.make(`prt_${row.id}_marker`),
      messageID: message.id,
      sessionID: message.sessionID,
      type: "text",
      text: rendered.marker,
      synthetic: false,
      metadata: rendered.metadata,
    },
  ]
  return { message, parts }
}

export * as S2SDelivery from "./delivery"
