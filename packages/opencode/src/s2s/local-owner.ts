import { Effect, Option } from "effect"
import { Messaging } from "@/messaging"
import { Session } from "@/session/session"
import { Marker } from "@/session/marker"
import { SessionID } from "@/session/schema"

export const isLocalForLatestUser = Effect.fn("S2SLocalOwner.isLocalForLatestUser")(function* (
  sessionID: SessionID,
  messaging: Messaging.Interface,
  sessions: Session.Interface,
) {
  if (!(yield* messaging.isLocal(sessionID))) return false
  if ((yield* messaging.localMessageID(sessionID)) === undefined) return true
  const latest = yield* sessions.findMessage(
    sessionID,
    (message) => message.info.role === "user" && !Marker.isMachineGeneratedUser(message.parts),
  )
  if (Option.isNone(latest)) return true
  return yield* messaging.isLocalFor(sessionID, latest.value.info.id)
})
