import { Effect, Option } from "effect"
import { Messaging } from "../messaging"
import { Session } from "@/session/session"
import { SessionID } from "../session/schema"

export namespace SubagentTarget {
  export const resolve = (
    sessions: Session.Interface,
    messaging: Messaging.Interface,
    taskId: string,
    callerSessionID: SessionID,
  ) =>
    Effect.gen(function* () {
      const childID =
        taskId.startsWith("ses_") ? Option.some(SessionID.make(taskId)) : yield* messaging.resolveSlug(taskId)
      if (Option.isNone(childID)) return { kind: "not_found" as const }

      // Slugs are process-global, so resolution must not bypass the descendant authorization below.
      const child = yield* sessions.get(childID.value).pipe(Effect.option)
      if (Option.isNone(child) || child.value.id === callerSessionID) return { kind: "not_found" as const }

      let ancestorID = child.value.parentID
      // Parent links should be acyclic, but corrupted data must not trap a request forever.
      for (let hop = 0; hop < 64; hop++) {
        if (!ancestorID) return { kind: "not_found" as const }
        if (ancestorID === callerSessionID) return { kind: "resolved" as const, childID: childID.value }
        const ancestor = yield* sessions.get(ancestorID).pipe(Effect.option)
        if (Option.isNone(ancestor)) return { kind: "not_found" as const }
        ancestorID = ancestor.value.parentID
      }
      return { kind: "not_found" as const }
    })
}
