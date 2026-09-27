import { Marker } from "@/session/marker"

export function render(input: { name: string; sessionID: string; time: number; sent: number; body: string; retracts?: string | null }) {
  const marker = { kind: "inbox", from: input.name, sessionId: input.sessionID } as const
  return {
    frame: `<external-context source="sibling-session" name="${Marker.escapeAttr(input.name)}" session="${Marker.escapeAttr(input.sessionID)}" time="${input.time}" sent="${Marker.escapeAttr(new Date(input.sent).toISOString())}"${input.retracts ? ` retracts="${Marker.escapeAttr(input.retracts)}"` : ""}>\n${Marker.escape(input.body)}\n</external-context>`,
    marker: Marker.render({ ...marker, body: input.body }),
    metadata: Marker.metadataFor(marker),
  }
}

export * as S2SFrame from "./frame"
