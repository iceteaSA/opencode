import { describe, expect } from "bun:test"
import { sql } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { Agent } from "../../src/agent/agent"
import { Config } from "../../src/config/config"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Messaging } from "../../src/messaging"
import { Session } from "../../src/session/session"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { S2SStore, DEDUPE_WINDOW_MS } from "../../src/s2s/store"
import { pollOnceImpl } from "../../src/s2s/poller"
import { S2STool } from "../../src/tool/s2s"
import { Truncate } from "../../src/tool/truncate"
import { testEffectIsolatedShared } from "../lib/effect"

const layer = LayerNode.compile(
  LayerNode.group([
    Database.node,
    Session.node,
    SessionProjector.node,
    EventV2Bridge.node,
    Config.node,
    S2SStore.node,
    Messaging.node,
    Agent.node,
    CrossSpawnSpawner.node,
    Truncate.node,
  ]),
  [
    [Database.node, Database.layerFromPath(":memory:")],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true, experimentalAgentMessaging: true, experimentalS2S: true })],
  ],
)

const it = testEffectIsolatedShared(layer as unknown as Layer.Layer<any, any, never>)
const ctxFor = (sessionID: SessionID) => ({
  sessionID,
  messageID: MessageID.ascending(),
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

describe("S2S sent history and owner presence", () => {
  it.instance("retains the sender's delivered receipts after dedupe pruning without leaking other senders", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const sender = yield* sessions.create({ title: "sender" })
      const other = yield* sessions.create({ title: "other" })
      const peer = yield* sessions.create({ title: "peer" })
      yield* store.insertAllow(sender.id, peer.id)
      yield* store.insertAllow(other.id, peer.id)
      const def = yield* (yield* S2STool).init()
      yield* def.execute({ command: "msg", target: peer.id, body: "mine" }, ctxFor(sender.id))
      yield* def.execute({ command: "msg", target: peer.id, body: "private-other" }, ctxFor(other.id))
      const pending = yield* def.execute({ command: "sent", target: peer.id }, ctxFor(sender.id))
      expect(pending.metadata.history).toHaveLength(1)
      expect(pending.metadata.history?.[0]).toMatchObject({ state: "pending", target: peer.id, deliveredAt: null })
      expect(pending.output).not.toContain("private-other")
      const id = pending.metadata.history![0]!.id
      const messageID = MessageID.make(`msg_${id}`)
      const deliveredAt = Date.now()
      expect(yield* store.receipt({ id, target: peer.id, deliveredAt, sessions, buildTranscript: (created) => ({
        message: {
          id: messageID,
          sessionID: peer.id,
          role: "user",
          origin: "s2s",
          agent: "build",
          model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") },
          time: { created },
        },
        parts: [{ id: PartID.make(`prt_${id}_frame`), messageID, sessionID: peer.id, type: "text", text: "receipt" }],
      }) })).toBe(true)
      yield* db.run(sql`DELETE FROM s2s_sent WHERE time_created <= ${Date.now() + DEDUPE_WINDOW_MS}`)
      const delivered = yield* def.execute({ command: "sent" }, ctxFor(sender.id))
      expect(delivered.metadata.history).toHaveLength(1)
      expect(delivered.metadata.history?.[0]).toMatchObject({ id, state: "delivered", deliveredAt })
      expect(delivered.output).toContain(new Date(deliveredAt).toISOString())
      expect(delivered.output).not.toContain("private-other")
      const sentAt = Date.now() + 100
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, expired_at) VALUES ('caps_expired', ${peer.id}, ${sender.id}, 'sender', '{}', ${sentAt}, ${sentAt})`)
      yield* db.run(sql`INSERT INTO s2s_message (id, target_session_id, from_session_id, from_slug, capsule, sent_at, superseded_at) VALUES ('caps_superseded', ${peer.id}, ${sender.id}, 'sender', '{}', ${sentAt}, ${sentAt})`)
      const recent = yield* store.sentHistory(sender.id, peer.id, 2)
      expect(recent.map((row) => [row.id, row.state])).toEqual([["caps_superseded", "superseded"], ["caps_expired", "expired"]])
      expect((yield* store.sentHistory(other.id, peer.id)).every((row) => row.id !== id)).toBe(true)
    }),
  )

  it.instance("reports running for a fresh owner and unknown for stale, absent or lower-version presence", () =>
    Effect.gen(function* () {
      const store = yield* S2SStore.Service
      const sessions = yield* Session.Service
      const peer = yield* sessions.create({ title: "presence-peer" })
      const caller = yield* sessions.create({ title: "presence-caller" })
      yield* store.insertAllow(caller.id, peer.id)
      const def = yield* (yield* S2STool).init()
      const now = Date.now()
      expect((yield* store.peerActivity([peer.id], now)).get(peer.id)).toBe("unknown")
      expect((yield* def.execute({ command: "list" }, ctxFor(caller.id))).metadata.peers?.[0]?.activity).toBe("unknown")
      yield* store.heartbeat(peer.id, "owner-A", now)
      expect((yield* store.peerActivity([peer.id], now)).get(peer.id)).toBe("running")
      expect((yield* def.execute({ command: "list" }, ctxFor(caller.id))).metadata.peers?.[0]?.activity).toBe("running")
      expect((yield* store.peerActivity([peer.id], now + 15_001)).get(peer.id)).toBe("unknown")
      yield* store.heartbeat(peer.id, "owner-B", now + 1)
      yield* store.clearPresence(peer.id, "owner-A")
      expect((yield* store.peerActivity([peer.id], now + 1)).get(peer.id)).toBe("running")
      const { db } = yield* Database.Service
      const rows = yield* db.all<{ owner_id: string }>(sql`SELECT owner_id FROM s2s_presence WHERE session_id = ${peer.id}`)
      expect(rows[0]?.owner_id).toBe("owner-B")
      yield* db.run(sql`UPDATE s2s_presence SET capability_version = 0 WHERE session_id = ${peer.id}`)
      expect((yield* store.peerActivity([peer.id], now + 1)).get(peer.id)).toBe("unknown")
    }),
  )

  it.instance("an evicted old owner neither heartbeats nor deletes a new owner's presence", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const messaging = yield* Messaging.Service
      const store = yield* S2SStore.Service
      const { db } = yield* Database.Service
      const chat = yield* sessions.create({ title: "handover" })
      yield* pollOnceImpl()
      expect((yield* db.all<{ owner_id: string }>(sql`SELECT owner_id FROM s2s_presence WHERE session_id = ${chat.id}`))).toHaveLength(0)
      const oldID = MessageID.ascending()
      yield* sessions.updateMessage({ id: oldID, sessionID: chat.id, role: "user", origin: "operator", agent: "build", model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }, time: { created: Date.now() } })
      yield* sessions.updatePart({ id: PartID.ascending(), messageID: oldID, sessionID: chat.id, type: "text", text: "owner A" })
      yield* messaging.registerLocal(chat.id, oldID)
      yield* pollOnceImpl()
      expect((yield* store.peerActivity([chat.id], Date.now())).get(chat.id)).toBe("running")
      const newID = MessageID.ascending()
      yield* sessions.updateMessage({ id: newID, sessionID: chat.id, role: "user", origin: "operator", agent: "build", model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }, time: { created: Date.now() + 1 } })
      yield* sessions.updatePart({ id: PartID.ascending(), messageID: newID, sessionID: chat.id, type: "text", text: "owner B" })
      yield* store.heartbeat(chat.id, "owner-B", Date.now() + 1)
      yield* pollOnceImpl()
      const rows = yield* db.all<{ owner_id: string }>(sql`SELECT owner_id FROM s2s_presence WHERE session_id = ${chat.id}`)
      expect(rows[0]?.owner_id).toBe("owner-B")
      expect(yield* messaging.isLocal(chat.id)).toBe(false)
    }),
  )
})
