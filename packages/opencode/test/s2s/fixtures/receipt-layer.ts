import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Agent } from "../../../src/agent/agent"
import { Config } from "../../../src/config/config"
import { EventV2Bridge } from "../../../src/event-v2-bridge"
import { RuntimeFlags } from "../../../src/effect/runtime-flags"
import { Messaging } from "../../../src/messaging"
import { Session } from "../../../src/session/session"
import { S2SStore } from "../../../src/s2s/store"
import { Truncate } from "../../../src/tool/truncate"

export function receiptLayer(filename: string) {
  return LayerNode.compile(
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
      [Database.node, Database.layerFromPath(filename)],
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true, experimentalAgentMessaging: true, experimentalS2S: true })],
    ],
  )
}
