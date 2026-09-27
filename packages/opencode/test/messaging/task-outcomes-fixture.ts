import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import path from "path"
import { Agent } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Command } from "../../src/command"
import { Config } from "@/config/config"
import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { Env } from "../../src/env"
import { EventV2Bridge } from "@/event-v2-bridge"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Format } from "../../src/format"
import { Git } from "../../src/git"
import { Image } from "@/image/image"
import { Instruction } from "@/session/instruction"
import { Interrupt } from "@/session/interrupt"
import { LLM } from "@/session/llm"
import { LSP } from "@/lsp/lsp"
import { MCP } from "../../src/mcp"
import { Messaging } from "../../src/messaging"
import { ModelV2 } from "@opencode-ai/core/model"
import { Permission } from "@/permission"
import { Plugin } from "../../src/plugin"
import { Provider } from "@/provider/provider"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Question } from "@/question"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Session } from "@/session/session"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionCompaction } from "@/session/compaction"
import { SessionProcessor } from "@/session/processor"
import { SessionPrompt } from "@/session/prompt"
import { SessionRevert } from "@/session/revert"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { SessionSummary } from "@/session/summary"
import { S2SStore } from "../../src/s2s/store"
import { Skill } from "../../src/skill"
import { Snapshot } from "@/snapshot"
import { SystemPrompt } from "@/session/system"
import { Todo } from "@/session/todo"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { TestInstance } from "../fixture/fixture"
import { testEffectIsolatedShared } from "../lib/effect"
import { TestLLMServer } from "../lib/llm-server"
import { TaskOutcomes } from "../../src/tool/task-outcomes"
import { ScheduledTaskStore } from "../../src/tool/scheduled-task-store"

const summaryStub = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const mcpStub = Layer.succeed(
  MCP.Service,
  MCP.Service.of({
    status: () => Effect.succeed({}),
    clients: () => Effect.succeed({}),
    tools: () => Effect.succeed({}),
    prompts: () => Effect.succeed({}),
    resources: () => Effect.succeed({}),
    add: () => Effect.succeed({ status: { status: "disabled" as const } }),
    connect: () => Effect.void,
    disconnect: () => Effect.void,
    getPrompt: () => Effect.succeed(undefined),
    readResource: () => Effect.succeed(undefined),
    startAuth: () => Effect.die("unexpected MCP auth in wake-real-path test"),
    authenticate: () => Effect.die("unexpected MCP auth in wake-real-path test"),
    finishAuth: () => Effect.die("unexpected MCP auth in wake-real-path test"),
    removeAuth: () => Effect.void,
    supportsOAuth: () => Effect.succeed(false),
    hasStoredTokens: () => Effect.succeed(false),
    instructions: () => Effect.succeed([]),
    resourceTemplates: () => Effect.succeed({}),
    getAuthStatus: () => Effect.succeed("not_authenticated" as const),
  }),
)

const lspStub = Layer.succeed(
  LSP.Service,
  LSP.Service.of({
    init: () => Effect.void,
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    hover: () => Effect.succeed(undefined),
    definition: () => Effect.succeed([]),
    references: () => Effect.succeed([]),
    implementation: () => Effect.succeed([]),
    documentSymbol: () => Effect.succeed([]),
    workspaceSymbol: () => Effect.succeed([]),
    prepareCallHierarchy: () => Effect.succeed([]),
    incomingCalls: () => Effect.succeed([]),
    outgoingCalls: () => Effect.succeed([]),
  }),
)

const statusNode = LayerNode.make({
  service: SessionStatus.Service,
  layer: SessionStatus.layer,
  deps: [EventV2Bridge.node],
})
const runStateNode = LayerNode.make({
  service: SessionRunState.Service,
  layer: SessionRunState.layer,
  deps: [BackgroundJob.node, statusNode, EffectFlock.node],
})

// experimentalS2S stays OFF here — wake-on-message (task.ts's wake_on_message
// param → Messaging.setWakePolicy) is independent of the s2s cross-process
// lifecycle. experimentalAgentMessaging is ON so runLoop's turn-boundary
// drain block (prompt.ts, gated on that flag) actually consumes the queued
// inbox item once the wake fires.
const wakeFlags = RuntimeFlags.layer({
  experimentalEventSystem: true,
  experimentalAgentMessaging: true,
  experimentalBackgroundSubagents: true,
  experimentalS2S: false,
})

export const providerCfgFor = (url: string): Partial<ConfigV1.Info> => ({
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: false,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: { apiKey: "test-key", baseURL: url },
    },
  },
})

function makeRunLoopLayer() {
  const root = LayerNode.group([
    Session.node,
    SessionProjector.node,
    Snapshot.node,
    LLM.node,
    Env.node,
    Agent.node,
    Command.node,
    Permission.node,
    Plugin.node,
    Config.node,
    Provider.node,
    FSUtil.node,
    BackgroundJob.node,
    TaskOutcomes.node,
    ScheduledTaskStore.node,
    Database.node,
    EventV2Bridge.node,
    Interrupt.node,
    S2SStore.node,
    Messaging.node,
    Todo.node,
    Question.node,
    ToolRegistry.node,
    Skill.node,
    CrossSpawnSpawner.node,
    Git.node,
    Ripgrep.node,
    Format.node,
    Truncate.node,
    SessionProcessor.node,
    Image.node,
    SessionCompaction.node,
    SessionRevert.node,
    Instruction.node,
    SystemPrompt.node,
    SessionPrompt.node,
    statusNode,
    runStateNode,
  ])
  const replacements: LayerNode.Replacements = [
    [SessionSummary.node, summaryStub],
    [LSP.node, lspStub],
    [MCP.node, mcpStub],
    [RuntimeFlags.node, wakeFlags],
    [SessionStatus.node, statusNode],
    [SessionRunState.node, runStateNode],
  ]
  return LayerNode.compile(root, replacements)
}

export const caseFor = () => testEffectIsolatedShared(Layer.mergeAll(TestLLMServer.layer, makeRunLoopLayer()))

const writeConfig = Effect.fn("WakeRealPath.writeConfig")(function* (dir: string, config: Partial<ConfigV1.Info>) {
  const fs = yield* FSUtil.Service
  yield* fs.writeWithDirs(
    path.join(dir, "opencode.json"),
    JSON.stringify({ $schema: "https://opencode.ai/config.json", ...config }),
  )
})

export const useServerConfig = Effect.fn("WakeRealPath.useServerConfig")(function* (
  config: (url: string) => Partial<ConfigV1.Info>,
) {
  const { directory: dir } = yield* TestInstance
  const llm = yield* TestLLMServer
  yield* writeConfig(dir, config(llm.url))
  return { dir, llm }
})
