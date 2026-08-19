# Architecture

## Pattern Overview

**Overall:** Effect-native, Location-scoped, multi-package monorepo with HttpApi-typed boundaries.

OpenCode is a single durable-conversation runtime (`SessionV2`) wrapped by typed HTTP (`Protocol`/`Server`), an LLM routing package (`llm`), a Location-aware service mesh (`core/location-services`), and a set of clients (`client`, `sdk-next`) and surfaces (`tui`, `app`, `web`, `desktop`, `cli`, `slack`, `enterprise`).

**Key Characteristics:**
- Runtime dependencies are strictly directed: `schema` → `core` + `protocol` → `server`; `client` only depends on `schema` + `protocol`; `sdk-next` composes `client` + `core` + `server`.
- All public values are Effect `Schema` (`Schema.Struct`/`Schema.TaggedErrorClass`) so the same definitions are runtime-validated and TS-typed.
- The HTTP surface is one `HttpApi` (`packages/server/src/api.ts`); `client` generates zero-Effect Promise and rich-Effect emitters from it.
- Durable Session storage (V2 Drizzle tables in `packages/core/src/database/`) is the single source of truth; `SessionV2.prompt(...)` admits one `session_input` row before scheduling advisory `SessionExecution.wake(sessionID)`.
- `System Context` is composed of typed, refreshable sources keyed by stable namespaced identifiers; one `Context Epoch` owns one immutable `Baseline System Context` and a `Snapshot` of last-admitted values.
- Services are split into **process-global nodes** (`makeGlobalNode`) and **Location-scoped nodes** (`makeLocationNode`) — `LocationServiceMap` (`packages/core/src/location-service-map.ts`) materializes a fresh per-`Location.Ref` Effect Layer so each working directory gets isolated ToolRegistry / FileSystem / Plugin / Skill state.
- `Embedded OpenCode` (`packages/sdk-next/src/opencode.ts`) executes Server's assembled `HttpRouter` in memory via the same handlers; only the `HttpClient` transport differs.

## Layers

**Schema (`packages/schema/`):**
- Purpose: Pure Effect `Schema` definitions for all public domain types and events.
- Location: `packages/schema/src/`
- Contains: `Session`, `SessionInput`, `SessionMessage`, `SessionEvent`, `MessagingEvent`, `InterruptEvent`, `Prompt`, `Provider`, `Model`, `Location`, `Project`, `Skill`, `Permission`, `TaskEvent`, `Event` + manifests (`durable-event-manifest`, `event-manifest`, `public-event-manifest`).
- Depends on: `effect` only.
- Used by: every other package.

**Protocol (`packages/protocol/`):**
- Purpose: Compose `Schema` values into HTTP shapes; declare API groups, paths, errors, and middleware placement.
- Location: `packages/protocol/src/`
- Contains: `api.ts` (HttpApi builder), `groups/*` (one HttpApiGroup per resource: agent, command, credential, event, file system, health, integration, location, message, model, permission, project-copy, provider, pty, question, reference, session, skill), `middleware/authorization`, `middleware/schema-error`.
- Depends on: `schema`.
- Used by: `server`, `client` (codegen), `opencode` (handler composition).

**Server (`packages/server/`):**
- Purpose: Concrete handlers, CORS, Location + session-location middleware, lifecycle, WebSocket tracker, OpenAPI document, error layers.
- Location: `packages/server/src/`
- Contains: `api.ts` (binds Location + SessionLocation middleware keys), `handlers.ts`, `handlers/*` (per-resource handlers), `location.ts`, `middleware/*` (authorization, schema-error, session-location), `pty-environment.ts`, `cors.ts`, `routes.ts`.
- Depends on: `core`, `protocol`.
- Used by: `opencode` (networked) and `sdk-next` (embedded).

**Core (`packages/core/`):**
- Purpose: Domain behavior — Session V2, LLM projection, tools, permissions, filesystem, plugins, PTY, system context, projects, snapshots.
- Location: `packages/core/src/`
- Contains: `session/` (v2 session + runner + execution + message diff storage `message-diff.ts`), `tool/`, `permission/`, `system-context/`, `database/` (WAL mode SQLite + `message_diff` side table), `filesystem/`, `pty/`, `plugin/`, `project/`, `event/`, `instruction-context.ts`, `tool-output-store.ts`, `location-services.ts`, `location-service-map.ts`, `effect/` (AppNode/LayerNode plumbing), `control-plane/`, `model.ts`, `provider.ts`, `github-copilot/`, `oauth/`, `integration/`, `v1/` (legacy V1 implementation).
- Depends on: `schema`, `llm`, `plugin`, `effect-drizzle-sqlite`, `effect-sqlite-node`.
- Used by: `opencode`, `server`, `sdk-next`.

**LLM (`packages/llm/`):**
- Purpose: Provider-routing facade with protocol adapters.
- Location: `packages/llm/src/`
- Contains: `route/` (Endpoint/Auth/Framing/Protocol/Transport/Client/Executor), `providers/` (Anthropic, OpenAI, OpenAI-compatible, Google Vertex, Bedrock, GitHub Copilot, OpenRouter, xAI, Azure, Cloudflare), `protocols/` (Anthropic Messages, OpenAI Chat, OpenAI Responses, OpenAI-compatible Chat, Bedrock Converse + Event Stream, Gemini), `schema/`, `tool.ts`, `tool-runtime.ts`, `cache-policy.ts`, `provider-error.ts`.
- Depends on: `schema`, `effect`.
- Used by: `core` (provider-turn runner), `opencode` (legacy V1 stream wrapper).

**Client (`packages/client/`):**
- Purpose: Generated HTTP client SDKs derived from `Protocol` + `Server` `HttpApi`.
- Location: `packages/client/src/`
- Contains: `contract.ts`, `index.ts` (Promise root), `effect.ts` (Effect `/effect` entry), `generated/`, `generated-effect/`.
- Depends on: `schema`, `protocol` (no Core / Server).
- Used by: `opencode` CLI, `sdk-next`, third-party consumers.

**SDK-Next (`packages/sdk-next/`):**
- Purpose: Embedded OpenCode host — assemble the Server `HttpRouter` in memory and bind it to an in-memory `HttpClient` against the same handlers; expose `ApplicationTools` and `PermissionSaved` for in-process consumers.
- Location: `packages/sdk-next/src/`
- Contains: `index.ts`, `opencode.ts` (creates scoped Web handler + Effect Client), `tool.ts`.
- Depends on: `client`, `core`, `server`.
- Used by: third-party integrations, the `enterprise` Solid app.

**Plugin (`packages/plugin/`):**
- Purpose: Plugin contract types (Server, Tool, TUI, Shell, Workspace adapter).
- Location: `packages/plugin/src/`
- Contains: `index.ts`, `tool.ts`, `tui.ts` (TUI plugin contract — `TuiPluginApi` exposes `prompt` as `TuiPromptApi` with `ref()`, `onChange(cb)`, `onCursorChange(cb)`, plus the `TuiPromptRef` surface `focused` / `current` / `set` / `reset` / `blur` / `focus` / `submit` / `text` / `getTextRange` / `replaceRange` (display-width offsets, inverted range = insertion, preserves undo + extmark controller) / guarded `extmarks` / `cursorOffset` / `setCursorOffset` / `offsetToScreen`, plus `TuiPromptProps` rendered by `api.ui.Prompt` and consumed by the `home_prompt` / `session_prompt` host slots), `shell.ts`, `example.ts`, `example-workspace.ts`, `v2/`.
- Depends on: `client` (typing only).
- Used by: `core`, `opencode` (loading + triggering).

**OpenCode (`packages/opencode/`):**
- Purpose: The shipped product — CLI entry (`packages/opencode/src/index.ts`), HTTP server bootstrap (`packages/opencode/src/server/`), instance + project + plugin + s2s + control-plane wiring, TUI host, embedded mode glue.
- Location: `packages/opencode/src/`
- Contains: `index.ts` (yargs CLI), `cli/cmd/*` (acp, attach, db, debug, export, generate, github, import, mcp, models, plug, providers, pr, run, serve, session, stats, tui, uninstall, upgrade, web), `server/` (server.ts, routes/instance/httpapi, auth, lifecycle, projectors, mdns, websocket-tracker, global-lifecycle), `session/` (V1: llm, prompt, processor, instruction, instruction-audience, interrupt, compaction, retry, revert, status, summary, todo, message-v2, event history retention `event-retention.ts`, llm/ai-sdk, llm/native-runtime), `agent/`, `config/`, `tool/` (V1 tools, task-interrupt, message, s2s, task-return + tool registry), `plugin/`, `permission/`, `skill/`, `project/`, `lsp/`, `mcp/`, `git/`, `auth/`, `account/`, `s2s/`, `messaging/`, `control-plane/`, `event-manifest.ts`, `event-v2-bridge.ts`, `effect/` (runtime/registry/bridge).
- Depends on: every other package.
- Used by: end users via `packages/opencode/bin/opencode` (or `opencode` binary).

**TUI (`packages/tui/`):**
- Purpose: Terminal UI (Solid.js + opentui) — only depends on the generated Client.
- Location: `packages/tui/src/`
- Contains: `index.tsx`, `app.tsx`, `runtime.tsx`, `component/` (single-file dialogs + the multi-file `component/prompt/` subdirectory: `index.tsx`, `autocomplete.tsx`, `cwd.ts`, `frecency.tsx`, `history.tsx`, `local-attachment.ts`, `move.tsx`, `stash.tsx`, `workspace.tsx`), `routes/`, `prompt/` (pure helpers: `display.ts` display-offset indexing + grapheme snap + extmark-preserving range replan + viewport screen coords + `safeExtmarks` facade + `makeGuardedAccessors`; `traits.ts`, `part.ts`, `frecency.tsx`, `history.tsx`, `stash.tsx`), `editor.ts`, `editor-zed.ts`, `keymap.tsx`, `theme/`, `ui/`, `feature-plugins/` (built-in TUI plugins exposed through `createBuiltinPlugins(...)`: `home/` footer+tips, `sidebar/` context/files/footer/lsp/mcp/todo, `system/` diff-viewer + notifications + plugin manager + which-key), `plugin/` (API adapters, command shim, route slots, runtime wrapper), `context/` (`prompt.tsx` exposes the surviving-prompt-remount `PromptRefContext` + per-channel reentrancy guards), `config/`, `util/`, `attention.ts`, `clipboard.ts`, `audio.ts`/`audio.d.ts`, `parsers-config.ts`, `terminal-win32.ts`, `logo.ts`.
- Used by: `opencode` CLI `tui` / `run` commands.

**App / Web / Desktop / Enterprise / Slack / CLI / Storybook / Containers / Identity / Docs:**
- `packages/app/` — Solid.js SPA used as the web/desktop UI; `app.tsx`, `entry.tsx`, `pages/`, `components/`, `i18n/`, `hooks/`, `addons/`, `wsl/`.
- `packages/web/` — Astro documentation site under `web/src/content`, `web/src/pages`, `web/src/i18n`.
- `packages/desktop/` — Tauri host (`main`, `preload`, `renderer`).
- `packages/enterprise/` — Solid enterprise SSO + dashboard; `packages/enterprise/src/core/share.ts` + `packages/enterprise/src/core/storage.ts` (share Cloudflare Durable Object adapter), `packages/enterprise/src/routes/share/`, `packages/enterprise/src/routes/api/`.
- `packages/slack/` — Slack integration entry under `packages/slack/src/`.
- `packages/cli/` — Standalone CLI binary build artifacts (no separate runtime, just `packages/cli/bin/`, `packages/cli/script/`, `packages/cli/src/`).
- `storybook/` — Storybook config for UI components.
- `containers/` — Container build definitions (`base`, `bun-node`, `publish`, `rust`, `tauri-linux`).
- `identity/` — Branding assets only (`mark.svg`, `mark-*.png`).
- `docs/` — Documentation source.

**Infra packages:**
- `codemode/` (`packages/codemode/src/`) — Effect-native confined JS execution environment over schema-described tools.
- `effect-drizzle-sqlite/` (`packages/effect-drizzle-sqlite/src/`) — Drizzle-on-Effect-SQLite bindings plus migration engine.
- `effect-sqlite-node/` (`packages/effect-sqlite-node/src/`) — Node SQLite Effect layer.
- `function/` — Cloudflare Worker Durable Object session-sync implementation (`packages/function/src/api.ts`).
- `http-recorder/`, `httpapi-codegen/`, `console/` — dev tooling and console surfaces.

## Data Flow

**Provider Turn Pipeline (V2 Session):**

1. Caller invokes `sessions.prompt({ sessionID, parts, ... })` over HTTP — `packages/protocol/src/groups/session.ts` defines the HttpApiEndpoint; `packages/server/src/middleware/session-location.ts` resolves the Location-bound middleware key.
2. `packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts` routes to the handler; `SessionV2.prompt(...)` in `packages/core/src/session.ts` admits one durable `session_input` row via `SessionInput` (`packages/core/src/session/input.ts`) and then schedules advisory `SessionExecution.wake(sessionID)`.
3. `SessionExecutionLocal.node` (`packages/core/src/session/execution/local.ts`) looks up the Session's `Location` via `LocationServiceMap.get(session.location)` and forwards into the process-local `SessionRunCoordinator` (`packages/core/src/session/run-coordinator.ts`), which coalesces duplicate wakeups.
4. `SessionRunner.run({ sessionID, force })` (`packages/core/src/session/runner/index.ts`) loads projected history through `SessionStore.runnerContext(...)` (`packages/core/src/session/store.ts`) up to the active `Context Epoch` baseline.
5. The runner reconciles the composed `SystemContext` (`packages/core/src/system-context/index.ts`): `initialize` → `reconcile` → `replace` — producing a `Mid-Conversation System Message` if a source changed, persisted via `SessionProjector` (`packages/core/src/session/projector.ts`) and `EventV2Bridge` (`packages/opencode/src/event-v2-bridge.ts`).
6. `SessionRunnerModel.resolve(...)` (`packages/core/src/session/runner/model.ts`) selects provider + agent; `packages/core/src/session/runner/llm.ts` issues exactly one `llm.stream(request)` call (`packages/llm/src/llm.ts`).
7. `LLMClient` (`packages/llm/src/route/client.ts`) routes through `Endpoint`/`Auth`/`Protocol`/`Framing`/`Transport` to the provider adapter; the stream is materialized as `LLMEvent` (`packages/llm/src/schema/`).
8. Tool calls pass through `ToolRegistry.materialize(...)` → `ToolRegistry.settle(...)` (`packages/core/src/tool/registry.ts`); outputs are bounded via `ToolOutputStore` (`packages/core/src/tool-output-store.ts`) and re-projected through `ToolRuntime` (`packages/llm/src/tool-runtime.ts`).
9. Once the drain reaches idle, durable events emit through `EventV2` (`packages/core/src/event.ts`) + `EventV2Bridge` → `EventGroup` SSE for networked consumers, or the embedded route for `sdk-next`.

**Embedded OpenCode Bootstrap:**

1. `OpenCode.create()` (`packages/sdk-next/src/opencode.ts`) acquires a `Scope`, builds a memoized `AppNode` over `ApplicationTools.node` + `PermissionSaved.node`, then wraps `createEmbeddedRoutes()` (`packages/server/src/routes.ts`) in a `HttpRouter.toWebHandler(...)`.
2. The in-memory `fetch` is fed to `FetchHttpClient.layer` and bound into `OpenCode.make({ baseUrl: "http://opencode.local" })` — the generated Effect Client now hits the same handlers the networked API hits, with no listener.
3. `PermissionSaved` overrides authorize the in-process caller; closing the Scope releases the embedded server resources, database, registrations, and fibers.

**Plugin Load + Trigger:**

1. `Plugin.init()` (`packages/opencode/src/plugin/index.ts`) loads internal plugins (Codex, Copilot, GitLab, Poe, Cloudflare, Azure, DigitalOcean, Snowflake, xAI) and external plugins (via `PluginLoader.loadExternal` in `packages/opencode/src/plugin/loader.ts`).
2. Each plugin's `server(input)` returns a `Hooks` object; `trigger(name, input, output)` calls the matching hook function on each registered plugin in registration order. Plugin clients issued during bootstrap are short-circuited with a `409` `pluginClientReentryResponse` until initialization completes (`packages/opencode/src/plugin/index.ts`).
3. Built-in TUI plugins (`HomeFooter`, `HomeTips`, `SidebarContext/Files/Footer/Lsp/Mcp/Todo`, `DiffViewer`, `Notifications`, `PluginManager`, `WhichKey`) are produced by `createBuiltinPlugins(...)` in `packages/tui/src/feature-plugins/builtins.ts`, routed to the host through `internalTuiPlugins(flags)` in `packages/opencode/src/plugin/tui/internal.ts`, and registered via `loadInternalPlugin(item)` in `packages/opencode/src/plugin/tui/runtime.ts` alongside external plugins.

## Key Abstractions

**`SystemContext` (`packages/core/src/system-context/index.ts`):**
- Purpose: Compose refreshable typed sources into one opaque baseline carrier.
- Location: `packages/core/src/system-context/index.ts`, registry at `registry.ts`, built-ins at `builtins.ts`.
- Pattern: Algebra — `Source<A>` defines `load` + `baseline` + `update` + `removed`. `make(...)` closes the value type, `combine(...)` rejects duplicate keys, `initialize/reconcile/replace` drive one `Context Epoch`.

**`HttpApi` (`packages/protocol/src/api.ts`):**
- Purpose: Authoritative public HTTP surface — composed of groups + middleware.
- Location: `packages/protocol/src/api.ts`, `packages/protocol/src/groups/*`, `packages/protocol/src/middleware/*`.
- Pattern: Effect `HttpApi` builder; Server supplies concrete Location + SessionLocation middleware keys; Client codegen produces both Promise and Effect emitters.

**`SessionV2` (`packages/core/src/session.ts` + `packages/core/src/session/*`):**
- Purpose: Durable conversational history + admission primitive.
- Location: `packages/core/src/session.ts` exports the `SessionV2` namespace, with files in `packages/core/src/session/` for execution, runner, store, schema, sql, projector, history, input, prompt, run-coordinator, runner/{llm,model,to-llm-message,publish-llm-event,max-steps}.
- Pattern: Durable admission (`SessionInput.admit`) → advisory `SessionExecution.wake` → process-local `SessionRunCoordinator` drain → exactly-one `llm.stream` per provider turn with reload-projected-history-before-continue.

**`LocationServices` (`packages/core/src/location-services.ts`):**
- Purpose: Per-Location subgraph of services (ToolRegistry, FileSystem, Plugin, Skill, Agent, Reference, ProjectCopy, Snapshot, SystemContext, Image, etc.).
- Location: `packages/core/src/location-services.ts` builds the `LayerNode.group([...])`; `buildLocationServiceMap()` returns a `LayerMap` keyed by `Location.Ref`.
- Pattern: Effect `LayerMap` + `LayerNode`; one fresh per-directory layer per Location, replaces `Location.node` with `Location.boundNode(ref)`.

**`ToolRegistry` (`packages/core/src/tool/registry.ts`):**
- Purpose: Materialize tool definitions for an agent and settle one tool call into a durable, bounded result.
- Location: `packages/core/src/tool/registry.ts`; primitives in `tool.ts`, defaults in `builtins.ts` + `tools.ts` + per-tool files.
- Pattern: Registration scoped to Effect `Scope`; `materialize(permissions)` returns `definitions` + `settle(call)`; the registry enforces the final output-size limit via `ToolOutputStore` after any tool-specific shaping.

**`LLMClient` (`packages/llm/src/route/client.ts`):**
- Purpose: Single entrypoint that routes `LLMRequest` to a provider over the right endpoint/auth/framing/protocol/transport combination.
- Location: `packages/llm/src/route/` (`client.ts`, `endpoint.ts`, `auth.ts`, `auth-options.ts`, `framing.ts`, `protocol.ts`, `executor.ts`), `packages/llm/src/route/transport/`, `packages/llm/src/providers/`.
- Pattern: Pluggable functions — each route stage is a function from request + previous-stage output to next-stage output. Providers declare their own `Endpoint`/`Auth`/`Protocol`/`Framing`/`Transport` mapping.

**`EventV2Bridge` (`packages/opencode/src/event-v2-bridge.ts`):**
- Purpose: Bridge V2 durable events to the legacy V1 event surface and to SSE subscribers (`EventGroup` in `packages/protocol/src/groups/event.ts`).
- Location: `packages/opencode/src/event-v2-bridge.ts`, `packages/opencode/src/event-manifest.ts`.
- Pattern: Observable + listener fan-out; events flow Session → projector → bridge → SSE / embedded route.

**`TuiPromptApi` / `TuiPromptRef` (`packages/plugin/src/tui.ts`):**
- Purpose: Plugin-facing prompt surface — lets a TUI plugin read prompt state, edit text, drive cursor, draw extmarks (underlined spans), and subscribe to prompt changes without owning the underlying `TextareaRenderable`.
- Location: contract types in `packages/plugin/src/tui.ts` (`TuiPromptInfo`, `TuiPromptRef`, `TuiPromptApi`, `TuiPromptProps`, host slot shapes on `TuiHostSlotMap` for `home_prompt` / `session_prompt`); implementation in `packages/tui/src/context/prompt.tsx` (`PromptRefContext` survives prompt remounts and applies per-channel reentrancy guards) + `packages/tui/src/component/prompt/index.tsx` (publishes the internal `PromptRef` into the context on every mount) + `packages/tui/src/prompt/display.ts` (`safeExtmarks` facade and `makeGuardedAccessors` that no-op against an absent or destroyed `TextareaRenderable`, plus `planRangeReplace` for extmark-preserving edits and `viewportScreenCoords` for overlay anchoring).
- Pattern: Context-anchored ref handle (`api.prompt.ref()` reads the live ref, replaced on every prompt remount) plus two disposable subscriptions (`onChange`, `onCursorChange`) living on the context rather than the component. Public guardrails: `replaceRange` with `start > end` is treated as a pure insertion at the smaller offset (no delete); a guarded extmark facade so a plugin that captured the ref before unmount cannot dereference a destroyed controller; `offsetToScreen` returns `null` when the prompt is unmounted, off-screen, or not laid out. The host slot system mirrors `Prompt` to `home_prompt` / `session_prompt` so plugins can swap a custom prompt without changing the rest of the shell.

**`MessageDiff` (`packages/core/src/session/message-diff.ts`):**
- Purpose: Separate storage for file diff patches offloaded from inline message diff events into the `message_diff` side table.
- Location: `packages/core/src/session/message-diff.ts`, database table at `packages/core/src/database/message-diff.sql.ts`.
- Pattern: Global Effect node (`MessageDiff.node`) wrapping Drizzle SQLite operations (`put`/`get`); inline diff events strip patch text while exports and UI resolve expanded patches from the side table.

## Entry Points

**CLI process (`packages/opencode/src/index.ts`):**
- Location: `packages/opencode/src/index.ts`
- Triggers: `packages/opencode/bin/opencode` (configured in `packages/opencode/package.json` `bin`).
- Responsibilities: yargs dispatch across `acp`, `mcp`, `tui`, `attach`, `run`, `generate`, `debug`, `account`, `providers`, `agent`, `upgrade`, `uninstall`, `serve`, `web`, `models`, `stats`, `export`, `import`, `github`, `pr`, `session`, `plug`, `db` commands. Sets `OPENCODE_PID`, `OPENCODE`, `AGENT`, `OPENCODE_PRINT_LOGS`, `OPENCODE_LOG_LEVEL`, `OPENCODE_PURE` from flags, then starts `Heap.start()` before command dispatch.

**HTTP server (`packages/opencode/src/server/server.ts`):**
- Location: `packages/opencode/src/server/server.ts` (the actual server bootstrap).
- Triggers: `serve` CLI command (`packages/opencode/src/cli/cmd/serve.ts`) and the embedded route in `packages/sdk-next/src/opencode.ts`.
- Responsibilities: Compose HttpApi routes (`packages/opencode/src/server/routes/instance/httpapi/server.ts`) over the `AppNode` service graph; bind Node HTTP server; optionally publish mDNS; expose `openapi()` for `/doc`; clean shutdown via Scope finalizer.

**TUI host (`packages/tui/src/index.tsx`):**
- Location: `packages/tui/src/index.tsx`
- Triggers: `opencode tui` / `opencode --mini` / `opencode attach`.
- Responsibilities: Solid.js render loop against the generated Client; exposes `app.tsx` + `runtime.tsx`.

**Session V2 drain (`packages/core/src/session.ts`):**
- Location: `packages/core/src/session.ts` + `packages/core/src/session/execution.ts` + `packages/core/src/session/execution/local.ts`.
- Triggers: `SessionExecution.wake(sessionID)` (advisory) or `SessionExecution.resume(sessionID)` (explicit) — both routed through the process-local `SessionRunCoordinator`.
- Responsibilities: Materialize the Location subgraph, then enter `SessionRunner.run({ sessionID, force })` for one drain.

**Embedded OpenCode (`packages/sdk-next/src/opencode.ts`):**
- Location: `packages/sdk-next/src/opencode.ts` + `packages/sdk-next/src/index.ts`.
- Triggers: Third-party `import { OpenCode } from "@opencode-ai/sdk-next"` consumers.
- Responsibilities: Scoped in-process OpenCode host; closes its Scope to release resources.

## Error Handling

**Strategy:** Effect-native tagged errors + fail-closed behaviors.

- Domain errors are `Schema.TaggedErrorClass` (`packages/core/src/system-context/index.ts` defines `InitializationBlocked`/`DuplicateKeyError`; `packages/core/src/session.ts` defines `NotFoundError`/`OperationUnavailableError`).
- Public API errors are encoded by `Protocol` HttpApi groups; Client decoders preserve tagged structural wire values with generated type guards so discrimination survives across package copies and realms.
- Embedded OpenCode reuses the same handlers — `schemaErrorLayer` + `errorLayer` + `compressionLayer` + `corsVaryFix` + `fenceLayer` are stacked at the route level in `packages/opencode/src/server/routes/instance/httpapi/server.ts`.
- Tool settlement: tools return `LLM.ToolFailure`; `ToolRegistry.settle` (`packages/core/src/tool/registry.ts`) maps it to a model-visible error result. Tool-output bounding failures raise `ToolOutputStore.Error` and are surfaced to operators via diagnostics; a successful tool operation is never reclassified as failed by bounding or storage loss.
- Session input admission: reusing a Session ID adopts the existing Session; reusing a prompt message ID reconciles an exact retry only when Session, prompt, and delivery mode match; conflicting reuse fails.
- `Context Snapshot` advances atomically with the corresponding durable `Mid-Conversation System Message`; unavailable sources block initialization (return `InitializationBlocked`) rather than persisting an incomplete baseline.
- `SessionExecution.wake` is advisory — coalescing, idempotent; `interrupt` is a no-op for idle or missing ownership.

## Cross-Cutting Concerns

**Logging:** `Effect.logInfo`/`logWarning`/`logError` (`packages/core/src/session/execution/local.ts` annotates logs with `sessionID`). The server mutes its own logger (`disableLogger: true` in `HttpRouter.serve` + `HttpRouter.toWebHandler`). OpenTelemetry traces use `@effect/opentelemetry` (`@opencode-ai/core/observability`).

**Caching:** `Layer.makeMemoMap` is used by both `Server.listen` (`packages/opencode/src/server/server.ts`) and the embedded SDK (`packages/sdk-next/src/opencode.ts`) so service layers built across requests share identity. `OpenApi.fromApi` is cached behind `lazy(() => HttpServerResponse.jsonUnsafe(...))` in `server.ts` so the `/doc` endpoint reuses one `Uint8Array`. The legacy `opencode` process has its own install/version cache in `Installation` (`packages/core/src/installation/`).

**Storage:** Two layers:
- **Durable SQL:** `packages/core/src/database/database.ts` opens one `EffectDrizzleSqlite` instance (WAL mode, `synchronous=NORMAL`, `busy_timeout=30000`, `cache_size=-64000`, foreign keys ON) with Drizzle migrations (`packages/core/src/database/migration/`). Tables for `Session` (with `result` and `context_mode` columns for structured child output and per-session context mode), `SessionMessage`, `SessionInput`, `EventV2`, `MessageDiff` (`message_diff` side table offloading patch text from inline message diff events), and `s2s.sql.ts` are emitted from `packages/core/src/database/`.
- **Filesystem Storage:** `packages/opencode/src/storage/storage.ts` reads/writes JSON-typed draft files (root config, session metadata, messages, summaries, diffs) via `immer` + `RcMap` + `TxReentrantLock`. Snapshots are persisted by `Snapshot` (`packages/core/src/snapshot.ts`). Shared tool-output files live under one flat directory and are written/read by `ToolOutputStore` (`packages/core/src/tool-output-store.ts`).

**Event Retention:** `SessionEventRetention` (`packages/opencode/src/session/event-retention.ts`) runs a background worker (hourly schedule) to sweep and purge `EventV2` aggregate event history for idle sessions exceeding `retention.event_idle_days`.

**Runtime isolation:** `LayerNode` separates **global nodes** (`makeGlobalNode` — Database, Ripgrep, Storage, Snapshot, ModelsDev, Provider, Agent, etc.) from **Location nodes** (`makeLocationNode` — ToolRegistry, FileSystem, Plugin, Skill, SystemContext, Watcher, Permission, Reference, ProjectCopy, Image, Pty, LocationMutation, FileMutation, Snapshot, BuiltInTools, etc.). `LocationServiceMap.get(ref)` materializes a fresh per-Location Layer; this is the seam where future remote placement will plug in.

**PTY Environment:** Constructed server-side (`packages/server/src/pty-environment.ts`) by merging caller values, then the host overlay, then Core-forced terminal invariants such as `TERM` and `OPENCODE_TERMINAL`. Plugin observers register through `PluginPtyEnvironment.layer` (`packages/opencode/src/plugin/pty-environment.ts`); standalone servers use an empty adapter.

**Authentication:** `packages/protocol/src/middleware/authorization.ts` declares `Authorization` middleware; `packages/opencode/src/server/routes/instance/httpapi/middleware/authorization.ts` provides `authorizationLayer`, `authorizationRouterMiddleware`, `ptyConnectAuthorizationLayer`, `serverAuthorizationLayer`. `ServerAuth.Config.layer` provides credentials from `OPENCODE_SERVER_PASSWORD`; `ptyConnectAuthorizationLayer` accepts a `PtyTicket` (`packages/core/src/pty/ticket.ts`).

**CORS:** `packages/server/src/cors.ts` exports `CorsOptions` + `isAllowedCorsOrigin`; `packages/opencode/src/server/routes/instance/httpapi/server.ts` mounts `HttpMiddleware.cors(...)` with `maxAge: 86_400` as a global router middleware.

**V2 → V1 Bridge:** `packages/opencode/src/event-v2-bridge.ts` translates EventV2 into V1-style events for legacy consumers; `packages/opencode/src/effect/bridge.ts` provides the in-process `EffectBridge` used by plugin + tool layers.
