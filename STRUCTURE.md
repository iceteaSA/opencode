# Codebase Structure

## Directory Layout

```
opencode/
├── AGENTS.md                        # Style guide + V2 Session Core rules
├── CONTEXT.md                       # Domain glossary (System Context, Context Epoch, etc.)
├── bunfig.toml                      # Bun config
├── package.json                     # Workspace root + scripts
├── turbo.json                       # Turborepo config
├── install                          # One-line installer
├── flake.nix / flake.lock          # Nix dev shell
├── sst.config.ts                    # SST deployment
├── infra/                           # SST infrastructure definitions
├── nix/                             # Nix scripts
├── script/                          # Repo-level scripts (CI helpers)
├── github/                          # GitHub Actions / workflow files
├── patches/                         # Patched dependencies (pinned via package.json patches)
├── perf/                            # Perf scaffolding
├── sdks/vscode/                     # VS Code extension SDK
├── specs/                           # Specs (storage, v2)
└── packages/
    ├── app/                         # Solid.js web/desktop SPA
    ├── cli/                         # Standalone CLI build
    ├── client/                      # Generated Promise + Effect HTTP clients
    ├── codemode/                    # Effect-native confined code execution over schema-described tools
    ├── console/                     # Console surface
    ├── containers/                  # Container builds (base, bun-node, rust, tauri-linux)
    ├── core/                        # Domain core (Session V2, tools, system context, database, LLM projection)
    ├── desktop/                     # Tauri host (main / preload / renderer)
    ├── docs/                        # Documentation source
    ├── effect-drizzle-sqlite/       # Drizzle-on-Effect-SQLite bindings
    ├── effect-sqlite-node/          # Node SQLite Effect layer
    ├── enterprise/                  # Solid enterprise SSO + dashboard + share Durable Object adapter
    ├── function/                    # Cloudflare Worker Durable Object (session sync)
    ├── http-recorder/               # HTTP recording for tests
    ├── httpapi-codegen/             # HttpApi codegen tooling
    ├── identity/                    # Branding assets (mark.svg, mark-*.png)
    ├── llm/                         # LLM routing facade + provider adapters
    ├── opencode/                    # The shipped CLI product (yargs, server bootstrap, V1 session, glue)
    ├── plugin/                      # Plugin contract types
    ├── protocol/                    # HttpApi + groups + middleware placement
    ├── schema/                      # Effect Schema domain types (no runtime deps)
    ├── script/                      # Build scripts shared across packages
    ├── sdk-next/                    # Embedded OpenCode host (in-memory HttpRouter)
    ├── sdk/                         # Legacy SDK + JS SDK build
    ├── server/                      # Server handlers + middleware + lifecycle
    ├── session-ui/                  # Shared session UI widgets
    ├── slack/                       # Slack integration
    ├── stats/                       # Stats collection / reporting
    ├── storybook/                   # Storybook config
    ├── tui/                         # Solid.js terminal UI
    ├── ui/                          # Shared UI primitives
    └── web/                         # Astro documentation site
```

## Directory Purposes

**`packages/core/`:**
- Purpose: Domain behavior — Session V2, tools, permissions, system context, projects, LLM projection, plugin core, filesystem, PTY, snapshot, durable database.
- Contains: `src/session/` (incl. `message-diff.ts`), `src/tool/`, `src/permission/`, `src/system-context/`, `src/database/` (incl. `message-diff.sql.ts`), `src/filesystem/`, `src/pty/`, `src/plugin/`, `src/project/`, `src/event/`, `src/control-plane/`, `src/integration/`, `src/credential/`, `src/oauth/`, `src/github-copilot/`, `src/effect/`, `src/llm.ts`, `src/location-services.ts`, `src/location-service-map.ts`, `src/snapshot.ts`, `src/tool-output-store.ts`, `src/instruction-context.ts`, `src/v1/` (legacy V1 implementation kept for compatibility).
- Key files: `src/session.ts` (V2 entry), `src/session/runner/index.ts`, `src/session/execution.ts`, `src/session/execution/local.ts`, `src/session/message-diff.ts`, `src/session/store.ts`, `src/system-context/index.ts`, `src/system-context/registry.ts`, `src/tool/registry.ts`, `src/database/database.ts`, `src/effect/layer-node.ts`, `src/effect/app-node.ts`.

**`packages/opencode/`:**
- Purpose: The shipped product — CLI, HTTP server bootstrap, V1 session implementation, instance + plugin + s2s + control-plane wiring.
- Contains: `src/index.ts` (yargs root), `src/cli/cmd/*` (one file per CLI command, plus `cmd/run/` subdirectory), `src/server/` (server.ts, routes/instance/httpapi, auth, lifecycle, projectors, mdns, websocket-tracker, global-lifecycle, init-projectors), `src/session/` (V1: llm, llm/ai-sdk, llm/native-runtime, prompt, processor, instruction, instruction-audience, interrupt, compaction, retry, revert, status, summary, todo, message-v2, event-retention), `src/agent/`, `src/tool/` (V1 tools, task-interrupt, message, s2s, task-return + per-tool docs in `*.txt`), `src/permission/`, `src/skill/`, `src/project/`, `src/lsp/`, `src/mcp/`, `src/git/`, `src/auth/`, `src/account/`, `src/s2s/`, `src/messaging/`, `src/control-plane/`, `src/config/`, `src/event-manifest.ts`, `src/event-v2-bridge.ts`, `src/effect/` (runtime/registry/bridge).
- Key files: `src/index.ts`, `src/server/server.ts`, `src/server/routes/instance/httpapi/server.ts`, `src/session/llm.ts`, `src/plugin/index.ts`, `src/cli/cmd/serve.ts`, `src/cli/cmd/run.ts`.

**`packages/schema/`:**
- Purpose: Pure Effect Schema definitions — every public domain type, error, event, and identifier lives here so it can be consumed without bringing in runtime code.
- Contains: `agent.ts`, `session.ts`, `session-input.ts`, `session-message.ts`, `session-event.ts`, `session-delivery.ts`, `session-todo.ts`, `session-status-event.ts`, `session-compaction-event.ts`, `session-v1.ts`, `session-id.ts`, `provider.ts`, `model.ts`, `catalog.ts`, `credential.ts`, `permission.ts`, `permission-v1.ts`, `permission-saved.ts`, `skill.ts`, `event.ts`, `event-manifest.ts`, `durable-event-manifest.ts`, `public-event-manifest.ts`, `legacy-event.ts`, `connection.ts`, `installation-event.ts`, `messaging-event.ts`, `interrupt-event.ts`, `ide-event.ts`, `lsp-event.ts`, `mcp-event.ts`, `vcs-event.ts`, `workspace-event.ts`, `worktree-event.ts`, `task-event.ts`, `server-event.ts`, `tui-event.ts`, `location.ts`, `project.ts`, `project-id.ts`, `project-directories.ts`, `project-copy.ts`, `workspace.ts`, `workspace-id.ts`, `filesystem.ts`, `filesystem-watcher.ts`, `file-diff.ts`, `pty.ts`, `pty-ticket.ts`, `integration.ts`, `integration-id.ts`, `prompt.ts`, `prompt-input.ts`, `revert.ts`, `question.ts`, `question-v1.ts`, `identifier.ts`, `command.ts`, `plugin.ts`, `llm.ts`, `models-dev.ts`, `reference.ts`, `schema.ts`, `index.ts`, `v1/`.
- Key files: `src/session.ts`, `src/durable-event-manifest.ts`, `src/public-event-manifest.ts`, `src/index.ts`.

**`packages/protocol/`:**
- Purpose: Compose Schema into HTTP API shapes — groups, paths, errors, middleware placement. The authoritative public `HttpApi` lives here; Server supplies concrete middleware keys.
- Contains: `api.ts` (HttpApi builder), `errors.ts`, `groups/agent.ts`, `groups/command.ts`, `groups/credential.ts`, `groups/event.ts`, `groups/fs.ts`, `groups/health.ts`, `groups/integration.ts`, `groups/location.ts`, `groups/message.ts`, `groups/model.ts`, `groups/permission.ts`, `groups/project-copy.ts`, `groups/provider.ts`, `groups/pty.ts`, `groups/question.ts`, `groups/reference.ts`, `groups/session.ts`, `groups/skill.ts`, `middleware/authorization.ts`, `middleware/schema-error.ts`.
- Key files: `src/api.ts`, `src/groups/session.ts`, `src/groups/event.ts`.

**`packages/server/`:**
- Purpose: Concrete handlers + middleware + lifecycle that turn the Protocol `HttpApi` into a runnable router.
- Contains: `api.ts` (binds Location + SessionLocation middleware keys), `routes.ts`, `handlers.ts`, `handlers/`, `location.ts`, `middleware/session-location`, `middleware/schema-error`, `cors.ts`, `auth.ts`, `pty-environment.ts`.
- Key files: `src/api.ts`, `src/handlers.ts`, `src/routes.ts`.

**`packages/llm/`:**
- Purpose: Provider-routing facade. Single `LLMClient.stream(request)` is the only entry point the Session runner calls per provider turn.
- Contains: `route/` (`client.ts`, `endpoint.ts`, `auth.ts`, `auth-options.ts`, `framing.ts`, `protocol.ts`, `executor.ts`, `transport/`), `providers/` (`anthropic.ts`, `amazon-bedrock.ts`, `azure.ts`, `cloudflare.ts`, `github-copilot.ts`, `google.ts`, `openai.ts`, `openai-compatible.ts`, `openai-compatible-profile.ts`, `openai-options.ts`, `openrouter.ts`, `xai.ts`, `index.ts`), `protocols/` (`anthropic-messages.ts`, `bedrock-converse.ts`, `bedrock-event-stream.ts`, `gemini.ts`, `openai-chat.ts`, `openai-compatible-chat.ts`, `openai-responses.ts`, `shared.ts`, `utils/`, `index.ts`), `schema/`, `llm.ts`, `tool.ts`, `tool-runtime.ts`, `cache-policy.ts`, `provider-error.ts`, `index.ts`.
- Key files: `src/route/client.ts`, `src/llm.ts`, `src/provider.ts`.

**`packages/client/`:**
- Purpose: Generated HTTP client SDKs — root is zero-Effect, `/effect` is the rich Effect emitter. Both are derived from the same `HttpApi`.
- Contains: `contract.ts`, `index.ts` (Promise root), `effect.ts` (`/effect` entry), `generated/`, `generated-effect/`.
- Key files: `src/index.ts`, `src/effect.ts`.

**`packages/codemode/`:**
- Purpose: Effect-native confined JS code execution engine over schema-described tools (`CodeMode`, `Tool`, `OpenAPI`).
- Contains: `src/codemode.ts`, `src/tool.ts`, `src/tool-runtime.ts`, `src/tool-schema.ts`, `src/tool-error.ts`, `src/values.ts`, `src/interpreter/`, `src/openapi/`, `src/stdlib/`.
- Key files: `src/index.ts`, `src/codemode.ts`, `src/tool.ts`.

**`packages/sdk-next/`:**
- Purpose: Embedded OpenCode host — assemble Server's `HttpRouter` in memory and bind it to an in-memory `HttpClient`.
- Contains: `index.ts`, `opencode.ts`, `tool.ts`.
- Key files: `src/opencode.ts`.

**`packages/plugin/`:**
- Purpose: Plugin contract types (Server, Tool, TUI, Shell, Workspace adapter). The TUI contract exposes the prompt facade surface (`TuiPromptApi` / `TuiPromptRef` / `TuiPromptInfo` / `TuiPromptProps`) consumed by plugins, with the `TuiHostSlotMap` shaped so `home_prompt` / `session_prompt` can hand the live prompt ref to a plugin-supplied render.
- Contains: `index.ts`, `tool.ts`, `tui.ts`, `shell.ts`, `example.ts`, `example-workspace.ts`, `v2/`.
- Key files: `src/index.ts`, `src/tui.ts`.

**`packages/tui/`:**
- Purpose: Terminal UI — Solid.js + `@opentui`. Driven by the generated Client. Hosts the TUI plugin prompt facade (`TuiPromptApi` / `TuiPromptRef`) consumed by TUI plugins through `packages/plugin/src/tui.ts`.
- Contains: `index.tsx`, `app.tsx`, `runtime.tsx`, `component/` (single-file dialogs + the multi-file `component/prompt/` subdirectory: `index.tsx`, `autocomplete.tsx`, `cwd.ts`, `frecency.tsx`, `history.tsx`, `local-attachment.ts`, `move.tsx`, `stash.tsx`, `workspace.tsx`), `routes/`, `prompt/` (pure helpers — display-offset indexing, grapheme snap, extmark-preserving range replan, viewport screen coords, `safeExtmarks` facade, `makeGuardedAccessors` — plus `traits.ts` / `part.ts` / `frecency.tsx` / `history.tsx` / `stash.tsx`), `editor.ts`, `editor-zed.ts`, `keymap.tsx`, `theme/`, `ui/`, `feature-plugins/` (built-in TUI plugins returned by `createBuiltinPlugins(...)` in `builtins.ts`: `home/` footer + tips, `sidebar/` context/files/footer/lsp/mcp/todo, `system/` diff-viewer + notifications + plugin manager + which-key), `plugin/` (API adapters, command shim, route slots, runtime wrapper), `config/`, `context/` (incl. `prompt.tsx` for the `PromptRefContext` that survives prompt remounts), `util/`, `attention.ts`, `clipboard.ts`, `audio.ts`/`audio.d.ts`, `parsers-config.ts`, `terminal-win32.ts`, `logo.ts`.
- Key files: `src/index.tsx`, `src/app.tsx`, `src/runtime.tsx`, `src/context/prompt.tsx`, `src/component/prompt/index.tsx`, `src/prompt/display.ts`, `src/plugin/adapters.tsx`, `src/feature-plugins/builtins.ts`.

**`packages/app/`:**
- Purpose: Solid.js web/desktop SPA used as the UI layer when running in browser or Tauri.
- Contains: `app.tsx`, `entry.tsx`, `index.ts`, `pages/`, `components/`, `constants/`, `context/`, `hooks/`, `i18n/`, `addons/`, `wsl/`, `utils/`, `updater.ts`, `desktop-menu.ts`.
- Key files: `src/app.tsx`, `src/entry.tsx`.

**`packages/web/`:**
- Purpose: Astro documentation site.
- Contains: `src/content/`, `src/content.config.ts`, `src/pages/`, `src/components/`, `src/styles/`, `src/i18n/`, `src/middleware.ts`, `src/types/`.
- Key files: `src/content.config.ts`, `src/middleware.ts`.

**`packages/desktop/`:**
- Purpose: Tauri host wrapping the Solid app.
- Contains: `main/`, `preload/`, `renderer/`.

**`packages/enterprise/`:**
- Purpose: Solid enterprise SSO + dashboard + share Durable Object adapter.
- Contains: `src/app.tsx`, `src/entry-client.tsx`, `src/entry-server.tsx`, `src/core/share.ts`, `src/core/storage.ts`, `src/routes/index.tsx`, `src/routes/api/`, `src/routes/share/`, `src/routes/share.tsx`, `src/routes/[...404].tsx`.

**`packages/slack/`, `packages/console/`, `packages/cli/`, `packages/storybook/`, `packages/containers/`, `packages/identity/`, `packages/docs/`, `packages/session-ui/`, `packages/ui/`, `packages/stats/`, `packages/sdk/`, `packages/function/`, `packages/http-recorder/`, `packages/httpapi-codegen/`, `packages/effect-drizzle-sqlite/`, `packages/effect-sqlite-node/`:**
- Deployment surfaces, build scripts, and supporting packages. Identity holds branding assets only (`mark.svg`, `mark-*.png`).

## Key File Locations

**Entry Points:**
- `packages/opencode/src/index.ts` — CLI process entry, yargs root.
- `packages/opencode/src/server/server.ts` — HTTP server bootstrap (`Server.listen`, `openapi`, `url`).
- `packages/opencode/src/cli/cmd/serve.ts` — `serve` CLI command.
- `packages/opencode/src/cli/cmd/run.ts` — `run` / `--mini` CLI command (single prompt + interactive modes).
- `packages/opencode/src/cli/cmd/tui.ts` — `tui` CLI command.
- `packages/tui/src/index.tsx` — TUI render root.
- `packages/sdk-next/src/opencode.ts` — Embedded OpenCode `OpenCode.create()` factory.

**Configuration:**
- `packages/opencode/package.json` — bin, exports, scripts, dependencies.
- `packages/opencode/src/config/config.ts` — OpenCode config loader.
- `packages/opencode/src/config/managed.ts` — Managed-config override.
- `packages/opencode/src/cli/network.ts` — Network flag resolution (`withNetworkOptions`, `resolveNetworkOptions`).
- `packages/opencode/src/cli/cmd/cmd.ts` — `effectCmd` base builder.
- `packages/core/src/flag/flag.ts` — Env-flag access (`OPENCODE_DB`, `OPENCODE_SERVER_PASSWORD`, etc.).
- `packages/core/src/global.ts` — `Global.Path` resolution.

**Core Logic:**
- `packages/core/src/session.ts` — V2 `SessionV2` namespace, `prompt`, `create`, `compact`, list operations.
- `packages/core/src/session/runner/index.ts` — `SessionRunner.run({ sessionID, force })`.
- `packages/core/src/session/execution.ts` — Process-global `SessionExecution` routing.
- `packages/core/src/session/execution/local.ts` — Current-process local drain.
- `packages/core/src/system-context/index.ts` — `SystemContext` algebra (`make`, `combine`, `initialize`, `reconcile`, `replace`).
- `packages/core/src/system-context/registry.ts` — Location-scoped registry of typed sources.
- `packages/core/src/tool/registry.ts` — `ToolRegistry.materialize` + `settle`.
- `packages/core/src/location-services.ts` — `buildLocationServiceMap()` + `LocationServices` type.
- `packages/core/src/database/database.ts` — Drizzle SQLite + WAL + migrations.
- `packages/llm/src/route/client.ts` — `LLMClient.stream` / `LLMClient.generate`.

**HTTP Surface:**
- `packages/protocol/src/api.ts` — Authoritative public `HttpApi` builder.
- `packages/server/src/api.ts` — `Api = makeDefaultApi({ locationMiddleware, sessionLocationMiddleware })`.
- `packages/opencode/src/server/routes/instance/httpapi/server.ts` — Route assembly (`createRoutes`, `webHandler`).
- `packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts` — Session handler.

**Generated:**
- `packages/client/src/index.ts` — Promise root client.
- `packages/client/src/effect.ts` — Effect `/effect` client.

**Storage:**
- `packages/opencode/src/storage/storage.ts` — V1 filesystem-backed draft storage.
- `packages/opencode/src/storage/schema.ts` — Storage schemas.
- `packages/core/src/session/message-diff.ts` — Message diff side-table storage service.
- `packages/core/src/database/message-diff.sql.ts` — Message diff Drizzle schema.
- `packages/core/src/database/schema.sql.ts` — Drizzle table schemas.
- `packages/core/src/database/migration/` — Generated Drizzle migrations.
- `packages/core/src/tool-output-store.ts` — Shared tool-output file store.
- `packages/core/src/snapshot.ts` — Snapshot persistence.

**Tests:** Co-located with source files as `*.test.ts`. Tests cannot run from repo root (see `AGENTS.md` `do-not-run-tests-from-root`); run from package dirs like `packages/opencode` (`bun test`). Test-support fixtures live in `packages/opencode/test/` and `packages/core/test/`. `packages/http-recorder/` is the HTTP recording library used by handler tests.

## Naming Conventions

**Files:**
- Lowercase, kebab-case: `run-state.ts`, `apply-patch.ts`, `event-v2-bridge.ts`, `instruction-context.ts`.
- Tests: `<source>.test.ts`, co-located.
- Per-tool docs: `<tool>.txt` next to `<tool>.ts` (e.g., `bash.txt`, `apply-patch.txt`).
- Namespaces: `export * as Foo from "./foo"` at top of a file (e.g., `export * as SessionRunner from "./index"` in `packages/core/src/session/runner/index.ts`).

**Directories:**
- Lowercase, single-word or kebab-case: `session`, `tool`, `system-context`, `effect`, `cli`, `server`, `database`, `filesystem`, `event`.
- Subdirectories of a package mirror the module they contain (`packages/opencode/src/session/llm/` contains `llm` module files).

## Where to Add New Code

**New CLI command:** `packages/opencode/src/cli/cmd/<name>.ts` using `effectCmd({ command, builder, describe, handler })` (`packages/opencode/src/cli/cmd/cmd.ts`). Register in the yargs root at `packages/opencode/src/index.ts`.

**New HttpApi group:** Add `packages/protocol/src/groups/<name>.ts`, export from `packages/protocol/src/api.ts`, add handlers in `packages/server/src/handlers/<name>.ts` (if Server-owned) or `packages/opencode/src/server/routes/instance/httpapi/handlers/<name>.ts`, then regenerate the client with `bun run generate` from `packages/client`.

**New LLM provider:** Add `packages/llm/src/providers/<name>.ts` implementing the provider mapping (Endpoint / Auth / Protocol / Framing / Transport); add `packages/llm/src/protocols/<protocol>.ts` if a new wire protocol is needed. Register in `packages/llm/src/providers/index.ts`.

**New tool (V1, in `opencode` package):** Add `packages/opencode/src/tool/<name>.ts` with documentation in `<name>.txt`. Register in `packages/opencode/src/tool/registry.ts` (V1 registry).

**New tool (V2, Location-scoped):** Add `packages/core/src/tool/<name>.ts`. Register in `packages/core/src/tool/builtins.ts` (`BuiltInTools.node`) so it lives in `LocationServices` and can be retrieved through `ToolRegistry`.

**New System Context source:** Add a producer in `packages/core/src/system-context/builtins.ts` (or a new file) using `SystemContext.make({ key, codec, load, baseline, update, removed })`. Register with `SystemContextRegistry.register(entry)` (`packages/core/src/system-context/registry.ts`). Keys must match the stable namespaced pattern `^[a-z0-9][a-z0-9._-]*\/[a-z0-9][a-z0-9._/-]*$`.

**New Schema type:** `packages/schema/src/<name>.ts` (pure Effect `Schema` only). Add to `packages/schema/src/index.ts` if it should be re-exported from the package root.

**New Location-scoped service:** Add `packages/core/src/<name>.ts` with `class Service extends Context.Service<...>()`, then expose `node = makeLocationNode({ service: Service, layer, deps })` and add to the `LayerNode.group([...])` in `packages/core/src/location-services.ts`.

**New global service:** Same as above, but use `makeGlobalNode(...)` so it lives at process scope instead of per-Location.

**New plugin hook:** Add to the `Hooks` interface in `packages/plugin/src/index.ts` and trigger through `Plugin.trigger(name, input, output)` in `packages/opencode/src/plugin/index.ts`.

**New shared Effect utility:** `packages/core/src/util/<name>.ts` or `packages/core/src/effect/<name>.ts` for service plumbing.

**New SDK surface:** Extend `packages/sdk-next/src/opencode.ts` to compose additional services or expose additional capabilities on the embedded host.

**Tests:** Co-locate as `*.test.ts` next to the source file. Run with `bun test` from the relevant package directory (never from repo root).