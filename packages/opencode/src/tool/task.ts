import * as Tool from "./tool"
import DESCRIPTION from "./task.txt"
import { ToolJsonSchema } from "./json-schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SESSION_SLUG_PATTERN } from "@/session/session"
import { SessionID } from "../session/schema"
import { MessageV2 } from "../session/message-v2"
import { Agent } from "../agent/agent"
import { deriveSubagentSessionPermission } from "../agent/subagent-permissions"
import type { SessionPrompt } from "../session/prompt"
import { writeMarker as writeMessageMarker } from "./message"
import { Messaging } from "../messaging"
import { Permission } from "../permission"
import { SessionRunState } from "../session/run-state"
import { TaskOutcomes } from "./task-outcomes"
import { Config } from "@/config/config"
import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { Cause, Effect, Exit, Option, Schema, Scope } from "effect"
import { EffectBridge } from "@/effect/bridge"
import { attach } from "@/effect/run-service"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Database } from "@opencode-ai/core/database/database"
import { Interrupt } from "../session/interrupt"
import { EventV2Bridge } from "@/event-v2-bridge"
import { TaskEvent } from "@opencode-ai/schema/task-event"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { PositiveInt } from "@opencode-ai/core/schema"
import { KeyedMutex } from "@opencode-ai/core/effect/keyed-mutex"
import { createHash } from "crypto"
import path from "path"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { InstanceState } from "@/effect/instance-state"
import { permissionPath } from "@/project/instance-context"
import { assertExternalDirectoryEffect } from "./external-directory"
import { ScheduledTaskStore } from "./scheduled-task-store"
import { parseInstant } from "./iso-instant"

export interface TaskPromptOps {
  cancel(sessionID: SessionID): Effect.Effect<void>
  cancelRun(sessionID: SessionID): Effect.Effect<void>
  resolvePromptParts(template: string): Effect.Effect<SessionPrompt.PromptInput["parts"]>
  prompt(input: SessionPrompt.PromptInput): Effect.Effect<SessionV1.WithParts, SessionRunState.LeaseLostError>
  loop(sessionID: SessionID): Effect.Effect<SessionV1.WithParts, SessionRunState.LeaseLostError>
}

export const Event = {
  Completed: TaskEvent.Completed,
}



const id = "task"
const BACKGROUND_DESCRIPTION = [
  "Background mode: background=true launches the subagent asynchronously and returns immediately.",
  "Foreground is the default; use it when you need the result before continuing.",
  "Use background only for independent work that can run while you continue elsewhere.",
  "You will be notified automatically when it finishes.",
].join(" ")
const BACKGROUND_STARTED = [
  "The task is working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.",
].join("\n")
const BACKGROUND_UPDATED = [
  "Additional context sent to the running background task.",
  "The task is still working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you sent and end your response.",
].join("\n")

function isSlug(taskId: string): boolean {
  return !taskId.startsWith("ses_")
}

function deriveSlugSessionID(slug: string, rootID: SessionID): SessionID {
  // The 12-hex root hash namespaces the slug per session tree so different roots
  // can reuse the same slug; within a tree the slug itself makes the ID unique.
  const hash = createHash("sha256").update(rootID).digest("hex").slice(0, 12)
  return SessionID.descending(`ses_${hash}_${slug}`)
}

const BaseParameterFields = {
  description: Schema.String.annotate({ description: "A short (3-5 words) description of the task" }),
  prompt: Schema.optional(Schema.String).annotate({ description: "The task for the agent to perform" }),
  prompt_file: Schema.optional(Schema.String).annotate({
    description: "Read the task prompt verbatim from this file. Specify exactly one of prompt or prompt_file.",
  }),
  subagent_type: Schema.String.annotate({ description: "The type of specialized agent to use for this task" }),
  model: Schema.optional(Schema.String).annotate({
    description:
      "Override the model for this subagent. Format: provider/model (e.g. anthropic/claude-sonnet-4, openai/gpt-4o). Takes precedence over the agent's configured model.",
  }),
  variant: Schema.optional(Schema.String).annotate({
    description:
      'Model variant for this dispatch (e.g. "thinking", "high", "none"). Variants are model-specific reasoning/effort presets; an unknown variant is ignored. Takes precedence over the parent turn\'s variant.',
  }),
  task_id: Schema.optional(Schema.String).annotate({
    description:
      'A human-readable slug (e.g. "explore-auth") to create or resume a named task session within this root session. If the slug has not been used yet, a new task is created with that identifier and the child session adopts the slug as its display handle. If it already exists, the existing session is resumed. Also accepts full "ses_..." session IDs to resume a specific session directly.',
  }),
  resume: Schema.optional(Schema.Boolean).annotate({
    description:
      "Explicit consent to resume an existing idle task session named by task_id. Required when task_id refers to a session with no currently-running background job. A live background task still accepts task_id updates without this flag.",
  }),
  command: Schema.optional(Schema.String).annotate({ description: "The command that triggered this task" }),
  message_allow: Schema.optional(Schema.Array(Schema.String)).annotate({
    description:
      "Optional slugs (other task_ids you spawn) this subagent may message. Empty/omitted → parent only.",
  }),
  timeout: Schema.optional(PositiveInt).annotate({
    description:
      "Maximum time in milliseconds for the subagent attempt. On expiry the attempt is interrupted; if fallback_model is set, the task is retried once on it, otherwise the task fails.",
  }),
  fallback_model: Schema.optional(Schema.String).annotate({
    description:
      "Model to retry on once (provider/model format) if the primary attempt times out or fails. Requires the model_override permission.",
  }),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Any)).annotate({
    description:
      "Opaque structured metadata stored on the child task session (visible to plugins, events, and session queries). Not shown to the subagent. On resume, keys are shallow-merged into the existing metadata.",
  }),
  completion: Schema.optional(Schema.Literals(["full", "terse"])).annotate({
    description: "Completion display mode for this dispatch (default: full — the full child output is shown inline)",
  }),
  context: Schema.optional(Schema.Literals(["full", "sparse"])).annotate({
    description:
      "Context mode for the subagent: 'full' sends all instruction files and skills; 'sparse' sends only the project AGENTS.md chain, dropping global instructions, skills, and MCP docs (default: full)",
  }),
  wake_on_message: Schema.optional(Schema.Boolean).annotate({
    description:
      "When true, if the dispatched child agent becomes idle and a sibling or coordinator message lands in its inbox, the child will be woken to process it instead of the message sitting undelivered",
  }),
}

const BaseParameters = Schema.Struct(BaseParameterFields)

export const Parameters = Schema.Struct({
  ...BaseParameterFields,
  start_at: Schema.optional(Schema.String).annotate({
    description: "Admit this task durably for a timezone-qualified ISO-8601 instant; future starts require background: true and are not executed until due",
  }),
  background: Schema.optional(Schema.Boolean).annotate({
    description:
      "Run the agent in the background. You will be notified when it completes. DO NOT sleep, poll, or proactively check on its progress",
  }),
})

// Escape untrusted strings rendered into the <task>/<summary> framing.
function escapeBody(body: string) {
  return body.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
}

export function renderOutput(input: {
  sessionID: SessionID
  state: "running" | "completed" | "error" | "aborted" | "timed_out"
  summary?: string
  text: string
}) {
  const tag =
    input.state === "error" || input.state === "timed_out"
      ? "task_error"
      : input.state === "aborted"
        ? "task_aborted"
        : "task_result"
  return [
    `<task id="${input.sessionID}" state="${input.state}">`,
    ...(input.summary ? [`<summary>${escapeBody(input.summary)}</summary>`] : []),
    `<${tag}>`,
    input.text,
    `</${tag}>`,
    "</task>",
  ].join("\n")
}

function renderMessage(input: { sessionID: SessionID; body: string }) {
  return [
    `<task id="${input.sessionID}" state="awaiting_reply">`,
    `<summary>Subagent sent a message and is awaiting your reply</summary>`,
    `<message>`,
    escapeBody(input.body),
    `</message>`,
    `Reply with the message tool: message(target:"subagent", task_id:"${input.sessionID}", body:"...").`,
    "</task>",
  ].join("\n")
}

function parseModelOverride(model: string): Effect.Effect<{ modelID: ModelV2.ID; providerID: ProviderV2.ID }, Error> {
  const slash = model.indexOf("/")
  if (slash <= 0 || slash === model.length - 1) {
    return Effect.fail(
      new Error(`Invalid model format: "${model}". Expected provider/model (e.g. anthropic/claude-sonnet-4)`),
    )
  }
  return Effect.succeed({
    providerID: ProviderV2.ID.make(model.slice(0, slash)),
    modelID: ModelV2.ID.make(model.slice(slash + 1)),
  })
}

export function childResultBlock(result: Record<string, unknown> | undefined): string {
  if (!result) return ""
  return `\n\n<task_return>\n${JSON.stringify(result, null, 2)}\n</task_return>`
}

export const TERSE_TAIL_CHARS = 500

export function resolveCompletionMode(
  dispatch: "full" | "terse" | undefined,
  agent: Agent.Info,
  cfg: ConfigV1.Info,
): "full" | "terse" {
  return dispatch ?? agent.completion ?? cfg.task?.completion ?? "full"
}

export function resolveContextMode(
  dispatch: "full" | "sparse" | undefined,
  agent: Agent.Info,
  cfg: ConfigV1.Info,
): "full" | "sparse" {
  return dispatch ?? agent.context ?? cfg.task?.context ?? "full"
}

function terseText(
  fullText: string,
  result: Record<string, unknown> | undefined,
  childID: string,
  slug: string | undefined,
) {
  const parts: string[] = []
  if (result) parts.push(JSON.stringify(result, null, 2))
  if (fullText) parts.push(`…${fullText.slice(-TERSE_TAIL_CHARS)}`)
  parts.push(`full result: task session ${childID}${slug ? ` (task_id: ${slug})` : ""}`)
  return parts.join("\n\n")
}

export const WAKE_BUDGET_DEFAULT = 5


const classifyFirstRun = (
  result: BackgroundJob.WaitResult,
  terminal: Option.Option<{ reason: string }>,
  evidence: {
    finalText: string
    failure?: TaskOutcomes.Result["failure"]
    timedOut: boolean
  },
): TaskOutcomes.Result => {
  if (Option.isSome(terminal))
    return { state: "aborted", text: result.info?.output ?? "", reason: terminal.value.reason }
  if (result.timedOut || evidence.timedOut)
    return {
      state: "timed_out",
      text: result.info?.error || "Task failed",
      hasFinalText: !!evidence.finalText.trim(),
    }
  if (result.info?.status === "error")
    return {
      state: "error",
      text: result.info.error || "Task failed",
      hasFinalText: !!evidence.finalText.trim(),
      failure: evidence.failure ?? { kind: "provider_or_tool_error", message: result.info.error || "Task failed" },
    }
  if (result.info?.status === "cancelled")
    return { state: "aborted", text: result.info.output ?? "", reason: "Aborted" }
  return {
    state: "completed",
    text: result.info?.output ?? "",
    hasFinalText: !!(evidence.finalText || result.info?.output || "").trim(),
    failure: evidence.failure,
  }
}

const completionReason = (outcome: TaskOutcomes.Settlement, result: unknown) => {
  if (outcome.hasFinalText ?? !!outcome.text.trim()) return undefined
  if (outcome.state === "timed_out") return "timed_out"
  if (outcome.state === "aborted") return "cancelled"
  if (outcome.failure) return outcome.failure.kind
  if (outcome.state === "error") return "provider_or_tool_error"
  if (result) return "structured_result_only"
  return "no_final_text"
}

const completedPayload = Effect.fn("TaskTool.completedPayload")(function* (
  sessions: Session.Interface,
  sessionID: SessionID,
  parentSessionID: SessionID,
  status: "ok" | "error" | "aborted",
  startedAt: number,
) {
  const base = { sessionID, parentSessionID, status }
  const exit = yield* Effect.exit(
    Effect.gen(function* () {
      const session = yield* sessions.get(sessionID).pipe(Effect.option)
      const s = Option.getOrUndefined(session)
      const messages = yield* sessions.messages({ sessionID }).pipe(Effect.option)
      const msgs = Option.getOrElse(messages, () => [] as SessionV1.WithParts[])

      const elapsedMs = Date.now() - startedAt

      let input = 0
      let output = 0
      let reasoning = 0
      let cacheRead = 0
      let cacheWrite = 0
      let totalCost = 0
      for (const msg of msgs) {
        if (msg.info.role !== "assistant") continue
        input += msg.info.tokens?.input ?? 0
        output += msg.info.tokens?.output ?? 0
        reasoning += msg.info.tokens?.reasoning ?? 0
        cacheRead += msg.info.tokens?.cache?.read ?? 0
        cacheWrite += msg.info.tokens?.cache?.write ?? 0
        totalCost += msg.info.cost ?? 0
      }

      return {
        sessionID,
        parentSessionID,
        status,
        slug: s?.slug,
        agent: s?.agent,
        model: s?.model ? `${s.model.providerID}/${s.model.id}` : undefined,
        variant: s?.model?.variant,
        elapsedMs,
        tokens: { input, output, reasoning, cacheRead, cacheWrite },
        cost: totalCost,
        result: s?.result,
      }
    }),
  )
  if (Exit.isSuccess(exit)) return exit.value
  return base
})


type PreparedTask = {
  params: Schema.Schema.Type<typeof Parameters>
  parentID: SessionID
  parentAgent: string
  nextSession: Session.Info
  session: Session.Info | undefined
  next: Agent.Info
  model: { modelID: ModelV2.ID; providerID: ProviderV2.ID }
  modelSource: string
  primaryVariant: string | undefined
  resumedVariant: string | undefined
  fallbackModel: { modelID: ModelV2.ID; providerID: ProviderV2.ID } | undefined
  completionMode: "full" | "terse"
  runInBackground: boolean
  taskPrompt: string
  metadata: {
    parentSessionId: SessionID
    sessionId: SessionID
    model: { modelID: ModelV2.ID; providerID: ProviderV2.ID }
    variant: string | undefined
    model_source: string
    background?: boolean
    scheduled?: boolean
    jobId?: string
    due_at?: string
  }
  variant: string | undefined
  ops: TaskPromptOps
  onMetadata: Tool.Context["metadata"]
  abort?: AbortSignal
  scheduled?: { row: ScheduledTaskStore.Row; owner: string; store: ScheduledTaskStore.Interface }
}

const startPrepared = Effect.fn("TaskTool.startPrepared")(function* (input: PreparedTask) {
  const { params, nextSession, session, next, model, modelSource, primaryVariant, resumedVariant, fallbackModel, completionMode, runInBackground, taskPrompt, metadata, variant, parentID, parentAgent, ops, onMetadata, abort, scheduled } = input
  const sessions = yield* Session.Service
  const background = yield* BackgroundJob.Service
  const interrupt = yield* Interrupt.Service
  const events = yield* EventV2Bridge.Service
  const outcomes = yield* TaskOutcomes.Service
  const scope = yield* Scope.Scope
  let actualModel = model
  let actualVariant = primaryVariant
  let actualModelSource = modelSource
  const evidence: {
    finalText: string
    failure?: TaskOutcomes.Result["failure"]
    timedOut: boolean
  } = { finalText: "", timedOut: false }

  const runAttempt = Effect.fn("TaskTool.runAttempt")(function* (attempt: {
    modelID: ModelV2.ID
    providerID: ProviderV2.ID
    variant: string | undefined
  }) {
    // Transcript enrichment must not turn a successful prompt into a failed task if its optional read dies.
    const before = yield* Effect.exit(sessions.messages({ sessionID: nextSession.id }))
    const previous = Exit.isSuccess(before) ? MessageV2.latest(before.value).assistant : undefined
    const parts = yield* ops.resolvePromptParts(taskPrompt)
    const result = yield* ops.prompt({
      sessionID: nextSession.id,
      origin: "subagent",
      model: {
        modelID: attempt.modelID,
        providerID: attempt.providerID,
      },
      variant: attempt.variant,
      agent: next.name,
      parts,
    })
    if (result.info.role === "assistant") {
      actualModel = { modelID: result.info.modelID, providerID: result.info.providerID }
      actualVariant = result.info.variant
    }
    evidence.finalText = result.parts
      .filter((part) => part.type === "text")
      .map((part) => part.text)
      .join("\n")
    if (result.info.role === "assistant" && result.info.error) {
      evidence.failure = TaskOutcomes.failureFromError(result.info.error)
      const message = evidence.failure.message ?? result.info.error.name
      return yield* Effect.fail(new Error(`Subagent failed (task_id: ${nextSession.id}): ${message}`))
    }
    const currentTool = result.parts.findLast((part) => part.type === "tool" && part.state.status === "error")
    const after =
      !currentTool && !evidence.finalText.trim() && Exit.isSuccess(before)
        ? yield* Effect.exit(sessions.messages({ sessionID: nextSession.id }))
        : undefined
    const failed =
      currentTool?.type === "tool" && currentTool.state.status === "error"
        ? currentTool.state.error
        : after && Exit.isSuccess(after)
          ? TaskOutcomes.toolErrorSince(after.value, previous)
          : undefined
    if (failed) {
      evidence.failure = { kind: "provider_or_tool_error", message: failed }
      return yield* Effect.fail(new Error(`Subagent failed (task_id: ${nextSession.id}): ${failed}`))
    }
    return result.parts.findLast((item) => item.type === "text")?.text ?? ""
  })

  let fallbackUsed = false
  const runTask = Effect.fn("TaskTool.runTask")(function* () {
    const attempt = (m: { modelID: ModelV2.ID; providerID: ProviderV2.ID }, v: string | undefined) => {
      evidence.finalText = ""
      evidence.failure = undefined
      evidence.timedOut = false
      const eff = runAttempt({ modelID: m.modelID, providerID: m.providerID, variant: v })
      return params.timeout === undefined ? eff : eff.pipe(Effect.timeout(params.timeout))
    }
    const recordFailure = (cause: Cause.Cause<unknown>) => {
      const error = Option.getOrUndefined(Cause.findErrorOption(cause))
      if (error instanceof Error && error.name === "TimeoutError") {
        evidence.timedOut = true
        return error
      }
      if (!evidence.failure && error instanceof Error)
        evidence.failure = { kind: "provider_or_tool_error", message: error.message }
      return error
    }
    const cancelRun = () => ops.cancelRun(nextSession.id).pipe(Effect.ignore)
    const exit = yield* Effect.exit(attempt(model, primaryVariant))
    if (Exit.isSuccess(exit)) return exit.value
    // The timeout interrupts the await, not the child runner; cancelRun stops that
    // runner without canceling the enclosing background job.
    yield* cancelRun()
    const error = recordFailure(exit.cause)
    if (
      Exit.hasInterrupts(exit) ||
      Exit.hasDies(exit) ||
      error instanceof SessionRunState.LeaseLostError ||
      fallbackModel === undefined
    )
      return yield* Effect.failCause(exit.cause)
    fallbackUsed = true
    actualModel = fallbackModel
    actualVariant = params.variant ?? resumedVariant
    actualModelSource = "fallback_model"
    const fallbackExit = yield* Effect.exit(attempt(fallbackModel, params.variant ?? resumedVariant))
    if (Exit.isFailure(fallbackExit)) {
      yield* cancelRun()
      recordFailure(fallbackExit.cause)
      return yield* Effect.failCause(fallbackExit.cause)
    }
    return fallbackExit.value
  })

  const inject = Effect.fn("TaskTool.injectBackgroundResult")(function* (outcome: TaskOutcomes.Settlement) {
    const state = outcome.state
    const text = outcome.text
    const reason = outcome.reason
    const currentParent = yield* sessions.get(parentID)
    const parentMessages = yield* sessions.messages({ sessionID: parentID }).pipe(Effect.option)
    if (Option.isNone(parentMessages)) return
    const { user: lastUser } = MessageV2.latest(parentMessages.value)
    if (!lastUser) return
    const child = yield* sessions.get(nextSession.id).pipe(Effect.option)
    const childVal = Option.getOrUndefined(child)
    const emptyReason = completionReason(outcome, childVal?.result)
    const body = emptyReason
      ? `reason: ${emptyReason}${
          emptyReason === "provider_or_tool_error"
            ? `: ${(outcome.failure?.message ?? reason ?? text) || "Task failed"}`
            : ""
        }`
      : text
    const followup = outcome.trigger === "wake"
    const frameBody =
      completionMode === "terse"
        ? (followup ? `Follow-up run #${outcome.sequence} of task ${nextSession.id}\n` : "") +
          terseText(body, childVal?.result, nextSession.id, childVal?.slug)
        : renderOutput({
            sessionID: nextSession.id,
            state,
             summary: followup
               ? `Follow-up run #${outcome.sequence} of task ${nextSession.id}: ${params.description}`
               : state === "completed"
                 ? `Background task completed: ${params.description}`
                 : state === "aborted"
                   ? `Background task aborted: ${reason ?? params.description}`
                   : state === "timed_out"
                     ? `Background task timed out: ${params.description}`
                     : `Background task failed: ${params.description}`,
            text: body,
          }) + childResultBlock(childVal?.result)
    const admission = yield* Effect.exit(
      ops.prompt({
        sessionID: parentID,
        origin: "subagent",
        agent: currentParent.agent ?? parentAgent,
        model: {
          providerID: lastUser.model.providerID,
          modelID: lastUser.model.modelID,
        },
        // Parent-session injection prefers the parent's own variant, falling back to the dispatch variant when absent.
        variant: lastUser.model.variant ?? variant,
        noReply: true,
        parts: [
          {
            type: "text",
            synthetic: true,
            text: frameBody,
          },
        ],
      }),
    )
    if (Exit.isFailure(admission)) {
      yield* Effect.logError("task outcome parent admission failed", {
        sessionID: parentID,
        cause: Cause.pretty(admission.cause),
      })
      return
    }
    yield* outcomes.continueParent(ops.loop(parentID))
  })

  const notify = Effect.fn("TaskTool.notifyBackgroundResult")(function* (jobID: SessionID) {
    yield* attach(
      background.wait({ id: jobID }).pipe(
        Effect.flatMap((result) =>
          Effect.gen(function* () {
        if (!result.info && !result.timedOut) return
        if (result.info?.status === "running" && !result.timedOut) return
        const settlement = classifyFirstRun(result, yield* interrupt.terminal(jobID), evidence)
        if (scheduled) {
          if (settlement.state === "completed") yield* scheduled.store.markCompleted(scheduled.row.id, scheduled.owner)
          else yield* scheduled.store.markFailed(scheduled.row.id, (settlement.reason ?? settlement.text) || "Scheduled task failed", scheduled.owner)
        }
            yield* events.publish(
              Event.Completed,
               yield* completedPayload(
                 sessions,
                jobID,
                parentID,
                settlement.state === "completed" ? "ok" : settlement.state === "aborted" ? "aborted" : "error",
                startedAt,
              ),
            )
            return yield* outcomes.settleInitial(jobID, settlement)
          }),
        ),
      ),
    ).pipe(Effect.forkIn(scope, { startImmediately: true }))
  })

  // Tracks whether a notify() fiber was forked to own this run's terminal
  // task.completed event. When true, the foreground release block must NOT
  // also emit (avoids double-fire); when false on a parent-interrupt, the
  // release block emits the terminal event itself (avoids zero-fire).
  let notified = false

  // Clear any stale interrupt/terminal state from a prior run of this session
  // before starting (or extending) so a reused task_id doesn't inherit a
  // cancelled terminal record from its previous run.
  yield* interrupt.clear(nextSession.id)
  // A reused task_id must not inherit a structured result envelope from its previous run.
  if (session) yield* sessions.setResult({ sessionID: nextSession.id, result: null })

  // The resume gate (above) already vetted idle sessions; a RUNNING job reaches
  // this point without the resume: true flag and can be extended normally.
  if (!scheduled && (yield* background.extend({ id: nextSession.id, run: runTask() }))) {
    return {
      title: params.description,
      metadata: {
        ...metadata,
        background: true,
        jobId: nextSession.id,
      },
      output: renderOutput({
        sessionID: nextSession.id,
        state: "running",
        summary: "Background task updated",
        text: BACKGROUND_UPDATED,
      }),
    }
  }

  const startedAt = Date.now()
  yield* outcomes.register({
    childID: nextSession.id,
    description: params.description,
    timeout: params.timeout,
    notify: (settlement) =>
      inject(settlement).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("task outcome notification failed", {
            sessionID: nextSession.id,
            cause: Cause.pretty(cause),
          }),
        ),
      ),
  })
  yield* outcomes.beginInitial(nextSession.id)
  if (scheduled && !(yield* scheduled.store.launchIntent(scheduled.row.id, scheduled.owner, Date.now())))
    return yield* Effect.fail(new Error(`Lost prelaunch claim for scheduled task ${scheduled.row.id}`))
  const info = yield* background.start({
    id: nextSession.id,
    type: id,
    title: params.description,
    metadata,
    onPromote: Effect.gen(function* () {
      notified = true
      yield* onMetadata({
        title: params.description,
        metadata: { ...metadata, background: true, jobId: nextSession.id },
      })
      yield* notify(nextSession.id)
    }),
    run: runTask().pipe(Effect.onInterrupt(() => ops.cancel(nextSession.id))),
  })
  if (scheduled) {
    yield* scheduled.store.markStarted(scheduled.row.id, scheduled.owner, Date.now())
    yield* Effect.forever(
      Effect.sleep("10 seconds").pipe(
        Effect.flatMap(() => background.get(nextSession.id)),
        Effect.flatMap((job) => job?.status === "running"
          ? scheduled.store.heartbeat(scheduled.row.id, scheduled.owner, Date.now()).pipe(Effect.asVoid)
          : Effect.interrupt),
      ),
    ).pipe(Effect.forkIn(scope))
  }

  function backgroundResult() {
    return {
      title: params.description,
      metadata: {
        ...metadata,
        background: true,
        jobId: info.id,
      },
      output: renderOutput({
        sessionID: nextSession.id,
        state: "running",
        summary: "Background task started",
        text: BACKGROUND_STARTED,
      }),
    }
  }

  if (runInBackground) {
    notified = true
    yield* notify(SessionID.make(info.id))
    return backgroundResult()
  }

  if (!abort) return yield* Effect.fail(new Error("Foreground task requires an abort signal"))
  const runCancel = yield* EffectBridge.make()
  const cancel = ops.cancel(nextSession.id)

  function onAbort() {
    runCancel.fork(cancel)
  }

  return yield* Effect.acquireUseRelease(
    Effect.sync(() => {
      abort.addEventListener("abort", onAbort)
    }),
    () =>
      Effect.gen(function* () {
        const outcome = yield* Effect.raceFirst(
          Effect.raceFirst(
            background
              .wait({ id: nextSession.id })
              .pipe(Effect.map((waited) => ({ kind: "settled" as const, waited }))),
            background.waitForPromotion(nextSession.id).pipe(Effect.map((info) => ({ kind: "promoted" as const, info }))),
          ),
          background.waitForMessage(nextSession.id).pipe(Effect.map((payload) => ({ kind: "message" as const, payload }))),
        )
        if (outcome.kind === "message") {
          // Child is parked awaiting the parent's reply and has been backgrounded;
          // fork notify so its eventual completion is still delivered to the parent.
          notified = true
          yield* notify(nextSession.id)
          // Visible "✉ Message from subagent" marker in the PARENT (this) transcript.
          // The tool's renderMessage output (returned below) is what the MODEL sees as
          // its tool-call result; the marker is what the HUMAN sees as a distinct row.
          // Best-effort: a marker write failure must not break the tool's return.
          yield* writeMessageMarker(sessions, {
            sessionID: parentID,
            peer: "subagent",
            body: outcome.payload.body,
            expectReply: true,
          }).pipe(Effect.ignore)
          return {
            title: params.description,
            metadata,
            output: renderMessage({ sessionID: nextSession.id, body: outcome.payload.body }),
          }
        }
        if (outcome.kind === "promoted") return backgroundResult()
        const result = outcome.waited.info
        if (result?.metadata?.background === true) return backgroundResult()
        const settlement = classifyFirstRun(outcome.waited, yield* interrupt.terminal(nextSession.id), evidence)
        yield* outcomes.settleInitial(nextSession.id, settlement, notified)
        const child = yield* sessions.get(nextSession.id).pipe(Effect.option)
        const childVal = Option.getOrUndefined(child)
        const childResult = childVal?.result
        if (result?.status === "error") {
           yield* events.publish(Event.Completed, yield* completedPayload(sessions, nextSession.id, parentID, "error", startedAt))
          return yield* Effect.fail(new Error(result.error || "Task failed"))
        }
        if (result?.status === "cancelled") {
          const aborted = yield* interrupt.terminal(nextSession.id)
           yield* events.publish(Event.Completed, yield* completedPayload(sessions, nextSession.id, parentID, "aborted", startedAt))
          const outputText = result?.output ?? ""
          return {
            title: params.description,
            metadata,
            output:
              completionMode === "terse"
                ? terseText(outputText, childResult, nextSession.id, childVal?.slug)
                : renderOutput({
                    sessionID: nextSession.id,
                    state: "aborted",
                    summary: Option.isSome(aborted) ? `Aborted: ${aborted.value.reason}` : "Aborted",
                    text: outputText,
                  }) + childResultBlock(childResult),
          }
          }
          const aborted = yield* interrupt.terminal(nextSession.id)
          if (Option.isSome(aborted)) {
             yield* events.publish(Event.Completed, yield* completedPayload(sessions, nextSession.id, parentID, "aborted", startedAt))
            const outputText = result?.output ?? ""
            return {
              title: params.description,
              metadata,
              output:
                completionMode === "terse"
                  ? terseText(outputText, childResult, nextSession.id, childVal?.slug)
                  : renderOutput({
                      sessionID: nextSession.id,
                      state: "aborted",
                      summary: `Aborted: ${aborted.value.reason}`,
                      text: outputText,
                    }) + childResultBlock(childResult),
            }
          }
         yield* events.publish(Event.Completed, yield* completedPayload(sessions, nextSession.id, parentID, "ok", startedAt))
          const modelChanged =
            actualModel.providerID !== model.providerID || actualModel.modelID !== model.modelID
          const variantChanged = (actualVariant ?? "default") !== (primaryVariant ?? "default")
          const modelNotice =
            modelChanged || variantChanged
              ? `Model used: ${actualModel.providerID}/${actualModel.modelID} ` +
                `(variant: ${actualVariant ?? "default"}; source: ${actualModelSource})\n`
              : ""
          const displayMetadata = {
            ...metadata,
            model: actualModel,
            variant: actualVariant,
            model_source: actualModelSource,
            ...(fallbackUsed ? { fallback_used: true as const } : {}),
          }
          const outputText = result?.output ?? ""
          return {
            title: params.description,
            metadata: displayMetadata,
            output:
              completionMode === "terse"
                ? modelNotice + terseText(outputText, childResult, nextSession.id, childVal?.slug)
                : renderOutput({
                    sessionID: nextSession.id,
                    state: "completed",
                    text: modelNotice + outputText,
                  }) +
                  childResultBlock(childResult),
          }
      }),
    (_, exit) =>
      Effect.gen(function* () {
        if (Exit.hasInterrupts(exit)) {
          // Parent interrupted while waiting on a foreground child. notify was
          // never forked (notified === false), so emit the terminal completion
          // here — otherwise the dashboard never sees this node die (zero-fire).
          // The promoted/message/background paths set notified=true and own
          // their own completion, so skip to avoid double-fire.
          if (!notified)
             yield* events.publish(Event.Completed, yield* completedPayload(sessions, nextSession.id, parentID, "aborted", startedAt))
          yield* Effect.all([cancel, background.cancel(nextSession.id)], { discard: true })
          if (!notified)
            yield* outcomes.settleInitial(nextSession.id, { state: "aborted", text: "", reason: "Aborted" }, false)
        }
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            abort.removeEventListener("abort", onAbort)
          }),
        ),
      ),
  )
})

function resolveChildPermission(parent: Session.Info, next: Agent.Info, cfg: ConfigV1.Info) {
  const childPermission = deriveSubagentSessionPermission({
    parentSessionPermission: parent.permission ?? [],
    subagent: next,
  })
  const childToolDenies = [
    ...(next.permission.some((rule) => rule.permission === "todowrite")
      ? []
      : [{ permission: "todowrite" as const, pattern: "*" as const, action: "deny" as const }]),
    ...(next.permission.some((rule) => rule.permission === id)
      ? []
      : [{ permission: id, pattern: "*" as const, action: "deny" as const }]),
    ...(cfg.experimental?.primary_tools?.map((permission) => ({
      permission,
      pattern: "*" as const,
      action: "deny" as const,
    })) ?? []),
  ]
  return [
    ...childPermission,
    ...childToolDenies.filter(
      (deny) =>
        !childPermission.some(
          (rule) =>
            rule.permission === deny.permission && rule.pattern === deny.pattern && rule.action === deny.action,
        ),
    ),
  ]
}

export const startScheduled = Effect.fn("TaskTool.startScheduled")(function* (
  row: ScheduledTaskStore.Row,
  ops: TaskPromptOps,
) {
  const store = yield* ScheduledTaskStore.Service
  const sessions = yield* Session.Service
  const agent = yield* Agent.Service
  const config = yield* Config.Service
  const permission = yield* Permission.Service
  const now = Date.now()
  if (row.state === "launch_intent" || row.state === "running") {
    if (yield* store.markAmbiguous(row.id, now)) {
      const messages = yield* sessions.messages({ sessionID: row.parentSessionID })
      const parentUser = MessageV2.latest(messages).user
      if (parentUser) yield* ops.prompt({
        sessionID: row.parentSessionID,
        origin: "wake",
        agent: parentUser.agent,
        model: parentUser.model,
        noReply: true,
        parts: [{ type: "text", synthetic: true, text: `Scheduled task ${row.id} may have started before its owner was lost. It was not replayed; inspect child session ${row.childSessionID} before retrying.` }],
      })
    }
    return
  }
  const owner = `${process.pid}:${crypto.randomUUID()}`
  const outcome = yield* Effect.exit(Effect.gen(function* () {
    const params = yield* Schema.decodeUnknownEffect(Parameters)(row.params.requested)
    const parent = yield* sessions.get(row.parentSessionID)
    const nextSession = yield* sessions.get(row.childSessionID)
    if (nextSession.parentID !== parent.id) return yield* Effect.fail(new Error("Scheduled child is no longer owned by this parent"))
    const history = yield* sessions.messages({ sessionID: parent.id })
    const lastUser = MessageV2.latest(history).user
    if (!lastUser) return yield* Effect.fail(new Error("Scheduled parent has no user message"))
    const callingAgent = yield* agent.get(lastUser.agent)
    const next = yield* agent.get(params.subagent_type)
    if (!callingAgent || !next) return yield* Effect.fail(new Error("Scheduled task agent is no longer available"))
    const cfg = yield* config.get()
    let ancestor = parent
    let depth = 0
    while (ancestor.parentID) {
      depth++
      ancestor = yield* sessions.get(ancestor.parentID)
    }
    if (depth >= (cfg.subagent_depth ?? 1)) return yield* Effect.fail(new Error("Scheduled task exceeds the current subagent depth limit"))
    const ruleset = Permission.merge(callingAgent.permission, parent.permission ?? [])
    const modelPatterns = [params.model, params.fallback_model].filter((value): value is string => value !== undefined)
    if (modelPatterns.length) yield* permission.ask({
      sessionID: parent.id,
      permission: "model_override",
      patterns: modelPatterns,
      always: modelPatterns,
      metadata: { description: params.description, subagent_type: params.subagent_type },
      ruleset,
    })
    yield* permission.ask({
      sessionID: parent.id,
      permission: id,
      patterns: [params.subagent_type],
      always: ["*"],
      metadata: { description: params.description, subagent_type: params.subagent_type },
      ruleset,
    })
    const persistedModel = row.params.model
    if (!persistedModel || typeof persistedModel !== "object" || !("modelID" in persistedModel) || typeof persistedModel.modelID !== "string" || !("providerID" in persistedModel) || typeof persistedModel.providerID !== "string")
      return yield* Effect.fail(new Error("Scheduled task has no valid pinned model"))
    if (typeof row.params.prompt !== "string") return yield* Effect.fail(new Error("Scheduled task has no concrete prompt"))
    const model = { modelID: ModelV2.ID.make(persistedModel.modelID), providerID: ProviderV2.ID.make(persistedModel.providerID) }
    const fallbackModel = params.fallback_model ? yield* parseModelOverride(params.fallback_model) : undefined
    const resolvedPermission = resolveChildPermission(parent, next, cfg)
    if (!(yield* store.claimStart(row.id, owner, Date.now()))) return
    yield* sessions.setPermission({ sessionID: nextSession.id, permission: resolvedPermission })
    if (params.wake_on_message !== undefined) yield* (yield* Messaging.Service).setWakePolicy({
      sessionID: nextSession.id,
      budget: params.wake_on_message ? WAKE_BUDGET_DEFAULT : 0,
    })
    const completionMode = resolveCompletionMode(params.completion, callingAgent, cfg)
    const modelSource = typeof row.params.modelSource === "string" ? row.params.modelSource : "scheduled_admission"
    const variant = typeof row.params.variant === "string" ? row.params.variant : undefined
    yield* startPrepared({
      params,
      parentID: parent.id,
      parentAgent: lastUser.agent,
      nextSession,
      session: undefined,
      next,
      model,
      modelSource,
      primaryVariant: variant,
      resumedVariant: undefined,
      fallbackModel,
      completionMode,
      runInBackground: true,
      taskPrompt: row.params.prompt,
      metadata: { parentSessionId: parent.id, sessionId: nextSession.id, model, variant, model_source: modelSource, background: true, scheduled: true, jobId: row.id, due_at: new Date(row.dueAt).toISOString() },
      variant,
      ops,
      onMetadata: () => Effect.void,
      scheduled: { row, owner, store },
    })
    if (now > row.dueAt) yield* ops.prompt({
      sessionID: parent.id,
      origin: "wake",
      agent: lastUser.agent,
      model: lastUser.model,
      noReply: true,
      parts: [{ type: "text", synthetic: true, text: `Scheduled task ${row.id} started late after its parent resumed. Child session: ${nextSession.id}.` }],
    })
  }))
  if (Exit.isSuccess(outcome)) return
  const error = Cause.squash(outcome.cause)
  const current = yield* store.get(row.id)
  const state = Option.getOrUndefined(current)?.state
  if (state !== "launch_intent" && state !== "running") yield* store.markFailed(row.id, error instanceof Error ? error.message : String(error), owner)
  yield* Effect.logError("scheduled task start failed", { id: row.id, cause: Cause.pretty(outcome.cause) })
  const history = yield* sessions.messages({ sessionID: row.parentSessionID })
  const lastUser = MessageV2.latest(history).user
  if (lastUser) yield* ops.prompt({
    sessionID: row.parentSessionID,
    origin: "wake",
    agent: lastUser.agent,
    model: lastUser.model,
    noReply: true,
    parts: [{ type: "text", synthetic: true, text: state === "launch_intent" || state === "running"
      ? `Scheduled task ${row.id} may have launched but its start could not be confirmed. It was not replayed. Inspect ${row.childSessionID}.`
      : `Scheduled task ${row.id} could not start: ${error instanceof Error ? error.message : String(error)}. Inspect ${row.childSessionID}.` }],
  })
})

export const TaskTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const background = yield* BackgroundJob.Service
    const config = yield* Config.Service
    const sessions = yield* Session.Service
    const scope = yield* Scope.Scope
    const flags = yield* RuntimeFlags.Service
    const database = yield* Database.Service
    const childLocks = KeyedMutex.makeUnsafe<SessionID>()
    const interrupt = yield* Interrupt.Service
    const messaging = yield* Messaging.Service
    const events = yield* EventV2Bridge.Service
    const outcomes = yield* TaskOutcomes.Service
    const fs = yield* FSUtil.Service

    const readPromptFile = Effect.fn("TaskTool.readPromptFile")(function* (filepath: string, ctx: Tool.Context) {
      const instance = yield* InstanceState.context
      const resolved = path.isAbsolute(filepath) ? filepath : path.resolve(instance.directory, filepath)
      yield* assertExternalDirectoryEffect(ctx, resolved, {
        bypass: Boolean(ctx.extra?.["bypassCwdCheck"]),
        kind: "file",
      })
      yield* ctx.ask({
        permission: "read",
        patterns: [permissionPath(resolved, instance)],
        always: ["*"],
        metadata: {},
      })
      return yield* fs.readFileString(resolved).pipe(
        Effect.catchReason("PlatformError", "NotFound", () =>
          Effect.fail(new Error(`Prompt file not found: ${filepath}`)),
        ),
      )
    })

    const run = Effect.fn("TaskTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      if ((params.prompt === undefined) === (params.prompt_file === undefined))
        return yield* Effect.fail(new Error("Specify exactly one of prompt or prompt_file"))
      const taskPrompt = params.prompt ?? (yield* readPromptFile(params.prompt_file!, ctx))
      const cfg = yield* config.get()
      const callingAgent = yield* agent.get(ctx.agent)
      const completionMode = resolveCompletionMode(params.completion, callingAgent!, cfg)
      const contextMode = resolveContextMode(params.context, callingAgent!, cfg)
      const runInBackground = params.background === true
      if (runInBackground && !flags.experimentalBackgroundSubagents) {
        return yield* Effect.fail(
          new Error("Background subagents require OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true"),
        )
      }
      const scheduledTime = params.start_at === undefined ? undefined : parseInstant(params.start_at)
      if (params.start_at !== undefined && scheduledTime === undefined)
        return yield* Effect.fail(new Error("start_at must be a timezone-qualified ISO-8601 instant"))
      const futureStart = scheduledTime !== undefined && scheduledTime > Date.now()
      if (futureStart && !runInBackground)
        return yield* Effect.fail(new Error("A future start_at requires background: true"))
      const scheduledStore = params.start_at === undefined
        ? Option.none<ScheduledTaskStore.Interface>()
        : yield* Effect.serviceOption(ScheduledTaskStore.Service)
      if (futureStart && Option.isNone(scheduledStore))
        return yield* Effect.fail(new Error("Durable task scheduling is not available in this runtime"))

      const parent = yield* sessions.get(ctx.sessionID)
      let current = parent
      let depth = 0
      while (current.parentID) {
        depth++
        current = yield* sessions.get(current.parentID)
      }
      if (depth >= (cfg.subagent_depth ?? 1)) {
        return yield* Effect.fail(
          new Error(
            `Subagent depth limit reached (${cfg.subagent_depth ?? 1}). Increase "subagent_depth" to allow nested subagents.`,
          ),
        )
      }

      const maxChildren = cfg.subagent_max_children ?? 32

      const modelOverride = params.model
      const overrideModel = modelOverride === undefined ? undefined : yield* parseModelOverride(modelOverride)
      const fallbackModel =
        params.fallback_model === undefined ? undefined : yield* parseModelOverride(params.fallback_model)

      const overridePatterns = [modelOverride, params.fallback_model].filter((x): x is string => x !== undefined)
      if (overridePatterns.length > 0) {
        yield* ctx.ask({
          permission: "model_override",
          patterns: overridePatterns,
          always: overridePatterns,
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type,
            ...(modelOverride ? { model: modelOverride } : {}),
            ...(params.fallback_model ? { fallback_model: params.fallback_model } : {}),
          },
        })
      }

      if (!ctx.extra?.bypassAgentCheck) {
        yield* ctx.ask({
          permission: id,
          patterns: [params.subagent_type],
          always: ["*"],
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type,
          },
        })
      }

      const next = yield* agent.get(params.subagent_type)
      if (!next) {
        return yield* Effect.fail(new Error(`Unknown agent type: ${params.subagent_type} is not a valid agent type`))
      }

      const slugTaskId = params.task_id && isSlug(params.task_id) ? params.task_id : undefined
      if (slugTaskId && !SESSION_SLUG_PATTERN.test(slugTaskId)) {
        return yield* Effect.fail(
          new Error(
            `Invalid task_id slug: "${slugTaskId}". Slugs must be lowercase letters, digits, hyphens, or underscores (max 64 chars).`,
          ),
        )
      }
      const derivedID = slugTaskId ? deriveSlugSessionID(slugTaskId, yield* sessions.root(ctx.sessionID)) : undefined
      const scheduledChildID = derivedID ?? (params.start_at !== undefined && ctx.callID
        ? SessionID.make(`ses_task_${createHash("sha256").update(`${ctx.sessionID}\0${ctx.callID}`).digest("hex").slice(0, 24)}`)
        : undefined)

      const found = params.task_id
        ? yield* sessions.get(derivedID ?? SessionID.make(params.task_id)).pipe(Effect.option)
        : scheduledChildID ? yield* sessions.get(scheduledChildID).pipe(Effect.option) : Option.none()
      if (Option.isSome(found) && found.value.parentID !== ctx.sessionID) {
        return yield* Effect.fail(
          new Error(
            slugTaskId
              ? `task_id slug "${slugTaskId}" is already used by another session in this session tree`
              : `task_id ${params.task_id} is not a child of this session`,
          ),
        )
      }
      const session = Option.getOrUndefined(found)
      const resumedModel =
        session?.model !== undefined
          ? { modelID: session.model.id, providerID: session.model.providerID }
          : undefined
      const resumedVariant =
        session?.model?.variant && session.model.variant !== "default" ? session.model.variant : undefined
      const previous = params.start_at !== undefined && session && Option.isSome(scheduledStore)
        ? yield* scheduledStore.value.get(`task_${session.id}`)
        : Option.none()
      if (Option.isSome(previous)) {
        const row = previous.value
        if (row.parentSessionID !== ctx.sessionID || JSON.stringify(row.params.requested) !== JSON.stringify(params))
          return yield* Effect.fail(new Error(`Conflicting reuse of scheduled task ${row.id}`))
        const persistedModel = row.params.model
        if (!persistedModel || typeof persistedModel !== "object" ||
            !("modelID" in persistedModel) || typeof persistedModel.modelID !== "string" ||
            !("providerID" in persistedModel) || typeof persistedModel.providerID !== "string")
          return yield* Effect.fail(new Error(`Scheduled task ${row.id} has no valid model`))
        const metadata = {
          parentSessionId: ctx.sessionID,
          sessionId: row.childSessionID,
          model: { modelID: ModelV2.ID.make(persistedModel.modelID), providerID: ProviderV2.ID.make(persistedModel.providerID) },
          variant: typeof row.params.variant === "string" ? row.params.variant : undefined,
          model_source: typeof row.params.modelSource === "string" ? row.params.modelSource : params.model ? "requested_model" : resumedModel ? "resumed_session" : next.model ? "agent_default" : "parent_model",
          background: true,
          scheduled: true,
          jobId: row.id,
          due_at: new Date(row.dueAt).toISOString(),
        }
        yield* ctx.metadata({ title: params.description, metadata })
        return { title: params.description, metadata, output: `Task ${row.childSessionID} is scheduled for ${metadata.due_at}. No child turn has started.` }
      }
      // Resume gate: an idle (finished) session needs explicit consent; a session with a
      // A child with no transcript can be an interrupted admission; running jobs retain their existing resume path.
      if (session && params.resume !== true) {
        const job = yield* background.get(session.id)
        const unstarted = futureStart && !job && (yield* sessions.messages({ sessionID: session.id, limit: 1 })).length === 0
        if (job?.status !== "running" && !unstarted)
          return yield* Effect.fail(
            new Error(
              `task_id ${params.task_id} refers to an existing idle task session; pass resume: true to continue it, or omit task_id to start a fresh task`,
            ),
          )
      }
      if (!session && params.resume === true) {
        return yield* Effect.fail(
          new Error(`resume: true was passed but task_id ${params.task_id} does not name an existing task session`),
        )
      }
      const resolvedPermission = resolveChildPermission(parent, next, cfg)
      const nextSession =
        session ??
        (yield* childLocks.withLock(ctx.sessionID)(
          Effect.gen(function* () {
            // Session execution is process-local, so this makes counting and child creation atomic for same-parent spawns.
            // Serialization is verified in packages/core/test/effect/keyed-mutex.test.ts.
            // Root sessions (depth=0) are exempt — the orchestrator is operator-supervised and may dispatch hundreds.
            const children = depth > 0 ? yield* sessions.children(ctx.sessionID) : []
            if (children.length >= maxChildren) {
              return yield* Effect.fail(
                new Error(
                  `Subagent child limit reached (${maxChildren}). Increase "subagent_max_children" to allow more direct subagents.`,
                ),
              )
            }
            return yield* sessions.create({
              id: futureStart ? scheduledChildID : derivedID,
              parentID: ctx.sessionID,
              title: params.description + ` (@${next.name} subagent)`,
              slug: slugTaskId,
              agent: next.name,
              model: overrideModel
                ? { id: overrideModel.modelID, providerID: overrideModel.providerID }
                : undefined,
              permission: resolvedPermission,
              metadata: {
                ...params.metadata,
                ...(params.message_allow === undefined ? {} : { message_allow: [...params.message_allow] }),
                ...(params.wake_on_message === undefined ? {} : { wake_on_message: params.wake_on_message }),
              },
              ...(contextMode === "sparse" ? { contextMode } : {}),
            })
          }),
        ))

      // Later child-specific session rules are replaced: resume resolves the same rules as a fresh dispatch.
      if (session) yield* sessions.setPermission({ sessionID: session.id, permission: resolvedPermission })

      if (params.task_id) yield* messaging.registerSlug(params.task_id, nextSession.id)
      if (!session || params.message_allow !== undefined || Array.isArray(session.metadata?.message_allow))
        yield* messaging.setAllow(nextSession.id, [
          ...(params.message_allow ??
            (Array.isArray(session?.metadata?.message_allow) ? session.metadata.message_allow : [])),
        ])
      if (!futureStart && (params.wake_on_message !== undefined || session?.metadata?.wake_on_message === true))
        yield* messaging.setWakePolicy({
          sessionID: nextSession.id,
          budget: (params.wake_on_message ?? session?.metadata?.wake_on_message) === true ? WAKE_BUDGET_DEFAULT : 0,
        })

      if (session) {
        yield* sessions.setMetadata({
          sessionID: session.id,
          metadata: {
            ...session.metadata,
            ...params.metadata,
            ...(params.message_allow === undefined ? {} : { message_allow: [...params.message_allow] }),
            ...(params.wake_on_message === undefined ? {} : { wake_on_message: params.wake_on_message }),
          },
        })
      }

      const msg = yield* MessageV2.get({ sessionID: ctx.sessionID, messageID: ctx.messageID }).pipe(
        Effect.provideService(Database.Service, database),
        Effect.orDie,
      )
      if (msg.info.role !== "assistant") return yield* Effect.fail(new Error("Not an assistant message"))
      const variant = msg.info.variant

      const model = overrideModel ?? resumedModel ?? next.model ?? {
        modelID: msg.info.modelID,
        providerID: msg.info.providerID,
      }
      const modelSource = overrideModel
        ? "requested_model"
        : resumedModel
          ? "resumed_session"
          : next.model
            ? "agent_default"
            : "parent_model"
      const primaryVariant = params.variant ?? resumedVariant ?? (overrideModel || resumedModel || next.model ? undefined : variant)
      let actualModel = model
      let actualVariant = primaryVariant
      let actualModelSource = modelSource
      const metadata: {
        parentSessionId: SessionID
        sessionId: SessionID
        model: typeof model
        variant: string | undefined
        model_source: string
        background?: boolean
        scheduled?: boolean
        jobId?: string
        due_at?: string
      } = {
        parentSessionId: ctx.sessionID,
        sessionId: nextSession.id,
        model,
        variant: primaryVariant,
        model_source: modelSource,
        ...(runInBackground ? { background: true } : {}),
      }

      if (futureStart && scheduledTime !== undefined && Option.isSome(scheduledStore)) {
        const admitted = yield* scheduledStore.value.admit({
          id: `task_${nextSession.id}`,
          parentSessionID: ctx.sessionID,
          childSessionID: nextSession.id,
          slug: slugTaskId,
          dueAt: scheduledTime,
          admittedAt: Date.now(),
          params: {
            requested: params,
            prompt: params.prompt,
            description: params.description,
            agent: next.name,
            model,
            modelSource,
            variant: primaryVariant,
            fallbackModel,
            permission: resolvedPermission,
            context: contextMode,
            completion: completionMode,
            metadata: params.metadata,
            messageAllow: params.message_allow,
            wakeOnMessage: params.wake_on_message,
            timeout: params.timeout,
          },
        })
        const scheduledMetadata = {
          ...metadata,
          jobId: admitted.row.id,
          scheduled: true,
          due_at: new Date(admitted.row.dueAt).toISOString(),
        }
        yield* ctx.metadata({ title: params.description, metadata: scheduledMetadata })
        return {
          title: params.description,
          metadata: scheduledMetadata,
          output: `Task ${nextSession.id} is scheduled for ${scheduledMetadata.due_at}. No child turn has started.`,
        }
      }

      yield* ctx.metadata({
        title: params.description,
        metadata,
      })

      const ops = ctx.extra?.promptOps as TaskPromptOps
      if (!ops) return yield* Effect.fail(new Error("TaskTool requires promptOps in ctx.extra"))
      return yield* startPrepared({ params, nextSession, session, next, model, modelSource, primaryVariant, resumedVariant, fallbackModel, completionMode, runInBackground, taskPrompt, metadata, variant, parentID: ctx.sessionID, parentAgent: ctx.agent, ops, onMetadata: ctx.metadata, abort: ctx.abort }).pipe(
        Effect.provideService(Session.Service, sessions),
        Effect.provideService(BackgroundJob.Service, background),
        Effect.provideService(Interrupt.Service, interrupt),
        Effect.provideService(EventV2Bridge.Service, events),
        Effect.provideService(TaskOutcomes.Service, outcomes),
        Effect.provideService(Scope.Scope, scope),
      )
    })

    return {
      description: flags.experimentalBackgroundSubagents
        ? [DESCRIPTION, BACKGROUND_DESCRIPTION].join("\n\n")
        : DESCRIPTION,
      parameters: Parameters,
      jsonSchema: flags.experimentalBackgroundSubagents ? undefined : ToolJsonSchema.fromSchema(BaseParameters),
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
