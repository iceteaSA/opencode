import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { KeyedMutex } from "@opencode-ai/core/effect/keyed-mutex"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Cause, Context, Effect, Exit, Layer, Option, Scope } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { attach } from "@/effect/run-service"
import { SessionID } from "@/session/schema"

export type Settlement = {
  sequence: number
  trigger: "dispatch" | "wake"
  state: "completed" | "error" | "aborted" | "timed_out"
  text: string
  reason?: string
  hasFinalText?: boolean
  failure?: { kind: "output_limit" | "provider_or_tool_error"; message?: string }
  startedAt: number
  settledAt: number
}

export type Result = Pick<Settlement, "state" | "text" | "reason" | "hasFinalText" | "failure">

export function failureFromError(error: NonNullable<SessionV1.Assistant["error"]>): NonNullable<Settlement["failure"]> {
  return {
    kind: SessionV1.OutputLengthError.isInstance(error) ? "output_limit" : "provider_or_tool_error",
    message: "message" in error.data && typeof error.data.message === "string" ? error.data.message : error.name,
  }
}

export function toolErrorSince(messages: SessionV1.WithParts[], previous?: SessionV1.Assistant) {
  // Imported messages need not have monotonic IDs; creation time comes before the ID tie-breaker.
  const failed = messages
    .filter(
      (message) =>
        message.info.role === "assistant" &&
        (!previous ||
          message.info.time.created > previous.time.created ||
          (message.info.time.created === previous.time.created && message.info.id > previous.id)),
    )
    .flatMap((message) => message.parts)
    .findLast((part) => part.type === "tool" && part.state.status === "error")
  return failed?.type === "tool" && failed.state.status === "error" ? failed.state.error : undefined
}

type Contract = {
  childID: SessionID
  description: string
  timeout?: number
  notify: (settlement: Settlement) => Effect.Effect<void>
}

type Entry = {
  contract: Contract
  active?: { sequence: number; trigger: Settlement["trigger"]; startedAt: number }
  last?: Settlement
}

type State = {
  entries: Map<SessionID, Entry>
  locks: ReturnType<typeof KeyedMutex.makeUnsafe<SessionID>>
  scope: Scope.Scope
}

export interface Interface {
  readonly register: (contract: Contract) => Effect.Effect<void>
  readonly beginInitial: (childID: SessionID) => Effect.Effect<void>
  readonly settleInitial: (
    childID: SessionID,
    result: Result,
    // False skips parent admission for foreground-only work; a promoted run passes true so either settling fiber notifies once.
    notify?: boolean,
  ) => Effect.Effect<Settlement | undefined>
  readonly runWake: (
    childID: SessionID,
    run: Effect.Effect<Result, unknown>,
    explicit?: Effect.Effect<boolean>,
  ) => Effect.Effect<Settlement | undefined>
  readonly current: (childID: SessionID) => Effect.Effect<Entry | undefined>
  readonly continueParent: (run: Effect.Effect<unknown, unknown>) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/TaskOutcomes") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const state = yield* InstanceState.make<State>(
      Effect.fn("TaskOutcomes.state")(function* () {
        const scope = yield* Scope.Scope
        return { entries: new Map<SessionID, Entry>(), locks: KeyedMutex.makeUnsafe<SessionID>(), scope }
      }),
    )

    const register: Interface["register"] = Effect.fn("TaskOutcomes.register")(function* (contract) {
      const data = yield* InstanceState.get(state)
      yield* data.locks.withLock(contract.childID)(
        Effect.sync(() => {
          if (data.entries.get(contract.childID)?.active)
            throw new Error("Cannot replace an active task outcome contract")
          data.entries.set(contract.childID, { contract, last: data.entries.get(contract.childID)?.last })
        }),
      )
    })

    const beginInitial: Interface["beginInitial"] = Effect.fn("TaskOutcomes.beginInitial")(function* (childID) {
      const data = yield* InstanceState.get(state)
      yield* data.locks.withLock(childID)(
        Effect.sync(() => {
          const entry = data.entries.get(childID)
          if (!entry || entry.active) throw new Error("Task outcome initial run is not available")
          entry.active = { sequence: (entry.last?.sequence ?? 0) + 1, trigger: "dispatch", startedAt: Date.now() }
        }),
      )
    })

    const settle = Effect.fn("TaskOutcomes.settle")(function* (
      childID: SessionID,
      result: Result,
      notify: boolean,
      explicit?: Effect.Effect<boolean>,
    ) {
      const data = yield* InstanceState.get(state)
      return yield* data.locks.withLock(childID)(
        Effect.gen(function* () {
          const entry = data.entries.get(childID)
          if (!entry?.active) return undefined
          // Check explicit intent under the child lock before publishing a timer's settlement.
          const selected =
            result.state === "timed_out" && explicit && (yield* explicit)
              ? { ...result, state: "aborted" as const, reason: "Cancelled" }
              : result
          const outcome: Settlement = { ...selected, ...entry.active, settledAt: Date.now() }
          entry.last = outcome
          entry.active = undefined
          // Admission stays under the lock so parent notices preserve the child's settlement sequence.
          if (notify) yield* entry.contract.notify(outcome)
          return outcome
        }),
      )
    })

    const settleInitial: Interface["settleInitial"] = Effect.fn("TaskOutcomes.settleInitial")(function* (
      childID,
      result,
      notify = true,
    ) {
      return yield* settle(childID, result, notify)
    })

    const runWake: Interface["runWake"] = Effect.fn("TaskOutcomes.runWake")(function* (childID, run, explicit) {
      // The wake execution is interruptible; settlement and parent admission must survive cancellation.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const data = yield* InstanceState.get(state)
          const entry = yield* data.locks.withLock(childID)(
            Effect.sync(() => {
              const found = data.entries.get(childID)
              if (!found?.last || found.active) return undefined
              found.active = { sequence: found.last.sequence + 1, trigger: "wake", startedAt: Date.now() }
              return found
            }),
          )
          if (!entry) return undefined
          const exit = yield* Effect.exit(
            restore(
              entry.contract.timeout === undefined
                ? run.pipe(Effect.map(Option.some))
                : run.pipe(Effect.timeoutOption(entry.contract.timeout)),
            ),
          )
          if (Exit.isSuccess(exit)) {
            if (Option.isSome(exit.value)) return yield* settle(childID, exit.value.value, true)
            // The timer wraps the runner's work, so its expiry already interrupts that work.
            return yield* settle(childID, { state: "timed_out", text: "", reason: "Timed out" }, true, explicit)
          }
          const error = Option.getOrUndefined(Exit.findErrorOption(exit))
          return yield* settle(
            childID,
            {
              state: Exit.hasInterrupts(exit) ? "aborted" : "error",
              text: "",
              reason: error instanceof Error ? error.message : String(error ?? "Task run failed"),
            },
            true,
          )
        }),
      )
    })

    const current: Interface["current"] = Effect.fn("TaskOutcomes.current")(function* (childID) {
      return (yield* InstanceState.get(state)).entries.get(childID)
    })

    const continueParent: Interface["continueParent"] = Effect.fn("TaskOutcomes.continueParent")(function* (run) {
      const data = yield* InstanceState.get(state)
      yield* attach(
        run.pipe(
          Effect.catchCause((cause) =>
            Effect.logError("task outcome parent loop failed", { cause: Cause.pretty(cause) }),
          ),
        ),
      ).pipe(Effect.forkIn(data.scope, { startImmediately: true }))
    })

    return Service.of({ register, beginInitial, settleInitial, runWake, current, continueParent })
  }),
)

export const node = LayerNode.make({ service: Service, layer, deps: [] })

export * as TaskOutcomes from "./task-outcomes"
