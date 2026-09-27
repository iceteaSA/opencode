import { Effect } from "effect"
import { AppRuntime } from "../../src/effect/app-runtime"
import { flushLogs } from "../../src/cli/exit"

await AppRuntime.runPromise(Effect.logInfo("flush-before-exit-marker"))
await flushLogs()
process.exit(0)
