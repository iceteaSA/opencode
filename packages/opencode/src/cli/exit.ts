export async function flushLogs(dispose?: () => Promise<void>, timeoutMs = 2_000) {
  const flush =
    dispose ??
    (async () => {
      const { AppRuntime } = await import("../effect/app-runtime")
      await AppRuntime.dispose()
    })
  let timer: ReturnType<typeof setTimeout> | undefined
  const pending = Promise.resolve()
    .then(flush)
    .then(
      () => true,
      (error) => {
        console.error("Failed to flush logs before exit", error)
        return true
      },
    )
  const timeout = new Promise<boolean>((resolve) => {
    timer = setTimeout(() => resolve(false), timeoutMs)
  })
  const flushed = await Promise.race([pending, timeout])
  if (timer) clearTimeout(timer)
  if (!flushed) console.error("Timed out waiting for log flush; exiting anyway")
  return flushed
}

export class CliExit extends Error {
  constructor(readonly code: number) {
    super("CLI exit")
  }
}
