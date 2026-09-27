import { expect, test } from "bun:test"
import { flushLogs } from "../../src/cli/exit"

test("flushLogs does not wait indefinitely for runtime disposal", async () => {
  let release = () => {}
  const dispose = new Promise<void>((resolve) => {
    release = resolve
  })
  const flushing = flushLogs(() => dispose, 20)
  const settled = await Promise.race([
    flushing.then(() => true),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 200)),
  ])

  release()
  await flushing
  expect(settled).toBe(true)
})
