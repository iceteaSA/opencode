import { afterEach, describe, expect, test } from "bun:test"
import { NodeFileSystem } from "@effect/platform-node"
import { Effect, FileSystem, Fiber, Layer } from "effect"
import { existsSync } from "fs"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { LOG_TRIM_LOCK_STALE_MS, trim } from "../../src/observability/logging"

const dirs: string[] = []

afterEach(async () => {
  await Promise.all(dirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })))
})

async function write(lines: number) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-logging-trim-"))
  dirs.push(dir)
  const file = path.join(dir, "opencode.log")
  await fs.writeFile(file, Array.from({ length: lines }, (_, i) => `line ${String(i).padStart(6, "0")}\n`).join(""))
  return file
}

const run = (file: string, options: { max: number; keep: number }) =>
  Effect.runPromise(trim(file, options).pipe(Effect.provide(NodeFileSystem.layer)))

describe("Logging.trim", () => {
  test("leaves a file at or below the limit alone", async () => {
    const file = await write(100)
    const before = await fs.readFile(file, "utf8")
    await run(file, { max: before.length, keep: 100 })
    expect(await fs.readFile(file, "utf8")).toBe(before)
  })

  test("keeps the tail, starting on a line boundary", async () => {
    const file = await write(10_000)
    const original = await fs.readFile(file, "utf8")
    // 12 bytes per line; a keep that is not a multiple of the line length forces a mid-line cut.
    await run(file, { max: 60_000, keep: 30_005 })
    const after = await fs.readFile(file, "utf8")
    expect(after.length).toBeLessThanOrEqual(30_005)
    expect(after.length).toBeGreaterThan(30_005 - 12)
    expect(after.startsWith("line ")).toBe(true)
    expect(original.endsWith(after)).toBe(true)
    expect(original.at(original.length - after.length - 1)).toBe("\n")
  })

  test("keeps nothing when the tail has no newline", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-logging-trim-"))
    dirs.push(dir)
    const file = path.join(dir, "opencode.log")
    await fs.writeFile(file, "x".repeat(1000))
    await run(file, { max: 500, keep: 100 })
    expect((await fs.stat(file)).size).toBe(0)
  })

  test("skips while another process holds the lock, and releases its own", async () => {
    const file = await write(10_000)
    const before = await fs.readFile(file, "utf8")
    await fs.mkdir(`${file}.trim`)
    await run(file, { max: 60_000, keep: 30_000 })
    expect(await fs.readFile(file, "utf8")).toBe(before)
    await fs.rmdir(`${file}.trim`)
    await run(file, { max: 60_000, keep: 30_000 })
    expect((await fs.stat(file)).size).toBeLessThanOrEqual(30_000)
    expect(existsSync(`${file}.trim`)).toBe(false)
  })

  test("removes a stale lock but still yields that round", async () => {
    const file = await write(10_000)
    const before = await fs.readFile(file, "utf8")
    await fs.mkdir(`${file}.trim`)
    const stale = new Date(Date.now() - LOG_TRIM_LOCK_STALE_MS - 1000)
    await fs.utimes(`${file}.trim`, stale, stale)
    await run(file, { max: 60_000, keep: 30_000 })
    expect(await fs.readFile(file, "utf8")).toBe(before)
    expect(existsSync(`${file}.trim`)).toBe(false)
    await run(file, { max: 60_000, keep: 30_000 })
    expect((await fs.stat(file)).size).toBeLessThanOrEqual(30_000)
  })

  test("an interrupt during lock acquisition leaves no lock behind", async () => {
    const file = await write(10_000)
    // The directory exists on disk but mkdir has not returned yet when the interrupt lands.
    const slow = Layer.effect(
      FileSystem.FileSystem,
      Effect.map(FileSystem.FileSystem, (real) =>
        FileSystem.make({
          ...real,
          makeDirectory: (dir, options) =>
            real.makeDirectory(dir, options).pipe(Effect.tap(() => Effect.sleep("200 millis"))),
        }),
      ),
    ).pipe(Layer.provide(NodeFileSystem.layer))
    await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* trim(file, { max: 60_000, keep: 30_000 }).pipe(Effect.forkChild)
        yield* Effect.sleep("50 millis")
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.provide(slow)),
    )
    expect(existsSync(`${file}.trim`)).toBe(false)
  })

  test("concurrent trimmers never empty the file", async () => {
    const file = await write(10_000)
    await Promise.all(Array.from({ length: 8 }, () => run(file, { max: 60_000, keep: 30_000 })))
    const after = await fs.readFile(file, "utf8")
    expect(after.length).toBeGreaterThanOrEqual(30_000 - 12)
    expect(after.length).toBeLessThanOrEqual(30_000)
    expect(after.startsWith("line ")).toBe(true)
    expect(after.endsWith("line 009999\n")).toBe(true)
  })

  test("scans past a chunk boundary to find the line start", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-logging-trim-"))
    dirs.push(dir)
    const file = path.join(dir, "opencode.log")
    // One 200 KiB line followed by a short one: the newline is several read chunks past the cut.
    await fs.writeFile(file, "a".repeat(200 * 1024) + "\nlast\n")
    await run(file, { max: 1024, keep: 150 * 1024 })
    expect(await fs.readFile(file, "utf8")).toBe("last\n")
  })
})
