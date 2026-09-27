export function parseInstant(value: string) {
  const match = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$/.exec(value)
  if (!match) return undefined
  const time = Date.parse(value)
  if (!Number.isFinite(time)) return undefined
  const zone = match[1]!
  const offset = zone === "Z" ? 0 : (zone[0] === "+" ? 1 : -1) * (Number(zone.slice(1, 3)) * 60 + Number(zone.slice(4, 6)))
  return new Date(time + offset * 60_000).toISOString().slice(0, 19) === value.slice(0, 19) ? time : undefined
}
