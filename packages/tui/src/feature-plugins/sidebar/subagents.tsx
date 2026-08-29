import type { Message, Part, SessionStatus, ToolPart } from "@opencode-ai/sdk/v2"
import type { TuiPlugin, TuiPluginApi } from "@opencode-ai/plugin/tui"
import type { BuiltinTuiPlugin } from "../builtins"
import { createMemo, createSignal, For, Show } from "solid-js"
import { Locale } from "../../util/locale"

const id = "internal:sidebar-subagents"

export type SidebarSubagent = {
  description: string
  status: "pending" | "active" | "done" | "failed"
  session_id: string | undefined
}

export type SidebarSubagentHistory = {
  description: string
  status: "done" | "failed"
  session_id: string
  activity: number
}

type DerivedSubagent = SidebarSubagent & { part: ToolPart }

function isActiveStatus(status: SessionStatus | undefined) {
  return status?.type === "busy" || status?.type === "retry"
}

function deriveEntries(
  messages: ReadonlyArray<{ id: string }>,
  getParts: (messageID: string) => ReadonlyArray<Part>,
): DerivedSubagent[] {
  const entries = messages.flatMap((message) =>
    getParts(message.id).flatMap((part) => {
      if (part.type !== "tool" || part.tool !== "task") return []

      const inputDescription = part.state.input.description
      const title = "title" in part.state ? part.state.title : undefined
      const description =
        typeof inputDescription === "string" ? inputDescription : typeof title === "string" ? title : "Subagent"
      const metadata = "metadata" in part.state ? part.state.metadata : undefined
      const sessionID =
        typeof metadata === "object" &&
        metadata !== null &&
        "sessionId" in metadata &&
        typeof metadata.sessionId === "string"
          ? metadata.sessionId
          : undefined

      const status: SidebarSubagent["status"] =
        part.state.status === "running"
          ? "active"
          : part.state.status === "completed"
            ? "done"
            : part.state.status === "error"
              ? "failed"
              : "pending"

      return [{ description, status, session_id: sessionID, part }]
    }),
  )
  const latestBySession = new Map<string, DerivedSubagent>()
  const pending: DerivedSubagent[] = []
  for (const entry of entries) {
    if (entry.session_id) latestBySession.set(entry.session_id, entry)
    else pending.push(entry)
  }
  return [...latestBySession.values(), ...pending]
}

export function deriveSubagents(
  messages: ReadonlyArray<{ id: string }>,
  getParts: (messageID: string) => ReadonlyArray<Part>,
): SidebarSubagent[] {
  return deriveEntries(messages, getParts).map((entry) => ({
    description: entry.description,
    status: entry.status,
    session_id: entry.session_id,
  }))
}

export function activeSubagents(
  messages: ReadonlyArray<{ id: string }>,
  getParts: (messageID: string) => ReadonlyArray<Part>,
  getStatus: (sessionID: string) => SessionStatus | undefined,
) {
  return deriveSubagents(messages, getParts).flatMap((entry) => {
    if (!entry.session_id) return entry.status === "active" || entry.status === "pending" ? [entry] : []

    if (!isActiveStatus(getStatus(entry.session_id))) return []
    return [{ ...entry, status: "active" as const }]
  })
}

// History ranks by the child's own last activity: a background dispatch's parent
// part closes at spawn, so part times cannot date the child's work. Session
// time.updated marks prompt start only — message completion is the real last
// activity — so rank by the max of the signals present.
function childActivity(
  entry: DerivedSubagent,
  sessionID: string,
  child: {
    session: (sessionID: string) => { time: { updated: number } } | undefined
    messages: (sessionID: string) => ReadonlyArray<Message>
  },
): number {
  const signals: number[] = []
  const session = child.session(sessionID)
  if (session) signals.push(session.time.updated)
  signals.push(
    ...child.messages(sessionID).map((message) => {
      if (message.role !== "assistant") return message.time.created
      return message.time.completed ?? message.time.created
    }),
  )
  const state = entry.part.state
  if (state.status === "running") signals.push(state.time.start)
  else if (state.status !== "pending") signals.push(state.time.end)
  return signals.length > 0 ? Math.max(...signals) : Number.NEGATIVE_INFINITY
}

function historyStatus(entry: DerivedSubagent, childMessages: ReadonlyArray<Message>): "done" | "failed" {
  if (entry.status === "failed") return "failed"
  const lastAssistant = childMessages.findLast((message) => message.role === "assistant")
  return lastAssistant?.error !== undefined ? "failed" : "done"
}

export function recentSubagents(
  messages: ReadonlyArray<{ id: string }>,
  getParts: (messageID: string) => ReadonlyArray<Part>,
  getStatus: (sessionID: string) => SessionStatus | undefined,
  child: {
    session: (sessionID: string) => { time: { updated: number } } | undefined
    messages: (sessionID: string) => ReadonlyArray<Message>
  },
  limit = 10,
): SidebarSubagentHistory[] {
  const ranked = deriveEntries(messages, getParts)
    .flatMap((entry, index) => {
      if (!entry.session_id || isActiveStatus(getStatus(entry.session_id))) return []
      const sessionID = entry.session_id
      return [
        {
          description: entry.description,
          status: historyStatus(entry, child.messages(sessionID)),
          session_id: sessionID,
          activity: childActivity(entry, sessionID, child),
          index,
        },
      ]
    })
    .sort((a, b) => b.activity - a.activity || b.index - a.index)
    .slice(0, limit)
  return ranked.map((entry) => ({
    description: entry.description,
    status: entry.status,
    session_id: entry.session_id,
    activity: entry.activity,
  }))
}

function View(props: { api: TuiPluginApi; session_id: string }) {
  const theme = () => props.api.theme.current
  const [historyOpen, setHistoryOpen] = createSignal(true)
  const list = createMemo(() =>
    activeSubagents(
      props.api.state.session.messages(props.session_id),
      (messageID) => props.api.state.part(messageID),
      props.api.state.session.status,
    ),
  )
  const history = createMemo(() =>
    recentSubagents(
      props.api.state.session.messages(props.session_id),
      (messageID) => props.api.state.part(messageID),
      props.api.state.session.status,
      {
        session: props.api.state.session.get,
        messages: props.api.state.session.messages,
      },
    ),
  )

  const statusColor = (status: SidebarSubagent["status"]) => {
    if (status === "active") return theme().success
    if (status === "failed") return theme().error
    if (status === "pending") return theme().warning
    return theme().textMuted
  }

  const statusLabel = (status: SidebarSubagent["status"]) => {
    if (status === "active") return "Active"
    if (status === "done") return "Done"
    if (status === "failed") return "Failed"
    return "Pending"
  }

  return (
    <Show when={list().length > 0 || history().length > 0}>
      <box gap={1}>
        <Show when={list().length > 0}>
          <box>
            <text fg={theme().text}>
              <b>Subagents</b>
            </text>
            <For each={list()}>
              {(item) => {
                const navigate = item.session_id
                  ? () => props.api.route.navigate("session", { sessionID: item.session_id })
                  : undefined
                return (
                  <box flexDirection="row" gap={1} onMouseUp={navigate}>
                    <text flexShrink={0} style={{ fg: statusColor(item.status) }}>
                      •
                    </text>
                    <text fg={theme().text} wrapMode="word">
                      {item.description} <span style={{ fg: theme().textMuted }}>{statusLabel(item.status)}</span>
                    </text>
                  </box>
                )
              }}
            </For>
          </box>
        </Show>
        <Show when={history().length > 0}>
          <box>
            <box flexDirection="row" gap={1} onMouseDown={() => history().length > 2 && setHistoryOpen((x) => !x)}>
              <Show when={history().length > 2}>
                <text fg={theme().text}>{historyOpen() ? "▼" : "▶"}</text>
              </Show>
              <text fg={theme().text}>
                <b>Recent subagents</b>
                <Show when={!historyOpen()}>
                  <span style={{ fg: theme().textMuted }}> ({history().length})</span>
                </Show>
              </text>
            </box>
            <Show when={history().length <= 2 || historyOpen()}>
              <For each={history()}>
                {(item) => (
                  <box
                    flexDirection="row"
                    gap={1}
                    onMouseUp={() => props.api.route.navigate("session", { sessionID: item.session_id })}
                  >
                    <text flexShrink={0} style={{ fg: statusColor(item.status) }}>
                      •
                    </text>
                    <text fg={theme().text} wrapMode="word">
                      {item.description}{" "}
                      <span style={{ fg: theme().textMuted }}>
                        {statusLabel(item.status)} · {Locale.relative(item.activity)}
                      </span>
                    </text>
                  </box>
                )}
              </For>
            </Show>
          </box>
        </Show>
      </box>
    </Show>
  )
}

const tui: TuiPlugin = async (api) => {
  api.slots.register({
    order: 150,
    slots: {
      sidebar_content(_ctx, props) {
        return <View api={api} session_id={props.session_id} />
      },
    },
  })
}

const plugin: BuiltinTuiPlugin = {
  id,
  tui,
}

export default plugin
