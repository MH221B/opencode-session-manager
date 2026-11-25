# OpenCode session manager

Server plugin targeting OpenCode 2.4.1. It exposes `session_manager` tools
`list`, `attach`, `start`, `prompt`, `wait`, `context`, `permission`, `answer`, and
`interrupt`, and `delete`, plus the read-only history tools `read`, `search`, and `info`.
Project sessions are owned by the creating managing session.

Install dependencies with `npm install`. In a test workspace's `opencode.json`,
set `plugins` to an array containing the absolute path of this package directory.
Start `opencode serve` from that workspace. For an installed npm package, use
its package name in `plugins` instead. See
https://opencode.ai/docs/build/plugins for current loading instructions.

## How a run completes

`start` and `prompt` return as soon as the prompt is admitted (`queued` /
`running`). The caller does not wait for the project.

Completion is driven by server events, not by polling and not by a quiet-window
heuristic:

- `ctx.session.hook("experimental.idle", ...)` fires when a session runs out of
  foreground and background work. Its `background` array is retained for ten
  minutes as evidence that background items are still running.
- `session.execution.succeeded` / `failed` / `interrupted` record the outcome.
- When the root session is idle, no descendant is active, and no background item
  is still live, the plugin reads the last assistant message and delivers a
  notice into the owning session with
  `ctx.session.synthetic({ delivery: "queue" })`.

The notice looks like:

```
Project ses_... finished.
Status: completed.
Answer:
<the project's final text, capped and labelled if truncated>
```

`Status` is `completed`, `failed`, `interrupted`, or `incomplete`. `incomplete`
means the plugin restarted with an unfinished entry and there is no outcome
record or error evidence to decide between completed and failed — it is honest
about not knowing rather than claiming a result.

A subagent spawned by a project is folded in before delivery: the tree must be
idle at every level. Duplicate notices are suppressed per entry (`watched` while
a `wait` is collecting, `delivered` afterwards), and restart reconciliation
re-checks entries that were pending when the server stopped.

## The tools

| Tool | Purpose |
| --- | --- |
| `list` | Recent sessions, with titles, directories and `parentID`; `limit` (1–100, default 20), `cursor` to page. |
| `attach` | Adopt an existing idle session by ID so it becomes manageable. Refuses running sessions, descendants of managed sessions, sessions owned by another managing session, and the managing session itself. |
| `start` | Create a session in `directory`, queue `task`, return its ID immediately. |
| `prompt` | Queue another task to a managed session. Refuses when a prompt is still pending. |
| `wait` | Convenience join. Reports state after `timeout` (default 5000 ms, max 3600000). Returns `completed`/`failed`/`interrupted`/`running`/`cancelled`/`permission_required`, plus bounded output marked partial unless completed. Does not interrupt. |
| `context` | Current state, pending permission, and bounded last answer for a managed session. |
| `permission` | Answer a request: `once`, `always`, or `reject`. Returns `stale` if it is no longer pending. |
| `answer` | Reply to a managed root or descendant's question with `sessionID`, `formID`, and an `answer` object keyed by field key. Returns `stale` when already answered or cancelled. |
| `interrupt` | Stop a managed session. Refuses and lists descendants instead of leaving them running. |
| `delete` | Permanently delete an owned managed root and its descendants. Refuses pending, watched, busy or blocked trees, independently managed descendants, and trees containing the caller. |
| `read` | Read text messages from any accessible session by `sessionID`; `limit` selects the newest 1-100 text-bearing messages, default 50. |
| `search` | Search text with a nonempty `query` (maximum 256 characters), optional `sessionID`, `caseSensitive` and result `limit` (1–100, default 20). |
| `info` | Read available metadata for any accessible `sessionID`. |

Because results arrive as messages, `wait` is a courtesy for the cases where the
managing agent wants the answer in its next turn. Long or unattended work should
not depend on it.

Questions from a managed root or descendant arrive as queued synthetic messages
containing the origin `sessionID`, `formID`, title and complete fields. `context`
and `wait` also return `question_required` while a form is pending. The server
validates the answer values. Pending forms are checked again on plugin startup;
delivery markers suppress repeat notices after restart. If the process stops
between queueing a notice and saving its marker, a duplicate notice is possible.

Question relay requires a registered managed local service. The plugin discovers
the service without starting one and checks that its server PID is the plugin's
own process before using its form API. Standalone, embedded and remote hosts
without that registration are unsupported: question-aware tool calls report an
explicit error. Form requests use the origin session's location, including when
it differs from the manager's directory. Permission replies use that location too.

## History access

`read`, `search`, and `info` do not check ownership. They can expose history
from other managers' sessions if the server permits access. Transcripts can
contain secrets; no redaction is promised. `read` and `search` include only
user/assistant text parts, not reasoning or tool data. Existing `context` and
`wait` remain owner-scoped.

History results use JSON. `read` returns `messages`, `truncated`, and
`messagesOmitted`; `info` returns session metadata and `truncated`. Search
returns `matches`, `sessionsScanned`, `truncated`, `incomplete`, and `reason`.
`truncated` reports clipped returned text or metadata, or an omitted matching
result. `messagesOmitted` means older text messages were not selected. `incomplete`
means search may have missed matches; its first reason is `session_limit`,
`message_limit`, `text_limit`, `timeout`, or `cancelled`.

Returned history text has one aggregate `maxOutput` character budget per call;
metadata strings are capped at 256 characters. Search visits at most the first
50 recent sessions and the newest 100 messages per session, scanning at most
100,000 characters per text part and 1,000,000 total. Matching is literal,
case-insensitive by default, with overlapping counts and up to 50 characters
of context on either side. There are no date or cross-scope filters.

The server's `context` call has no verified server-side message limit here:
one response can be larger than the client-side scan caps. Search has a
five-second deadline and stops scheduling calls on timeout or cancellation,
but an already-started server call may continue after the result returns.

History browsing was inspired by Jacob's MIT-licensed
[opencode-session-manager](https://github.com/jacobjmc/opencode-session-manager).

## Plugin options

- `maxOutput`: positive character limit for returned text (default 4000, maximum
  100000). Truncation is labelled in both tool results and notices.
- `allowPermissions`: array of exact `{ "action", "resource" }` rules. A request
  is auto-approved only when every resource matches an allowed resource for that
  action. None are auto-approved by default.

Without a matching rule, `wait` and `context` return `permission_required` with
the requesting session ID, request ID, action, resources, message, and an
instruction line. The managing agent relays that to the operator: map `allow` to
`once`, `always` to `always`, and `deny` to `reject`, then call `permission`.
A stale request cannot be replayed.

Permission replies use the requesting session's directory, including for
descendants in other locations. They require the authenticated local service
to be discoverable and to run in the same server process. If the service or
session location is unavailable, the reply fails rather than using another location.

Deletion requires the local service for form and permission checks. It never
interrupts work or answers requests. It cancels known queued notices before
removal, but cannot retract notices already delivered. A removal error leaves a
durable deletion marker because the outcome may be uncertain; inspect the
session and reconcile the marker before managing it again. Cleanup errors after
confirmed removal are reported separately.

Deletion checks and plugin operations are coordinated within one server process.
Other clients can still change the tree or start work between the checks and
the server's recursive removal; the server does not expose an atomic guarded
delete. Background-work evidence is retained for ten minutes, so deletion may
be refused until that evidence expires.

## Distribution

Run `npm pack` to inspect the distributable. To publish later, authenticate with
npm, check name/version availability and package contents, then run
`npm publish` only with the operator's authorization.
