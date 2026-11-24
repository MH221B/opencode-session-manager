# OpenCode session manager

Server plugin targeting OpenCode 2.4.1. It exposes `session_manager` tools
`list`, `attach`, `start`, `prompt`, `wait`, `context`, `permission`, and
`interrupt`. Project sessions are owned by the creating managing session; the
managing session gets bounded results, not transcripts.

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
| `interrupt` | Stop a managed session. Refuses and lists descendants instead of leaving them running. |

Because results arrive as messages, `wait` is a courtesy for the cases where the
managing agent wants the answer in its next turn. Long or unattended work should
not depend on it.

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

## Known limitation: permission replies are location-scoped

`permission.asked` events are server-wide, so this plugin can surface a request
raised by a project session in any directory. `ctx.permission.reply` is not: it
answers against the plugin's own location, so replying to a request raised in
another directory fails with "Permission request not found". The client exposes
no location override (`RequestOptions` is `{ signal, headers, onActivity }`).

Consequences:

- Propagation works everywhere: `wait` and `context` return `permission_required`
  with the request ID, action, resources and an instruction line.
- Answering works only for sessions in the plugin's own location.
- Run project sessions with allow-all permissions (or matching
  `allowPermissions` rules) so requests do not arise in unattended use, or answer
  them in the session's own client.

## Distribution

Run `npm pack` to inspect the distributable. To publish later, authenticate with
npm, check name/version availability and package contents, then run
`npm publish` only with the operator's authorization.
