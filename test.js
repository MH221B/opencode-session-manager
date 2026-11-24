import assert from "node:assert/strict"
import plugin from "./index.js"

const storage = new Map()
const sessions = new Map()
const tools = {}
let sequence = 0
let permission = []
let replied = 0
let interrupted = true
let idle = false
const ctx = {
  options: { maxOutput: 4 },
  storage: {
    get: async (key) => storage.get(key),
    set: async (key, value) => storage.set(key, structuredClone(value)),
  },
  tool: { transform: async (fn) => fn({ add: (tool) => { tools[tool.name] = tool } }) },
  event: { subscribe: async function* () {} },
  permission: {
    list: async ({ sessionID }) => permission.filter((p) => p.sessionID === sessionID),
    reply: async ({ sessionID, requestID }) => {
      replied++
      permission = permission.filter((p) => p.sessionID !== sessionID || p.id !== requestID)
    },
  },
  session: {
    hook: async () => ({ dispose: async () => {} }),
    create: async (input) => {
      assert.equal(input.parentID, undefined, "project session must be a root session")
      const id = `ses_${++sequence}`
      sessions.set(id, [])
      return { id }
    },
    context: async ({ sessionID }) => sessions.get(sessionID),
    get: async ({ sessionID }) => {
      if (!sessions.has(sessionID)) throw new Error("not found")
      return { id: sessionID, parentID: sessionID === "ses_child" ? "ses_1" : undefined }
    },
    list: async ({ parentID }) => ({
      data: parentID === "ses_1" && sessions.has("ses_child") ? [{ id: "ses_child", parentID }] : [],
      cursor: {},
    }),
    prompt: async () => {},
    wait: async (_, { signal }) => new Promise((resolve, reject) => {
      if (idle) resolve()
      else if (signal.aborted) reject(signal.reason)
      else signal.addEventListener("abort", () => reject(signal.reason), { once: true })
    }),
    interrupt: async () => ({ interrupted }),
  },
}
await plugin.setup(ctx)
const call = (sessionID = "ses_owner", signal = new AbortController().signal) => ({
  sessionID, signal, progress: async () => {},
})
const run = async (name, input, context = call()) =>
  JSON.parse((await tools[name].execute(input, context)).content)

const [a, b] = await Promise.all([
  run("start", { directory: "a", task: "A" }),
  run("start", { directory: "b", task: "B" }),
])
assert.notEqual(a.sessionID, b.sessionID)
assert.equal(a.status, "running")
await assert.rejects(() => run("context", { sessionID: a.sessionID }, call("ses_other")), /unowned/)
await assert.rejects(() => run("prompt", { sessionID: a.sessionID, task: "again" }), /pending/)
sessions.get(a.sessionID).push({ id: "msg1", type: "assistant", content: [{ type: "text", text: "partial" }] })
const timed = await run("wait", { sessionID: a.sessionID, timeout: 10 })
assert.equal(timed.status, "running")
assert.deepEqual(timed.output, { text: "part", truncated: true })
assert.equal(timed.partial, true)
permission = [{ id: "p1", sessionID: a.sessionID, action: "edit", resources: ["x"] }]
const requested = await run("wait", { sessionID: a.sessionID, timeout: 100 })
assert.equal(requested.status, "permission_required")
assert.equal(requested.requestID, "p1")
assert.equal(replied, 0)
assert.equal((await run("permission", { sessionID: a.sessionID, requestID: "p1", decision: "reject" })).status, "replied")
assert.equal(replied, 1)
assert.equal((await run("permission", { sessionID: a.sessionID, requestID: "p1", decision: "once" })).status, "stale")
permission = []
const originalSetInterval = globalThis.setInterval
const originalClearInterval = globalThis.clearInterval
const originalSetTimeout = globalThis.setTimeout
const originalClearTimeout = globalThis.clearTimeout
const intervals = new Set()
const timers = new Set()
globalThis.setInterval = (...args) => { const id = originalSetInterval(...args); intervals.add(id); return id }
globalThis.clearInterval = (id) => { intervals.delete(id); return originalClearInterval(id) }
globalThis.setTimeout = (...args) => { const id = originalSetTimeout(...args); timers.add(id); return id }
globalThis.clearTimeout = (id) => { timers.delete(id); return originalClearTimeout(id) }
const controller = new AbortController()
let ready
const entered = new Promise((resolve) => { ready = resolve })
const waiting = run("wait", { sessionID: a.sessionID, timeout: 1000 }, {
  ...call("ses_owner", controller.signal),
  progress: async () => { ready() },
})
await entered
await new Promise((resolve) => setImmediate(resolve))
controller.abort()
assert.equal((await waiting).status, "cancelled")
assert.equal(intervals.size, 0, "cancellation leaked an interval")
assert.equal(timers.size, 0, "cancellation leaked a timer")
globalThis.setInterval = originalSetInterval
globalThis.clearInterval = originalClearInterval
globalThis.setTimeout = originalSetTimeout
globalThis.clearTimeout = originalClearTimeout
interrupted = false
await assert.rejects(() => run("interrupt", { sessionID: a.sessionID }), /not confirmed/i)
assert.equal((await run("context", { sessionID: a.sessionID })).status, "pending")
interrupted = true
assert.equal((await run("interrupt", { sessionID: a.sessionID })).interrupted, true)
assert.equal((await run("wait", { sessionID: a.sessionID })).status, "idle")
assert.equal((await run("prompt", { sessionID: a.sessionID, task: "again" })).status, "queued")
sessions.set("ses_child", [])
permission = [{ id: "p2", sessionID: "ses_child", action: "shell", resources: ["test"], message: "approval" }]
const childRequest = await run("context", { sessionID: a.sessionID })
assert.equal(childRequest.status, "permission_required")
assert.equal(childRequest.sessionID, "ses_child")
assert.equal(childRequest.message, "approval")
await assert.rejects(() => run("context", { sessionID: "ses_child" }), /descendant of ses_1/)
await assert.rejects(() => run("interrupt", { sessionID: a.sessionID }), /descendants exist/)
assert.equal((await run("permission", { sessionID: "ses_child", requestID: "p2", decision: "once" })).status, "replied")
sessions.set("ses_existing", [{ id: "old", type: "assistant", content: [{ type: "text", text: "old" }] }])
await assert.rejects(() => run("attach", { sessionID: "ses_owner" }), /managing session/)
await assert.rejects(() => run("attach", { sessionID: "ses_existing" }), /running session/)
idle = true
assert.equal((await run("attach", { sessionID: "ses_existing" })).status, "attached")
assert.equal((await run("context", { sessionID: "ses_existing" })).output, null)
await assert.rejects(() => run("attach", { sessionID: "ses_existing" }, call("ses_other")), /another session/)
assert.ok(Array.isArray((await run("list", {})).sessions))
console.log("PASS: ownership, concurrency, pending guard, timeout, cap, permission, cancellation")
