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

// History tools deliberately read unowned sessions but must never mutate them.
const history = new Map()
const historyTools = {}
const mutations = []
let listing = { data: [], cursor: {} }
let contextCalls = []
let hanging = false
const historyCtx = {
  ...ctx,
  options: { maxOutput: 100 },
  storage: {
    get: async () => undefined,
    set: async () => { mutations.push("storage.set") },
    scan: async () => ({ entries: [] }),
  },
  tool: { transform: async (fn) => fn({ add: (tool) => { historyTools[tool.name] = tool } }) },
  permission: { ...ctx.permission, reply: async () => { mutations.push("permission.reply") } },
  session: {
    ...ctx.session,
    create: async () => { mutations.push("create") },
    prompt: async () => { mutations.push("prompt") },
    interrupt: async () => { mutations.push("interrupt") },
    get: async ({ sessionID }) => {
      if (!history.has(sessionID)) throw new Error("not found")
      return { id: sessionID, title: "History", location: { directory: "/other" }, time: { updated: 123 }, parentID: "parent" }
    },
    list: async () => listing,
    context: async ({ sessionID }) => {
      contextCalls.push(sessionID)
      if (hanging) return new Promise(() => {})
      if (!history.has(sessionID)) throw new Error("not found")
      return history.get(sessionID)
    },
  },
}
await plugin.setup(historyCtx)
const hrun = async (name, input, context = call()) =>
  JSON.parse((await historyTools[name].execute(input, context)).content)
const message = (id, text, type = "user") => ({ id, type, content: [{ type: "text", text }] })
history.set("foreign", [message("old", "old"), message("new", "Banana banana"), {
  id: "ignored", type: "assistant", content: [{ type: "reasoning", text: "banana" }, { type: "tool", text: "banana" }],
}])
assert.deepEqual(await hrun("info", { sessionID: "foreign" }), {
  sessionID: "foreign", title: "History", directory: "/other", updated: 123, parentID: "parent", truncated: false,
})
assert.deepEqual(await hrun("read", { sessionID: "foreign", limit: 2 }), {
  sessionID: "foreign", messages: [{ id: "old", type: "user", text: "old" }, { id: "new", type: "user", text: "Banana banana" }], truncated: false, messagesOmitted: false,
})
assert.equal((await hrun("search", { sessionID: "foreign", query: "ana" })).matches[0].matchCount, 4)
assert.equal((await hrun("search", { sessionID: "foreign", query: "BANANA", caseSensitive: true })).matches.length, 0)
assert.equal((await hrun("search", { sessionID: "foreign", query: "absent" })).matches.length, 0)
await assert.rejects(() => hrun("search", { query: "" }), /Query/)
for (const name of ["read", "info", "search"]) {
  await assert.rejects(() => hrun(name, { sessionID: "missing", ...(name === "search" ? { query: "x" } : {}) }), /not found/)
}
history.set("empty", [])
assert.deepEqual((await hrun("read", { sessionID: "empty" })).messages, [])
const savedGet = historyCtx.session.get
historyCtx.session.get = async ({ sessionID }) => ({ id: sessionID, title: "t".repeat(300) })
const clippedInfo = await hrun("info", { sessionID: "foreign" })
assert.equal(clippedInfo.title.length, 256)
assert.equal(clippedInfo.truncated, true)
historyCtx.session.get = savedGet
history.set("long", [message("1", "x".repeat(80)), message("2", "y".repeat(80))])
const readLong = await hrun("read", { sessionID: "long" })
assert.equal(readLong.messages.map((m) => m.text).join("").length, 100)
assert.equal(readLong.truncated, true)
history.set("matches", [message("1", "a".repeat(60) + "needle" + "b".repeat(60)), message("2", "needle")])
const capped = await hrun("search", { sessionID: "matches", query: "needle", limit: 1 })
assert.equal(capped.matches.length, 1)
assert.equal(capped.matches[0].excerpt.length, 100)
assert.equal(capped.truncated, true)
history.set("many", Array.from({ length: 101 }, (_, i) => message(String(i), "none")))
const many = await hrun("search", { sessionID: "many", query: "none", limit: 100 })
assert.equal(many.reason, "message_limit")
assert.equal(many.matches.length, 25) // Aggregate 100-character text budget.
listing = { data: Array.from({ length: 50 }, (_, i) => ({ id: `s${i}` })), cursor: { next: "next" } }
for (const item of listing.data) history.set(item.id, [])
const pageSearch = await hrun("search", { query: "x" })
assert.equal(pageSearch.sessionsScanned, 50)
assert.equal(pageSearch.reason, "session_limit")
listing = { data: [{ id: "foreign" }, { id: "empty" }], cursor: {} }
const ordered = await hrun("search", { query: "banana" })
assert.deepEqual(ordered.matches.map((m) => m.sessionID), ["foreign"])
history.set("partcap", [message("1", "x".repeat(100000) + "needle")])
const partcap = await hrun("search", { sessionID: "partcap", query: "needle" })
assert.equal(partcap.reason, "text_limit")
assert.equal(partcap.matches.length, 0)
history.set("totalcap", Array.from({ length: 11 }, (_, i) => message(String(i), "x".repeat(100000))))
assert.equal((await hrun("search", { sessionID: "totalcap", query: "z" })).reason, "text_limit")
const controller2 = new AbortController()
hanging = true
contextCalls = []
const aborted = hrun("search", { sessionID: "foreign", query: "x" }, call("ses_owner", controller2.signal))
await new Promise((resolve) => setImmediate(resolve))
controller2.abort()
assert.equal((await aborted).reason, "cancelled")
assert.equal(contextCalls.length, 1)
const timedOut = await hrun("search", { sessionID: "foreign", query: "x" })
assert.equal(timedOut.reason, "timeout")
hanging = false
const savedContext = historyCtx.session.context
historyCtx.session.context = async () => { throw new Error("SDK failure") }
await assert.rejects(() => hrun("search", { query: "x" }), /SDK failure/)
historyCtx.session.context = savedContext
assert.deepEqual(mutations, [])
assert.deepEqual(historyTools.search.options, { namespace: "session_manager", codemode: true })
console.log("PASS: read-only history tools, search caps, cancellation and timeout")
