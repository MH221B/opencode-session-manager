import assert from "node:assert/strict"
import { createPlugin } from "./index.js"

const storage = new Map()
const sessions = new Map()
sessions.set("ses_owner", [])
const tools = {}
let sequence = 0
let permission = []
let replied = 0
let interrupted = true
let idle = false
let forms = []
const notices = []
let noticeSequence = 0
let syntheticGate
const formCalls = []
const permissionCalls = []
const subscribers = []
const fakeClient = {
  permission: { list: async ({ sessionID }) => permission.filter((p) => p.sessionID === sessionID), reply: async (input, options) => {
    permissionCalls.push({ input, options })
    replied++
    permission = permission.filter((p) => p.sessionID !== input.sessionID || p.id !== input.requestID)
  } },
  session: { inbox: { list: async ({ sessionID }) => notices.filter((item) => item.sessionID === sessionID), cancel: async ({ sessionID, inboxID }) => {
    const index = notices.findIndex((item) => item.id === inboxID && item.sessionID === sessionID)
    if (index >= 0) notices.splice(index, 1)
  } }, form: {
    list: async ({ sessionID }, options) => {
      formCalls.push({ sessionID, options })
      return forms.filter((form) => form.sessionID === sessionID)
    },
    reply: async ({ sessionID, formID, answer }, options) => {
      formCalls.push({ sessionID, formID, answer, options })
      forms = forms.filter((form) => form.sessionID !== sessionID || form.id !== formID)
    },
  } },
}
const plugin = createPlugin(async () => fakeClient)
assert.equal(fakeClient.session.permission, undefined, "permission API must exist only at the client root")
const emit = (event) => { for (const subscriber of subscribers) subscriber(event) }
const ctx = {
  location: { directory: "D:\\owner" },
  options: { maxOutput: 4 },
  storage: {
    get: async (key) => storage.get(key),
    set: async (key, value) => storage.set(key, structuredClone(value)),
    remove: async (key) => storage.delete(key),
    scan: async () => ({ entries: [] }),
  },
  tool: { transform: async (fn) => fn({ add: (tool) => { tools[tool.name] = tool } }) },
  event: { subscribe: async function* ({ types, signal }) {
    const queue = []
    let wake
    const receive = (event) => { if (types.includes(event.type)) { queue.push(event); wake?.() } }
    subscribers.push(receive)
    signal.addEventListener("abort", () => wake?.(), { once: true })
    while (!signal.aborted) {
      if (!queue.length) await new Promise((resolve) => { wake = resolve })
      while (queue.length) yield queue.shift()
    }
  } },
  permission: {
    list: async ({ sessionID }) => permission.filter((p) => p.sessionID === sessionID),
    reply: async () => { throw new Error("location-bound permission reply must not be used") },
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
      return { id: sessionID, location: { directory: sessionID === "ses_child" ? "D:\\other project" : "D:\\project" }, parentID: sessionID === "ses_child" ? "ses_1" : undefined }
    },
    list: async ({ parentID }) => ({
      data: parentID === "ses_1" && sessions.has("ses_child") ? [{ id: "ses_child", parentID }] : [],
      cursor: {},
    }),
    prompt: async () => {},
    synthetic: async (input) => { if (syntheticGate) await syntheticGate; const item = { ...input, id: `notice_${++noticeSequence}` }; notices.push(item); return item },
    wait: async (_, { signal }) => new Promise((resolve, reject) => {
      if (idle) resolve()
      else if (signal.aborted) reject(signal.reason)
      else signal.addEventListener("abort", () => reject(signal.reason), { once: true })
    }),
    interrupt: async () => ({ interrupted }),
  },
}
const dispose = await plugin.setup(ctx)
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
assert.equal(permissionCalls.at(-1).options.headers["x-opencode-directory"], encodeURIComponent("D:\\project"))
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
await assert.rejects(() => run("permission", { sessionID: "ses_child", requestID: "p2", decision: "once" }, call("ses_other")), /unowned/)
assert.equal((await run("permission", { sessionID: "ses_child", requestID: "p2", decision: "once" })).status, "replied")
assert.deepEqual(permissionCalls.at(-1), {
  input: { sessionID: "ses_child", requestID: "p2", decision: "once" },
  options: { headers: { "x-opencode-directory": encodeURIComponent("D:\\other project") } },
})
assert.equal((await run("permission", { sessionID: "ses_child", requestID: "p2", decision: "once" })).status, "stale")
permission = [{ id: "race-permission", sessionID: "ses_child", action: "shell", resources: ["test"] }]
const normalReply = fakeClient.permission.reply
fakeClient.permission.reply = async () => { permission = []; throw new Error("already answered") }
assert.equal((await run("permission", { sessionID: "ses_child", requestID: "race-permission", decision: "once" })).status, "stale")
permission = [{ id: "failed-permission", sessionID: "ses_child", action: "shell", resources: ["test"] }]
fakeClient.permission.reply = async () => { throw new Error("service unavailable") }
await assert.rejects(() => run("permission", { sessionID: "ses_child", requestID: "failed-permission", decision: "once" }), /service unavailable/)
fakeClient.permission.reply = normalReply
permission = [{ id: "auto", sessionID: "ses_child", action: "shell", resources: ["test"] }]
const autoTools = {}
const disposeAuto = await plugin.setup({
  ...ctx,
  options: { ...ctx.options, allowPermissions: [{ action: "shell", resource: "test" }] },
  tool: { transform: async (fn) => fn({ add: (tool) => { autoTools[tool.name] = tool } }) },
})
await autoTools.context.execute({ sessionID: a.sessionID }, call())
assert.deepEqual(permissionCalls.at(-1), {
  input: { sessionID: "ses_child", requestID: "auto", decision: "once" },
  options: { headers: { "x-opencode-directory": encodeURIComponent("D:\\other project") } },
})
disposeAuto()
forms = [{ id: "form1", sessionID: "ses_child", title: "Choose", fields: [{ key: "choice", type: "string", title: "Choice" }] }]
const duplicateTools = {}
const disposeDuplicate = await plugin.setup({
  ...ctx,
  location: { directory: "D:\\other project" },
  tool: { transform: async (fn) => fn({ add: (tool) => { duplicateTools[tool.name] = tool } }) },
})
emit({ type: "form.created", data: { form: forms[0] } })
await new Promise((resolve) => setTimeout(resolve, 20))
assert.equal(notices.length, 1, "two plugin instances must enqueue only one notice")
disposeDuplicate()
assert.equal(notices[0].sessionID, "ses_owner")
assert.equal(notices[0].delivery, "queue")
assert.match(notices[0].text, /form1/)
assert.equal((await run("context", { sessionID: a.sessionID })).formID, "form1")
assert.equal((await run("wait", { sessionID: a.sessionID })).sessionID, "ses_child")
await assert.rejects(() => run("answer", { sessionID: "ses_child", formID: "form1", answer: { choice: "yes" } }, call("ses_other")), /unowned/)
assert.equal((await run("answer", { sessionID: "ses_child", formID: "form1", answer: { choice: "yes" } })).status, "replied")
assert.equal(notices.length, 0, "answered question must not remain queued")
assert.equal(formCalls.at(-1).options.headers["x-opencode-directory"], encodeURIComponent("D:\\other project"))
assert.equal((await run("answer", { sessionID: "ses_child", formID: "form1", answer: { choice: "yes" } })).status, "stale")
forms = [{ id: "cancelled", sessionID: "ses_child", title: "Cancelled", fields: [{ key: "choice", type: "string" }] }]
forms = []
emit({ type: "form.cancelled", data: { id: "cancelled", sessionID: "ses_child" } })
assert.equal((await run("answer", { sessionID: "ses_child", formID: "cancelled", answer: { choice: "yes" } })).status, "stale")
emit({ type: "form.created", data: { form: { id: "form1", sessionID: "ses_child" } } })
await new Promise((resolve) => setTimeout(resolve, 20))
assert.equal(notices.length, 0)
sessions.set("ses_existing", [{ id: "old", type: "assistant", content: [{ type: "text", text: "old" }] }])
await assert.rejects(() => run("attach", { sessionID: "ses_owner" }), /managing session/)
await assert.rejects(() => run("attach", { sessionID: "ses_existing" }), /running session/)
idle = true
assert.equal((await run("attach", { sessionID: "ses_existing" })).status, "attached")
assert.equal((await run("context", { sessionID: "ses_existing" })).output, null)
await assert.rejects(() => run("attach", { sessionID: "ses_existing" }, call("ses_other")), /another session/)
assert.ok(Array.isArray((await run("list", {})).sessions))
forms = [{ id: "restart", sessionID: "ses_child", title: "Again", fields: [{ key: "n", type: "integer", title: "Number" }] }]
const recoveryNotices = []
const recoveryTools = {}
const recoveryCtx = {
  ...ctx,
  storage: {
    ...ctx.storage,
    scan: async ({ after }) => after ? { entries: [] } : {
      entries: [{ key: "session/ses_1", value: storage.get("session/ses_1") }],
    },
  },
  tool: { transform: async (fn) => fn({ add: (tool) => { recoveryTools[tool.name] = tool } }) },
  session: { ...ctx.session, synthetic: async (input) => recoveryNotices.push(input) },
}
const disposeRecovery = await plugin.setup(recoveryCtx)
await new Promise((resolve) => setTimeout(resolve, 20))
assert.equal(recoveryNotices.length, 1, "restart must relay an outstanding descendant form")
assert.match(recoveryNotices[0].text, /restart/)
disposeRecovery()
const disposeRecoveryAgain = await plugin.setup(recoveryCtx)
await new Promise((resolve) => setTimeout(resolve, 20))
assert.equal(recoveryNotices.length, 1, "restart must not relay a delivered form twice")
disposeRecoveryAgain()
forms = []
const idleHooks = []
const completionNotices = []
const completionCtx = {
  ...ctx,
  tool: { transform: async (fn) => fn({ add: () => {} }) },
  session: {
    ...ctx.session,
    hook: async (_, fn) => { idleHooks.push(fn); return { dispose: async () => {} } },
    synthetic: async (input) => completionNotices.push(input),
  },
}
const disposeCompletion1 = await plugin.setup(completionCtx)
const disposeCompletion2 = await plugin.setup({ ...completionCtx, location: { directory: "D:\\other project" } })
const pendingEntry = storage.get("session/ses_1")
await ctx.storage.set("session/ses_1", { ...pendingEntry, pending: true, delivered: false, watched: false })
await Promise.all(idleHooks.map((hook) => hook({ sessionID: "ses_1" })))
assert.equal(completionNotices.length, 1, "two plugin instances must enqueue only one completion")
disposeCompletion1()
disposeCompletion2()
// A completion queued while the owner is busy is removed only after that owner reads it.
const scopedHooks = []
const scopedTools = {}
const disposeScoped = await plugin.setup({
  ...ctx,
  tool: { transform: async (fn) => fn({ add: (tool) => { scopedTools[tool.name] = tool } }) },
  session: { ...ctx.session, hook: async (_, fn) => { scopedHooks.push(fn) } },
})
const scopedRun = async (name, input, context = call()) =>
  JSON.parse((await scopedTools[name].execute(input, context)).content)
const scoped = await scopedRun("start", { directory: "new", task: "test" })
sessions.get(scoped.sessionID).push({ id: "new-answer", type: "assistant", content: [{ type: "text", text: "done" }] })
notices.push({ id: "unrelated", sessionID: "ses_owner", text: "older unrelated notice" })
await scopedHooks[0]({ sessionID: scoped.sessionID })
assert.equal(notices.filter((item) => item.text.includes(`Project ${scoped.sessionID} finished`)).length, 1)
assert.equal((await scopedRun("context", { sessionID: scoped.sessionID })).output.text, "done")
assert.deepEqual(notices.map((item) => item.id), ["unrelated"], "only the consumed completion may be cancelled")
await scopedRun("prompt", { sessionID: scoped.sessionID, task: "again" })
sessions.get(scoped.sessionID).push({ id: "new-answer-2", type: "assistant", content: [{ type: "text", text: "next" }] })
assert.equal((await scopedRun("context", { sessionID: scoped.sessionID })).output.text, "next")
await scopedHooks[0]({ sessionID: scoped.sessionID })
assert.deepEqual(notices.map((item) => item.id), ["unrelated"], "reading an idle result must suppress a later completion")
await scopedRun("prompt", { sessionID: scoped.sessionID, task: "question" })
forms = [{ id: "race-form", sessionID: scoped.sessionID, title: "Race", fields: [{ key: "x", type: "string" }] }]
let releaseSynthetic
syntheticGate = new Promise((resolve) => { releaseSynthetic = resolve })
emit({ type: "form.created", data: { form: forms[0] } })
await new Promise((resolve) => setTimeout(resolve, 20))
const readingQuestion = scopedRun("context", { sessionID: scoped.sessionID })
releaseSynthetic()
assert.equal((await readingQuestion).formID, "race-form")
syntheticGate = undefined
assert.deepEqual(notices.map((item) => item.id), ["unrelated"], "an in-flight enqueue must be cancelled after reading the question")
await scopedRun("answer", { sessionID: scoped.sessionID, formID: "race-form", answer: { x: "yes" } })
sessions.get(scoped.sessionID).push({ id: "new-answer-3", type: "assistant", content: [{ type: "text", text: "last" }] })
const waited = await scopedRun("wait", { sessionID: scoped.sessionID, timeout: 1000 }, {
  ...call(), progress: async () => scopedHooks[0]({ sessionID: scoped.sessionID }),
})
assert.equal(waited.status, "completed")
assert.deepEqual(notices.map((item) => item.id), ["unrelated"], "wait must cancel its queued completion")
await scopedRun("prompt", { sessionID: scoped.sessionID, task: "timeout" })
idle = false
assert.equal((await scopedRun("wait", { sessionID: scoped.sessionID, timeout: 10 })).status, "running")
sessions.get(scoped.sessionID).push({ id: "new-answer-4", type: "assistant", content: [{ type: "text", text: "later" }] })
idle = true
await scopedHooks[0]({ sessionID: scoped.sessionID })
assert.equal(notices.filter((item) => item.text.includes(`Project ${scoped.sessionID} finished`)).length, 1, "timed-out wait must not suppress later notification")
disposeScoped()
const unsupportedTools = {}
const disposeUnsupported = await createPlugin(async () => { throw new Error("unsupported host") }).setup({
  ...ctx,
  tool: { transform: async (fn) => fn({ add: (tool) => { unsupportedTools[tool.name] = tool } }) },
})
await assert.rejects(() => unsupportedTools.context.execute({ sessionID: a.sessionID }, call()), /Question relay unavailable: unsupported host/)
await assert.rejects(() => unsupportedTools.start.execute({ directory: "a", task: "test" }, call()), /Question relay unavailable: unsupported host/)
disposeUnsupported()
dispose()
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
const disposeHistory = await plugin.setup(historyCtx)
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
disposeHistory()
console.log("PASS: read-only history tools, search caps, cancellation and timeout")

// Delete operates only on owned, quiescent trees and never removes the caller.
const deleteStore = new Map()
const tree = new Map([["root", null], ["child", "root"], ["owner", null], ["other", null]])
const deleteTools = {}
let removals = 0
let failRemove = false
let busyID
let removeGate
let removeEntered
const deleteCtx = {
  ...ctx,
  storage: {
    get: async (key) => deleteStore.get(key),
    set: async (key, value) => deleteStore.set(key, structuredClone(value)),
    remove: async (key) => deleteStore.delete(key),
    scan: async ({ prefix }) => ({ entries: [...deleteStore].filter(([key]) => key.startsWith(prefix)).map(([key, value]) => ({ key, value })) }),
  },
  tool: { transform: async (fn) => fn({ add: (tool) => { deleteTools[tool.name] = tool } }) },
  session: {
    ...ctx.session,
    get: async ({ sessionID }) => {
      if (!tree.has(sessionID)) throw new Error("not found")
      return { id: sessionID, parentID: tree.get(sessionID), location: { directory: "D:\\delete-test" } }
    },
    list: async ({ parentID }) => ({ data: [...tree].filter(([, parent]) => parent === parentID).map(([id, parentID]) => ({ id, parentID })), cursor: {} }),
    wait: async ({ sessionID }, { signal }) => {
      if (sessionID !== busyID) return
      return new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }))
    },
    remove: async ({ sessionID }) => {
      removals++
      removeEntered?.()
      if (removeGate) await removeGate
      if (failRemove) throw new Error("uncertain")
      tree.delete(sessionID)
      tree.delete("child")
    },
  },
}
const disposeDelete = await createPlugin(async () => fakeClient).setup(deleteCtx)
const deleteRun = async (id = "root", owner = "owner") => JSON.parse((await deleteTools.delete.execute({ sessionID: id }, call(owner))).content)
await deleteCtx.storage.set("session/root", { owner: "owner", pending: false, previous: [] })
await assert.rejects(() => deleteRun("root", "other"), /unowned/)
await assert.rejects(() => deleteRun("child"), /descendant/)
tree.set("owner", "child")
await assert.rejects(() => deleteRun(), /containing/)
tree.set("owner", null)
await deleteCtx.storage.set("session/child", { owner: "other" })
await assert.rejects(() => deleteRun(), /managed descendant/)
await deleteCtx.storage.remove("session/child")
await deleteCtx.storage.set("session/root", { owner: "owner", pending: true, previous: [] })
await assert.rejects(() => deleteRun(), /pending/)
await deleteCtx.storage.set("session/root", { owner: "owner", watched: true, previous: [] })
await assert.rejects(() => deleteRun(), /watched/)
await deleteCtx.storage.set("session/root", { owner: "owner", pending: false, previous: [] })
busyID = "child"
await assert.rejects(() => deleteRun(), /busy/)
busyID = undefined
forms = [{ id: "delete-form", sessionID: "child" }]
await assert.rejects(() => deleteRun(), /forms/)
forms = []
permission = [{ id: "delete-permission", sessionID: "child" }]
await assert.rejects(() => deleteRun(), /permissions/)
permission = []
const permissionList = fakeClient.permission.list
fakeClient.permission.list = async () => { throw new Error("permission check failed") }
await assert.rejects(() => deleteRun(), /permission check failed/)
fakeClient.permission.list = permissionList
assert.equal(removals, 0)
notices.push({ id: "delete-notice", sessionID: "owner" })
await deleteCtx.storage.set("form/old-delete-form", { owner: "owner", sessionID: "child", inboxID: "delete-notice" })
const otherDeleteTools = {}
const disposeOtherDelete = await createPlugin(async () => fakeClient).setup({
  ...deleteCtx,
  tool: { transform: async (fn) => fn({ add: (tool) => { otherDeleteTools[tool.name] = tool } }) },
})
let releaseRemove
removeGate = new Promise((resolve) => { releaseRemove = resolve })
let enteredRemove
const removing = new Promise((resolve) => { enteredRemove = resolve })
removeEntered = enteredRemove
const deleting = deleteRun()
await removing
const concurrentPrompt = otherDeleteTools.prompt.execute({ sessionID: "root", task: "must not run" }, call("owner"))
releaseRemove()
assert.equal((await deleting).status, "deleted")
await assert.rejects(() => concurrentPrompt, /unowned/)
disposeOtherDelete()
removeGate = undefined
removeEntered = undefined
assert.equal(removals, 1)
assert.equal(deleteStore.has("session/root"), false)
assert.equal(deleteStore.has("form/old-delete-form"), false)
assert.equal(notices.some((item) => item.id === "delete-notice"), false)
assert.equal(tree.has("owner"), true)
tree.set("root", null)
await deleteCtx.storage.set("session/root", { owner: "owner", pending: false, previous: [] })
failRemove = true
await assert.rejects(() => deleteRun(), /uncertain/)
assert.equal(deleteStore.get("session/root").deleting, true)
await assert.rejects(() => deleteRun(), /reconciliation/)
disposeDelete()
console.log("PASS: safe deletion and uncertain removal")
