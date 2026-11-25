import { Plugin } from "@opencode/plugin"
import { OpenCode } from "@opencode/client"
import { Service } from "@opencode/client/service"

// ponytail: process-local deduplication assumes one server process; durable cross-process
// exactly-once delivery requires an atomic inbox idempotency key.
const formNotices = globalThis[Symbol.for("opencode-session-manager.formNotices")] ??= new Map()
const deliveryLocks = globalThis[Symbol.for("opencode-session-manager.deliveryLocks")] ??= new Set()
const noticeLocks = globalThis[Symbol.for("opencode-session-manager.noticeLocks")] ??= new Map()
// ponytail: serialize manager mutations process-wide; use per-tree locks if throughput matters.
const operationLocks = globalThis[Symbol.for("opencode-session-manager.operationLocks")] ??= new Map()
const running = globalThis[Symbol.for("opencode-session-manager.running")] ??= new Map()
const asked = globalThis[Symbol.for("opencode-session-manager.asked")] ??= new Map()

async function withLock(locks, id, fn) {
  const previous = locks.get(id) ?? Promise.resolve()
  let release
  const next = new Promise((resolve) => { release = resolve })
  locks.set(id, next)
  await previous
  try { return await fn() } finally {
    release()
    if (locks.get(id) === next) locks.delete(id)
  }
}
const withNoticeLock = (id, fn) => withLock(noticeLocks, id, fn)
const withOperationLock = (fn) => withLock(operationLocks, "operations", fn)

async function localClient() {
  const endpoint = await Service.discover()
  if (!endpoint) throw new Error("Question relay requires the managed local OpenCode service; no service was discovered")
  const client = OpenCode.make({ baseUrl: endpoint.url, headers: Service.headers(endpoint) })
  const info = await client.server.info()
  if (info.pid !== process.pid) throw new Error("Question relay unsupported: discovered OpenCode service is not this server process")
  return client
}

const properties = {
  sessionID: { type: "string", minLength: 1 },
  directory: { type: "string", minLength: 1 },
  task: { type: "string", minLength: 1 },
  timeout: { type: "integer", minimum: 1, maximum: 3600000 },
  requestID: { type: "string", minLength: 1 },
  decision: { type: "string", enum: ["once", "always", "reject"] },
  limit: { type: "integer", minimum: 1, maximum: 100 },
  cursor: { type: "string", minLength: 1 },
  query: { type: "string", minLength: 1, maxLength: 256 },
  caseSensitive: { type: "boolean" },
  formID: { type: "string", minLength: 1 },
  answer: { type: "object", additionalProperties: { anyOf: [{ type: "string" }, { type: "number" }, { type: "boolean" }, { type: "array", items: { type: "string" } }] } },
}

export function createPlugin(connect = localClient) { return Plugin.define({
  id: "session-manager",
  async setup(ctx) {
    const limit = Number.isInteger(ctx.options?.maxOutput) && ctx.options.maxOutput > 0
      ? Math.min(ctx.options.maxOutput, 100000) : 4000
    const rules = Array.isArray(ctx.options?.allowPermissions) ? ctx.options.allowPermissions : []
    const locks = new Set()
    // permission.asked arrives with a location of its own, so session-scoped polling misses
    // requests raised by project sessions that live outside this plugin's location.
    const stop = new AbortController()
    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({
          types: ["permission.asked", "permission.replied"],
          signal: stop.signal,
        })) {
          const data = event.data ?? {}
          if (event.type === "permission.asked" && data.id) {
            asked.set(data.id, {
              id: data.id, sessionID: data.sessionID, action: data.action,
              resources: data.resources ?? [], save: data.save,
              ...(data.message ? { message: data.message } : {}), at: Date.now(),
            })
          } else {
            if (data.requestID) asked.delete(data.requestID)
          }
        }
      } catch {}
    })()
    const key = (id) => `session/${id}`
    const result = (value) => ({ content: JSON.stringify(value) })
    const bounded = (text) => ({ text: text.slice(0, limit), truncated: text.length > limit })
    const resolveOwned = async (id, owner) => {
      if (!id || id === owner) throw new Error("Unknown, unowned, or self-targeted session")
      const entry = await ctx.storage.get(key(id))
      if (entry?.owner === owner) return { root: id, entry }
      {
        let current = id
        const seen = new Set()
        while (current && !seen.has(current)) {
          seen.add(current)
          let info
          try { info = await ctx.session.get({ sessionID: current }) } catch { break }
          current = info?.parentID
          if (!current) break
          const parent = await ctx.storage.get(key(current))
          if (parent?.owner === owner) return { root: current, entry: parent }
        }
        throw new Error("Unknown, unowned, or self-targeted session")
      }
    }
    const owned = async (id, owner) => {
      const match = await resolveOwned(id, owner)
      if (match.root !== id) throw new Error(`Session ${id} is a descendant of ${match.root}; use the parent`)
      if (match.entry.deleting) throw new Error("Session deletion requires reconciliation")
      return match.entry
    }
    const descendants = async (root) => {
      const ids = [root]
      const seen = new Set(ids)
      for (let i = 0; i < ids.length; i++) {
        let cursor
        do {
          const page = await ctx.session.list(cursor ? { cursor } : { parentID: ids[i] })
          for (const child of page.data) {
            if (child.parentID === ids[i] && !seen.has(child.id)) {
              seen.add(child.id)
              ids.push(child.id)
            }
          }
          cursor = page.cursor?.next
        } while (cursor)
      }
      return ids
    }
    let clientPromise
    const client = () => clientPromise ??= Promise.resolve().then(connect).catch((error) => {
      clientPromise = undefined
      throw new Error(`Question relay unavailable: ${error.message}`)
    })
    const formOptions = async (sessionID) => {
      const session = await ctx.session.get({ sessionID })
      const directory = session.location?.directory
      if (!directory) throw new Error(`Question relay unavailable: no location for ${sessionID}`)
      return { headers: { "x-opencode-directory": encodeURIComponent(directory) } }
    }
    const replyPermission = async (input) =>
      (await client()).permission.reply(input, await formOptions(input.sessionID))
    const formsFor = async (ids) => {
      const api = await client()
      const forms = await Promise.all(ids.map(async (sessionID) =>
        api.session.form.list({ sessionID }, await formOptions(sessionID))))
      return forms.flat()
    }
    const cancelNotice = async (owner, inboxID) => {
      if (!inboxID) return true
      try {
        const api = await client()
        const options = await formOptions(owner)
        const pending = await api.session.inbox.list({ sessionID: owner }, options)
        if (pending.some((item) => item.id === inboxID)) await api.session.inbox.cancel({ sessionID: owner, inboxID }, options)
        return true
      } catch { return false }
    }
    const consumeForm = async (form, owner, known = true) => {
      const marker = `form/${form.id}`
      const notice = await ctx.storage.get(marker)
      let cancelled = true
      if (notice?.owner === owner && notice.sessionID === form.sessionID) cancelled = await cancelNotice(owner, notice.inboxID)
      else if (!known) return
      await ctx.storage.set(marker, { owner, sessionID: form.sessionID, consumed: true, ...(!cancelled ? { inboxID: notice.inboxID } : {}) })
      formNotices.set(form.id, "delivered")
    }
    const relayForm = async (form) => {
      const match = await rootOf(form.sessionID)
      if (!match) return
      return withNoticeLock(match.root, async () => {
      const current = await ctx.storage.get(key(match.root))
      if (!current?.owner || current.owner !== match.entry.owner || current.deleting) return
      if (formNotices.has(form.id)) return
      // Claim synchronously before any storage or network operation.
      formNotices.set(form.id, "pending")
      try {
        const marker = `form/${form.id}`
        if (await ctx.storage.get(marker)) {
          formNotices.set(form.id, "delivered")
          return
        }
        if (!(await formsFor([form.sessionID])).some((item) => item.id === form.id)) {
          formNotices.delete(form.id)
          return
        }
        const notice = { status: "question_required", managedSessionID: match.root, sessionID: form.sessionID, formID: form.id, title: form.title, fields: form.fields }
        const item = await ctx.session.synthetic({
          sessionID: match.entry.owner, delivery: "queue",
          text: `A managed session needs an answer. Use session_manager.answer with sessionID, formID and an answer object keyed by field key.\n${JSON.stringify(notice)}`,
        })
        formNotices.set(form.id, "delivered")
        await ctx.storage.set(marker, { owner: match.entry.owner, sessionID: form.sessionID, inboxID: item?.id })
      } catch (error) {
        if (formNotices.get(form.id) === "pending") formNotices.delete(form.id)
        throw error
      }
      })
    }
    const questionFor = async (ids, relay = true) => {
      const form = (await formsFor(ids))[0]
      if (form && relay) void relayForm(form).catch(() => {})
      return form ? { status: "question_required", sessionID: form.sessionID, formID: form.id, title: form.title, fields: form.fields } : null
    }
    let recoveryTimer
    let recover
    const scheduleRecovery = () => {
      if (!stop.signal.aborted && !recoveryTimer) recoveryTimer = setTimeout(() => {
        recoveryTimer = undefined
        void recover()
      }, 5000)
    }
    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({ types: ["form.created", "form.replied", "form.cancelled"], signal: stop.signal })) {
          if (event.type === "form.created" && event.data?.form) {
            void relayForm(event.data.form).catch(scheduleRecovery)
          }
        }
      } catch {}
    })()
    const snapshot = async (id, entry) => {
      const messages = await ctx.session.context({ sessionID: id })
      const answer = [...messages].reverse().find((m) =>
        m.type === "assistant" && !entry.previous.includes(m.id))
      const text = answer?.content?.filter((part) => part.type === "text").map((part) => part.text).join("") ?? ""
      return { output: text ? bounded(text) : null, failed: !!answer?.error || answer?.finish === "error" }
    }
    const pendingFor = async (ids) => {
      const cutoff = Date.now() - 900000
      for (const [id, item] of asked) if (item.at < cutoff) asked.delete(id)
      const found = new Map()
      for (const id of ids) {
        for (const item of asked.values()) if (item.sessionID === id) found.set(item.id, item)
        let requests = []
        try { requests = await ctx.permission.list({ sessionID: id }) } catch {}
        for (const request of requests ?? []) if (request.sessionID === id) found.set(request.id, request)
      }
      return [...found.values()]
    }
    const permissions = async (ids) => {
      for (const request of await pendingFor(ids)) {
        const id = request.sessionID
        const resources = request.resources ?? []
        const allowed = resources.length > 0 && rules.some((rule) =>
          rule && rule.action === request.action && typeof rule.resource === "string" &&
          resources.every((resource) => resource === rule.resource))
        if (!allowed) return {
          status: "permission_required", sessionID: id, requestID: request.id,
          action: request.action, resources, ...(request.message ? { message: request.message } : {}),
          instruction: `Permission requested by ${id}: ${request.action} ${resources.join(", ")}${request.message ? ` (${request.message})` : ""}. Reply allow, always, or deny.`,
          replies: { allow: "once", always: "always", deny: "reject" },
        }
        await replyPermission({ sessionID: id, requestID: request.id, decision: "once" })
        asked.delete(request.id)
      }
      return null
    }
    const outcome = new Map()
    const ttl = 600000
    const blocking = (id) => {
      const snapshot = running.get(id)
      if (!snapshot?.items?.length) return false
      return Date.now() - snapshot.at < ttl
    }
    const idled = async (sessionID) => {
      const match = await rootOf(sessionID)
      if (!match) return
      if (deliveryLocks.has(match.root)) return
      deliveryLocks.add(match.root)
      try {
      await withNoticeLock(match.root, async () => {
      match.entry = await ctx.storage.get(key(match.root))
      if (!match.entry?.pending || match.entry.deleting) return
      const ids = await descendants(match.root)
      if (await questionFor(ids)) return
      if (match.entry.delivered) return
      const signal = new AbortController().signal
      const active = await Promise.all(ids.map((id) => probe(id, signal, 150)))
      const busy = ids.some((id, i) => active[i]) || ids.some((id) => blocking(id))
      if (busy || match.entry.delivered) return
      const current = await snapshot(match.root, match.entry)
      const text = current.output?.text ?? ""
      const seen = outcome.get(match.root)
      const status = seen === "interrupted" ? "interrupted"
        : match.entry.recovered && !seen ? "incomplete"
        : current.failed || seen === "failed" ? "failed" : "completed"
      const lines = [
        `Project ${match.root} finished.`,
        `Status: ${status}.`,
        text ? `Answer:\n${text}` : "No answer text was produced.",
        ...(current.output?.truncated ? ["[Output truncated]"] : []),
      ]
      try {
        const item = await ctx.session.synthetic({ sessionID: match.entry.owner, text: lines.join("\n"), delivery: "queue" })
        match.entry.completionInboxID = item?.id
        match.entry.delivered = true
        match.entry.pending = false
        match.entry.status = status
        await ctx.storage.set(key(match.root), match.entry)
      } catch {}
      })
      } finally {
        deliveryLocks.delete(match.root)
      }
    }
    const rootOf = async (id) => {
      let current = id
      const seen = new Set()
      while (current && !seen.has(current)) {
        seen.add(current)
        const entry = await ctx.storage.get(key(current))
        if (entry?.owner) return { root: current, entry }
        let info
        try { info = await ctx.session.get({ sessionID: current }) } catch { break }
        current = info?.parentID
      }
      return null
    }
    await ctx.session.hook("experimental.idle", async (event) => {
      running.set(event.sessionID, { items: event.background ?? [], at: Date.now() })
      await idled(event.sessionID)
    })
    recover = async () => {
      try {
        let after
        do {
        const page = await ctx.storage.scan({ prefix: "session/", limit: 200, ...(after ? { after } : {}) })
        for (const { key: stored, value } of page.entries ?? []) {
          if (value?.owner) {
            const id = stored.slice("session/".length)
            for (const form of await formsFor(await descendants(id))) await relayForm(form)
          }
          if (value?.pending && !value.delivered && !value.watched) {
            const id = stored.slice("session/".length)
            await withNoticeLock(id, async () => {
              const latest = await ctx.storage.get(stored)
              if (!latest?.pending || latest.delivered || latest.watched || latest.deleting) return
              latest.recovered = true
              await ctx.storage.set(stored, latest)
              setTimeout(() => { void idled(id).catch(() => {}) }, 1500)
            })
          }
        }
        after = page.next
        } while (after)
      } catch {
        scheduleRecovery()
      }
    }
    void recover()
    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({
          types: ["session.execution.succeeded", "session.execution.failed", "session.execution.interrupted"],
          signal: stop.signal,
        })) {
          const id = event.data?.sessionID
          if (!id) continue
          outcome.set(id, event.type === "session.execution.failed" ? "failed"
            : event.type === "session.execution.interrupted" ? "interrupted" : "succeeded")
          setTimeout(() => { void idled(id).catch(() => {}) }, 400)
        }
      } catch {}
    })()
    const probe = async (id, signal, duration = 200) => {
      const controller = new AbortController()
      const cancel = () => controller.abort()
      if (signal.aborted) cancel()
      else signal.addEventListener("abort", cancel, { once: true })
      let timer
      try {
        return await Promise.race([
          ctx.session.wait({ sessionID: id }, { signal: controller.signal }).then(
            () => false,
            (error) => { if (controller.signal.aborted) return true; throw error },
          ),
          new Promise((resolve) => { timer = setTimeout(() => resolve(true), duration) }),
          new Promise((resolve) => {
            if (signal.aborted) resolve(true)
            else signal.addEventListener("abort", () => resolve(true), { once: true, signal: controller.signal })
          }),
        ])
      } finally {
        clearTimeout(timer)
        controller.abort()
        signal.removeEventListener("abort", cancel)
      }
    }
    const enqueue = async (id, owner, task) => {
      if (locks.has(id)) throw new Error("Session already has an active operation")
      locks.add(id)
      try {
        const entry = await owned(id, owner)
        await client()
        if (entry.pending) throw new Error("Session has a pending prompt; wait or interrupt first")
        const messages = await ctx.session.context({ sessionID: id })
        entry.previous = messages.filter((m) => m.type === "assistant").map((m) => m.id)
        entry.delivered = false
        delete entry.completionInboxID
        outcome.delete(id)
        running.delete(id)
        delete entry.recovered
        delete entry.status
        entry.pending = true
        await ctx.storage.set(key(id), entry)
        try {
          await ctx.session.prompt({ sessionID: id, text: task })
        } catch (error) {
          // Admission may have succeeded even when the call failed: retain pending.
          throw new Error(`Prompt admission uncertain for ${id}: ${error.message}`)
        }
        return { sessionID: id, status: "queued" }
      } finally {
        locks.delete(id)
      }
    }
    // History access intentionally does not check ownership. Only text parts are exposed.
    const historyInfo = (session) => ({
      sessionID: session.id,
      title: session.title ?? null,
      directory: session.location?.directory ?? null,
      updated: session.time?.updated ?? null,
      parentID: session.parentID ?? null,
    })
    const clipMetadata = (value, state) => {
      if (typeof value !== "string" || value.length <= 256) return value
      state.truncated = true
      return value.slice(0, 256)
    }
    const textParts = (message) => (["user", "assistant"].includes(message.type) && Array.isArray(message.content) ? message.content : [])
      .filter((part) => part.type === "text" && typeof part.text === "string")
    const historyRead = async (input) => {
      await ctx.session.get({ sessionID: input.sessionID })
      const messages = await ctx.session.context({ sessionID: input.sessionID })
      const textMessages = messages.filter((message) => textParts(message).length > 0)
      const selected = textMessages.slice(-(input.limit ?? 50))
      const state = { truncated: false }
      let remaining = limit
      const output = []
      for (const message of selected) {
        const text = textParts(message).map((part) => part.text).join("")
        if (!text) continue
        if (!remaining) { state.truncated = true; break }
        const clipped = text.slice(0, remaining)
        if (clipped.length < text.length) state.truncated = true
        remaining -= clipped.length
        output.push({ id: clipMetadata(message.id ?? null, state), type: clipMetadata(message.type ?? null, state), text: clipped })
      }
      return { sessionID: clipMetadata(input.sessionID, state), messages: output, truncated: state.truncated, messagesOmitted: textMessages.length > selected.length }
    }
    const historySearch = async (input, call) => {
      if (typeof input.query !== "string" || !input.query.length || input.query.length > 256) throw new Error("Query must contain 1 to 256 characters")
      const response = { matches: [], sessionsScanned: 0, truncated: false, incomplete: false, reason: null }
      const mark = (reason) => { if (!response.incomplete) { response.incomplete = true; response.reason = reason } }
      const deadline = Date.now() + 5000
      const termination = () => call.signal.aborted ? "cancelled" : Date.now() >= deadline ? "timeout" : null
      const check = () => { const reason = termination(); if (reason) mark(reason); return !!reason }
      let timer
      let abort
      const stopped = new Promise((resolve) => {
        timer = setTimeout(() => resolve("timeout"), 5000)
        abort = () => resolve("cancelled")
        if (call.signal.aborted) abort()
        else call.signal.addEventListener("abort", abort, { once: true })
      })
      const retrieve = async (operation) => {
        if (check()) return null
        const value = await Promise.race([operation().then((data) => ({ data })), stopped.then((reason) => ({ reason }))])
        if (value.reason) { mark(value.reason); return null }
        if (check()) return null
        return value.data
      }
      const yieldNow = () => new Promise((resolve) => setTimeout(resolve, 0))
      let textRemaining = limit
      let scanRemaining = 1000000
      let ended = false
      try {
        let ids
        if (input.sessionID) {
          const session = await retrieve(() => ctx.session.get({ sessionID: input.sessionID }))
          if (session === null) return response
          ids = [input.sessionID]
        } else {
          const page = await retrieve(() => ctx.session.list({ limit: 50, order: "desc" }))
          if (page === null) return response
          ids = page.data.slice(0, 50).map((session) => session.id)
          if (page.cursor?.next || page.data.length > 50) mark("session_limit")
        }
        const needle = input.caseSensitive ? input.query : input.query.toLowerCase()
        for (const id of ids) {
          if (check() || ended) break
          const messages = await retrieve(() => ctx.session.context({ sessionID: id }))
          if (messages === null) break
          response.sessionsScanned++
          if (messages.length > 100) mark("message_limit")
          for (const message of messages.slice(-100)) {
            if (check() || ended) break
            for (const part of textParts(message)) {
              if (check() || ended) break
              const length = Math.min(part.text.length, 100000, scanRemaining)
              if (length < part.text.length) mark("text_limit")
              const original = part.text.slice(0, length)
              scanRemaining -= length
              const text = input.caseSensitive ? original : original.toLowerCase()
              let first = -1
              let count = 0
              let offset = 0
              while (offset <= text.length - needle.length) {
                const end = Math.min(text.length - needle.length + 1, offset + 4096)
                for (; offset < end; offset++) {
                  if (text.startsWith(needle, offset)) { if (first < 0) first = offset; count++ }
                }
                await yieldNow()
                if (check()) { ended = true; break }
              }
              if (first >= 0) {
                if (response.matches.length >= (input.limit ?? 20) || !textRemaining) {
                  response.truncated = true
                  ended = true
                  break
                }
                const start = Math.max(0, first - 50)
                const end = Math.min(original.length, first + input.query.length + 50)
                const excerpt = original.slice(start, end)
                const clipped = excerpt.slice(0, textRemaining)
                if (clipped.length < excerpt.length) response.truncated = true
                textRemaining -= clipped.length
                response.matches.push({
                  sessionID: clipMetadata(id, response), messageID: clipMetadata(message.id ?? null, response),
                  type: clipMetadata(message.type ?? null, response), excerpt: clipped, matchCount: count,
                })
              }
              if (!scanRemaining && length < part.text.length) ended = true
            }
          }
        }
        return response
      } finally {
        clearTimeout(timer)
        call.signal.removeEventListener("abort", abort)
      }
    }
    const definitions = {
      list: {
        required: [],
        async run(input) {
          const page = await ctx.session.list(input.cursor ? { cursor: input.cursor } : { limit: input.limit ?? 20, order: "desc" })
          return { sessions: page.data.map((s) => ({ sessionID: s.id, title: s.title ?? null, directory: s.location?.directory ?? null, updated: s.time?.updated ?? null, parentID: s.parentID ?? null })), next: page.cursor?.next ?? null }
        },
      },
      read: { required: ["sessionID"], run: historyRead },
      search: { required: ["query"], run: historySearch },
      info: {
        required: ["sessionID"],
        async run(input) {
          const session = await ctx.session.get({ sessionID: input.sessionID })
          const state = { truncated: false }
          const info = historyInfo(session)
          for (const field of Object.keys(info)) info[field] = clipMetadata(info[field], state)
          return { ...info, truncated: state.truncated }
        },
      },
      attach: {
        required: ["sessionID"],
        async run(input, call) {
          const id = input.sessionID
          await client()
          if (id === call.sessionID) throw new Error("Cannot attach the managing session")
          if (locks.has(id)) throw new Error("Session already has an active operation")
          locks.add(id)
          try {
            const existing = await ctx.storage.get(key(id))
            if (existing?.owner && existing.owner !== call.sessionID) throw new Error("Session is already managed by another session")
            if (existing?.owner === call.sessionID) throw new Error("Session is already managed")
            await ctx.session.get({ sessionID: id })
            if (await rootOf(id)) throw new Error("Session is a descendant of a managed session")
            if (await probe(id, call.signal)) throw new Error("Cannot attach a running session")
            const messages = await ctx.session.context({ sessionID: id })
            await ctx.storage.set(key(id), { owner: call.sessionID, previous: messages.filter((m) => m.type === "assistant").map((m) => m.id), pending: false, delivered: true })
            return { sessionID: id, status: "attached" }
          } finally { locks.delete(id) }
        },
      },
      start: {
        required: ["directory", "task"],
        async run(input, call) {
          await client()
          const session = await ctx.session.create({ location: { directory: input.directory } })
          if (!session?.id || session.id === call.sessionID) throw new Error("Invalid created session")
          await ctx.storage.set(key(session.id), { owner: call.sessionID, previous: [], pending: false, delivered: false })
          const queued = await enqueue(session.id, call.sessionID, input.task)
          return { sessionID: queued.sessionID, status: "running" }
        },
      },
      prompt: { required: ["sessionID", "task"], run: (input, call) => enqueue(input.sessionID, call.sessionID, input.task) },
      context: {
        required: ["sessionID"],
        async run(input, call) {
          return withNoticeLock(input.sessionID, async () => {
          const entry = await owned(input.sessionID, call.sessionID)
          const ids = await descendants(input.sessionID)
          const blocked = await questionFor(ids, false) ?? await permissions(ids)
          if (blocked?.status === "question_required") await consumeForm({ id: blocked.formID, sessionID: blocked.sessionID }, call.sessionID)
          const current = await snapshot(input.sessionID, entry)
          if (!blocked && entry.pending && current.output) {
            const active = await Promise.all(ids.map((id) => probe(id, call.signal, 150)))
            if (!active.some(Boolean) && !ids.some((id) => blocking(id)) && !call.signal.aborted) {
              entry.pending = false
              entry.delivered = true
              entry.status = current.failed ? "failed" : outcome.get(input.sessionID) === "interrupted" ? "interrupted" : outcome.get(input.sessionID) === "failed" ? "failed" : "completed"
              await ctx.storage.set(key(input.sessionID), entry)
            }
          }
          if (!blocked && !entry.pending && entry.completionInboxID) {
            if (await cancelNotice(call.sessionID, entry.completionInboxID)) {
              delete entry.completionInboxID
              await ctx.storage.set(key(input.sessionID), entry)
            }
          }
          return { managedSessionID: input.sessionID, ...(blocked ?? { sessionID: input.sessionID, status: entry.pending ? "pending" : "idle" }), output: current.output }
          })
        },
      },
      answer: {
        required: ["sessionID", "formID", "answer"],
        async run(input, call) {
          const { root } = await resolveOwned(input.sessionID, call.sessionID)
          return withNoticeLock(root, async () => {
          if (!input.answer || typeof input.answer !== "object" || Array.isArray(input.answer)) throw new Error("Answer must be an object keyed by field key")
          const api = await client()
          const options = await formOptions(input.sessionID)
          const pending = await api.session.form.list({ sessionID: input.sessionID }, options)
          if (!pending.some((form) => form.id === input.formID)) {
            await consumeForm({ id: input.formID, sessionID: input.sessionID }, call.sessionID, false)
            return { sessionID: input.sessionID, managedSessionID: root, formID: input.formID, status: "stale" }
          }
          try {
            await api.session.form.reply({ sessionID: input.sessionID, formID: input.formID, answer: input.answer }, options)
          } catch (error) {
            const remaining = await api.session.form.list({ sessionID: input.sessionID }, options)
            if (!remaining.some((form) => form.id === input.formID)) {
              await consumeForm({ id: input.formID, sessionID: input.sessionID }, call.sessionID, false)
              return { sessionID: input.sessionID, managedSessionID: root, formID: input.formID, status: "stale" }
            }
            throw error
          }
          await consumeForm({ id: input.formID, sessionID: input.sessionID }, call.sessionID)
          return { sessionID: input.sessionID, managedSessionID: root, formID: input.formID, status: "replied" }
          })
        },
      },
      permission: {
        required: ["sessionID", "requestID", "decision"],
        async run(input, call) {
          if (!["once", "always", "reject"].includes(input.decision)) throw new Error("Invalid permission decision")
          const { root } = await resolveOwned(input.sessionID, call.sessionID)
          const pending = await pendingFor([input.sessionID])
          const request = pending.find((item) => item.id === input.requestID)
          if (!request) return { sessionID: input.sessionID, requestID: input.requestID, status: "stale" }
          try {
            await replyPermission({ sessionID: input.sessionID, requestID: input.requestID, decision: input.decision })
            asked.delete(input.requestID)
          } catch (error) {
            const remaining = await pendingFor([input.sessionID])
            if (!remaining.some((item) => item.id === input.requestID)) {
              return { sessionID: input.sessionID, requestID: input.requestID, status: "stale" }
            }
            throw error
          }
          const running = await probe(input.sessionID, call.signal)
          return { sessionID: input.sessionID, managedSessionID: root, requestID: input.requestID, status: "replied", decision: input.decision, running }
        },
      },
      interrupt: {
        required: ["sessionID"],
        async run(input, call) {
          const entry = await owned(input.sessionID, call.sessionID)
          const children = (await descendants(input.sessionID)).slice(1)
          if (children.length) throw new Error(`Interrupt refused; descendants exist: ${children.join(", ")}`)
          const response = await ctx.session.interrupt({ sessionID: input.sessionID, resume: false })
          if (!response.interrupted) throw new Error(`Interrupt not confirmed for ${input.sessionID}`)
          entry.pending = false
          await ctx.storage.set(key(input.sessionID), entry)
          return { sessionID: input.sessionID, status: "interrupted", interrupted: true }
        },
      },
      delete: {
        required: ["sessionID"],
        async run(input, call) {
          const id = input.sessionID
          return withNoticeLock(id, async () => {
            const entry = await owned(id, call.sessionID)
            if (entry.deleting) throw new Error("Session deletion requires reconciliation")
            if (entry.pending || entry.watched) throw new Error("Session has pending or watched work")
            const ids = await descendants(id)
            if (ids.includes(call.sessionID)) throw new Error("Cannot delete a tree containing the managing session")
            for (const child of ids.slice(1)) {
              if (await ctx.storage.get(key(child))) throw new Error(`Independently managed descendant: ${child}`)
            }
            for (const sessionID of ids) {
              if (blocking(sessionID) || await probe(sessionID, call.signal)) throw new Error(`Session is busy: ${sessionID}`)
            }
            if (call.signal.aborted) throw new Error("Deletion cancelled")
            if ((await formsFor(ids)).length) throw new Error("Session has pending forms")
            for (const sessionID of ids) {
              const requests = await (await client()).permission.list({ sessionID }, await formOptions(sessionID))
              if (!Array.isArray(requests)) throw new Error("Permission check returned no result")
              if (requests.some((request) => request.sessionID === sessionID) ||
                  [...asked.values()].some((request) => request.sessionID === sessionID)) {
                throw new Error("Session has pending permissions")
              }
            }
            const markers = []
            let after
            do {
              const page = await ctx.storage.scan({ prefix: "form/", limit: 200, ...(after ? { after } : {}) })
              for (const item of page.entries ?? []) {
                if (item.value?.owner === call.sessionID && ids.includes(item.value.sessionID)) markers.push(item)
              }
              after = page.next
            } while (after)
            for (const marker of markers) {
              if (!(await cancelNotice(call.sessionID, marker.value.inboxID))) throw new Error("Could not cancel question notice")
            }
            if (!(await cancelNotice(call.sessionID, entry.completionInboxID))) throw new Error("Could not cancel completion notice")
            if (call.signal.aborted) throw new Error("Deletion cancelled")
            // Persist before removal: an uncertain result must never restart notifications.
            await ctx.storage.set(key(id), { ...entry, deleting: true })
            try {
              await ctx.session.remove({ sessionID: id })
            } catch (error) {
              throw new Error(`Session removal uncertain for ${id}; reconciliation required: ${error.message}`)
            }
            try {
              for (const marker of markers) {
                await ctx.storage.remove(marker.key)
                formNotices.delete(marker.key.slice("form/".length))
              }
              for (const sessionID of ids) {
                running.delete(sessionID)
                outcome.delete(sessionID)
                for (const [requestID, request] of asked) if (request.sessionID === sessionID) asked.delete(requestID)
              }
              await ctx.storage.remove(key(id))
            } catch (error) {
              throw new Error(`Session ${id} deleted but plugin cleanup incomplete: ${error.message}`)
            }
            return { sessionID: id, status: "deleted" }
          })
        },
      },
      wait: {
        required: ["sessionID"],
        async run(input, call) {
          let entry
          const initial = await withNoticeLock(input.sessionID, async () => {
            entry = await owned(input.sessionID, call.sessionID)
            const question = await questionFor(await descendants(input.sessionID), false)
            if (question) {
              await consumeForm({ id: question.formID, sessionID: question.sessionID }, call.sessionID)
              return { managedSessionID: input.sessionID, ...question, output: (await snapshot(input.sessionID, entry)).output, partial: true }
            }
            if (!entry.pending) {
              const output = (await snapshot(input.sessionID, entry)).output
              if (entry.completionInboxID) {
                if (await cancelNotice(call.sessionID, entry.completionInboxID)) {
                  delete entry.completionInboxID
                  await ctx.storage.set(key(input.sessionID), entry)
                }
              }
              return { sessionID: input.sessionID, status: "idle", output }
            }
            entry.watched = true
            await ctx.storage.set(key(input.sessionID), entry)
          })
          if (initial) return initial
          const deadline = Date.now() + (input.timeout ?? 5000)
          let state
          try {
          await call.progress({ status: "waiting" })
          while (Date.now() < deadline && !call.signal.aborted) {
            const ids = await descendants(input.sessionID)
            const blocked = await withNoticeLock(input.sessionID, async () => {
              const question = await questionFor(ids, false)
              if (question) await consumeForm({ id: question.formID, sessionID: question.sessionID }, call.sessionID)
              return question ?? await permissions(ids)
            })
            if (blocked) { state = blocked; break }
            const latest = await owned(input.sessionID, call.sessionID)
            if (!latest.pending) {
              state = { status: latest.status ?? "completed" }
              break
            }
            await new Promise((resolve) => {
              const timer = setTimeout(done, Math.max(1, Math.min(100, deadline - Date.now())))
              function done() { clearTimeout(timer); call.signal.removeEventListener("abort", done); resolve() }
              if (call.signal.aborted) done()
              else call.signal.addEventListener("abort", done, { once: true })
            })
          }
          if (!state) state = { status: call.signal.aborted ? "cancelled" : "running" }
          const current = await snapshot(input.sessionID, entry)
          const status = state.status === "completed" && current.failed ? "failed" : state.status
          if (["completed", "failed", "interrupted", "incomplete"].includes(status)) {
            await withNoticeLock(input.sessionID, async () => {
              const latest = await ctx.storage.get(key(input.sessionID))
              if (latest?.owner === call.sessionID && latest.completionInboxID) {
                if (await cancelNotice(call.sessionID, latest.completionInboxID)) {
                  delete latest.completionInboxID
                  await ctx.storage.set(key(input.sessionID), latest)
                }
              }
            })
          }
          return { managedSessionID: input.sessionID, sessionID: input.sessionID, ...state, status, output: current.output, partial: status !== "completed", ...(current.output ? {} : { outputUnavailable: true }) }
          } finally {
            await withNoticeLock(input.sessionID, async () => {
            const latest = await ctx.storage.get(key(input.sessionID))
            if (latest?.watched) {
              delete latest.watched
              await ctx.storage.set(key(input.sessionID), latest)
            }
            })
          }
        },
      },
    }
    const descriptions = {
      list: "List recent sessions, optionally continuing with a cursor.",
      attach: "Adopt an existing idle session for this managing session.",
      start: "Start a managed project session in a directory and return immediately; its answer arrives later as a message in this session.",
      prompt: "Send another task to a managed project session; returns immediately.",
      wait: "Convenience join: reports current state after a few seconds at most. Results also arrive as messages.",
      context: "Report current state, pending permission and bounded answer for a managed project session.",
      permission: "Answer a permission request raised by a managed project session: once, always, or reject.",
      answer: "Answer a pending question form from an owned project session or descendant using an object keyed by field key.",
      interrupt: "Stop a managed project session that has no descendants.",
      delete: "Permanently delete an idle owned managed root and its descendants; refuses pending work.",
      read: "Read bounded text history from an accessible session, including one not owned by this manager.",
      search: "Search bounded text history in an accessible session or recent sessions.",
      info: "Read metadata for an accessible session.",
    }
    await ctx.tool.transform((editor) => {
      for (const [name, definition] of Object.entries(definitions)) {
        editor.add({
          name, description: descriptions[name] ?? `Manage an owned project session: ${name}`,
          input: { type: "object", properties: Object.fromEntries(definition.required.concat(name === "wait" ? ["timeout"] : name === "list" ? ["limit", "cursor"] : name === "read" ? ["limit"] : name === "search" ? ["sessionID", "caseSensitive", "limit"] : []).map((field) => [field, properties[field]])), required: definition.required, additionalProperties: false },
          options: { namespace: "session_manager", codemode: true },
          execute: async (input, call) => result(await (["attach", "start", "prompt", "interrupt", "permission", "answer", "delete"].includes(name)
            ? withOperationLock(() => definition.run(input, call))
            : definition.run(input, call))),
        })
      }
    })
    return () => { stop.abort(); clearTimeout(recoveryTimer) }
  },
}) }

export default createPlugin()
