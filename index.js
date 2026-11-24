import { Plugin } from "@opencode/plugin"

const properties = {
  sessionID: { type: "string", minLength: 1 },
  directory: { type: "string", minLength: 1 },
  task: { type: "string", minLength: 1 },
  timeout: { type: "integer", minimum: 1, maximum: 3600000 },
  requestID: { type: "string", minLength: 1 },
  decision: { type: "string", enum: ["once", "always", "reject"] },
  limit: { type: "integer", minimum: 1, maximum: 100 },
  cursor: { type: "string", minLength: 1 },
}

export default Plugin.define({
  id: "session-manager",
  async setup(ctx) {
    const limit = Number.isInteger(ctx.options?.maxOutput) && ctx.options.maxOutput > 0
      ? Math.min(ctx.options.maxOutput, 100000) : 4000
    const rules = Array.isArray(ctx.options?.allowPermissions) ? ctx.options.allowPermissions : []
    const locks = new Set()
    // permission.asked arrives with a location of its own, so session-scoped polling misses
    // requests raised by project sessions that live outside this plugin's location.
    const asked = new Map()
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
        await ctx.permission.reply({ sessionID: id, requestID: request.id, decision: "once" })
        asked.delete(request.id)
      }
      return null
    }
    const running = new Map()
    const outcome = new Map()
    const deliveryLocks = new Set()
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
      match.entry = await ctx.storage.get(key(match.root))
      if (!match.entry?.pending) return
      if (match.entry.watched) {
        // A wait call is already collecting this result; suppress the duplicate notice.
        match.entry.pending = false
        match.entry.delivered = true
        await ctx.storage.set(key(match.root), match.entry)
        return
      }
      if (match.entry.delivered) return
      const ids = await descendants(match.root)
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
        await ctx.session.synthetic({ sessionID: match.entry.owner, text: lines.join("\n"), delivery: "queue" })
        match.entry.delivered = true
        match.entry.pending = false
        await ctx.storage.set(key(match.root), match.entry)
      } catch {}
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
    void (async () => {
      try {
        const page = await ctx.storage.scan({ prefix: "session/", limit: 200 })
        for (const { key: stored, value } of page.entries ?? []) {
          if (value?.pending && !value.delivered && !value.watched) {
            const id = stored.slice("session/".length)
            value.recovered = true
            await ctx.storage.set(stored, value)
            setTimeout(() => { void idled(id).catch(() => {}) }, 1500)
          }
        }
      } catch {}
    })()
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
        if (entry.pending) throw new Error("Session has a pending prompt; wait or interrupt first")
        const messages = await ctx.session.context({ sessionID: id })
        entry.previous = messages.filter((m) => m.type === "assistant").map((m) => m.id)
        entry.delivered = false
        outcome.delete(id)
        running.delete(id)
        delete entry.recovered
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
    const definitions = {
      list: {
        required: [],
        async run(input) {
          const page = await ctx.session.list(input.cursor ? { cursor: input.cursor } : { limit: input.limit ?? 20, order: "desc" })
          return { sessions: page.data.map((s) => ({ sessionID: s.id, title: s.title ?? null, directory: s.location?.directory ?? null, updated: s.time?.updated ?? null, parentID: s.parentID ?? null })), next: page.cursor?.next ?? null }
        },
      },
      attach: {
        required: ["sessionID"],
        async run(input, call) {
          const id = input.sessionID
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
          const entry = await owned(input.sessionID, call.sessionID)
          const blocked = await permissions(await descendants(input.sessionID))
          const current = await snapshot(input.sessionID, entry)
          return { managedSessionID: input.sessionID, ...(blocked ?? { sessionID: input.sessionID, status: entry.pending ? "pending" : "idle" }), output: current.output }
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
            // Events are server-wide but reply is location-scoped: requests from other directories surface and cannot be answered (see README).
            await ctx.permission.reply({ sessionID: input.sessionID, requestID: input.requestID, decision: input.decision })
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
      wait: {
        required: ["sessionID"],
        async run(input, call) {
          const entry = await owned(input.sessionID, call.sessionID)
          if (!entry.pending) return { sessionID: input.sessionID, status: "idle", output: (await snapshot(input.sessionID, entry)).output }
          await call.progress({ status: "waiting" })
          entry.watched = true
          await ctx.storage.set(key(input.sessionID), entry)
          const deadline = Date.now() + (input.timeout ?? 5000)
          let state
          try {
          while (Date.now() < deadline && !call.signal.aborted) {
            const ids = await descendants(input.sessionID)
            const blocked = await permissions(ids)
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
          return { managedSessionID: input.sessionID, sessionID: input.sessionID, ...state, status, output: current.output, partial: status !== "completed", ...(current.output ? {} : { outputUnavailable: true }) }
          } finally {
            const latest = await ctx.storage.get(key(input.sessionID))
            if (latest?.watched) {
              delete latest.watched
              await ctx.storage.set(key(input.sessionID), latest)
            }
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
      interrupt: "Stop a managed project session that has no descendants.",
    }
    await ctx.tool.transform((editor) => {
      for (const [name, definition] of Object.entries(definitions)) {
        editor.add({
          name, description: descriptions[name] ?? `Manage an owned project session: ${name}`,
          input: { type: "object", properties: Object.fromEntries(definition.required.concat(name === "wait" ? ["timeout"] : name === "list" ? ["limit", "cursor"] : []).map((field) => [field, properties[field]])), required: definition.required, additionalProperties: false },
          options: { namespace: "session_manager", codemode: true },
          execute: async (input, call) => result(await definition.run(input, call)),
        })
      }
    })
    return () => stop.abort()
  },
})
