// Run against a separately started, authenticated local server with this plugin loaded.
import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { createWriteStream } from "node:fs"
import { mkdtemp, mkdir, writeFile, access } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"

const server = process.env.SESSION_MANAGER_SERVER
const password = process.env.OPENCODE_SERVER_PASSWORD
if (!server || !password) throw new Error("Set SESSION_MANAGER_SERVER and OPENCODE_SERVER_PASSWORD")
const root = await mkdtemp(path.join(tmpdir(), "session-manager-live-"))
const a = path.join(root, "a")
const b = path.join(root, "b")
await Promise.all([mkdir(a), mkdir(b)])
const helper = path.join(root, "barrier.cjs")
const release = path.join(root, "release")
await writeFile(helper, `const fs=require("fs");const path=require("path");const base=${JSON.stringify(root)};const id=process.argv[2];fs.writeFileSync(path.join(base,id+".ready"),"ready");const deadline=setTimeout(()=>{clearInterval(timer);process.exit(2)},60000);const timer=setInterval(()=>{if(fs.existsSync(path.join(base,"release"))){clearInterval(timer);clearTimeout(deadline);console.log(id==="a"?"ALPHA":"BETA")}},100);`)
const task = (id) => `Run the shell command node "${helper}" ${id}, wait for it to finish, then reply with its output. Do not use any other tool.`
const prompt = `Use execute to search for session_manager, then use execute to run exactly this JavaScript:
const [a,b] = await Promise.all([tools.session_manager.start({directory:${JSON.stringify(a)},task:${JSON.stringify(task("a"))}}),tools.session_manager.start({directory:${JSON.stringify(b)},task:${JSON.stringify(task("b"))}})]);
console.log("STARTS",a,b);
const x=JSON.parse(a),y=JSON.parse(b);
const [ra,rb]=await Promise.all([tools.session_manager.wait({sessionID:x.sessionID,timeout:60000}),tools.session_manager.wait({sessionID:y.sessionID,timeout:60000})]);
console.log("RESULTS",ra,rb);
Return the results then stop.`
const cli = path.join(process.env.APPDATA, "npm", "node_modules", "@opencode", "cli", "bin", "opencode.exe")
const child = spawn(cli, ["--server", server, "run", "--format", "json", "--auto", prompt])
let stdout = ""
let stderr = ""
const stdoutFile = createWriteStream(path.join(root, "child.stdout"))
const stderrFile = createWriteStream(path.join(root, "child.stderr"))
child.stdout.on("data", (data) => { stdout += data; stdoutFile.write(data) })
child.stderr.on("data", (data) => { stderr += data; stderrFile.write(data) })
console.log(`Child output files: ${root}`)
const exited = new Promise((resolve, reject) => {
  child.on("error", reject)
  child.on("close", resolve)
})
const deadline = setTimeout(() => child.kill(), 180000)
const exists = async (file) => access(file).then(() => true, () => false)
let barrier = false
for (let n = 0; n < 90; n++) {
  if (await exists(path.join(root, "a.ready")) && await exists(path.join(root, "b.ready"))) {
    barrier = true
    break
  }
  await new Promise((resolve) => setTimeout(resolve, 500))
}
await writeFile(release, "released")
const status = await exited
clearTimeout(deadline)
stdoutFile.end()
stderrFile.end()
if (/provider\.rate-limit|inference service is overloaded|\b429\b/i.test(`${stdout}\n${stderr}`) && status !== 0) {
  console.log("RETRYABLE environment failure: provider rate limit")
  process.exit(2)
}
if (status !== 0) throw new Error(`opencode failed: ${status} ${stderr}`)
const events = stdout.split(/\r?\n/).filter((line) => line.startsWith("{")).map((line) => JSON.parse(line))
const manager = events.find((event) => event.sessionID)?.sessionID
assert.ok(manager, "missing managing session")
const toolOutput = events.filter((event) => event.type === "tool_use").map((event) => event.part?.state?.output ?? "").join("\n")
const ids = [...new Set(toolOutput.match(/ses_[A-Za-z0-9]+/g) ?? [])].filter((id) => id !== manager)
assert.ok(ids.length >= 2, `expected two project session IDs, got ${ids.length}`)
const headers = { Authorization: `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}` }
async function get(route) {
  const response = await fetch(`${server}${route}`, { headers })
  if (!response.ok) throw new Error(`${route}: HTTP ${response.status}`)
  return response.json()
}
for (const id of ids.slice(0, 2)) {
  const session = (await get(`/api/session/${encodeURIComponent(id)}`)).data
  assert.ok(!session.parentID, `${id} is a child session`)
}
const transcript = JSON.stringify((await get(`/api/session/${encodeURIComponent(manager)}/context`)).data)
assert.ok(!transcript.includes("<background-result>"), "background result injected")
assert.ok(toolOutput.includes("ALPHA") && toolOutput.includes("BETA"), "project results missing")
console.log(`PASS root sessions: ${ids.slice(0, 2).join(", ")}`)
console.log("PASS managing transcript: no <background-result> after two results")
console.log(`${barrier ? "PASS" : "FAIL"} controlled barrier: both ready before release=${barrier}`)
console.log("FAIL partial timeout: not exercised by this script")
console.log("FAIL real permission and cancellation: not exercised by this script")
process.exitCode = 1
