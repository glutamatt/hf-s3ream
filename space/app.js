// hf-s3ream Space — fully client-side. OAuth (Sign in with HF) → call the HF
// Jobs/Buckets API directly (CORS-enabled). AWS keys stay in the browser and go
// only into the Job's encrypted `secrets`.
//
// Run model: launch ONE *planner* job (`--plan`). The planner lists the source
// once, cuts the keyspace into ranges, and spawns a copier job per range — ALL
// orchestration happens in the planner. The Space only OBSERVES: it follows the
// planner's log for RANGE/COPIER/PLAN_DONE/VERIFY/PLAN_RESULT, then follows each
// discovered copier's log for LISTING/PROGRESS/DONE to draw the aggregate graph.
import {
  oauthLoginUrl, oauthHandleRedirectIfPresent,
  runJob as hubRunJob, streamJobLogs, getJob,
} from "https://esm.sh/@huggingface/hub@2";

const HF = "https://huggingface.co";
// Released image the planner and copiers run. Bump alongside each vX.Y.Z tag.
const IMAGE = "ghcr.io/glutamatt/hf-s3ream:v0.3.11";
const RUST_LOG = "hf_s3ream=info,xet_data=warn,xet_client=warn";
const PART_BYTES = 16 * 1024 * 1024;
// The dry-run only lists metadata: a normal bucket finishes in seconds, and 2
// minutes is enough to conclude OR gather enough of the listing to call it "a
// long run". Billed per started minute, killed AT this cap.
const DRY_RUN_TIMEOUT_S = 120;
// The planner staggers its own copier launches; the Space no longer launches
// shards, so no wave logic here.

let token = null;
let tokenExpiresAt = null; // ms since epoch, from the sign-in result
let userNs = null;

const $ = (id) => document.getElementById(id);
const show = (id) => $(id).classList.remove("hidden");
const hide = (id) => $(id).classList.add("hidden");

// ---------- auth ----------
async function init() {
  // ?demo=1 → drive the live view with synthetic data through the real code
  // paths (chart, tiles, range map). For UI work without launching jobs.
  if (new URLSearchParams(location.search).has("demo")) return startDemo();
  let res = null;
  try { res = await oauthHandleRedirectIfPresent(); } catch (e) { console.warn(e); }
  if (res && res.accessToken) {
    token = res.accessToken;
    const exp = new Date(res.accessTokenExpiresAt).getTime();
    tokenExpiresAt = Number.isFinite(exp) ? exp : null;
    const u = res.userInfo || {};
    userNs = u.preferred_username || u.name || u.sub;
    $("who-name").textContent = userNs;
    if (u.picture) $("who-pic").src = u.picture;
    show("who"); show("form");
    $("dst").value = userNs ? `${userNs}/s3ream` : "";
    readAccount();
  } else {
    show("signin");
  }
}
$("login").onclick = async () => { window.location.href = await oauthLoginUrl(); };
$("logout").onclick = (e) => {
  e.preventDefault();
  try { localStorage.clear(); sessionStorage.clear(); } catch {}
  window.location.href = window.location.origin + window.location.pathname;
};

// The sign-in token is the HF_TOKEN of every Job: the planner and its copiers
// use it until the planner ends. It expires a fixed time after sign-in
// (hf_oauth_expiration_minutes in README.md), not after Run. The page keeps it
// in memory only — a reload shows the sign-in button again — so the expiry
// read at sign-in is the one that applies. A Job that could outlive the token
// is not launched: past the expiry, the planner can no longer watch or
// respawn its copiers, and the copiers can no longer write to the bucket.
const TOKEN_MARGIN_S = 3600; // spare life past the Job timeout
// True when the token outlives a Job of `timeoutSeconds` plus the margin.
// Otherwise says why in #form-msg: `what` names the Job, `alt` is a second
// way out besides a new sign-in.
function tokenOutlives(timeoutSeconds, what, alt = "") {
  if (tokenExpiresAt == null) return true;
  const leftS = (tokenExpiresAt - Date.now()) / 1000;
  if (leftS >= timeoutSeconds + TOKEN_MARGIN_S) return true;
  $("form-msg").textContent = leftS > 0
    ? `Your Hugging Face sign-in expires in ${fmtDur(leftS)}. The Jobs use it until they end, and ${what} ` +
      `can run for up to ${fmtDur(timeoutSeconds)}, plus ${fmtDur(TOKEN_MARGIN_S)} of margin. ` +
      `Sign out (top right) and sign in again${alt ? `, or ${alt}` : ""}.`
    : "Your Hugging Face sign-in has expired. Sign out (top right) and sign in again.";
  $("form-msg").className = "msg err";
  return false;
}
const SHORTER_TIMEOUT = "set a shorter “Planner timeout” under Advanced";

// Jobs need a positive credit balance. whoami-v2 says whether the account can
// pay (no balance figure is exposed): true / false, or null when unknown —
// unknown never blocks. Read again before each launch, so credits added in
// another tab count without a new sign-in.
let canPay = null;
async function readAccount() {
  try {
    const r = await hf("/api/whoami-v2");
    if (!r.ok) return;
    const w = await r.json();
    if (typeof w.canPay === "boolean") canPay = w.canPay;
  } catch { /* unknown stays null */ }
}
// True unless the account is known to be unable to pay; then says so in #form-msg.
async function accountCanRunJobs() {
  await readAccount();
  if (canPay !== false) return true;
  $("form-msg").innerHTML = `Your account cannot run Jobs yet: Jobs need a positive credit balance. ` +
    `<a href="${HF}/settings/billing" target="_blank" rel="noopener">Add credits</a>, then try again.`;
  $("form-msg").className = "msg err";
  return false;
}

// ---------- HF API helpers ----------
async function hf(path, opts = {}) {
  return fetch(`${HF}${path}`, {
    ...opts,
    headers: { Authorization: `Bearer ${token}`, ...(opts.headers || {}) },
  });
}

function splitBucketDestination(dst) {
  const value = dst.trim();
  if (value.startsWith("hf://") && !value.startsWith("hf://buckets/")) {
    throw new Error("destination HF URI must use the buckets/ type");
  }
  const isUri = value.startsWith("hf://buckets/");
  const parts = (isUri ? value.slice("hf://buckets/".length) : value).replace(/^\/+|\/+$/g, "").split("/");
  if (!isUri && parts.length === 1 && parts[0]) return [userNs, parts[0]];
  if (parts.length < 2 || !parts[0] || !parts[1] || parts[0].includes("@") || parts[1].includes("@")) {
    throw new Error("destination must be org/name or hf://buckets/org/name[/path]");
  }
  if (!isUri && parts.length > 2) throw new Error("destination prefixes require an hf://buckets/ URI");
  if (parts.slice(2).some((part) => !part)) throw new Error("destination path must not contain empty segments");
  return [parts[0], parts[1]];
}

// A missing destination bucket is created private: the source may be private
// data, and the Hub creates a bucket public when `private` is not sent. An
// existing bucket keeps its visibility; we read it only to show it.
async function ensureBucket(dst) {
  let ns, name;
  try {
    [ns, name] = splitBucketDestination(dst);
  } catch (e) {
    return { ok: false, error: e.message };
  }
  const r = await hf(`/api/buckets/${ns}/${name}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ private: true }),
  });
  if (r.ok) return { ok: true, created: true, private: true };
  if (r.status === 409) return { ok: true, created: false, private: await bucketIsPrivate(ns, name) };
  const body = await r.text();
  return { ok: false, error: `HTTP ${r.status}: ${body.slice(0, 200)}` };
}

// true / false, or null when the visibility could not be read.
async function bucketIsPrivate(ns, name) {
  try {
    const r = await hf(`/api/buckets/${ns}/${name}`);
    if (!r.ok) return null;
    const info = await r.json();
    return typeof info.private === "boolean" ? info.private : null;
  } catch {
    return null;
  }
}

function bucketCheckText(dst, b) {
  if (b.created) return `bucket <b>${dst}</b> created, <b>private</b> — write OK`;
  const visibility =
    b.private === true ? ", private"
    : b.private === false ? ", <b>public</b> (anyone can read the copied files)"
    : "";
  return `bucket <b>${dst}</b> exists${visibility} — write OK`;
}

function toSeconds(t) {
  const m = String(t).trim().match(/^([\d.]+)\s*([smhd])?$/);
  if (!m) return 12 * 3600;
  return Math.round(parseFloat(m[1]) * { s: 1, m: 60, h: 3600, d: 86400 }[m[2] || "s"]);
}

// ---------- source access mode ----------
// "Public bucket": the Jobs read S3 anonymously (`--no-sign-request`, as in
// aws-cli) and no AWS credential is sent at all. The switch is the single
// source of truth; this projects it onto the form — the .cred inputs (access
// key, secret, session token) are disabled, so they leave the tab order and
// are never read, and `#form.public` drives the dimmed look, the hint and the
// withdrawn required markers.
const publicSource = () => $("public").checked;
function applySourceMode() {
  const on = publicSource();
  $("form").classList.toggle("public", on);
  for (const el of document.querySelectorAll(".cred input")) el.disabled = on;
}
$("public").addEventListener("change", () => {
  applySourceMode();
  // The pre-flight message described the form as it was — notably the "keys
  // are required" one this switch answers — so it no longer applies.
  $("form-msg").textContent = ""; $("form-msg").className = "msg";
});
// Browsers restore form-control state across a soft reload: project it once
// at load so the switch and the fields never disagree.
applySourceMode();

// Launch one Job via @huggingface/hub. `extra` = extra hf-s3ream args. Returns job id.
async function runJob({ src, dst, extra = [], flavor, timeoutSeconds, secrets, dryRun = false }) {
  const command = ["hf-s3ream", src, dst, ...extra];
  if (dryRun) command.push("--dry-run");
  // Public source: read S3 unsigned. Goes on the command line, not in the
  // environment, so the mode is legible on the Job's page; the planner
  // forwards it to every copier it spawns.
  if (publicSource()) command.push("--no-sign-request");
  // Only pin AWS_REGION when the user typed one; otherwise the Job auto-detects
  // the bucket's region (HeadBucket, see detect_bucket_region in src/sync.rs).
  // The planner forwards the resolved region to its copiers via --aws-region.
  const environment = { RUST_LOG };
  const region = $("region").value.trim();
  if (region) environment.AWS_REGION = region;
  const job = await hubRunJob({
    accessToken: token,
    namespace: userNs,
    dockerImage: IMAGE,
    command,
    environment,
    secrets,
    flavor,
    timeoutSeconds,
  });
  return job.id || job._id || job.jobId;
}

// Job secrets: the HF token always; the AWS credentials only when the source
// is read with them — a public source sends nothing AWS-side.
function collectSecrets() {
  const s = { HF_TOKEN: token };
  if (publicSource()) return s;
  s.AWS_ACCESS_KEY_ID = $("ak").value.trim();
  s.AWS_SECRET_ACCESS_KEY = $("sk").value.trim();
  if ($("st").value.trim()) s.AWS_SESSION_TOKEN = $("st").value.trim();
  return s;
}

async function jobStage(id) {
  try {
    const j = await getJob({ accessToken: token, namespace: userNs, jobId: id });
    return (j.status && j.status.stage) || j.stage || null;
  } catch { return null; }
}

async function cancelJob(id) {
  try {
    await fetch(`${HF}/api/jobs/${encodeURIComponent(userNs)}/${id}/cancel`, {
      method: "POST", headers: { Authorization: `Bearer ${token}` },
    });
  } catch (e) { console.warn("cancelJob failed", e); }
}

// Follow a Job's logs via @huggingface/hub's streamJobLogs async generator (it
// unwraps the SSE envelope and yields {message}). Reconnect until a terminal
// stage (covers "logs not ready just after submit" + dropped streams).
async function followJob(id, onLine, signal) {
  const TERMINAL = ["COMPLETED", "ERROR", "CANCELED", "DELETED"];
  for (let attempt = 0; attempt < 100000; attempt++) {
    if (signal && signal.aborted) return "ABORTED";
    try {
      for await (const ev of streamJobLogs({ accessToken: token, namespace: userNs, jobId: id })) {
        if (signal && signal.aborted) return "ABORTED";
        // The /logs SSE emits a synthetic "===== Job started" banner + an empty
        // line IMMEDIATELY — even while the job is still SCHEDULING. The python
        // client filters it (hf_api.fetch_job_logs); the JS SDK does not. Skip
        // it so "a line arrived" reliably means "the container is running"
        // (followCopier promotes scheduling→running on the first line).
        if (!ev.message || ev.message.startsWith("===== Job started")) continue;
        onLine(ev.message);
      }
    } catch (e) { /* stream dropped; re-check stage below */ }
    if (signal && signal.aborted) return "ABORTED";
    const st = await jobStage(id);
    if (st && TERMINAL.includes(st)) return st;
    await new Promise((r) => setTimeout(r, 2000));
  }
  return await jobStage(id);
}

// ---------- formatting ----------
function fmtMMSS(s) { s = Math.max(0, Math.round(s)); return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`; }
// Auto-scaled data size (950 MiB / 128.1 GiB / 3.7 PiB) — delegates to
// fmtData (defined below) so multi-TiB totals never print as "25414.7 GiB".
function fmtSize(bytes) { return fmtData(bytes); }

// ---------- dry-run countdown ----------
let dryTimer = null;
function startDryCountdown(seconds) {
  stopDryCountdown();
  const deadline = performance.now() + seconds * 1000;
  const el = $("dry-timer");
  const tick = () => {
    const left = (deadline - performance.now()) / 1000;
    el.textContent = left > 0 ? `max remaining ~${fmtMMSS(left)}` : "max time reached — wrapping up…";
  };
  el.classList.remove("hidden"); tick();
  dryTimer = setInterval(tick, 1000);
}
function stopDryCountdown() {
  if (dryTimer) { clearInterval(dryTimer); dryTimer = null; }
  $("dry-timer").classList.add("hidden");
}

// ---------- recommendation ----------
// Translate size into planner knobs, from the 2026-07-12 flavor shootout
// (403 jobs, identical copy config): sustained per-copier S3 read is set by the
// FLAVOR — cpu-performance ~1348 MiB/s (tight spread: dedicated NIC), cpu-xl
// ~477, cpu-upgrade ~16 and cpu-basic ~8 (bursty, minute-long zero-stalls:
// small flavors share hosts and starve on the NIC). At those speeds
// cpu-performance is also the CHEAPEST per byte moved (~$0.41/TiB vs ~$0.55
// upgrade / ~$0.61 xl), so it wins on both axes once the copy outgrows the
// ~75s per-job startup tax (schedule + image pull + ramp). Below ~5 GiB even
// cpu-upgrade finishes inside that overhead window — one cheap copier is fine;
// the flavor premium there is bounded by the overhead cost (~4¢).
const FLAVOR_MIBPS = { "cpu-performance": 1300, "cpu-xl": 450, "cpu-upgrade": 16, "cpu-basic": 8 };
const FLAVOR_USD_HR = { "cpu-performance": 1.9, "cpu-xl": 1.0, "cpu-upgrade": 0.03, "cpu-basic": 0.01 };
const OVERHEAD_S = 75;          // per-copier startup tax (schedule+pull+ramp)
const PERF_MIN_GIB = 5;         // below this, wall-clock is all overhead → cheap flavor
// Commit ceiling: the destination accepts ~1000 file registrations/s PER
// DESTINATION REPO (server-side rate limit; throttled beyond). File-heavy
// copies are bound by this, not by bandwidth — the planner caps small-file
// copiers at 2 for the same reason.
const CAS_COMMIT_FPS = 1000;
// Longest planner timeout proposed here. The sign-in lasts 60 h (README.md),
// so this stays well below it — see tokenOutlives().
const MAX_TIMEOUT_H = 48;
function recommend(gib, files = 0) {
  let r;
  if (gib < PERF_MIN_GIB) {
    r = { rangeGib: Math.max(1, Math.ceil(gib)), inflight: 1, flavor: "cpu-upgrade", timeout: "2h" };
  } else {
    // ~64 GiB ranges keep each copier busy ≥ ~50s of pure transfer (amortizes the
    // startup tax) while fanning out wide; cap at 256 copiers / 128 in-flight.
    const targetCopiers = Math.max(1, Math.min(256, Math.ceil(gib / 64)));
    const rangeGib = Math.max(8, Math.ceil(gib / targetCopiers));
    const inflight = Math.min(128, targetCopiers);
    // Planner lives until every copier finishes, so its timeout must outlast the
    // whole copy. Generous (cheap cpu-basic, billed only while it runs; it
    // exits early on done).
    const aggMiBps = inflight * FLAVOR_MIBPS["cpu-performance"];
    const estSec = 900 + (gib * 1024 / aggMiBps) * 3;
    const hours = Math.min(MAX_TIMEOUT_H, Math.max(2, Math.ceil(estSec / 3600)));
    r = { rangeGib, inflight, flavor: "cpu-performance", timeout: `${hours}h` };
  }
  // Commit-bound overlay: when registrations (files/1000 s) outlast the byte
  // work AND files are genuinely tiny (average ≤ 4 MiB — the same classifier
  // the planner uses; a %-of-small-files test misfires on mixed buckets),
  // wide fan-out is pure startup tax — the planner holds commit-heavy copiers
  // at 2 anyway. Recommend the smallest fleet whose byte work still fits
  // inside the commit floor (≥2: that saturates the files/s budget), on the
  // cheap flavor when even it moves the bytes within that floor.
  const commitS = files / CAS_COMMIT_FPS;
  const copiers = Math.max(1, Math.ceil(gib / r.rangeGib));
  const byteS = (gib * 1024) / (Math.min(r.inflight, copiers) * FLAVOR_MIBPS[r.flavor]);
  const commitHeavy = files > 0 && (gib * 1024) / files <= 4;
  if (commitHeavy && commitS > byteS) {
    if ((gib * 1024) / (2 * FLAVOR_MIBPS["cpu-upgrade"]) <= commitS) r.flavor = "cpu-upgrade";
    const need = Math.max(2, Math.ceil((gib * 1024) / (FLAVOR_MIBPS[r.flavor] * commitS)));
    r.inflight = Math.min(r.inflight, need);
    // ~2 ranges per slot for balance; the one extra wave costs 75s vs hours.
    r.rangeGib = Math.max(r.rangeGib, Math.ceil(gib / (r.inflight * 2)) || 1);
    r.timeout = `${Math.min(MAX_TIMEOUT_H, Math.max(2, Math.ceil((900 + commitS * 3) / 3600)))}h`;
  }
  return r;
}
// Jobs are billed per started clock minute (docs/hub/jobs-pricing; checked
// live: 2 min 29 s → 3 min), so a Job pays about one minute more than it runs.
const BILLING_ROUND_S = 60;
const PLANNER_FLAVOR = "cpu-basic";
function fmtEstimate(usd) { return usd < 1 ? `$${usd.toFixed(2)}` : `$${usd.toFixed(usd < 20 ? 1 : 0)}`; }
// Expected wall-clock + compute cost for a reco — shown so the flavor choice is
// legible ("fast AND cheaper"), not a black box. Wall-clock is the max of the
// byte work and the CAS commit floor (files / ~1k f/s per destination repo).
// The cost counts every copier's minute rounding and the planner, which lives
// for the whole copy.
function estimate(gib, r, files = 0) {
  const speed = FLAVOR_MIBPS[r.flavor];
  const copiers = Math.max(1, Math.ceil(gib / r.rangeGib));
  const waves = Math.ceil(copiers / r.inflight);
  const commitS = files / CAS_COMMIT_FPS;
  const byteWallS = waves * OVERHEAD_S + (gib * 1024) / (Math.min(r.inflight, copiers) * speed);
  const wallS = Math.max(byteWallS, commitS);
  const commitBound = commitS > byteWallS;
  // Commit-bound: ~2 copiers stay busy for the whole commit floor — unless
  // the byte work keeps the fleet busy even longer (mixed buckets).
  const busyS = Math.max((gib * 1024) / speed, commitBound ? 2 * commitS : 0);
  const usdNum = (FLAVOR_USD_HR[r.flavor] / 3600) * (copiers * (OVERHEAD_S + BILLING_ROUND_S) + busyS)
    + (FLAVOR_USD_HR[PLANNER_FLAVOR] / 3600) * (wallS + OVERHEAD_S + BILLING_ROUND_S);
  const wall = wallS < 90 ? `${Math.ceil(wallS)}s` : wallS < 5400 ? `${Math.ceil(wallS / 60)} min` : `${(wallS / 3600).toFixed(1)}h`;
  return { copiers, wall, commitBound, usdNum, usd: fmtEstimate(usdNum) };
}
function applyReco(r) {
  $("flavor").value = r.flavor;
  $("rangegib").value = r.rangeGib;
  $("inflight").value = r.inflight;
  $("timeout").value = r.timeout;
  // Warn now when the sign-in would not last that long; Run checks again.
  tokenOutlives(toSeconds(r.timeout), "the copy", SHORTER_TIMEOUT);
}

// Stat cells [label, value, sub?] for a .band container.
function statCells(cells) {
  return cells.map(([k, v, sub]) =>
    `<div class="stat"><span class="k">${k}</span><span class="v">${v}</span>${sub ? `<span class="s">${sub}</span>` : ""}</div>`).join("");
}
// Flavor names short enough for a stat cell.
const FLAVOR_SHORT = { "cpu-performance": "cpu-perf" };
const flavorShort = (f) => FLAVOR_SHORT[f] || f;

function checkLine(state, text) {
  const cls = state === "ok" ? "ok" : state === "err" ? "err" : "run";
  const sym = state === "ok" ? "✓" : state === "err" ? "✗" : "…";
  return `<div class="check"><span class="dot ${cls}">${sym}</span><span>${text}</span></div>`;
}

// {src, gib, files} of the last Analyze that sized its source: the Run's cost
// estimate is computed from it with the settings actually launched.
let lastAnalysis = null;

// Dry-run timed out mid-listing (bucket too big to enumerate in the window):
// recommend from the partial listing (we can't size it, so use the ~25 GiB default).
function bigBucketAdvisory(l) {
  const listed = l.listed || 0, kept = l.kept || listed, bytes = l.bytes || 0, le16 = l.le16 || 0;
  const smallPct = kept > 0 ? Math.round((le16 * 100) / kept) : 0;
  const r = { rangeGib: 64, inflight: 128, flavor: "cpu-performance", timeout: "24h" };
  $("stats").innerHTML = statCells([
    ["Files", `${kept.toLocaleString()}+`, "listing not finished"],
    ["Size", bytes ? `${fmtSize(bytes)}+` : "—"],
    ["Small files", kept ? `${smallPct}%` : "—", "≤ 16 MiB"],
  ]);
  $("stats").classList.add("cols-3");
  $("stats").classList.remove("hidden");
  lastAnalysis = null; // size unknown: no cost estimate for this copy
  $("reco").innerHTML = `<h2>Plan</h2>` +
    `<div class="band">${statCells([
      ["Copiers", "?", `${r.rangeGib} GiB each`],
      ["Flavor", flavorShort(r.flavor), `$${FLAVOR_USD_HR[r.flavor].toFixed(2)} / h`],
      ["At once", `${r.inflight}`, "copiers"],
      ["Cost", "?", "size unknown"],
    ])}</div>` +
    `<p class="plan-note"><b>Very large bucket.</b> Scanned <b>${listed.toLocaleString()}+</b> objects` +
    `${bytes ? ` (<b>${fmtSize(bytes)}+</b>)` : ""}; the dry-run timed out before finishing the listing — the planner will do the full listing itself. ` +
    `On ${r.flavor}, each copier reads ~1.3 GiB/s, and it costs less per TiB than the small flavors (which starve on shared-host NICs). ` +
    `Give a generous planner timeout under “Advanced”: it stays alive until every copier finishes.</p>`;
  $("reco").classList.remove("hidden");
  applyReco(r);
  $("run").disabled = false;
}

// Why a dry-run came back without stats, for the access mode it was launched
// with (`anon`). A region probe that came back empty means the Job assumed
// us-east-1, and on a bucket that lives elsewhere every list call then fails:
// the fix is the "AWS region" field under Advanced, so name it instead of
// sending people back to their keys.
function s3FailureHint(anon, regionFallback) {
  if (regionFallback) {
    return "region could not be auto-detected, so us-east-1 was assumed — set “AWS region” under Advanced if the bucket lives elsewhere" +
      (anon ? "; the bucket must also allow anonymous reads" : "; otherwise check keys");
  }
  return anon
    ? "the bucket must allow anonymous reads; check the region"
    : "check keys/region; VPC-locked buckets are unreachable from HF Jobs";
}

$("analyze").onclick = async () => {
  const src = $("src").value.trim();
  const dst = $("dst").value.trim();
  $("form-msg").textContent = "";
  if (!/^s3:\/\//.test(src)) return (($("form-msg").textContent = "source must be s3://bucket/prefix/"), ($("form-msg").className = "msg err"));
  if (!publicSource() && (!$("ak").value.trim() || !$("sk").value.trim())) return (($("form-msg").textContent = "AWS access key + secret are required — or switch on “Public bucket” for a source that allows anonymous reads"), ($("form-msg").className = "msg err"));
  if (!tokenOutlives(DRY_RUN_TIMEOUT_S, "the dry-run")) return;
  if (!(await accountCanRunJobs())) return;

  show("analysis"); hide("live"); $("stats").classList.add("hidden"); $("stats").classList.remove("cols-3"); $("reco").classList.add("hidden");
  stopDryCountdown();
  const checks = $("checks");
  const lines = {
    bucket: checkLine("run", "creating / checking destination bucket…"),
    billing: canPay ? checkLine("ok", `billing — Jobs run on <b>${userNs}</b>'s account, billed per started minute`) : "",
    job: checkLine("run", `launching dry-run (${publicSource() ? "anonymous " : ""}S3 read + region + size)…`),
  };
  const render = () => (checks.innerHTML = lines.bucket + lines.billing + lines.job);
  render();
  $("analyze").disabled = true; $("run").disabled = true;

  // 1. bucket
  const b = await ensureBucket(dst);
  lines.bucket = b.ok ? checkLine("ok", bucketCheckText(dst, b)) : checkLine("err", `bucket: ${b.error}`);
  render();
  if (!b.ok) { $("analyze").disabled = false; return; }

  // 2. dry-run job → DRYRUN_STATS / DRYRUN_BUCKET / LISTING progress
  let stats = null, bucketOk = null, lastListing = null, regionFallback = false;
  // The switch stays live while the dry-run runs (up to the full timeout), so
  // remember the mode this run was launched with: the failure hint must
  // describe the run that happened, not the switch as it is now. Read in the
  // same synchronous step as collectSecrets() and runJob() just below.
  const anon = publicSource();
  try {
    const id = await runJob({ src, dst, flavor: "cpu-basic", timeoutSeconds: DRY_RUN_TIMEOUT_S, secrets: collectSecrets(), dryRun: true });
    lines.job = checkLine("run", `dry-run job <code>${id}</code> running…`); render();
    startDryCountdown(DRY_RUN_TIMEOUT_S);
    const signal = { aborted: false };
    const follow = followJob(id, (line) => {
      if (line.startsWith("DRYRUN_STATS ")) { try { stats = JSON.parse(line.slice(13)); } catch {} }
      else if (line.startsWith("DRYRUN_BUCKET ")) bucketOk = line.slice(14).trim() === "ok";
      // The Job's own region warning — the `warn!` in resolve_region()
      // (src/sync.rs) — matched as a plain log line like `back-pressure` in
      // the planner follow: the HeadBucket probe named no region (the bucket
      // does not exist, or S3 was unreachable) and us-east-1 was assumed. Kept
      // to explain a listing failure.
      else if (line.includes("could not auto-detect region")) regionFallback = true;
      else if (line.startsWith("LISTING ")) {
        try { lastListing = JSON.parse(line.slice(8)); } catch {}
        if (lastListing) {
          const n = (lastListing.listed || 0).toLocaleString();
          const sz = lastListing.bytes ? ` · <b>${fmtSize(lastListing.bytes)}</b> so far` : "";
          lines.job = checkLine("run", `listing… <b>${n}</b> objects scanned${sz}`); render();
        }
      }
    }, signal);
    const deadline = new Promise((res) => setTimeout(res, DRY_RUN_TIMEOUT_S * 1000, "deadline"));
    const winner = await Promise.race([follow.then(() => "done"), deadline]);
    if (winner === "deadline" && !stats) { signal.aborted = true; await cancelJob(id); }
    stopDryCountdown();
  } catch (e) {
    stopDryCountdown();
    lines.job = checkLine("err", `dry-run failed: ${e.message}`); render();
    $("analyze").disabled = false; return;
  }

  if (!stats) {
    if (lastListing && lastListing.listed && !lastListing.done) {
      lines.job = checkLine("err", `listing didn't finish — <b>${lastListing.listed.toLocaleString()}+</b> objects scanned in ${DRY_RUN_TIMEOUT_S}s`);
      render(); bigBucketAdvisory(lastListing);
    } else {
      lines.job = checkLine("err", `dry-run returned no stats — S3 access failed (${s3FailureHint(anon, regionFallback)}).`);
      render();
    }
    $("analyze").disabled = false; return;
  }
  lines.job = checkLine("ok", `S3 read OK — listed <b>${stats.count.toLocaleString()}</b> objects`);
  render();
  showPlan(stats, src, bucketOk);
  $("analyze").disabled = false; $("run").disabled = false;
};

// The sized source and the plan proposed for it, from the dry-run's
// DRYRUN_STATS. `bucketOk` false = the write-token check failed inside the Job.
function showPlan(stats, src, bucketOk) {
  const gib = stats.total_bytes / 2 ** 30;
  const r = recommend(gib, stats.count);
  applyReco(r);
  if (stats.region && !$("region").value.trim()) $("region").value = stats.region;
  const est = estimate(gib, r, stats.count);
  lastAnalysis = { src, gib, files: stats.count };
  $("stats").innerHTML = statCells([
    ["Files", stats.count.toLocaleString()],
    ["Size", fmtSize(stats.total_bytes)],
    ["Avg file", stats.count ? fmtSize(stats.total_bytes / stats.count) : "—", `median ${fmtSize(stats.median)}`],
    ["Limited by", est.commitBound ? "commits" : "bandwidth"],
  ]);
  $("stats").classList.remove("hidden");

  const why = est.commitBound
    ? `<b>Commit-bound:</b> the destination registers ~1k files/s per repo, so extra copiers or a faster NIC wouldn't finish sooner.`
    : r.flavor === "cpu-performance"
      ? `~1.3 GiB/s per copier on ${r.flavor}: the fastest <i>and</i> the cheapest per TiB.`
      : `A copy this small is startup-dominated: a pricier flavor wouldn't finish sooner.`;
  const bmsg = bucketOk === false ? ` <span class="dot err">✗ bucket write-token failed inside the job</span>` : "";
  $("reco").innerHTML = `<h2>Plan</h2>` +
    `<div class="band">${statCells([
      ["Copiers", est.copiers.toLocaleString(), `${r.rangeGib} GiB each`],
      ["Flavor", flavorShort(r.flavor), `$${FLAVOR_USD_HR[r.flavor].toFixed(2)} / h`],
      ["Time", `≈ ${est.wall}`, `${r.inflight} at once`],
      ["Cost", `≈ ${est.usd}`, "on your account"],
    ])}</div>` +
    `<p class="plan-note">${why} Jobs are billed per started minute; the estimate counts that and the planner. ` +
    `Planner timeout ${r.timeout}. Adjust under “Advanced”, then Run.${bmsg}</p>`;
  $("reco").classList.remove("hidden");
}

// ---------- Run: one planner → observe ----------
const series = [];              // aggregate samples {t, s3, hf, cr}
const copierState = {};         // copier job_id -> live state (see followCopier)
const ranges = {};              // range idx -> {idx, files, bytes, jobId, attempts}
let runDone = false;            // PLAN_RESULT landed — live view is terminal
let verifyResult = null;        // the VERIFY line: the planner's destination check
let hasHf = false;              // any copier emitted hf_mibps_5s (new image)
let hasCommit = false;          // any copier emitted `committed` (newer image)
let planTotalFiles = 0, planTotalBytes = 0, rangesCut = 0, planDone = false;
let planText = "", pausedNote = "";
let runStartMs = 0;     // performance.now() at Run: the origin of every duration
let runStartEpoch = 0;  // Date.now() at Run: the origin of the clock times shown
let runEndMs = 0;       // performance.now() when PLAN_RESULT landed (freezes the clock)
let peakSpeed = 0;      // max aggregate S3 rate seen this run, MiB/s
let peakCrate = 0;      // max aggregate commit rate seen this run, files/s
let lastCommitted = 0, lastCommitMs = 0; // when the committed count last grew
let plannerId = null, plannerStage = null;
// Ranges already counted. followJob reconnects when the log stream drops, and
// the stream then replays from the start: without this, every RANGE line of a
// long plan would count twice.
const rangesSeen = new Set();
let runFlavor = "";     // copier flavor of this run
let runDst = "";        // destination of this run (the banner links to the bucket)
let runEstimate = null; // estimate() of the launched plan; null when Analyze could not size it
// The person's pick on the Bytes/s ↔ Files/s switch; null = follow the regime.
let metricChoice = null;

// Status kicker in the hero ("Ready" → "Copying…" → "Done").
function setKicker(t) { const el = $("kicker"); if (el) el.textContent = t; }

function renderPlan() { $("plan-status").innerHTML = planText + (pausedNote ? ` <span class="paused">${pausedNote}</span>` : ""); }
function setPlan(html) { planText = html; renderPlan(); }

// Compact data sizes with an auto unit (12 KiB / 950 MiB / 128.1 GiB / 3.7 PiB)
// so petabyte-scale runs never blow out their stat cell.
function fmtData(bytes) {
  if (bytes < 2 ** 20) return `${Math.max(bytes > 0 ? 1 : 0, Math.round(bytes / 1024))} KiB`;
  const units = ["MiB", "GiB", "TiB", "PiB"];
  let v = bytes / 2 ** 20, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v >= 100 ? Math.round(v).toLocaleString() : v.toFixed(1)} ${units[i]}`;
}
// Compact file counts: 1.2M / 180k / 950. Rates reuse it (files/s).
function fmtCount(v) {
  v = Math.max(0, Math.round(v));
  if (v >= 1e6) return `${(v / 1e6).toFixed(v >= 1e7 ? 0 : 1)}M`;
  if (v >= 1e4) return `${Math.round(v / 1e3)}k`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
  return v.toLocaleString();
}
function fmtDur(s) {
  s = Math.max(0, Math.round(s));
  if (s < 90) return `${s}s`;
  const m = Math.round(s / 60);
  if (m < 90) return `${m}m`;
  return `${Math.floor(m / 60)}h ${String(m % 60).padStart(2, "0")}m`;
}
// Local clock time (14:52) for an epoch in ms.
function fmtClock(ms) { return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }); }
function fmtUsd(v) {
  if (v === 0) return "$0.00";
  if (v < 0.01) return "< $0.01";
  return v < 1000 ? `$${v.toFixed(2)}` : `$${Math.round(v).toLocaleString()}`;
}
const shortId = (id) => `${String(id).slice(0, 6)}…`;

// Byte rates use ONE unit across a readout: GiB/s once the run has peaked past
// 1000 MiB/s, so the figures don't flip units as they move (and never print
// four-digit MiB).
const GIB_FROM_MIBPS = 1000;
function byteUnit() { return peakSpeed >= GIB_FROM_MIBPS ? "GiB/s" : "MiB/s"; }
function fmtRate(mibps, unit = byteUnit()) {
  const v = unit === "GiB/s" ? mibps / 1024 : mibps;
  if (v === 0) return "0";
  return v >= 100 ? Math.round(v).toLocaleString() : v >= 1 ? v.toFixed(1) : v.toFixed(2);
}
// One copier's rate, in the unit that suits it.
function fmtRateAuto(mibps) { const u = mibps >= GIB_FROM_MIBPS ? "GiB/s" : "MiB/s"; return `${fmtRate(mibps, u)} ${u}`; }

// Commit-heavy = an average file of 4 MiB or less: the planner's own
// classifier. Such a copy is paced by commits, so files/s is the figure that
// matters, not bytes/s.
function commitHeavy() { return planTotalFiles > 0 && planTotalBytes / planTotalFiles <= 4 * 2 ** 20; }
function heroMetric() {
  if (metricChoice === "files" && hasCommit) return "files";
  if (metricChoice === "bytes") return "bytes";
  return hasCommit && commitHeavy() ? "files" : "bytes";
}
for (const b of document.querySelectorAll(".seg [data-metric]")) {
  b.addEventListener("click", () => { metricChoice = b.dataset.metric; updateLive(); scheduleRender(); });
}

function runElapsedS() {
  if (!runStartMs) return 0;
  return ((runDone && runEndMs ? runEndMs : performance.now()) - runStartMs) / 1000;
}

// ---------- billing ----------
// What the copy costs, from the account's Jobs usage: one call lists every Job
// of the current billing period, a running one included (listed ~2 s after it
// starts, its cost grows once per clock minute). Polling faster than once a
// minute shows nothing new. Needs the read-billing scope.
const BILLING_POLL_MS = 60_000;
const jobCost = {};          // job id -> {minutes, usd, flavor}
let billingState = "off";    // off | pending | ok | unavailable
let billingFinal = false;    // the last read after the run has landed
let billingTimer = null;

async function readBilling() {
  try {
    const r = await hf("/api/settings/billing/usage/jobs");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    if (d.hasAccess === false) throw new Error("no access to billing");
    for (const j of (d.usage && d.usage.jobDetails) || []) {
      const usd = (j.totalCostMicroUsd || 0) / 1e6;
      // The list only covers the current period: a Job of last month leaves it
      // on the 1st. Keep the most seen, never let a figure go down.
      const prev = jobCost[j.jobId];
      if (!prev || usd >= prev.usd) jobCost[j.jobId] = { minutes: j.totalMinutes || 0, usd, flavor: j.hardwareFlavor };
    }
    billingState = "ok";
  } catch (e) {
    console.warn("billing:", e.message);
    // Once read, keep the last figures through a failed poll.
    if (billingState !== "ok") { billingState = "unavailable"; stopBilling(); }
  }
  updateLive(); scheduleRender();
}
// At Run the planner is not listed yet (it is still scheduling): read again
// soon after, so the first billed minute shows up early, then once a minute.
const BILLING_EARLY_MS = [15_000, 35_000];
let billingEarly = [];
function startBilling() {
  stopBilling();
  billingState = "pending"; billingFinal = false;
  readBilling();
  billingEarly = BILLING_EARLY_MS.map((ms) => setTimeout(readBilling, ms));
  billingTimer = setInterval(readBilling, BILLING_POLL_MS);
}
function stopBilling() {
  for (const t of billingEarly) clearTimeout(t);
  billingEarly = [];
  if (billingTimer) { clearInterval(billingTimer); billingTimer = null; }
}
// After the run: one read now, one when the last started minute is billed.
function finishBilling() {
  stopBilling();
  if (demo || billingState === "unavailable" || billingState === "off") return;
  setTimeout(readBilling, 5_000);
  setTimeout(() => { billingFinal = true; readBilling(); }, BILLING_POLL_MS + 10_000);
}
// The run's Jobs: the planner and every copier attempt (respawns included).
function runJobIds() { return [plannerId, ...Object.keys(copierState)].filter(Boolean); }
function runCost() {
  let usd = 0, minutes = 0, listed = 0;
  for (const id of runJobIds()) { const c = jobCost[id]; if (c) { usd += c.usd; minutes += c.minutes; listed++; } }
  return { usd, minutes, listed };
}

// ---------- chart ----------
function chartColors() {
  const css = getComputedStyle(document.documentElement);
  const v = (n, fb) => css.getPropertyValue(n).trim() || fb;
  return {
    line: v("--line", "#ffd21e"), fill: v("--line-fill", "rgba(255,210,30,0.22)"),
    s1: v("--s1", "#b28d00"), s2: v("--s2", "#4a80e8"),
    grid: v("--border", "rgba(245,242,234,0.12)"), muted: v("--muted", "#9b948a"),
    bg: v("--surface", "#14110e"),
  };
}
// Grid step for ~3 lines: 1/2/5 × 10^k.
function niceStep(max) {
  const raw = max / 3, p = 10 ** Math.floor(Math.log10(raw || 1)), m = raw / p;
  return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
}

// One series: the main figure (bytes/s or files/s committed) over the run.
// Area wash, faint grid with its values on the right, an emphasized endpoint,
// crosshair + tooltip on hover. The tooltip names every series at that time.
let chartGeom = null; // scales captured by drawChart, reused by the hover layer
let hoverIdx = null;  // index into `series` under the pointer (null = no hover)

function drawChart() {
  const c = $("chart");
  const cw = c.clientWidth || 640, ch = c.clientHeight || 150;
  const dpr = window.devicePixelRatio || 1;
  if (c.width !== Math.round(cw * dpr) || c.height !== Math.round(ch * dpr)) {
    c.width = Math.round(cw * dpr); c.height = Math.round(ch * dpr);
  }
  const ctx = c.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cw, ch);
  if (series.length < 2) { chartGeom = null; return; }
  const C = chartColors();
  const metric = heroMetric();
  const unit = byteUnit();
  const val = metric === "files" ? (p) => p.cr || 0 : (p) => (unit === "GiB/s" ? p.s3 / 1024 : p.s3);
  const unitLabel = metric === "files" ? "files/s" : unit;
  // padR keeps the endpoint's halo inside the canvas.
  const padT = 14, padB = 8, padR = 10;
  const tMax = Math.max(series[series.length - 1].t, 30);
  const vPeak = Math.max(...series.map(val)) * 1.08;
  const step = niceStep(Math.max(vPeak, metric === "files" ? 10 : unit === "GiB/s" ? 1 : 10));
  const vTop = Math.max(step, Math.ceil(vPeak / step) * step);
  const x = (t) => (cw - padR) * (t / tMax);
  const y = (v) => padT + (1 - v / vTop) * (ch - padT - padB);
  chartGeom = { x, tMax, cw, padR };

  // Faint grid, values on the left: the live endpoint sits on the right edge.
  ctx.font = '10px "IBM Plex Mono", ui-monospace, Menlo, Consolas, monospace';
  ctx.textAlign = "left"; ctx.textBaseline = "bottom";
  for (let v = step; v <= vTop + 1e-9; v += step) {
    const yy = Math.round(y(v)) + 0.5;
    ctx.strokeStyle = C.grid; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, yy); ctx.lineTo(cw, yy); ctx.stroke();
    ctx.fillStyle = C.muted;
    ctx.fillText(`${v >= 10 || Number.isInteger(v) ? Math.round(v).toLocaleString() : v.toFixed(1)} ${unitLabel}`, 18, yy - 3);
  }

  // Area wash + line.
  const grad = ctx.createLinearGradient(0, padT, 0, ch);
  grad.addColorStop(0, C.fill); grad.addColorStop(1, "rgba(0,0,0,0)");
  ctx.beginPath(); ctx.moveTo(x(series[0].t), y(0));
  for (const p of series) ctx.lineTo(x(p.t), y(val(p)));
  ctx.lineTo(x(series[series.length - 1].t), ch); ctx.lineTo(x(series[0].t), ch); ctx.closePath();
  ctx.fillStyle = grad; ctx.fill();
  ctx.beginPath();
  series.forEach((p, i) => (i ? ctx.lineTo(x(p.t), y(val(p))) : ctx.moveTo(x(p.t), y(val(p)))));
  ctx.strokeStyle = C.line; ctx.lineWidth = 1.75; ctx.lineJoin = "round"; ctx.lineCap = "round"; ctx.stroke();

  // Endpoint: a dot with a soft halo.
  const ring = (p) => {
    const px = x(p.t), py = y(val(p));
    ctx.beginPath(); ctx.arc(px, py, 7, 0, 7); ctx.strokeStyle = C.fill; ctx.lineWidth = 2; ctx.stroke();
    ctx.beginPath(); ctx.arc(px, py, 3.5, 0, 7); ctx.fillStyle = C.line; ctx.fill();
  };
  ring(series[series.length - 1]);

  // Hover: crosshair snapped to the sample.
  if (hoverIdx != null && series[hoverIdx]) {
    const p = series[hoverIdx], hx = Math.round(x(p.t)) + 0.5;
    ctx.strokeStyle = C.muted; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(hx, padT); ctx.lineTo(hx, ch - padB); ctx.stroke();
    ring(p);
  }
}

// Tooltip: every series at the hovered time; values lead, labels follow.
// Built with textContent.
function updateTip() {
  const tip = $("chart-tip");
  if (hoverIdx == null || !chartGeom || !series[hoverIdx]) { tip.classList.add("hidden"); return; }
  const p = series[hoverIdx];
  const C = chartColors();
  tip.innerHTML = "";
  const t = document.createElement("div");
  t.className = "t"; t.textContent = `t+${fmtDur(p.t)}`;
  tip.appendChild(t);
  const row = (color, text, label) => {
    const r = document.createElement("div"); r.className = "row";
    const k = document.createElement("i"); k.className = "key"; k.style.background = color;
    const b = document.createElement("b"); b.textContent = text;
    const l = document.createElement("span"); l.className = "lbl"; l.textContent = label;
    r.append(k, b, l); tip.appendChild(r);
  };
  const unit = byteUnit();
  row(heroMetric() === "bytes" ? C.line : C.s1, `${fmtRate(p.s3, unit)} ${unit}`, "S3 read");
  if (hasHf) row(C.s2, `${fmtRate(p.hf || 0, unit)} ${unit}`, "upload");
  if (hasCommit) row(heroMetric() === "files" ? C.line : C.muted, `${fmtCount(p.cr || 0)} files/s`, "committed");
  tip.classList.remove("hidden");
  const px = chartGeom.x(p.t);
  const left = px + 14 + tip.offsetWidth > chartGeom.cw ? px - 14 - tip.offsetWidth : px + 14;
  tip.style.left = `${Math.max(0, left)}px`;
}

{
  const c = $("chart");
  c.addEventListener("pointermove", (e) => {
    if (!chartGeom || series.length < 2) return;
    const r = c.getBoundingClientRect();
    const t = ((e.clientX - r.left) / (r.width - chartGeom.padR)) * chartGeom.tMax;
    // Snap to the nearest sample (samples are ~1s apart and sorted by t).
    let lo = 0, hi = series.length - 1;
    while (hi - lo > 1) { const mid = (lo + hi) >> 1; (series[mid].t < t ? (lo = mid) : (hi = mid)); }
    hoverIdx = Math.abs(series[lo].t - t) <= Math.abs(series[hi].t - t) ? lo : hi;
    drawChart(); updateTip();
  });
  c.addEventListener("pointerleave", () => { hoverIdx = null; drawChart(); updateTip(); });
}
window.addEventListener("resize", () => drawChart());
matchMedia("(prefers-color-scheme: light)").addEventListener("change", () => drawChart());

// ---------- ranges and copiers ----------
// Range state helpers: a range's live stage/progress comes from its CURRENT
// copier attempt (respawns re-point jobId).
function rangeView(r) {
  const s = (r.jobId && copierState[r.jobId]) || {};
  const stage = (s.stage || (r.jobId ? "scheduling" : "planned")).toLowerCase();
  const done = (s.bytes || 0) + (s.skippedBytes || 0);
  const pct = r.bytes > 0 && done ? Math.min(100, Math.round((100 * done) / r.bytes)) : null;
  // ≥4 flat PROGRESS ticks (~20s) with the job still running = probable stall
  // (mirrors the copier's own watchdog). Finalize/commit move no S3 bytes by
  // design, so in those phases the state already says what's happening — only
  // flag a stall there after a much longer flat window (~2min, past which the
  // copier's own finalize timeout is closing in).
  const flatCap = s.phase === "committing" || s.phase === "finalizing" ? 24 : 4;
  const stalled = stage === "running" && (s.zeroTicks || 0) >= flatCap;
  return { stage, pct, stalled, s };
}

let renderQueued = false;
function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => { renderQueued = false; renderRangeMap(); renderJobs(); });
}

function renderRangeMap() {
  $("rangemap").innerHTML = Object.values(ranges)
    .sort((a, b) => a.idx - b.idx)
    .map((r) => {
      const v = rangeView(r);
      const title = `range ${r.idx} · ${fmtSize(r.bytes || 0)} · ${v.stage}` +
        (v.pct != null ? ` ${v.pct}%` : "") +
        (v.stalled ? " · ⚠ no progress" : "") +
        ((r.attempts || 1) > 1 ? ` · attempt ${r.attempts}` : "");
      const cls = `cell ${v.stage}${v.stalled ? " stalled" : ""}`;
      const style = v.pct != null ? ` style="--p:${v.pct}%"` : "";
      return r.jobId
        ? `<a class="${cls}"${style} title="${title}" href="${HF}/jobs/${userNs}/${r.jobId}" target="_blank" rel="noopener"></a>`
        : `<span class="${cls}" title="${title}"></span>`;
    }).join("");
}

// A copier's state as a tag. Stalled is a state of its own (it implies
// running); a finalize shows its clock against its budget (it moves no tracked
// bytes — without the clock it would look dead).
function stateTag(v) {
  const s = v.s;
  if (v.stage === "running") {
    if (v.stalled) return ["stall", "Stalled"];
    if (s.finS != null) return ["copy", `Finalizing ${fmtDur(s.finS)} / ${fmtDur(s.finB || 300)}`];
    return ["copy", s.phase === "committing" ? "Committing" : "Copying"];
  }
  if (v.stage === "completed") return ["done", "Done"];
  if (v.stage === "error") return ["fail", "Failed"];
  if (v.stage === "canceled" || v.stage === "deleted") return ["", "Canceled"];
  return ["", "Waiting"];
}
// While copying: what needs a look first — failed, then stalled, then the
// least advanced. Once done: what cost the most.
const STAGE_RANK = { error: 0, running: 2, scheduling: 3, planned: 3, completed: 4 };
const JOB_ROWS = 40;

function renderJobs() {
  const all = Object.values(ranges).filter((r) => r.jobId).map((r) => ({ r, v: rangeView(r) }));
  const billed = billingState === "ok";
  const costOf = (r) => (jobCost[r.jobId] ? jobCost[r.jobId].usd : null);
  const files = heroMetric() === "files";
  const link = (r) => `<a href="${HF}/jobs/${userNs}/${r.jobId}" target="_blank" rel="noopener">#${r.idx}</a>`;
  const cost = (r) => (costOf(r) != null ? fmtUsd(costOf(r)) : "–");
  const time = (s) => (s.elapsed != null ? fmtDur(s.elapsed) : "–");
  // A copier's speed: live while it runs, its average once it is done.
  const speed = (v) => {
    const s = v.s;
    if (v.stage === "running") return files ? `${fmtCount(s.crate || 0)} files/s` : fmtRateAuto(s.speed || 0);
    if (v.stage === "completed" && s.elapsed > 0) {
      return files ? `${fmtCount((s.committed || s.files || 0) / s.elapsed)} files/s` : fmtRateAuto((s.bytes || 0) / 2 ** 20 / s.elapsed);
    }
    return "–";
  };
  // Row title: the rare diagnostics that would crowd the table.
  const title = (r, v) => {
    const s = v.s, bits = [];
    if ((r.attempts || 1) > 1) bits.push(`attempt ${r.attempts}`);
    if ((s.retries || 0) > 0) bits.push(`${s.retries} retries`);
    if (v.stalled) bits.push("no progress for a while");
    if (v.stage === "running" && (s.inflightN || 0) <= 4 && (s.tailSz || 0) >= 2 ** 33 && s.tailR != null) {
      bits.push(`last file ${Math.min(100, Math.round((100 * s.tailR) / s.tailSz))}% of ${fmtData(s.tailSz)}`);
    }
    return bits.length ? ` title="${bits.join(" · ")}"` : "";
  };

  let rows, head, foot = "";
  if (!runDone) {
    all.sort((a, b) => {
      const ra = a.v.stalled ? 1 : STAGE_RANK[a.v.stage] ?? 5, rb = b.v.stalled ? 1 : STAGE_RANK[b.v.stage] ?? 5;
      return ra - rb || (a.v.pct ?? 0) - (b.v.pct ?? 0) || a.r.idx - b.r.idx;
    });
    rows = all.slice(0, JOB_ROWS).map(({ r, v }) => {
      const [cls, text] = stateTag(v);
      const pct = v.stage === "completed" ? 100 : v.pct;
      const bar = v.stalled ? "stall" : v.stage === "completed" ? "done" : v.stage === "error" ? "fail" : "";
      return `<tr${v.stalled ? ' class="hl"' : ""}${title(r, v)}>` +
        `<td>${link(r)}</td><td class="id">${shortId(r.jobId)}</td><td><span class="tag ${cls}">${text}</span></td>` +
        `<td class="pc"><span class="pbar ${bar}"><i style="width:${pct ?? 0}%"></i></span></td>` +
        `<td class="n">${pct != null ? `${pct} %` : "–"}</td>` +
        `<td class="n${v.stalled ? " warn" : ""}">${speed(v)}</td><td class="n">${time(v.s)}</td><td class="n">${cost(r)}</td></tr>`;
    });
    head = `<tr><th>Range</th><th>Job</th><th>State</th><th class="pc">Progress</th><th class="n"></th>` +
      `<th class="n">Speed</th><th class="n">Time</th><th class="n">Cost</th></tr>`;
    $("jobs-h").textContent = "Copiers · least advanced first";
    $("jobs-sub").textContent = billed ? "billed so far, updated every minute" : "";
  } else {
    all.sort((a, b) => (billed ? (costOf(b.r) ?? 0) - (costOf(a.r) ?? 0) : 0) || (b.v.s.elapsed || 0) - (a.v.s.elapsed || 0));
    rows = all.slice(0, JOB_ROWS).map(({ r, v }) => {
      const flavor = (jobCost[r.jobId] && jobCost[r.jobId].flavor) || runFlavor;
      return `<tr${title(r, v)}><td>${link(r)}</td><td class="id">${shortId(r.jobId)}</td><td>${flavorShort(flavor)}</td>` +
        `<td class="n">${speed(v)}</td><td class="n">${time(v.s)}</td><td class="n">${cost(r)}</td></tr>`;
    });
    head = `<tr><th>Range</th><th>Job</th><th>Flavor</th><th class="n">Speed</th><th class="n">Time</th><th class="n">Cost</th></tr>`;
    if (billed) {
      const total = runCost();
      const copierJobs = Object.keys(copierState).length;
      const planner = plannerId && jobCost[plannerId] ? ` (${fmtUsd(jobCost[plannerId].usd)})` : "";
      foot = `<tfoot><tr><td colspan="4">${copierJobs.toLocaleString()} copier Job${copierJobs > 1 ? "s" : ""} + planner${planner}</td>` +
        `<td class="n">${total.minutes.toLocaleString()} min</td><td class="n">${fmtUsd(total.usd)}</td></tr></tfoot>`;
    }
    $("jobs-h").textContent = billed ? "Copiers · most expensive first" : "Copiers · longest first";
    $("jobs-sub").textContent = billed ? (billingFinal ? "final, from your billing" : "from your billing, final in a minute") : "";
  }
  $("jobs").innerHTML = all.length ? `<thead>${head}</thead><tbody>${rows.join("")}</tbody>${foot}` : "";
  const more = all.length > JOB_ROWS ? `+ ${(all.length - JOB_ROWS).toLocaleString()} more copiers` : "";
  const planner = plannerId
    ? `planner <a href="${HF}/jobs/${userNs}/${plannerId}" target="_blank" rel="noopener">${shortId(plannerId)}</a> · ` +
      `${(plannerStage || "running").toLowerCase()} · ${fmtDur(runElapsedS())}` +
      (jobCost[plannerId] ? ` · ${fmtUsd(jobCost[plannerId].usd)}` : "")
    : "";
  $("jobs-more").innerHTML = [more, planner].filter(Boolean).join(" · ");
}

// ---------- the live view ----------
const COMMIT_STALL_S = 60; // no commit for this long while data flows = finalize stalled
function setStat(n, k, v, sub) {
  if (k != null) $(`c${n}-k`).textContent = k;
  $(`c${n}-v`).innerHTML = v;
  $(`c${n}-s`).textContent = sub || "";
}

function renderCost() {
  const est = runEstimate;
  const meter = $("cost-meter");
  meter.classList.add("hidden");
  if (billingState === "ok" && runCost().listed === 0) {
    // Read, but no Job of this run is billed yet: "$0.00" would read as a fact.
    setStat(4, null, "…", "first minute not billed yet");
  } else if (billingState === "ok") {
    const { usd } = runCost();
    const sub = !runDone
      ? (est ? `of ≈ ${est.usd}` : "billed so far")
      : (est ? `estimate ≈ ${est.usd}` : billingFinal ? "final" : "final in a minute");
    setStat(4, null, fmtUsd(usd), sub);
    if (est && est.usdNum > 0) {
      meter.classList.remove("hidden");
      meter.classList.toggle("over", usd > est.usdNum * 1.2);
      meter.firstElementChild.style.width = `${Math.min(100, (100 * usd) / est.usdNum).toFixed(1)}%`;
    }
  } else if (billingState === "pending") {
    setStat(4, null, est ? `≈ ${est.usd}` : "…", "reading your billing…");
  } else {
    setStat(4, null, est ? `≈ ${est.usd}` : "–", est ? "estimate · billing not readable" : "billing not readable");
  }
}

function updateLive() {
  const cs = Object.values(copierState);
  const filesDone = cs.reduce((a, s) => a + (s.files || 0), 0);
  const bytes = cs.reduce((a, s) => a + (s.bytes || 0), 0);
  // --skip-existing: files already at the destination count as done, but not
  // in the rates (nothing was read).
  const skippedFiles = cs.reduce((a, s) => a + (s.skipped || 0), 0);
  const skippedBytes = cs.reduce((a, s) => a + (s.skippedBytes || 0), 0);
  const landedFiles = filesDone + skippedFiles, landedBytes = bytes + skippedBytes;
  // Aggregate instant rate = sum of per-copier 5s rates (they run concurrently,
  // so their instantaneous rates ARE additive).
  const s3Speed = cs.reduce((a, s) => a + (s.speed || 0), 0);
  const hfSpeed = cs.reduce((a, s) => a + (s.hf || 0), 0);
  // Commit stage (files landed in the bucket). committedFiles trails filesDone;
  // the gap is the backlog. Aggregate rate = sum of per-copier rates (additive,
  // same as throughput — copiers commit concurrently).
  const committedFiles = cs.reduce((a, s) => a + (s.committed || 0), 0);
  const commitRate = cs.reduce((a, s) => a + (s.crate || 0), 0);
  const backlog = Math.max(0, filesDone - committedFiles);
  // Fleet metadata pressure: sealed shards awaiting the destination's ack.
  const shardsPending = cs.reduce((a, s) => a + (s.shards || 0), 0);
  // Monotonic wall-clock since Run — NOT max(copier.elapsed). Each copier's
  // elapsed is relative to its own staggered start, so that max jumps around and
  // decreases as copiers finish → the chart line crossed back on itself.
  const elapsed = runElapsedS();
  // True aggregate averages = totals / wall-clock. (Summing per-copier avgs
  // over-counts — copiers don't each span the full wall-clock.)
  const avgMiB = elapsed > 0 ? bytes / 2 ** 20 / elapsed : 0;
  const avgCrate = elapsed > 0 ? committedFiles / elapsed : 0;
  if (s3Speed > peakSpeed) peakSpeed = s3Speed;
  if (commitRate > peakCrate) peakCrate = commitRate;

  // Fleet count, top right.
  const counts = { running: 0, done: 0, failed: 0, pending: 0 };
  for (const r of Object.values(ranges)) {
    const st = rangeView(r).stage;
    if (st === "running") counts.running++;
    else if (st === "completed") counts.done++;
    else if (st === "error") counts.failed++;
    else counts.pending++;
  }
  // Every range copied: the planner is checking the destination (or about to
  // report). Live rates are 0 from here on, which would read as a stall, so
  // the readout switches to averages and the chart stops at the real end.
  const copyDone = runDone || (planDone && rangesCut > 0 && counts.done === rangesCut);
  $("fleet").innerHTML = rangesCut
    ? `<b>${counts.done}</b> done · <b>${counts.running}</b> running` +
      (counts.pending ? ` · <b>${counts.pending}</b> waiting` : "") +
      (counts.failed ? ` · <span class="bad">${counts.failed} failed</span>` : "")
    : "";

  // Main figure: bytes/s, or files/s committed for a commit-bound copy. The
  // switch overrides the regime; Files/s needs an image that reports commits.
  const metric = heroMetric();
  $("metric-files").disabled = !hasCommit;
  for (const b of document.querySelectorAll(".seg [data-metric]")) b.setAttribute("aria-pressed", String(b.dataset.metric === metric));
  $("regime-chip").classList.toggle("hidden", !(hasCommit && commitHeavy()));
  const unit = byteUnit();
  const avgFile = planTotalFiles ? ` · avg file ${fmtData(planTotalBytes / planTotalFiles)}` : "";
  if (metric === "bytes") {
    $("r-hero").textContent = fmtRate(copyDone ? avgMiB : s3Speed, unit);
    $("r-hero-unit").textContent = copyDone ? `${unit} average` : unit;
    $("r-hero-sub").innerHTML = copyDone
      ? `peak <b>${fmtRate(peakSpeed, unit)}</b> · ${fmtDur(elapsed)}`
      : `peak <b>${fmtRate(peakSpeed, unit)}</b> · avg <b>${fmtRate(avgMiB, unit)}</b> · S3 → bucket`;
  } else {
    $("r-hero").textContent = fmtCount(copyDone ? avgCrate : commitRate);
    $("r-hero-unit").textContent = copyDone ? "files/s average" : "files/s committed";
    $("r-hero-sub").innerHTML = copyDone
      ? `peak <b>${fmtCount(peakCrate)}</b> · ${fmtDur(elapsed)}${avgFile}`
      : `peak <b>${fmtCount(peakCrate)}</b> · avg <b>${fmtCount(avgCrate)}</b>${avgFile}`;
  }

  // Progress track + the first stat: whichever the main figure counts on the
  // track, the other one in the band, so both are always in view.
  const toCommit = planTotalFiles - skippedFiles;
  const filesLanded = hasCommit ? committedFiles + skippedFiles : landedFiles;
  const more = planDone ? "" : "+";
  let pct;
  if (metric === "bytes") {
    pct = planTotalBytes > 0 ? Math.min(100, (100 * landedBytes) / planTotalBytes) : 0;
    $("track-left").innerHTML = planTotalBytes > 0
      ? `<b>${fmtData(landedBytes)}</b> of ${fmtData(planTotalBytes)}${more}${planDone ? "" : " · listing…"}`
      : "listing…";
    setStat(1, "Files", landedFiles.toLocaleString(), planTotalFiles ? `of ${planTotalFiles.toLocaleString()}${more}` : "");
  } else {
    pct = planTotalFiles > 0 ? Math.min(100, (100 * filesLanded) / planTotalFiles) : 0;
    $("track-left").innerHTML = planTotalFiles > 0
      ? `<b>${fmtCount(filesLanded)}</b> of ${fmtCount(planTotalFiles)}${more} files${planDone ? "" : " · listing…"}`
      : "listing…";
    setStat(1, "Data", fmtData(landedBytes), planTotalBytes ? `of ${fmtData(planTotalBytes)}${more}` : "");
  }
  if (runDone && $("live").classList.contains("fail") === false) pct = 100;
  $("bar").style.width = `${pct.toFixed(1)}%`;
  $("track-right").innerHTML = `<b>${pct >= 10 || pct === 0 ? Math.floor(pct) : pct.toFixed(1)}</b> %`;

  // ETA from the aggregate averages once the plan total is known: max of the
  // byte-based and commit-based (files landed) extrapolations. Tiny-file runs
  // are commit-bound — bytes-only ETA read 26m on a run whose true bound was
  // the ~1k files/s CAS ceiling (~7m).
  const bytesPerSec = elapsed > 0 ? bytes / elapsed : 0;
  const byteEtaS = planDone && planTotalBytes > landedBytes && bytesPerSec > 0
    ? (planTotalBytes - landedBytes) / bytesPerSec : 0;
  const commitPerSec = elapsed > 0 ? committedFiles / elapsed : 0;
  const fileEtaS = planDone && hasCommit && toCommit > committedFiles && commitPerSec > 0
    ? (toCommit - committedFiles) / commitPerSec : 0;
  const etaS = Math.max(byteEtaS, fileEtaS);
  if (!runDone && copyDone) {
    setStat(2, "ETA", "–", "verifying the destination");
    setStat(3, "Elapsed", fmtDur(elapsed), runStartEpoch ? `since ${fmtClock(runStartEpoch)}` : "");
  } else if (!runDone) {
    setStat(2, "ETA", etaS > 0 ? fmtDur(etaS) : "–", etaS > 0 ? `at ${fmtClock(Date.now() + etaS * 1000)}` : "");
    setStat(3, "Elapsed", fmtDur(elapsed), runStartEpoch ? `since ${fmtClock(runStartEpoch)}` : "");
  } else {
    setStat(2, "Time", fmtDur(elapsed), `ended ${fmtClock(runStartEpoch + elapsed * 1000)}`);
    const billedMin = runCost().minutes;
    if (billingState === "ok" && billedMin > 0) {
      setStat(3, "Compute", billedMin >= 90 ? `${(billedMin / 60).toFixed(1)} h` : `${billedMin} min`, "Job time billed");
    } else {
      setStat(3, "Copiers", Object.keys(ranges).length.toLocaleString(), counts.failed ? `${counts.failed} failed` : "all done");
    }
  }
  renderCost();

  // Details: commit pipeline — only once a copier has reported `committed`.
  if (hasCommit) {
    const denom = toCommit || filesDone || 1;
    const cPct = Math.min(100, (100 * committedFiles) / denom);
    const bPct = Math.min(100 - cPct, (100 * backlog) / denom);
    // "Behind": the backlog isn't draining — no commit has landed for a minute
    // while files are still flowing in. This is the wedge signature, ~300s
    // before an error. A 5 s commit rate of 0 is not enough: with big files a
    // copier commits once per --commit-gib, so the fleet's rate drops to 0
    // between commits, and every copier starts with a backlog.
    if (committedFiles > lastCommitted) { lastCommitted = committedFiles; lastCommitMs = performance.now(); }
    const sinceCommitS = (performance.now() - Math.max(lastCommitMs, runStartMs)) / 1000;
    const behind = !runDone && sinceCommitS > COMMIT_STALL_S && backlog > 0 && (s3Speed > 0 || hfSpeed > 0);
    show("commits");
    $("pl-committed").style.width = `${cPct.toFixed(1)}%`;
    $("pl-backlog").style.left = `${cPct.toFixed(1)}%`;
    $("pl-backlog").style.width = `${bPct.toFixed(1)}%`;
    $("pipeline").classList.toggle("behind", behind);
    $("pipeline-label").classList.toggle("behind", behind);
    $("pipeline-label").innerHTML =
      `<b>${fmtCount(committedFiles)}</b> committed · <span class="pl-backlog-v">${fmtCount(backlog)}</span> uploaded, waiting` +
      (shardsPending > 0 ? ` · ${fmtCount(shardsPending)} shard${shardsPending > 1 ? "s" : ""} in ack` : "") +
      (runDone ? "" : ` · <b>${fmtCount(commitRate)}</b> files/s`) +
      (behind ? " — finalize stalled" : "");
  }
  $("ranges-h").textContent = rangesCut ? `${rangesCut.toLocaleString()} key ranges` : "Key ranges";
  $("ranges-sub").textContent = rangesCut && planDone ? `≈ ${fmtData(planTotalBytes / rangesCut)} each` : rangesCut ? "listing…" : "";

  // Append at ~1s resolution; t is monotonic so the line only advances left→right.
  if (!copyDone) {
    const last = series[series.length - 1];
    if (!last || elapsed - last.t >= 1) series.push({ t: elapsed, s3: s3Speed, hf: hfSpeed, cr: commitRate });
    else { last.s3 = s3Speed; last.hf = hfSpeed; last.cr = commitRate; }
  }
  drawChart();
}

// [TRANSFER COMPLETE] — flip the live view into its terminal state: banner with
// the final tallies above the card, averages in the readout, final costs.
// Paths from the planner's log go into innerHTML.
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);

// The destination check's failure, in full (PLAN_RESULT.verified is null when
// it did not run, true when it passed).
function verifyFailure(r, v) {
  if (r.verified !== false) return "";
  if (!v || v.error) return `<p>✗ The destination check could not run${v ? ` — ${esc(v.error)}` : ""}.</p>`;
  return `<p>✗ <b class="errtxt">${fmtCount(v.missing)} missing · ${fmtCount(v.short)} short</b>` +
    ` in range ${v.failed_ranges.join(", ")} (${fmtCount(v.dest_files)} files listed / ${fmtCount(v.planned_files)} planned)</p>` +
    `<div class="paths">${v.sample.map(esc).join("<br>")}</div>`;
}

function bucketUrl(dst) {
  try { const [ns, name] = splitBucketDestination(dst); return `${HF}/buckets/${ns}/${name}`; } catch { return null; }
}

function finishRun(r) {
  runDone = true; runEndMs = performance.now();
  const ok = !r.failed && r.verified !== false;
  $("live").classList.add("done");
  $("live").classList.toggle("fail", !ok);
  $("live-h2").textContent = ok ? "Transfer complete" : "Transfer complete — failures";
  document.querySelector(".kicker .dot")?.classList.add(ok ? "ok" : "err");
  const cs = Object.values(copierState);
  const bytes = cs.reduce((a, s) => a + (s.bytes || 0), 0);
  const files = cs.reduce((a, s) => a + (s.files || 0), 0);
  const skipped = cs.reduce((a, s) => a + (s.skipped || 0), 0);
  const skippedBytes = cs.reduce((a, s) => a + (s.skippedBytes || 0), 0);
  const already = skipped ? ` · <b>${skipped.toLocaleString()}</b> already there (${fmtSize(skippedBytes)})` : "";
  const elapsed = runElapsedS();
  const title = r.failed
    ? `<span class="tick">✗</span> ${r.failed} of ${r.ranges} ranges failed`
    : r.verified === false ? `<span class="tick">✗</span> Copied, but the destination check failed`
    : r.verified ? `<span class="tick">✓</span> Copied and verified`
    : `<span class="tick">✓</span> Copied`;
  const checked = r.verified ? ", checked against the source" : r.verified == null && !r.failed ? " · destination not verified" : "";
  const url = bucketUrl(runDst);
  const banner = $("done-banner");
  banner.classList.remove("hidden");
  banner.classList.toggle("fail", !ok);
  banner.innerHTML = `<div class="t">${title}</div>` +
    `<p><b>${files.toLocaleString()}</b> files · <b>${fmtSize(bytes)}</b> in <b>${fmtDur(elapsed)}</b>${already}${checked}` +
    `${url ? ` · <a href="${url}" target="_blank" rel="noopener">Open the bucket</a>` : ""}</p>` +
    verifyFailure(r, verifyResult);
  finishBilling();
  updateLive(); scheduleRender();
}

// Follow one copier's own log for LISTING/PROGRESS/DONE → aggregate graph + range map.
function followCopier(jobId) {
  followJob(jobId, (line) => {
    if (copierState[jobId] && copierState[jobId].stage === "scheduling") {
      copierState[jobId].stage = "running"; scheduleRender();
    }
    if (line.startsWith("PROGRESS ")) {
      try {
        const p = JSON.parse(line.slice(9));
        const prev = copierState[jobId] || {};
        // Committing files counts as progress: tiny-file runs read well below
        // 1 MiB/s, so byte rates alone would flag a live copier as stalled.
        // A finalize within its budget also counts — it moves no tracked
        // bytes by design (the state shows its own elapsed/budget clock).
        const finalizing = p.finalizing_s != null && p.finalizing_s <= (p.finalize_budget_s || 300);
        const flat = (p.mibps_5s || 0) === 0 && (p.hf_mibps_5s || 0) === 0
          && (p.committed_fps_5s || 0) === 0 && !finalizing;
        if (p.hf_mibps_5s != null) hasHf = true;
        if (p.committed != null) hasCommit = true;
        copierState[jobId] = {
          ...prev,
          files: p.files, bytes: p.bytes_done,
          speed: p.mibps_5s, hf: p.hf_mibps_5s || 0,
          committed: p.committed, crate: p.committed_fps_5s || 0,
          avg: p.mibps_avg, elapsed: p.elapsed_s,
          phase: p.phase, inflightN: p.inflight,
          // Metadata queue + phase-internal progress (newer image).
          shards: p.shards_pending, fack: p.files_pending_ack,
          finS: p.finalizing_s, finB: p.finalize_budget_s,
          tailR: p.tail_read, tailSz: p.tail_size,
          retries: (p.s3_part_retries || 0) + (p.file_retries || 0),
          zeroTicks: flat ? (prev.zeroTicks || 0) + 1 : 0,
        };
        updateLive(); scheduleRender();
      } catch {}
    } else if (line.startsWith("LISTING ")) {
      try {
        const l = JSON.parse(line.slice(8));
        copierState[jobId] = { ...copierState[jobId], skipped: l.skipped_existing || 0, skippedBytes: l.skipped_existing_bytes || 0 };
        updateLive(); scheduleRender();
      } catch {}
    } else if (line.startsWith("DONE ")) {
      try {
        const d = JSON.parse(line.slice(5));
        copierState[jobId] = {
          // The last PROGRESS line is up to 5 s old: files finished after it
          // count only from here (#23).
          ...copierState[jobId], files: d.files != null ? d.files : copierState[jobId]?.files,
          bytes: d.bytes, speed: 0, hf: 0, crate: 0, zeroTicks: 0,
          skipped: d.skipped_existing || 0, skippedBytes: d.skipped_existing_bytes || 0,
          shards: 0, fack: 0, finS: null, tailR: null, tailSz: null,
          committed: d.committed != null ? d.committed : copierState[jobId]?.committed,
        };
        updateLive(); scheduleRender();
      } catch {}
    }
  }).then((st) => {
    if (copierState[jobId]) {
      copierState[jobId].stage = st || "completed";
      copierState[jobId].speed = 0; copierState[jobId].hf = 0; copierState[jobId].crate = 0; copierState[jobId].zeroTicks = 0;
      // A dead job holds no pending metadata — don't let stale gauges linger
      // in the fleet sums (an ERROR'd copier never reports them back to 0).
      copierState[jobId].shards = 0; copierState[jobId].fack = 0;
      copierState[jobId].finS = null; copierState[jobId].tailR = null;
    }
    updateLive(); scheduleRender();
  });
}

// Clear the previous run's state and terminal look.
function resetLive() {
  series.length = 0;
  for (const k in copierState) delete copierState[k];
  for (const k in ranges) delete ranges[k];
  planTotalFiles = 0; planTotalBytes = 0; rangesCut = 0; planDone = false; pausedNote = "";
  rangesSeen.clear();
  hasHf = false; hasCommit = false; hoverIdx = null; peakSpeed = 0; peakCrate = 0;
  lastCommitted = 0; lastCommitMs = 0;
  runDone = false; verifyResult = null; runEndMs = 0; plannerId = null; plannerStage = null;
  metricChoice = null; stopBilling(); billingState = "off"; billingFinal = false;
  $("live").classList.remove("done", "fail");
  $("live-h2").textContent = "Transfer";
  $("done-banner").classList.add("hidden"); $("done-banner").innerHTML = "";
  document.querySelector(".kicker .dot")?.classList.remove("ok", "err");
  $("jobs").innerHTML = ""; $("jobs-more").innerHTML = ""; $("rangemap").innerHTML = "";
  $("chart-tip").classList.add("hidden"); hide("commits");
}

$("run").onclick = async () => {
  const src = $("src").value.trim(), dst = $("dst").value.trim();
  const flavor = $("flavor").value;
  const rangeGib = Math.max(1, parseInt($("rangegib").value, 10) || 25);
  const inflight = Math.min(256, Math.max(1, parseInt($("inflight").value, 10) || 16));
  const timeoutSeconds = toSeconds($("timeout").value);
  if (!tokenOutlives(timeoutSeconds, "the copy", SHORTER_TIMEOUT)) return;
  if (!(await accountCanRunJobs())) return;
  $("form-msg").textContent = ""; $("form-msg").className = "msg";
  const secrets = collectSecrets();

  $("run").disabled = true; $("analyze").disabled = true;
  show("live");
  resetLive();
  billingState = "pending"; // until the first read answers
  runFlavor = flavor; runDst = dst;
  // The estimate of the plan as launched (Advanced may differ from the reco).
  runEstimate = lastAnalysis && lastAnalysis.src === src
    ? estimate(lastAnalysis.gib, { flavor, rangeGib, inflight }, lastAnalysis.files) : null;
  runStartMs = performance.now(); runStartEpoch = Date.now();
  setKicker("Copying…");
  updateLive();

  const label = `s3ream-${Date.now().toString(36)}`;
  const extra = [
    "--plan",
    "--range-gib", String(rangeGib),
    "--max-inflight", String(inflight),
    "--launch-stagger-ms", "1500",
    "--copier-image", IMAGE,
    "--copier-flavor", flavor,
    "--jobs-namespace", userNs,
    "--run-label", label,
  ];
  if ($("skip-existing").checked) extra.push("--skip-existing");

  try {
    // The planner runs on cheap cpu-basic — it only lists + orchestrates.
    plannerId = await runJob({ src, dst, extra, flavor: PLANNER_FLAVOR, timeoutSeconds, secrets });
    plannerStage = "running";
    startBilling();
    setPlan(`Planner <a href="${HF}/jobs/${userNs}/${plannerId}" target="_blank" rel="noopener">${plannerId}</a> starting…`);

    plannerStage = await followJob(plannerId, (line) => {
      // After PLAN_RESULT, a replayed log could only rewind the view.
      if (runDone) return;
      if (line.startsWith("PLANNING ")) {
        try { const p = JSON.parse(line.slice(9)); pausedNote = ""; setPlan(`Planner listing… <b>${(p.listed || 0).toLocaleString()}</b> objects · <b>${p.ranges || 0}</b> ranges · ${fmtSize(p.bytes || 0)}`); } catch {}
      } else if (line.startsWith("RANGE ")) {
        try {
          const r = JSON.parse(line.slice(6));
          if (rangesSeen.has(r.idx)) return;
          rangesSeen.add(r.idx);
          rangesCut++; planTotalFiles += r.files || 0; planTotalBytes += r.bytes || 0;
          ranges[r.idx] = { ...ranges[r.idx], idx: r.idx, files: r.files || 0, bytes: r.bytes || 0 };
          if (!planDone) updateLive();
          scheduleRender();
        } catch {}
      } else if (line.startsWith("COPIER ")) {
        try {
          const c = JSON.parse(line.slice(7));
          if (c.job_id && !copierState[c.job_id]) {
            // A respawn re-points the range at its new copier attempt.
            ranges[c.idx] = { ...ranges[c.idx], idx: c.idx, jobId: c.job_id, attempts: c.attempt || 1 };
            copierState[c.job_id] = { idx: c.idx, stage: "scheduling" };
            scheduleRender(); followCopier(c.job_id);
          }
        } catch {}
      } else if (line.includes("back-pressure")) {
        pausedNote = "⏸ max in-flight reached — pausing listing"; renderPlan();
      } else if (line.startsWith("PLAN_DONE ")) {
        try { const d = JSON.parse(line.slice(10)); planDone = true; planTotalFiles = d.files || planTotalFiles; planTotalBytes = d.bytes || planTotalBytes; pausedNote = ""; setPlan(`Plan complete: <b>${d.ranges}</b> ranges · <b>${(d.files || 0).toLocaleString()}</b> files · ${fmtSize(d.bytes || 0)} — copying…`); updateLive(); } catch {}
      } else if (line.startsWith("VERIFYING ")) {
        try { const v = JSON.parse(line.slice(10)); pausedNote = ""; setKicker("Verifying…"); setPlan(`Copy done — verifying the destination… <b>${fmtCount(v.dest_files)}</b> / ${fmtCount(v.planned_files)} files listed`); } catch {}
      } else if (line.startsWith("VERIFY ")) {
        try { verifyResult = JSON.parse(line.slice(7)); } catch {}
      } else if (line.startsWith("PLAN_RESULT ")) {
        try {
          const r = JSON.parse(line.slice(12));
          const bad = r.failed ? ` · <b class="errtxt">${r.failed} failed</b>` : "";
          const ret = r.retried ? ` · ${r.retried} retried` : "";
          setPlan(`Done: <b>${r.completed}</b>/${r.ranges} ranges completed${bad}${ret}.`);
          setKicker(r.failed ? "Done — with failures" : r.verified === false ? "Done — destination check failed" : "Done");
          finishRun(r);
        } catch {}
      }
    });
    // A planner that ends without PLAN_RESULT (timeout, cancel, crash) still
    // stops the cost polling after its last minute is billed.
    if (!runDone) finishBilling();
    scheduleRender();
  } catch (e) {
    stopBilling();
    $("plan-status").insertAdjacentHTML("beforeend", `<div class="msg err">planner launch failed: ${e.message}</div>`);
  }
  $("analyze").disabled = false;
};

// ---------- demo mode ----------
// ?demo — a big-file copy; &files — a commit-bound one (many small files);
// &done — the finished view; &stall — the whole fleet stops committing (the
// red alarm after a minute);
// &analyze — the Analyze result for a sized source.
// Synthetic data through the real code paths: chart, band, range map, table, costs.
let demo = false; // demo mode: costs are synthetic, nothing is read from the Hub
function startDemo() {
  demo = true;
  // The form too, so its controls can be exercised without signing in.
  hide("signin"); show("form"); show("live");
  userNs = "demo";
  const q = new URLSearchParams(location.search);
  const small = q.has("files"), stallDemo = q.has("stall"), doneDemo = q.has("done");
  if (q.has("analyze")) {
    hide("live"); show("analysis"); canPay = true;
    const total = small ? 1.4 * 2 ** 40 : 107.3 * 2 ** 40, count = small ? 2_100_000 : 500_009;
    $("checks").innerHTML =
      checkLine("ok", "bucket <b>demo/s3ream</b> created, <b>private</b> — write OK") +
      checkLine("ok", "billing — Jobs run on <b>demo</b>'s account, billed per started minute") +
      checkLine("ok", `S3 read OK — listed <b>${count.toLocaleString()}</b> objects`);
    showPlan({ count, total_bytes: total, median: total / count, pct_le_16mib: small ? 100 : 0 }, "s3://demo/", true);
    $("run").disabled = false;
    return;
  }
  setKicker("Copying…");
  const N = small ? 2 : 24;
  const RANGE_BYTES = small ? 700 * 2 ** 30 : 430 * 2 ** 30;
  const RANGE_FILES = small ? 1_050_000 : 1950;
  const RATE = small ? 0.03 / 60 : 1.9 / 60; // $ per billed minute
  for (let i = 0; i < N; i++) {
    ranges[i] = { idx: i, files: RANGE_FILES + i * 7, bytes: RANGE_BYTES, jobId: `demo${String(i).padStart(4, "0")}ab`, attempts: i === 7 ? 2 : 1 };
    planTotalFiles += ranges[i].files; planTotalBytes += ranges[i].bytes;
  }
  rangesCut = N; planDone = true; hasHf = true; hasCommit = true;
  plannerId = "demoplanner01"; plannerStage = "running"; runFlavor = small ? "cpu-upgrade" : "cpu-performance";
  runDst = "demo/s3ream";
  runEstimate = { usd: small ? "$0.08" : "$22", usdNum: small ? 0.08 : 22 };
  billingState = "ok";
  $("live-details").open = true; // demo showcases the full instrument
  setPlan(`Plan complete: <b>${N}</b> ranges · <b>${planTotalFiles.toLocaleString()}</b> files · ${fmtSize(planTotalBytes)} — copying…`);
  // Back-date history so the chart opens with a real line.
  const HIST = small ? 2160 : 900;
  runStartMs = performance.now() - HIST * 1000; runStartEpoch = Date.now() - HIST * 1000;
  for (let t = 0; t < HIST; t += 3) {
    const ramp = Math.min(1, t / 60);
    const s3 = small ? ramp * (420 + 90 * Math.sin(t / 50)) : ramp * (16000 + 4500 * Math.sin(t / 80) + 1500 * Math.sin(t / 13));
    const cr = small ? ramp * (600 + 120 * Math.sin(t / 45) + 40 * Math.sin(t / 7)) : ramp * (40 + 12 * Math.sin(t / 60));
    series.push({ t, s3, hf: s3 * 0.94, cr });
    peakSpeed = Math.max(peakSpeed, s3); peakCrate = Math.max(peakCrate, cr);
  }
  const stage = (i) => (small ? "running" : i < 6 ? "COMPLETED" : i < 18 ? "running" : "scheduling");
  const tick = () => {
    const now = (performance.now() - runStartMs) / 1000;
    for (let i = 0; i < N; i++) {
      const st = stage(i);
      const id = ranges[i].jobId;
      const cs = copierState[id] || (copierState[id] = { idx: i, stage: st });
      cs.stage = st;
      if (st === "COMPLETED") {
        cs.bytes = RANGE_BYTES; cs.files = ranges[i].files; cs.committed = ranges[i].files;
        cs.speed = 0; cs.hf = 0; cs.crate = 0; cs.elapsed = 330 + 9 * i;
      }
      if (st === "running") {
        const stalled = !small && i === 9;
        cs.elapsed = small ? now - 30 : 60 + 25 * (i % 7);
        cs.speed = stalled ? 0 : small
          ? Math.max(0, 210 + 40 * Math.sin(performance.now() / 9000 + i))
          : Math.max(0, 1300 + 160 * Math.sin(performance.now() / 9000 + i) + 40 * Math.random());
        cs.hf = cs.speed * 0.94;
        const startFrac = small ? 0.62 + 0.04 * i : 0.08 + 0.8 * ((i * 5) % 12) / 12;
        cs.bytes = Math.min(RANGE_BYTES, (cs.bytes || startFrac * RANGE_BYTES) + cs.speed * 2 ** 20);
        cs.files = Math.min(ranges[i].files, Math.round(ranges[i].files * (cs.bytes / RANGE_BYTES)));
        // Committed trails uploaded — commits pipeline behind uploads. The
        // `stall` variant: the fleet stops committing while uploads keep
        // flowing → aggregate rate 0, backlog grows → the red alarm.
        cs.committed = (stalled || stallDemo) ? (cs.committed || Math.round(cs.files * 0.5)) : Math.round(cs.files * 0.995);
        cs.crate = (stalled || stallDemo) ? 0 : small ? Math.round(300 + 50 * Math.sin(performance.now() / 7000 + i)) : 2;
        cs.phase = "uploading";
        cs.retries = i === 8 ? 3 : 0;
        cs.zeroTicks = stalled ? (cs.zeroTicks || 0) + 1 : 0;
        cs.finS = null; cs.inflightN = 24; cs.shards = stalled ? 0 : i % 2;
      }
      if (st !== "scheduling") jobCost[id] = { minutes: Math.ceil((cs.elapsed || 0) / 60) + 1, usd: (Math.ceil((cs.elapsed || 0) / 60) + 1) * RATE, flavor: runFlavor };
    }
    jobCost[plannerId] = { minutes: Math.ceil(now / 60), usd: Math.ceil(now / 60) * (0.01 / 60), flavor: PLANNER_FLAVOR };
    updateLive(); scheduleRender();
  };
  for (let k = 0; k < 5; k++) tick(); // warm up zeroTicks → the stalled state shows
  if (doneDemo) {
    for (let i = 0; i < N; i++) {
      const cs = copierState[ranges[i].jobId];
      Object.assign(cs, { stage: "COMPLETED", bytes: RANGE_BYTES, files: ranges[i].files, committed: ranges[i].files,
        speed: 0, hf: 0, crate: 0, zeroTicks: 0, shards: 0, elapsed: cs.elapsed || 340 + 11 * i });
      jobCost[ranges[i].jobId] = { minutes: Math.ceil(cs.elapsed / 60) + 1, usd: (Math.ceil(cs.elapsed / 60) + 1) * RATE, flavor: runFlavor };
    }
    plannerStage = "completed"; billingFinal = true;
    setKicker("Done");
    setPlan(`Done: <b>${N}</b>/${N} ranges completed · 1 retried.`);
    verifyResult = null;
    finishRun({ completed: N, ranges: N, failed: 0, retried: 1, verified: true });
    stopBilling(); billingFinal = true; updateLive(); scheduleRender();
    return;
  }
  setInterval(tick, 1000);
}

init();
