// hf-s3ream Space — fully client-side. OAuth (Sign in with HF) → call the HF
// Jobs/Buckets API directly (CORS-enabled). AWS keys stay in the browser and go
// only into the Job's encrypted `secrets`.
//
// Run model: launch ONE *planner* job (`--plan`). The planner lists the source
// once, cuts the keyspace into ranges, and spawns a copier job per range — ALL
// orchestration happens in the planner. The Space only OBSERVES: it follows the
// planner's log for RANGE/COPIER/PLAN_DONE/PLAN_RESULT, then follows each
// discovered copier's log for PROGRESS/DONE to draw the aggregate graph.
import {
  oauthLoginUrl, oauthHandleRedirectIfPresent,
  runJob as hubRunJob, streamJobLogs, getJob,
} from "https://esm.sh/@huggingface/hub@2";

const HF = "https://huggingface.co";
// WIP image (has the planner + DRYRUN_STATS/PROGRESS/DONE markers). Flip to vX.Y.Z at release.
const IMAGE = "ghcr.io/glutamatt/hf-s3ream:wip";
const RUST_LOG = "hf_s3ream=info,xet_data=warn,xet_client=warn";
const PART_BYTES = 16 * 1024 * 1024;
// The dry-run only lists metadata: a normal bucket finishes in seconds, and 2
// minutes is enough to conclude OR gather enough of the listing to call it "a
// long run". Billed per-second, killed AT this cap.
const DRY_RUN_TIMEOUT_S = 120;
// The planner staggers its own copier launches; the Space no longer launches
// shards, so no wave logic here.

let token = null;
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
    const u = res.userInfo || {};
    userNs = u.preferred_username || u.name || u.sub;
    $("who-name").textContent = userNs;
    if (u.picture) $("who-pic").src = u.picture;
    show("who"); show("form");
    $("dst").value = userNs ? `${userNs}/s3ream` : "";
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

// ---------- HF API helpers ----------
async function hf(path, opts = {}) {
  return fetch(`${HF}${path}`, {
    ...opts,
    headers: { Authorization: `Bearer ${token}`, ...(opts.headers || {}) },
  });
}

function splitRepo(dst) {
  const parts = dst.trim().split("/");
  if (parts.length === 1) return [userNs, parts[0]];
  return [parts[0], parts.slice(1).join("/")];
}

async function ensureBucket(dst) {
  const [ns, name] = splitRepo(dst);
  const r = await hf(`/api/buckets/${ns}/${name}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ private: false }),
  });
  if (r.ok) return { ok: true, created: true };
  if (r.status === 409) return { ok: true, created: false };
  const body = await r.text();
  return { ok: false, error: `HTTP ${r.status}: ${body.slice(0, 200)}` };
}

function toSeconds(t) {
  const m = String(t).trim().match(/^([\d.]+)\s*([smhd])?$/);
  if (!m) return 12 * 3600;
  return Math.round(parseFloat(m[1]) * { s: 1, m: 60, h: 3600, d: 86400 }[m[2] || "s"]);
}

// Launch one Job via @huggingface/hub. `extra` = extra hf-s3ream args. Returns job id.
async function runJob({ src, dst, extra = [], flavor, timeoutSeconds, secrets, dryRun = false }) {
  const command = ["hf-s3ream", src, dst, ...extra];
  if (dryRun) command.push("--dry-run");
  // Only pin AWS_REGION when the user typed one; otherwise the Job auto-detects
  // the bucket's region (GetBucketLocation). The planner forwards the resolved
  // region to its copiers via --aws-region.
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

function collectSecrets() {
  const s = { HF_TOKEN: token, AWS_ACCESS_KEY_ID: $("ak").value.trim(), AWS_SECRET_ACCESS_KEY: $("sk").value.trim() };
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
function fmtSize(bytes) { const gib = bytes / 2 ** 30; return gib >= 1 ? `${gib.toFixed(1)} GiB` : `${(bytes / 2 ** 20).toFixed(0)} MiB`; }

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
function recommend(gib) {
  if (gib < PERF_MIN_GIB)
    return { rangeGib: Math.max(1, Math.ceil(gib)), inflight: 1, flavor: "cpu-upgrade", timeout: "2h" };
  // ~64 GiB ranges keep each copier busy ≥ ~50s of pure transfer (amortizes the
  // startup tax) while fanning out wide; cap at 256 copiers / 128 in-flight.
  const targetCopiers = Math.max(1, Math.min(256, Math.ceil(gib / 64)));
  const rangeGib = Math.max(8, Math.ceil(gib / targetCopiers));
  const inflight = Math.min(128, targetCopiers);
  // Planner lives until every copier finishes, so its timeout must outlast the
  // whole copy. Generous (billed/sec on cheap cpu-basic; it exits early on done).
  const aggMiBps = inflight * FLAVOR_MIBPS["cpu-performance"];
  const estSec = 900 + (gib * 1024 / aggMiBps) * 3;
  const hours = Math.min(48, Math.max(2, Math.ceil(estSec / 3600)));
  return { rangeGib, inflight, flavor: "cpu-performance", timeout: `${hours}h` };
}
// Expected wall-clock + compute cost for a reco — shown so the flavor choice is
// legible ("fast AND cheaper"), not a black box.
function estimate(gib, r) {
  const speed = FLAVOR_MIBPS[r.flavor];
  const copiers = Math.max(1, Math.ceil(gib / r.rangeGib));
  const waves = Math.ceil(copiers / r.inflight);
  const wallS = waves * OVERHEAD_S + (gib * 1024) / (Math.min(r.inflight, copiers) * speed);
  const usd = (FLAVOR_USD_HR[r.flavor] / 3600) * (copiers * OVERHEAD_S + (gib * 1024) / speed);
  const wall = wallS < 90 ? `${Math.ceil(wallS)}s` : wallS < 5400 ? `${Math.ceil(wallS / 60)} min` : `${(wallS / 3600).toFixed(1)}h`;
  return { copiers, wall, usd: usd < 1 ? `$${usd.toFixed(2)}` : `$${usd.toFixed(usd < 20 ? 1 : 0)}` };
}
function applyReco(r) {
  $("flavor").value = r.flavor;
  $("rangegib").value = r.rangeGib;
  $("inflight").value = r.inflight;
  $("timeout").value = r.timeout;
}

function checkLine(state, text) {
  const cls = state === "ok" ? "ok" : state === "err" ? "err" : "run";
  const sym = state === "ok" ? "✓" : state === "err" ? "✗" : "…";
  return `<div class="check"><span class="dot ${cls}">${sym}</span><span>${text}</span></div>`;
}

// Dry-run timed out mid-listing (bucket too big to enumerate in the window):
// recommend from the partial listing (we can't size it, so use the ~25 GiB default).
function bigBucketAdvisory(l) {
  const listed = l.listed || 0, kept = l.kept || listed, bytes = l.bytes || 0, le16 = l.le16 || 0;
  const smallPct = kept > 0 ? Math.round((le16 * 100) / kept) : 0;
  const r = { rangeGib: 64, inflight: 128, flavor: "cpu-performance", timeout: "24h" };
  $("stats").innerHTML = [
    ["files", `${kept.toLocaleString()}+`],
    ["total", bytes ? `${fmtSize(bytes)}+` : "—"],
    ["≤16 MiB", kept ? `${smallPct}%` : "—"],
  ].map(([k, v]) => `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");
  $("stats").classList.remove("hidden");
  $("reco").innerHTML =
    `<b>Very large bucket.</b> Scanned <b>${listed.toLocaleString()}+</b> objects` +
    `${bytes ? ` (<b>${fmtSize(bytes)}+</b>)` : ""}; the dry-run timed out before finishing the listing — the planner will do the full listing itself.<br>` +
    `The planner lists once and fans out copiers of ~<b>${r.rangeGib} GiB</b> each (up to <b>${r.inflight}</b> in-flight), on <b>${r.flavor}</b> — measured ~1.3 GiB/s per copier, and cheaper per TiB than the small flavors (which starve on shared-host NICs).<br>` +
    `<span class="hint">Adjust under &ldquo;Advanced&rdquo; and Run. Give a generous planner timeout — it stays alive until every copier finishes.</span>`;
  $("reco").classList.remove("hidden");
  applyReco(r);
  $("run").disabled = false;
}

$("analyze").onclick = async () => {
  const src = $("src").value.trim();
  const dst = $("dst").value.trim();
  $("form-msg").textContent = "";
  if (!/^s3:\/\//.test(src)) return (($("form-msg").textContent = "source must be s3://bucket/prefix/"), ($("form-msg").className = "msg err"));
  if (!$("ak").value.trim() || !$("sk").value.trim()) return (($("form-msg").textContent = "AWS access key + secret are required"), ($("form-msg").className = "msg err"));

  show("analysis"); hide("live"); $("stats").classList.add("hidden"); $("reco").classList.add("hidden");
  stopDryCountdown();
  const checks = $("checks");
  const lines = { bucket: checkLine("run", "creating / checking destination bucket…"), job: checkLine("run", "launching dry-run (S3 read + region + size)…") };
  const render = () => (checks.innerHTML = lines.bucket + lines.job);
  render();
  $("analyze").disabled = true; $("run").disabled = true;

  // 1. bucket
  const b = await ensureBucket(dst);
  lines.bucket = b.ok ? checkLine("ok", `bucket <b>${dst}</b> ${b.created ? "created" : "exists"} — write OK`) : checkLine("err", `bucket: ${b.error}`);
  render();
  if (!b.ok) { $("analyze").disabled = false; return; }

  // 2. dry-run job → DRYRUN_STATS / DRYRUN_BUCKET / LISTING progress
  let stats = null, bucketOk = null, lastListing = null;
  try {
    const id = await runJob({ src, dst, flavor: "cpu-basic", timeoutSeconds: DRY_RUN_TIMEOUT_S, secrets: collectSecrets(), dryRun: true });
    lines.job = checkLine("run", `dry-run job <code>${id}</code> running…`); render();
    startDryCountdown(DRY_RUN_TIMEOUT_S);
    const signal = { aborted: false };
    const follow = followJob(id, (line) => {
      if (line.startsWith("DRYRUN_STATS ")) { try { stats = JSON.parse(line.slice(13)); } catch {} }
      else if (line.startsWith("DRYRUN_BUCKET ")) bucketOk = line.slice(14).trim() === "ok";
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
      lines.job = checkLine("err", "dry-run returned no stats — S3 access failed (check keys/region; VPC-locked buckets are unreachable from HF Jobs).");
      render();
    }
    $("analyze").disabled = false; return;
  }
  lines.job = checkLine("ok", `S3 read OK — listed <b>${stats.count.toLocaleString()}</b> objects`);
  render();

  // stats + recommendation
  const gib = stats.total_bytes / 2 ** 30;
  $("stats").innerHTML = [
    ["files", stats.count.toLocaleString()],
    ["total", gib >= 1 ? `${gib.toFixed(1)} GiB` : `${(stats.total_bytes / 2 ** 20).toFixed(0)} MiB`],
    ["median", stats.median >= 2 ** 20 ? `${(stats.median / 2 ** 20).toFixed(1)} MiB` : `${(stats.median / 1024).toFixed(0)} KiB`],
    ["≤16 MiB", `${stats.pct_le_16mib}%`],
  ].map(([k, v]) => `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");
  $("stats").classList.remove("hidden");

  const r = recommend(gib);
  applyReco(r);
  if (stats.region && !$("region").value.trim()) $("region").value = stats.region;
  const est = estimate(gib, r);
  const why = r.flavor === "cpu-performance"
    ? ` (~1.3 GiB/s per copier — fastest <i>and</i> cheapest per TiB)`
    : ` (copy this small is startup-dominated — a pricier flavor wouldn't finish sooner)`;
  const bmsg = bucketOk === false ? ` <span class="dot err">✗ bucket write-token failed inside the job</span>` : "";
  $("reco").innerHTML =
    `Recommended: one planner fans out ~<b>${est.copiers}</b> copier${est.copiers > 1 ? "s" : ""} of <b>~${r.rangeGib} GiB</b> each, up to <b>${r.inflight}</b> in-flight, on <b>${r.flavor}</b>${why}. Est. <b>~${est.wall}</b> wall-clock · <b>~${est.usd}</b> compute. Planner timeout ${r.timeout}.${bmsg} <span class="hint">Adjust under “Advanced”, then Run.</span>`;
  $("reco").classList.remove("hidden");
  $("analyze").disabled = false; $("run").disabled = false;
};

// ---------- Run: one planner → observe ----------
const series = [];              // aggregate samples {t, s3, hf}
const copierState = {};         // copier job_id -> live state (see followCopier)
const ranges = {};              // range idx -> {idx, files, bytes, jobId, attempts}
let hasHf = false;              // any copier emitted hf_mibps_5s (new image)
let hasCommit = false;          // any copier emitted `committed` (newer image)
let planTotalFiles = 0, planTotalBytes = 0, rangesCut = 0, planDone = false;
let planText = "", pausedNote = "";
let runStartMs = 0;   // wall-clock origin for the chart x-axis (set at Run)
let peakSpeed = 0;    // max aggregate S3 rate seen this run (the "peak →" readout)

// Status kicker in the hero ("Ready" → "Copying…" → "Done").
function setKicker(t) { const el = $("kicker"); if (el) el.textContent = t; }

function renderPlan() { $("plan-status").innerHTML = planText + (pausedNote ? ` <span class="paused">${pausedNote}</span>` : ""); }
function setPlan(html) { planText = html; renderPlan(); }

function fmtSpeed(v) { return v >= 10 || v === 0 ? Math.round(v).toLocaleString() : v.toFixed(1); }
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
// Round a max up to a clean axis top (1/2/5 × 10^k).
function niceMax(v) {
  if (v <= 10) return 10;
  const p = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 2, 5, 10]) if (m * p >= v) return m * p;
  return 10 * p;
}
function pickTimeStep(tMax) {
  for (const s of [15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400]) if (tMax / s <= 6) return s;
  return 28800;
}
function seriesColors() {
  const css = getComputedStyle(document.documentElement);
  const v = (n, fb) => css.getPropertyValue(n).trim() || fb;
  return {
    s1: v("--s1", "#b28d00"), s2: v("--s2", "#4a80e8"),
    grid: v("--border", "rgba(245,242,234,0.12)"), muted: v("--muted", "#9b948a"),
    // Ring color for end-dots: the panel surface the canvas now sits on.
    bg: v("--surface", "#14110e"),
  };
}

// Chart: 2px lines, 10% area wash under the S3 series, hairline gridlines with
// clean tick values, end-dots with a surface ring, crosshair + tooltip on hover.
let chartGeom = null; // scales captured by drawChart, reused by the hover layer
let hoverIdx = null;  // index into `series` under the pointer (null = no hover)

function drawChart() {
  const c = $("chart");
  const cw = c.clientWidth || 720, ch = c.clientHeight || 160;
  const dpr = window.devicePixelRatio || 1;
  if (c.width !== Math.round(cw * dpr) || c.height !== Math.round(ch * dpr)) {
    c.width = Math.round(cw * dpr); c.height = Math.round(ch * dpr);
  }
  const ctx = c.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cw, ch);
  if (series.length < 2) { chartGeom = null; return; }
  const C = seriesColors();
  // padT clears the hero-number overlay (Grafana stat-panel style): lines and
  // gridlines live in the lower band, the figure floats above them.
  const padL = 46, padR = 14, padT = 120, padB = 24;
  const tMax = Math.max(series[series.length - 1].t, 30);
  const vMax = niceMax(Math.max(10, ...series.map((p) => Math.max(p.s3, p.hf || 0))));
  const x = (t) => padL + (cw - padL - padR) * (t / tMax);
  const y = (v) => ch - padB - (ch - padT - padB) * (v / vMax);
  chartGeom = { x, y, tMax, vMax, cw, ch, padL, padR };

  // Gridlines + y ticks (recessive hairlines, clean numbers).
  ctx.font = '10px "IBM Plex Mono", ui-monospace, Menlo, Consolas, monospace';
  ctx.strokeStyle = C.grid; ctx.lineWidth = 1; ctx.fillStyle = C.muted;
  // Stop at 3/4: the top gridline + label would sit right under the overlay.
  for (let i = 0; i <= 3; i++) {
    const v = (vMax * i) / 4, yy = Math.round(y(v)) + 0.5;
    ctx.beginPath(); ctx.moveTo(padL, yy); ctx.lineTo(cw - padR, yy); ctx.stroke();
    ctx.textAlign = "right"; ctx.textBaseline = "middle";
    ctx.fillText(Math.round(v).toLocaleString(), padL - 6, yy);
  }
  // X ticks (elapsed mm:ss).
  const step = pickTimeStep(tMax);
  ctx.textAlign = "center"; ctx.textBaseline = "top";
  for (let t = step; t <= tMax; t += step) ctx.fillText(fmtMMSS(t), x(t), ch - padB + 5);

  // Area wash under the S3 line (~10% opacity).
  ctx.globalAlpha = 0.1; ctx.fillStyle = C.s1; ctx.beginPath();
  series.forEach((p, i) => (i ? ctx.lineTo(x(p.t), y(p.s3)) : ctx.moveTo(x(p.t), y(p.s3))));
  ctx.lineTo(x(series[series.length - 1].t), y(0)); ctx.lineTo(x(series[0].t), y(0));
  ctx.closePath(); ctx.fill(); ctx.globalAlpha = 1;

  const line = (get, color) => {
    ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.lineJoin = "round"; ctx.lineCap = "round";
    ctx.beginPath();
    series.forEach((p, i) => (i ? ctx.lineTo(x(p.t), y(get(p))) : ctx.moveTo(x(p.t), y(get(p)))));
    ctx.stroke();
  };
  if (hasHf) line((p) => p.hf || 0, C.s2);
  line((p) => p.s3, C.s1);

  // End markers: r4 dot + 2px surface ring so they read over the lines.
  const dot = (px, py, color) => {
    ctx.beginPath(); ctx.arc(px, py, 4, 0, 7);
    ctx.fillStyle = color; ctx.fill();
    ctx.lineWidth = 2; ctx.strokeStyle = C.bg; ctx.stroke();
  };
  const lastP = series[series.length - 1];
  if (hasHf) dot(x(lastP.t), y(lastP.hf || 0), C.s2);
  dot(x(lastP.t), y(lastP.s3), C.s1);

  // Hover layer: crosshair snapped to the sample + rings on each series.
  if (hoverIdx != null && series[hoverIdx]) {
    const p = series[hoverIdx], hx = Math.round(x(p.t)) + 0.5;
    ctx.strokeStyle = C.muted; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(hx, padT); ctx.lineTo(hx, ch - padB); ctx.stroke();
    if (hasHf) dot(x(p.t), y(p.hf || 0), C.s2);
    dot(x(p.t), y(p.s3), C.s1);
  }
}

// Tooltip: one readout for every series at the hovered X; values lead, labels
// follow; series keyed by a short color stroke. Built with textContent.
function updateTip() {
  const tip = $("chart-tip");
  if (hoverIdx == null || !chartGeom || !series[hoverIdx]) { tip.classList.add("hidden"); return; }
  const p = series[hoverIdx];
  const C = seriesColors();
  tip.innerHTML = "";
  const t = document.createElement("div");
  t.className = "t"; t.textContent = `t+${fmtMMSS(p.t)}`;
  tip.appendChild(t);
  const row = (color, val, label) => {
    const r = document.createElement("div"); r.className = "row";
    const k = document.createElement("i"); k.className = "key"; k.style.background = color;
    const b = document.createElement("b"); b.textContent = `${fmtSpeed(val)} MiB/s`;
    const l = document.createElement("span"); l.className = "lbl"; l.textContent = label;
    r.append(k, b, l); tip.appendChild(r);
  };
  row(C.s1, p.s3, "S3 read");
  if (hasHf) row(C.s2, p.hf || 0, "CAS ingest");
  tip.classList.remove("hidden");
  const wrap = tip.parentElement.getBoundingClientRect();
  const px = chartGeom.x(p.t);
  const left = px + 14 + tip.offsetWidth > wrap.width ? px - 14 - tip.offsetWidth : px + 14;
  tip.style.left = `${Math.max(0, left)}px`;
}

{
  const c = $("chart");
  c.addEventListener("pointermove", (e) => {
    if (!chartGeom || series.length < 2) return;
    const r = c.getBoundingClientRect();
    const t = ((e.clientX - r.left - chartGeom.padL) / (r.width - chartGeom.padL - chartGeom.padR)) * chartGeom.tMax;
    // Snap to the nearest sample (samples are ~1s apart and sorted by t).
    let lo = 0, hi = series.length - 1;
    while (hi - lo > 1) { const mid = (lo + hi) >> 1; (series[mid].t < t ? (lo = mid) : (hi = mid)); }
    hoverIdx = Math.abs(series[lo].t - t) <= Math.abs(series[hi].t - t) ? lo : hi;
    drawChart(); updateTip();
  });
  c.addEventListener("pointerleave", () => { hoverIdx = null; drawChart(); updateTip(); });
}

// The chart lives inside the collapsed "Details" disclosure; while closed the
// canvas measures 0×0, so re-measure and redraw the moment it opens.
$("live-details").addEventListener("toggle", () => drawChart());

// Range state helpers: a range's live stage/progress comes from its CURRENT
// copier attempt (respawns re-point jobId).
function rangeView(r) {
  const s = (r.jobId && copierState[r.jobId]) || {};
  const stage = (s.stage || (r.jobId ? "scheduling" : "planned")).toLowerCase();
  const pct = r.bytes > 0 && s.bytes ? Math.min(100, Math.round((100 * s.bytes) / r.bytes)) : null;
  // ≥4 flat PROGRESS ticks (~20s) with the job still running = probable stall
  // (mirrors the copier's own watchdog). Finalize/commit move no S3 bytes by
  // design, so in those phases the pill already says what's happening — only
  // badge a stall there after a much longer flat window (~2min, past which the
  // copier's own finalize timeout is closing in).
  const flatCap = s.phase === "committing" || s.phase === "finalizing" ? 24 : 4;
  const stalled = stage === "running" && (s.zeroTicks || 0) >= flatCap;
  return { stage, pct, stalled, s };
}

let renderQueued = false;
function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => { renderQueued = false; renderRangeMap(); renderActiveJobs(); });
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

// Detailed rows only for copiers still doing work; the map keeps the history.
// Running rows always show; scheduling rows are capped so a 200-range plan
// doesn't bury the live ones.
function renderActiveJobs() {
  const active = Object.values(ranges)
    .sort((a, b) => a.idx - b.idx)
    .map((r) => ({ r, v: rangeView(r) }))
    .filter(({ v }) => v.stage === "scheduling" || v.stage === "running");
  const running = active.filter(({ v }) => v.stage === "running");
  const scheduling = active.filter(({ v }) => v.stage === "scheduling");
  const shown = running.concat(scheduling.slice(0, Math.max(0, 40 - running.length)));
  const hidden = active.length - shown.length;
  $("jobs").innerHTML = shown.map(({ r, v }) => {
    const s = v.s;
    const pill = v.stalled
      ? `<span class="pill stalled">⚠ stalled?</span>`
      : `<span class="pill ${v.stage}">${v.stage === "running" && s.phase ? s.phase : v.stage}</span>`;
    const note = (s.retries || 0) > 0 ? ` <span class="note">↻${s.retries}</span>` : "";
    // Second line: commit rate (files/s landed in the bucket) — the stage-2
    // counterpart to MiB/s. Shown only for the newer image that reports it.
    const crate = hasCommit && v.stage === "running"
      ? `<small class="crate">${fmtCount(s.crate || 0)} f/s ✓</small>` : "";
    const spd = v.stage === "running"
      ? `<span class="spd">${fmtSpeed(s.speed || 0)} <small>MiB/s</small>${note}${crate}</span>`
      : `<span class="spd"></span>`;
    return `<div class="job">` +
      `<a href="${HF}/jobs/${userNs}/${r.jobId}" target="_blank" rel="noopener">range ${r.idx}</a>` +
      `${pill}<span class="minibar"><i style="width:${v.pct ?? 0}%"></i></span>${spd}</div>`;
  }).join("") + (hidden > 0 ? `<div class="note">+${hidden} more scheduling…</div>` : "");
}

function updateLive() {
  const cs = Object.values(copierState);
  const filesDone = cs.reduce((a, s) => a + (s.files || 0), 0);
  const bytes = cs.reduce((a, s) => a + (s.bytes || 0), 0);
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
  // Monotonic wall-clock since Run — NOT max(copier.elapsed). Each copier's
  // elapsed is relative to its own staggered start, so that max jumps around and
  // decreases as copiers finish → the chart line crossed back on itself.
  const elapsed = runStartMs ? (performance.now() - runStartMs) / 1000 : 0;
  // True aggregate average = total bytes / wall-clock. (Summing per-copier avgs
  // over-counts — copiers don't each span the full wall-clock.)
  const avg = elapsed > 0 ? bytes / 2 ** 20 / elapsed : 0;

  const gib = (v) => { const g = v / 2 ** 30; return g >= 100 ? Math.round(g).toLocaleString() : g.toFixed(1); };
  if (s3Speed > peakSpeed) peakSpeed = s3Speed;
  $("r-speed").textContent = fmtSpeed(s3Speed);
  $("r-peak").textContent = fmtSpeed(peakSpeed);
  $("r-avg").textContent = fmtSpeed(avg);
  $("r-files").textContent = `${filesDone.toLocaleString()} / ${(planTotalFiles || 0).toLocaleString()}${planDone ? "" : "+"}`;
  $("r-data").textContent = planTotalBytes > 0
    ? `${gib(bytes)} / ${gib(planTotalBytes)}${planDone ? "" : "+"} GiB`
    : `${gib(bytes)} GiB`;
  const counts = { running: 0, done: 0, failed: 0, pending: 0 };
  for (const r of Object.values(ranges)) {
    const st = rangeView(r).stage;
    if (st === "running") counts.running++;
    else if (st === "completed") counts.done++;
    else if (st === "error") counts.failed++;
    else counts.pending++;
  }
  $("r-copiers").textContent =
    `${counts.running} run · ${counts.done} done` +
    `${counts.failed ? ` · ${counts.failed} failed` : ""}${counts.pending ? ` · ${counts.pending} wait` : ""}`;
  // ETA from the aggregate average once the plan total is known.
  const bytesPerSec = elapsed > 0 ? bytes / elapsed : 0;
  $("r-eta").textContent = planDone && planTotalBytes > bytes && bytesPerSec > 0
    ? `~${fmtDur((planTotalBytes - bytes) / bytesPerSec)}` : "–";
  $("r-elapsed").textContent = fmtDur(elapsed);

  // Progress bar: bytes-based when the plan total is known (honest), else files.
  const pct = planTotalBytes > 0
    ? Math.min(100, (100 * bytes) / planTotalBytes)
    : Math.min(100, (100 * filesDone) / (planTotalFiles || filesDone || 1));
  $("bar").style.width = `${pct.toFixed(1)}%`;
  $("bar-label").textContent = planTotalBytes > 0
    ? `${pct.toFixed(1)}% of ${fmtSize(planTotalBytes)}${planDone ? "" : "+ (listing…)"}`
    : "";

  // Commit pipeline — only once a copier has reported `committed` (newer image).
  if (hasCommit) {
    const denom = planTotalFiles || filesDone || 1;
    const cPct = Math.min(100, (100 * committedFiles) / denom);
    const bPct = Math.min(100 - cPct, (100 * backlog) / denom);
    // "Behind": backlog isn't draining — no commits landing while files are
    // still flowing in. This is the wedge signature, ~300s before an error.
    const behind = commitRate === 0 && backlog > 0 && (s3Speed > 0 || hfSpeed > 0);
    $("pipeline").classList.remove("hidden");
    $("pipeline-label").classList.remove("hidden");
    $("ov-commit").classList.remove("hidden");
    $("pl-committed").style.width = `${cPct.toFixed(1)}%`;
    $("pl-backlog").style.left = `${cPct.toFixed(1)}%`;
    $("pl-backlog").style.width = `${bPct.toFixed(1)}%`;
    $("pipeline").classList.toggle("behind", behind);
    $("pipeline-label").classList.toggle("behind", behind);
    $("r-crate").textContent = fmtCount(commitRate);
    $("pipeline-label").innerHTML =
      `<span class="pl-committed-v">${fmtCount(committedFiles)}</span>&nbsp;<span class="pl-k">committed</span>` +
      ` · <span class="pl-backlog-v">${fmtCount(backlog)}</span>&nbsp;<span class="pl-k">awaiting commit</span>` +
      (behind ? ` <span class="pl-k">— finalize stalled</span>` : "");
  }

  // Append at ~1s resolution; t is monotonic so the line only advances left→right.
  const last = series[series.length - 1];
  if (!last || elapsed - last.t >= 1) series.push({ t: elapsed, s3: s3Speed, hf: hfSpeed });
  else { last.s3 = s3Speed; last.hf = hfSpeed; }
  drawChart();
}

// Follow one copier's own log for PROGRESS/DONE → aggregate graph + range map.
function followCopier(jobId) {
  followJob(jobId, (line) => {
    if (copierState[jobId] && copierState[jobId].stage === "scheduling") {
      copierState[jobId].stage = "running"; scheduleRender();
    }
    if (line.startsWith("PROGRESS ")) {
      try {
        const p = JSON.parse(line.slice(9));
        const prev = copierState[jobId] || {};
        const flat = (p.mibps_5s || 0) === 0 && (p.hf_mibps_5s || 0) === 0;
        if (p.hf_mibps_5s != null && !hasHf) { hasHf = true; $("legend").classList.remove("hidden"); }
        if (p.committed != null && !hasCommit) { hasCommit = true; }
        copierState[jobId] = {
          ...prev,
          files: p.files, bytes: p.bytes_done,
          speed: p.mibps_5s, hf: p.hf_mibps_5s || 0,
          committed: p.committed, crate: p.committed_fps_5s || 0,
          avg: p.mibps_avg, elapsed: p.elapsed_s,
          phase: p.phase,
          retries: (p.s3_part_retries || 0) + (p.file_retries || 0),
          zeroTicks: flat ? (prev.zeroTicks || 0) + 1 : 0,
        };
        updateLive(); scheduleRender();
      } catch {}
    } else if (line.startsWith("DONE ")) {
      try {
        const d = JSON.parse(line.slice(5));
        copierState[jobId] = {
          ...copierState[jobId], bytes: d.bytes, speed: 0, hf: 0, crate: 0, zeroTicks: 0,
          committed: d.committed != null ? d.committed : copierState[jobId]?.committed,
        };
        updateLive(); scheduleRender();
      } catch {}
    }
  }).then((st) => {
    if (copierState[jobId]) {
      copierState[jobId].stage = st || "completed";
      copierState[jobId].speed = 0; copierState[jobId].hf = 0; copierState[jobId].zeroTicks = 0;
    }
    updateLive(); scheduleRender();
  });
}

$("run").onclick = async () => {
  const src = $("src").value.trim(), dst = $("dst").value.trim();
  const flavor = $("flavor").value;
  const rangeGib = Math.max(1, parseInt($("rangegib").value, 10) || 25);
  const inflight = Math.min(256, Math.max(1, parseInt($("inflight").value, 10) || 16));
  const timeoutSeconds = toSeconds($("timeout").value);
  const secrets = collectSecrets();

  $("run").disabled = true; $("analyze").disabled = true;
  show("live");
  series.length = 0;
  for (const k in copierState) delete copierState[k];
  for (const k in ranges) delete ranges[k];
  planTotalFiles = 0; planTotalBytes = 0; rangesCut = 0; planDone = false; pausedNote = "";
  hasHf = false; hasCommit = false; hoverIdx = null; peakSpeed = 0;
  runStartMs = performance.now();
  setKicker("Copying…");
  $("jobs").innerHTML = ""; $("rangemap").innerHTML = ""; $("bar-label").textContent = "";
  $("legend").classList.add("hidden"); $("chart-tip").classList.add("hidden");
  $("pipeline").classList.add("hidden"); $("pipeline-label").classList.add("hidden");
  $("ov-commit").classList.add("hidden");

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

  try {
    // The planner runs on cheap cpu-basic — it only lists + orchestrates.
    const plannerId = await runJob({ src, dst, extra, flavor: "cpu-basic", timeoutSeconds, secrets });
    setPlan(`Planner <a href="${HF}/jobs/${userNs}/${plannerId}" target="_blank" rel="noopener">${plannerId}</a> starting…`);

    await followJob(plannerId, (line) => {
      if (line.startsWith("PLANNING ")) {
        try { const p = JSON.parse(line.slice(9)); pausedNote = ""; setPlan(`Planner listing… <b>${(p.listed || 0).toLocaleString()}</b> objects · <b>${p.ranges || 0}</b> ranges · ${fmtSize(p.bytes || 0)}`); } catch {}
      } else if (line.startsWith("RANGE ")) {
        try {
          const r = JSON.parse(line.slice(6));
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
      } else if (line.startsWith("PLAN_RESULT ")) {
        try {
          const r = JSON.parse(line.slice(12));
          const bad = r.failed ? ` · <b class="errtxt">${r.failed} failed</b>` : "";
          const ret = r.retried ? ` · ${r.retried} retried` : "";
          setPlan(`Done: <b>${r.completed}</b>/${r.ranges} ranges completed${bad}${ret}.`);
          setKicker(r.failed ? "Done — with failures" : "Done");
        } catch {}
      }
    });
  } catch (e) {
    $("plan-status").insertAdjacentHTML("beforeend", `<div class="msg err">planner launch failed: ${e.message}</div>`);
  }
  $("analyze").disabled = false;
};

// ---------- demo mode ----------
function startDemo() {
  hide("signin"); show("live");
  setKicker("Copying…");
  userNs = "demo";
  const N = 24, RANGE_BYTES = 25 * 2 ** 30;
  for (let i = 0; i < N; i++) {
    ranges[i] = { idx: i, files: 12000 + i * 137, bytes: RANGE_BYTES, jobId: `demo${i}`, attempts: i === 7 ? 2 : 1 };
    planTotalFiles += ranges[i].files; planTotalBytes += ranges[i].bytes;
  }
  planDone = true; hasHf = true; hasCommit = true;
  $("legend").classList.remove("hidden");
  $("live-details").open = true; // demo showcases the full instrument
  const stallDemo = new URLSearchParams(location.search).has("stall");
  setPlan(`Plan complete: <b>${N}</b> ranges · <b>${planTotalFiles.toLocaleString()}</b> files · ${fmtSize(planTotalBytes)} — copying…`);
  // Back-date 5 minutes of history so the chart opens with a real line.
  const HIST = 300;
  runStartMs = performance.now() - HIST * 1000;
  const agg = (t) => Math.max(0, 2600 + 700 * Math.sin(t / 40) + 250 * Math.sin(t / 7) + 120 * Math.random());
  for (let t = 0; t < HIST; t++) { const v = agg(t); series.push({ t, s3: v, hf: v * 0.92 }); }
  const stage = (i) => (i < 6 ? "COMPLETED" : i < 14 ? "running" : "scheduling");
  const tick = () => {
    for (let i = 0; i < N; i++) {
      const st = stage(i);
      const cs = copierState[`demo${i}`] || (copierState[`demo${i}`] = { idx: i, stage: st });
      cs.stage = st;
      if (st === "COMPLETED") { cs.bytes = RANGE_BYTES; cs.files = ranges[i].files; cs.committed = ranges[i].files; cs.speed = 0; cs.hf = 0; cs.crate = 0; }
      if (st === "running") {
        const stalled = i === 9;
        cs.speed = stalled ? 0 : Math.max(0, 320 + 140 * Math.sin(performance.now() / 9000 + i) + 40 * Math.random());
        cs.hf = cs.speed * 0.92;
        cs.bytes = Math.min(RANGE_BYTES, (cs.bytes || (0.2 + 0.6 * (i % 7) / 7) * RANGE_BYTES) + cs.speed * 2 ** 20);
        cs.files = Math.min(ranges[i].files, Math.round(ranges[i].files * (cs.bytes / RANGE_BYTES)));
        // Committed trails uploaded — commits pipeline behind uploads. The
        // finalizing copier (12) lags more; the stalled one (9) doesn't commit.
        const lag = i === 12 ? 0.45 : 0.9;
        // `stall` variant: whole fleet stops committing while uploads keep
        // flowing → aggregate rate 0, backlog grows → the amber alarm.
        cs.committed = (stalled || stallDemo) ? (cs.committed || Math.round(cs.files * 0.5)) : Math.round(cs.files * lag);
        cs.crate = (stalled || stallDemo) ? 0 : Math.round(cs.speed * 0.55);
        cs.phase = i === 12 ? "finalizing" : "uploading";
        cs.retries = i === 8 ? 3 : 0;
        cs.zeroTicks = stalled ? (cs.zeroTicks || 0) + 1 : 0;
      }
    }
    updateLive(); scheduleRender();
  };
  for (let k = 0; k < 5; k++) tick(); // warm up zeroTicks → the stalled badge shows
  setInterval(tick, 1000);
}

init();
