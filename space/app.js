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
// Translate size into planner knobs. Fewer/bigger copiers win (they amortize the
// per-job schedule+image-pull+ramp startup tax — validated at ~20 GiB/copier), so
// aim for ~25 GiB ranges, cap the copier count so huge buckets don't explode into
// thousands of jobs, and default the flavor to cpu-upgrade (matches cpu-performance
// throughput on bandwidth-bound copies at ~1/60th the price).
function recommend(gib) {
  const targetCopiers = Math.max(1, Math.min(256, Math.ceil(gib / 25)));
  const rangeGib = Math.max(5, Math.ceil(gib / targetCopiers));
  const inflight = Math.min(24, targetCopiers);
  // Planner lives until every copier finishes, so its timeout must outlast the
  // whole copy. Generous (billed/sec on cheap cpu-basic; it exits early on done).
  const aggMiBps = inflight * 300;
  const estSec = 900 + (gib * 1024 / aggMiBps) * 3;
  const hours = Math.min(48, Math.max(2, Math.ceil(estSec / 3600)));
  return { rangeGib, inflight, flavor: "cpu-upgrade", timeout: `${hours}h` };
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
  const r = { rangeGib: 25, inflight: 24, flavor: "cpu-upgrade", timeout: "24h" };
  $("stats").innerHTML = [
    ["files", `${kept.toLocaleString()}+`],
    ["total", bytes ? `${fmtSize(bytes)}+` : "—"],
    ["≤16 MiB", kept ? `${smallPct}%` : "—"],
  ].map(([k, v]) => `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");
  $("stats").classList.remove("hidden");
  $("reco").innerHTML =
    `<b>Very large bucket.</b> Scanned <b>${listed.toLocaleString()}+</b> objects` +
    `${bytes ? ` (<b>${fmtSize(bytes)}+</b>)` : ""}; the dry-run timed out before finishing the listing — the planner will do the full listing itself.<br>` +
    `The planner lists once and fans out copiers of ~<b>${r.rangeGib} GiB</b> each (up to <b>${r.inflight}</b> in-flight), on <b>cpu-upgrade</b>. Copies are bandwidth-bound, so this flavor is as fast as pricier ones.<br>` +
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
  const copiers = Math.max(1, Math.ceil(gib / r.rangeGib));
  const bmsg = bucketOk === false ? ` <span class="dot err">✗ bucket write-token failed inside the job</span>` : "";
  $("reco").innerHTML =
    `Recommended: one planner fans out ~<b>${copiers}</b> copier${copiers > 1 ? "s" : ""} of <b>~${r.rangeGib} GiB</b> each, up to <b>${r.inflight}</b> in-flight, on <b>cpu-upgrade</b>. Planner timeout ${r.timeout}.${bmsg} <span class="hint">Adjust under “Advanced”, then Run.</span>`;
  $("reco").classList.remove("hidden");
  $("analyze").disabled = false; $("run").disabled = false;
};

// ---------- Run: one planner → observe ----------
const series = [];              // aggregate {t, speed}
const copierState = {};         // copier job_id -> {idx, stage, files, bytes, speed, avg, elapsed}
let planTotalFiles = 0, planTotalBytes = 0, rangesCut = 0, planDone = false;
let chartMax = 10;
let planText = "", pausedNote = "";
let runStartMs = 0;   // wall-clock origin for the chart x-axis (set at Run)

function renderPlan() { $("plan-status").innerHTML = planText + (pausedNote ? ` <span class="paused">${pausedNote}</span>` : ""); }
function setPlan(html) { planText = html; renderPlan(); }

function drawChart() {
  const c = $("chart"), ctx = c.getContext("2d");
  const W = c.width, H = c.height, pad = 6;
  ctx.clearRect(0, 0, W, H);
  if (series.length < 2) return;
  const tMax = series[series.length - 1].t || 1;
  chartMax = Math.max(chartMax, ...series.map((p) => p.speed));
  const x = (t) => pad + (W - 2 * pad) * (t / tMax);
  const y = (s) => H - pad - (H - 2 * pad) * (s / chartMax);
  const style = getComputedStyle(document.documentElement);
  ctx.strokeStyle = style.getPropertyValue("--accent").trim() || "#ffd21e";
  ctx.lineWidth = 2; ctx.beginPath();
  series.forEach((p, i) => (i ? ctx.lineTo(x(p.t), y(p.speed)) : ctx.moveTo(x(p.t), y(p.speed))));
  ctx.stroke();
}

function updateLive() {
  const cs = Object.values(copierState);
  const filesDone = cs.reduce((a, s) => a + (s.files || 0), 0);
  const bytes = cs.reduce((a, s) => a + (s.bytes || 0), 0);
  // Aggregate instant rate = sum of per-copier 5s rates (they run concurrently,
  // so their instantaneous rates ARE additive).
  const speed = cs.reduce((a, s) => a + (s.speed || 0), 0);
  // Monotonic wall-clock since Run — NOT max(copier.elapsed). Each copier's
  // elapsed is relative to its own staggered start, so that max jumps around and
  // decreases as copiers finish → the chart line crossed back on itself.
  const elapsed = runStartMs ? (performance.now() - runStartMs) / 1000 : 0;
  // True aggregate average = total bytes / wall-clock. (Summing per-copier avgs
  // over-counts — copiers don't each span the full wall-clock.)
  const avg = elapsed > 0 ? bytes / 2 ** 20 / elapsed : 0;
  const total = planTotalFiles || filesDone || 1;
  $("bar").style.width = `${Math.min(100, 100 * filesDone / total).toFixed(1)}%`;
  $("r-speed").textContent = speed.toFixed(0);
  $("r-avg").textContent = avg.toFixed(0);
  $("r-files").textContent = `${filesDone.toLocaleString()} / ${(planTotalFiles || 0).toLocaleString()}${planDone ? "" : "+"}`;
  $("r-data").textContent = `${(bytes / 2 ** 30).toFixed(2)} GiB`;
  $("r-elapsed").textContent = elapsed.toFixed(0);
  // Append at ~1s resolution; t is monotonic so the line only advances left→right.
  const last = series[series.length - 1];
  if (!last || elapsed - last.t >= 1) series.push({ t: elapsed, speed });
  else last.speed = speed;
  drawChart();
}

function renderJobs() {
  $("jobs").innerHTML = Object.entries(copierState)
    .sort((a, b) => (a[1].idx ?? 0) - (b[1].idx ?? 0))
    .map(([id, s]) => {
      const st = (s.stage || "running").toLowerCase();
      return `<div class="job"><span class="pill ${st}">${st}</span><a href="${HF}/jobs/${userNs}/${id}" target="_blank" rel="noopener">range ${s.idx}: ${id}</a></div>`;
    }).join("");
}

// Follow one copier's own log for PROGRESS/DONE → aggregate graph.
function followCopier(jobId) {
  followJob(jobId, (line) => {
    if (copierState[jobId] && copierState[jobId].stage === "scheduling") {
      copierState[jobId].stage = "running"; renderJobs();
    }
    if (line.startsWith("PROGRESS ")) {
      try {
        const p = JSON.parse(line.slice(9));
        copierState[jobId] = { ...copierState[jobId], files: p.files, bytes: p.bytes_done, speed: p.mibps_5s, avg: p.mibps_avg, elapsed: p.elapsed_s };
        updateLive();
      } catch {}
    } else if (line.startsWith("DONE ")) {
      try { const d = JSON.parse(line.slice(5)); copierState[jobId] = { ...copierState[jobId], bytes: d.bytes, speed: 0 }; updateLive(); } catch {}
    }
  }).then((st) => {
    if (copierState[jobId]) { copierState[jobId].stage = st || "completed"; copierState[jobId].speed = 0; }
    renderJobs(); updateLive();
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
  series.length = 0; for (const k in copierState) delete copierState[k];
  planTotalFiles = 0; planTotalBytes = 0; rangesCut = 0; planDone = false; chartMax = 10; pausedNote = "";
  runStartMs = performance.now();
  $("jobs").innerHTML = "";

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
        try { const r = JSON.parse(line.slice(6)); rangesCut++; planTotalFiles += r.files || 0; planTotalBytes += r.bytes || 0; if (!planDone) updateLive(); } catch {}
      } else if (line.startsWith("COPIER ")) {
        try {
          const c = JSON.parse(line.slice(7));
          if (c.job_id && !copierState[c.job_id]) {
            copierState[c.job_id] = { idx: c.idx, stage: "scheduling" };
            renderJobs(); followCopier(c.job_id);
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
        } catch {}
      }
    });
  } catch (e) {
    $("plan-status").insertAdjacentHTML("beforeend", `<div class="msg err">planner launch failed: ${e.message}</div>`);
  }
  $("analyze").disabled = false;
};

init();
