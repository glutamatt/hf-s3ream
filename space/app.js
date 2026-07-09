// hf-s3ream Space — fully client-side. OAuth (Sign in with HF) → call the HF
// Jobs/Buckets API directly (CORS-enabled) → run hf-s3ream on HF Jobs. AWS keys
// stay in the browser and go only into the Job's encrypted `secrets`.
import {
  oauthLoginUrl, oauthHandleRedirectIfPresent,
  runJob as hubRunJob, streamJobLogs, getJob,
} from "https://esm.sh/@huggingface/hub@2";

const HF = "https://huggingface.co";
// WIP image (has DRYRUN_STATS / PROGRESS / DONE markers). Flip to a vX.Y.Z tag at release.
const IMAGE = "ghcr.io/glutamatt/hf-s3ream:wip";
const RUST_LOG = "hf_s3ream=info,xet_data=warn,xet_client=warn";
const PART_BYTES = 16 * 1024 * 1024;

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
    // sensible default destination namespace
    $("dst").value = userNs ? `${userNs}/s3ream` : "";
  } else {
    show("signin");
  }
}
$("login").onclick = async () => { window.location.href = await oauthLoginUrl(); };
$("logout").onclick = (e) => {
  e.preventDefault();
  try { localStorage.clear(); sessionStorage.clear(); } catch {}
  // reload clean (drop any ?code=… in the URL) → forces a fresh Sign-in,
  // which re-prompts consent for any newly-added scope.
  window.location.href = window.location.origin + window.location.pathname;
};

// ---------- HF API helpers ----------
async function hf(path, opts = {}) {
  const r = await fetch(`${HF}${path}`, {
    ...opts,
    headers: { Authorization: `Bearer ${token}`, ...(opts.headers || {}) },
  });
  return r;
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
  if (r.status === 409) return { ok: true, created: false }; // already exists
  const body = await r.text();
  return { ok: false, error: `HTTP ${r.status}: ${body.slice(0, 200)}` };
}

function toSeconds(t) {
  const m = String(t).trim().match(/^([\d.]+)\s*([smhd])?$/);
  if (!m) return 7200;
  return Math.round(parseFloat(m[1]) * { s: 1, m: 60, h: 3600, d: 86400 }[m[2] || "s"]);
}

// Launch one Job via @huggingface/hub. `extra` = extra hf-s3ream args. Returns job id.
async function runJob({ src, dst, extra = [], flavor, timeoutSeconds, secrets, dryRun = false }) {
  const command = ["hf-s3ream", src, dst, ...extra];
  if (dryRun) command.push("--dry-run");
  const job = await hubRunJob({
    accessToken: token,
    namespace: userNs,
    dockerImage: IMAGE,
    command,
    environment: { AWS_REGION: ($("region").value.trim() || "us-east-1"), RUST_LOG },
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

// Follow a Job's logs via @huggingface/hub's streamJobLogs async generator —
// it unwraps the SSE envelope and yields {message, timestamp}. Reconnect until
// a terminal stage (covers the "logs not ready yet just after submit" case).
async function followJob(id, onLine) {
  const TERMINAL = ["COMPLETED", "ERROR", "CANCELED", "DELETED"];
  for (let attempt = 0; attempt < 400; attempt++) {
    try {
      for await (const ev of streamJobLogs({ accessToken: token, namespace: userNs, jobId: id })) {
        onLine(ev.message);
      }
    } catch (e) { /* stream dropped; re-check stage below */ }
    const st = await jobStage(id);
    if (st && TERMINAL.includes(st)) return st;
    await new Promise((r) => setTimeout(r, 2000));
  }
  return await jobStage(id);
}

// ---------- Analyze (dry-run preflight) ----------
function checkLine(state, text) {
  const cls = state === "ok" ? "ok" : state === "err" ? "err" : "run";
  const sym = state === "ok" ? "✓" : state === "err" ? "✗" : "…";
  return `<div class="check"><span class="dot ${cls}">${sym}</span><span>${text}</span></div>`;
}

$("analyze").onclick = async () => {
  const src = $("src").value.trim();
  const dst = $("dst").value.trim();
  $("form-msg").textContent = "";
  if (!/^s3:\/\//.test(src)) return (($("form-msg").textContent = "source must be s3://bucket/prefix/"), ($("form-msg").className = "msg err"));
  if (!$("ak").value.trim() || !$("sk").value.trim()) return (($("form-msg").textContent = "AWS access key + secret are required"), ($("form-msg").className = "msg err"));

  show("analysis"); hide("live"); $("stats").classList.add("hidden"); $("reco").classList.add("hidden");
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

  // 2. dry-run job → parse DRYRUN_STATS / DRYRUN_BUCKET
  let stats = null, bucketOk = null;
  try {
    const id = await runJob({ src, dst, flavor: "cpu-basic", timeoutSeconds: 900, secrets: collectSecrets(), dryRun: true });
    lines.job = checkLine("run", `dry-run job <code>${id}</code> running…`); render();
    await followJob(id, (line) => {
      if (line.startsWith("DRYRUN_STATS ")) { try { stats = JSON.parse(line.slice(13)); } catch {} }
      else if (line.startsWith("DRYRUN_BUCKET ")) bucketOk = line.slice(14).trim() === "ok";
    });
  } catch (e) {
    lines.job = checkLine("err", `dry-run failed: ${e.message}`); render();
    $("analyze").disabled = false; return;
  }

  if (!stats) {
    lines.job = checkLine("err", "dry-run finished but returned no stats — S3 access failed (check keys/region; VPC-locked buckets are unreachable from HF Jobs).");
    render(); $("analyze").disabled = false; return;
  }
  lines.job = checkLine("ok", `S3 read OK — listed <b>${stats.count.toLocaleString()}</b> objects`);
  render();

  // stats + recommendation
  const gib = stats.total_bytes / 2 ** 30;
  const smallFiles = stats.median <= PART_BYTES;
  $("stats").innerHTML = [
    ["files", stats.count.toLocaleString()],
    ["total", gib >= 1 ? `${gib.toFixed(1)} GiB` : `${(stats.total_bytes / 2 ** 20).toFixed(0)} MiB`],
    ["median", stats.median >= 2 ** 20 ? `${(stats.median / 2 ** 20).toFixed(1)} MiB` : `${(stats.median / 1024).toFixed(0)} KiB`],
    ["≤16 MiB", `${stats.pct_le_16mib}%`],
  ].map(([k, v]) => `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");
  $("stats").classList.remove("hidden");

  const shards = Math.min(16, Math.max(1, Math.ceil(gib / 200), Math.ceil(stats.count / 50000)));
  const pf = smallFiles ? 128 : 32;
  $("shards").value = shards;
  $("pf").value = pf;
  $("flavor").value = "cpu-basic";
  const perShardGiB = gib / shards;
  // Timeout = fixed overhead (image pull + scheduling + commit/finalize tail,
  // which dominate a small shard) + 2× transfer at a conservative rate (small
  // files run ~70 MiB/s effective, not 300). Jobs bill per second and are only
  // killed AT the timeout, so err generous — min 10m.
  const effMiBps = smallFiles ? 70 : 300;
  const estSec = 240 + (perShardGiB * 1024 / effMiBps) * 2;
  $("timeout").value = `${Math.max(10, Math.ceil(estSec / 60))}m`;

  const bmsg = bucketOk === false ? ` <span class="dot err">✗ bucket write-token failed inside the job</span>` : "";
  $("reco").innerHTML = `Recommended: <b>${shards}</b> shard${shards > 1 ? "s" : ""} · <b>cpu-basic</b> · <b>--parallel-files ${pf}</b> (${smallFiles ? "small files" : "large files"}). Timeout ${$("timeout").value}.${bmsg} <span class="hint">Adjust under “Advanced”, then Run.</span>`;
  $("reco").classList.remove("hidden");

  $("analyze").disabled = false; $("run").disabled = false;
};

// ---------- Run (± shards) + live graph ----------
const series = [];          // aggregate {t, speed}
const shardState = {};       // id -> {files,total,bytes,speed,avg,elapsed,stage}
let chartMax = 10;

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
  const shards = Object.values(shardState);
  const filesDone = shards.reduce((a, s) => a + (s.files || 0), 0);
  const total = shards.reduce((a, s) => a + (s.total || 0), 0);
  const bytes = shards.reduce((a, s) => a + (s.bytes || 0), 0);
  const speed = shards.reduce((a, s) => a + (s.speed || 0), 0);
  const avg = shards.reduce((a, s) => a + (s.avg || 0), 0);
  const elapsed = Math.max(0, ...shards.map((s) => s.elapsed || 0));
  $("bar").style.width = total ? `${(100 * filesDone / total).toFixed(1)}%` : "0%";
  $("r-speed").textContent = speed.toFixed(0);
  $("r-avg").textContent = avg.toFixed(0);
  $("r-files").textContent = `${filesDone.toLocaleString()} / ${total.toLocaleString()}`;
  $("r-data").textContent = `${(bytes / 2 ** 30).toFixed(2)} GiB`;
  $("r-elapsed").textContent = elapsed;
  const last = series[series.length - 1];
  if (!last || last.t !== elapsed) series.push({ t: elapsed, speed });
  else last.speed = speed;
  drawChart();
}

function renderJobs() {
  $("jobs").innerHTML = Object.entries(shardState).map(([id, s], i) => {
    const st = (s.stage || "running").toLowerCase();
    return `<div class="job"><span class="pill ${st}">${st}</span><a href="${HF}/jobs/${userNs}/${id}" target="_blank" rel="noopener">shard ${i}: ${id}</a></div>`;
  }).join("");
}

$("run").onclick = async () => {
  const src = $("src").value.trim(), dst = $("dst").value.trim();
  const flavor = $("flavor").value, pf = parseInt($("pf").value, 10) || 32;
  const shards = Math.max(1, parseInt($("shards").value, 10) || 1);
  const timeoutSeconds = toSeconds($("timeout").value);
  const secrets = collectSecrets();

  $("run").disabled = true; $("analyze").disabled = true;
  show("live"); series.length = 0; for (const k in shardState) delete shardState[k]; chartMax = 10;
  $("jobs").innerHTML = "";

  try {
    for (let i = 0; i < shards; i++) {
      const extra = ["--parallel-files", String(pf)];
      if (shards > 1) extra.push("--shard-id", String(i), "--shard-count", String(shards));
      const id = await runJob({ src, dst, extra, flavor, timeoutSeconds, secrets });
      shardState[id] = { stage: "running" };
      renderJobs();
      // follow each shard concurrently
      followJob(id, (line) => {
        if (line.startsWith("PROGRESS ")) {
          try {
            const p = JSON.parse(line.slice(9));
            shardState[id] = { ...shardState[id], files: p.files, total: p.total, bytes: p.bytes_done, speed: p.mibps_5s, avg: p.mibps_avg, elapsed: p.elapsed_s };
            updateLive();
          } catch {}
        } else if (line.startsWith("DONE ")) {
          try { const d = JSON.parse(line.slice(5)); shardState[id] = { ...shardState[id], files: shardState[id].total, bytes: d.bytes }; updateLive(); } catch {}
        }
      }).then((st) => { shardState[id].stage = st || "completed"; shardState[id].speed = 0; renderJobs(); updateLive(); });
    }
  } catch (e) {
    $("jobs").insertAdjacentHTML("beforeend", `<div class="msg err">launch failed: ${e.message}</div>`);
  }
  $("analyze").disabled = false;
};

init();
