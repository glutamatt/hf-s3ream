// Run the Space's sizing code (space/app.js: recommend + estimate) on a grid
// of sources and print the results as JSON. The Rust port (src/reco.rs) must
// give the same plans while the Space still computes them itself:
//
//   node scripts/reco-parity.mjs > /tmp/reco.json
//   RECO_PARITY_JSON=/tmp/reco.json cargo test reco -- --ignored
import { readFileSync } from "node:fs";

const src = readFileSync(new URL("../space/app.js", import.meta.url), "utf8");
const start = src.indexOf("const FLAVOR_MIBPS");
const end = src.indexOf("function applyReco");
if (start < 0 || end < 0) throw new Error("sizing code not found in space/app.js");
const { recommend, estimate } = new Function(`${src.slice(start, end)}; return { recommend, estimate };`)();

const gibs = [0, 0.001, 0.5, 1, 2.5, 4.99, 5, 7, 20, 64, 100, 200, 500, 1000, 4096, 10000, 50000, 109875, 300000, 512000, 2e6];
const avgMib = [0.01, 0.1, 0.4, 1, 2, 4, 4.1, 16, 64, 225, 1024, 10240];
const out = [];
for (const gib of gibs) {
  for (const avg of avgMib) {
    const files = gib === 0 ? 0 : Math.max(1, Math.round((gib * 1024) / avg));
    const r = recommend(gib, files);
    const e = estimate(gib, r, files);
    out.push({
      gib, files,
      plan: { copier_flavor: r.flavor, range_gib: r.rangeGib, max_inflight: r.inflight, timeout_h: parseInt(r.timeout, 10) },
      copiers: e.copiers, commit_bound: e.commitBound, usd: e.usdNum,
    });
  }
}
console.log(JSON.stringify(out));
