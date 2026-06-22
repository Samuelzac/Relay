import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const pagesRoot = path.join(root, "pages_out", "pages");

const htmlFiles = [
  "admin/index.html",
  "watch/index.html",
  "broadcast/index.html",
  "create/index.html",
  "success/index.html",
  "index.html",
  "support/index.html",
  "faq/index.html",
  "private-livestreaming/index.html",
  "paid-livestream-events/index.html",
  "livestreaming-for-clubs-schools-venues/index.html",
  "hls-vs-webrtc/index.html",
  "phone-broadcasting/index.html",
  "terms/index.html",
  "privacy/index.html",
  "ended/index.html",
];

const requiredFiles = [
  "assets/relay-config.js",
  "_redirects",
  "robots.txt",
  "sitemap.xml",
  ...htmlFiles,
];

const stalePatterns = [
  /stream-platform-api\.kiwismurph/i,
  /relay-cp1/i,
  /\/api\/test\//i,
  /2-minute test/i,
  /Standard event/i,
  /Extended event/i,
  /\$99 NZD/i,
  /\$179 NZD/i,
  /ap-southeast-2/i,
  /base_price_with_metered/i,
  /castlink-recording-worker\.kiwismurph\.workers\.dev/i,
  /[âāÂ�]/,
];

const apiPages = [
  "admin/index.html",
  "watch/index.html",
  "broadcast/index.html",
  "create/index.html",
  "success/index.html",
  "ended/index.html",
];

const results = [];

function pass(label, detail = "") {
  results.push({ status: "pass", label, detail });
}

function fail(label, detail = "") {
  results.push({ status: "fail", label, detail });
}

function warn(label, detail = "") {
  results.push({ status: "warn", label, detail });
}

function readRel(rel) {
  return fs.readFileSync(path.join(pagesRoot, rel), "utf8");
}

for (const rel of requiredFiles) {
  const full = path.join(pagesRoot, rel);
  if (fs.existsSync(full)) pass(`file exists: ${rel}`);
  else fail(`missing file: ${rel}`);
}

for (const rel of htmlFiles) {
  const full = path.join(pagesRoot, rel);
  if (!fs.existsSync(full)) continue;
  const html = readRel(rel);
  const inlineScripts = [...html.matchAll(/<script(?![^>]*src)(?![^>]*type=["']application\/ld\+json["'])[^>]*>([\s\S]*?)<\/script>/gi)].map((m) => m[1]);
  try {
    for (const code of inlineScripts) new Function(code);
    pass(`inline scripts parse: ${rel}`, `${inlineScripts.length} script(s)`);
  } catch (e) {
    fail(`inline script error: ${rel}`, e?.message || String(e));
  }

  for (const pattern of stalePatterns) {
    if (pattern.test(html)) fail(`stale text in ${rel}`, String(pattern));
  }
}

for (const rel of apiPages) {
  const full = path.join(pagesRoot, rel);
  if (!fs.existsSync(full)) continue;
  const html = readRel(rel);
  const usesConfig = /RelayConfig/.test(html);
  const hasConfigScript = /<script\s+src="\/assets\/relay-config\.js"><\/script>/i.test(html);
  if (usesConfig && hasConfigScript) pass(`config script present: ${rel}`);
  else if (usesConfig) fail(`RelayConfig used before config script check failed: ${rel}`);
  else warn(`no RelayConfig usage: ${rel}`);
}

{
  const config = readRel("assets/relay-config.js");
  if (/recordingApiBase/.test(config)) pass("recording API base is configurable");
  else fail("recording API base is configurable", "RelayConfig.recordingApiBase missing");
}

{
  const success = readRel("success/index.html");
  if (/RelayConfig\.recordingApiBase/.test(success)) pass("success page uses configured recording API");
  else fail("success page uses configured recording API", "success page should not hardcode the recording Worker URL");
}

const redirects = fs.existsSync(path.join(pagesRoot, "_redirects")) ? readRel("_redirects") : "";
for (const route of [
  "/broadcast/*",
  "/watch/*",
  "/success/*",
  "/ended/*",
  "/admin/*",
  "/create/*",
  "/faq/*",
  "/support/*",
  "/private-livestreaming/*",
  "/paid-livestream-events/*",
  "/livestreaming-for-clubs-schools-venues/*",
  "/hls-vs-webrtc/*",
  "/phone-broadcasting/*",
  "/terms/*",
  "/privacy/*",
]) {
  if (redirects.includes(route)) pass(`redirect exists: ${route}`);
  else fail(`missing redirect: ${route}`);
}

const failures = results.filter((r) => r.status === "fail");
const warnings = results.filter((r) => r.status === "warn");

for (const r of results) {
  const prefix = r.status === "pass" ? "PASS" : r.status === "warn" ? "WARN" : "FAIL";
  console.log(`${prefix} ${r.label}${r.detail ? ` - ${r.detail}` : ""}`);
}

console.log("");
console.log(`Summary: ${failures.length} failed, ${warnings.length} warnings, ${results.length} checks`);

if (failures.length) process.exit(1);
