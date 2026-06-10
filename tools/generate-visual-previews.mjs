import fs from "node:fs/promises";
import fsSync from "node:fs";
import http from "node:http";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const root = process.cwd();
const pagesRoot = path.join(root, "pages_out", "pages");
const outDir = path.join(root, "visual-previews");
const port = Number(process.env.CASTLINK_PREVIEW_PORT || 8789);

const chromeCandidates = [
  process.env.CHROME_PATH,
  "C:/Program Files/Google/Chrome/Application/chrome.exe",
  "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe",
  "C:/Program Files/Microsoft/Edge/Application/msedge.exe",
  "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
].filter(Boolean);

const playwrightCandidates = [
  "playwright",
  "C:/Users/Bidr/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/.pnpm/playwright@1.60.0/node_modules/playwright",
  "C:/Users/Bidr/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright",
];

function loadPlaywright() {
  for (const candidate of playwrightCandidates) {
    try {
      return require(candidate);
    } catch {}
  }
  throw new Error("Playwright is not available. Install it with `npm install -D playwright` or run this in Codex desktop runtime.");
}

function findChrome() {
  for (const candidate of chromeCandidates) {
    if (candidate && fsSync.existsSync(candidate)) return candidate;
  }
  return null;
}

function contentType(file) {
  if (file.endsWith(".html")) return "text/html; charset=utf-8";
  if (file.endsWith(".js")) return "text/javascript; charset=utf-8";
  if (file.endsWith(".css")) return "text/css; charset=utf-8";
  if (file.endsWith(".png")) return "image/png";
  return "application/octet-stream";
}

function startServer() {
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url || "/", `http://127.0.0.1:${port}`);
      let rel = decodeURIComponent(url.pathname.replace(/^\/+/, ""));
      if (!rel || rel.endsWith("/")) rel += "index.html";
      const full = path.normalize(path.join(pagesRoot, rel));
      if (!full.startsWith(path.normalize(pagesRoot))) {
        res.writeHead(403);
        res.end("Forbidden");
        return;
      }
      const data = await fs.readFile(full);
      res.writeHead(200, { "content-type": contentType(full) });
      res.end(data);
    } catch {
      res.writeHead(404, { "content-type": "text/plain" });
      res.end("Not found");
    }
  });

  return new Promise((resolve) => {
    server.listen(port, "127.0.0.1", () => resolve(server));
  });
}

const mockPricing = {
  ok: true,
  currency: "nzd",
  pricing_model: "base_price_with_optional_extension",
  tiers: {
    hls: {
      "1": { hours: 1, price_nzd: 5900, included: { hls_viewer_hours: 50 } },
      "3": { hours: 3, price_nzd: 12900, included: { hls_viewer_hours: 150 } },
      "8": { hours: 8, price_nzd: 24900, included: { hls_viewer_hours: 250 } },
    },
    rtc: {
      "1": { hours: 1, price_nzd: 7900, included: { rtc_participant_hours: 25 } },
      "2": { hours: 2, price_nzd: 14900, included: { rtc_participant_hours: 80 } },
    },
    both: {
      "3": { hours: 3, price_nzd: 19900, included: { hls_viewer_hours: 150, rtc_participant_hours: 50 } },
      "8": { hours: 8, price_nzd: 34900, included: { hls_viewer_hours: 300, rtc_participant_hours: 120 } },
    },
  },
  extensions: { one_hour_nzd: 4900 },
};

const pages = [
  ["01-home-desktop", "/", 1440, 1000],
  ["02-create-desktop", "/create/", 1440, 1000],
  ["03-admin-launch-checks-desktop", "/admin/", 1440, 1100],
  ["04-watch-report-desktop", "/watch/?event=demo&key=demo", 1440, 1000],
  ["05-broadcast-desktop", "/broadcast/?event=demo&key=demo", 1440, 1000],
  ["06-faq-desktop", "/faq/", 1440, 1000],
  ["07-support-desktop", "/support/", 1440, 1000],
  ["08-home-mobile", "/", 390, 900],
  ["09-create-mobile", "/create/", 390, 900],
  ["10-watch-mobile", "/watch/?event=demo&key=demo", 390, 900],
];

await fs.mkdir(outDir, { recursive: true });

const { chromium } = loadPlaywright();
const executablePath = findChrome();
if (!executablePath) throw new Error("Chrome or Edge was not found. Set CHROME_PATH to a browser executable.");

const server = await startServer();
const browser = await chromium.launch({ headless: true, executablePath });

try {
  for (const [name, route, width, height] of pages) {
    const page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: 1 });
    await page.route("**/api/pricing", (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(mockPricing) })
    );
    await page.route("https://api.castlink.stream/api/pricing", (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(mockPricing) })
    );
    await page.goto(`http://127.0.0.1:${port}${route}`, { waitUntil: "domcontentloaded", timeout: 15000 });
    await page.waitForTimeout(900);
    if (name.includes("admin")) await page.locator("#key").fill("demo-admin-key");
    const target = path.join(outDir, `${name}.png`);
    const temp = path.join(outDir, `${name}.${Date.now()}.tmp.png`);
    await page.screenshot({ path: temp, fullPage: true });
    await fs.rm(target, { force: true }).catch(() => {});
    await fs.rename(temp, target).catch(async () => {
      const fallback = path.join(outDir, `${name}.${Date.now()}.png`);
      await fs.rename(temp, fallback);
      console.log(`target locked, wrote ${path.basename(fallback)}`);
    });
    await page.close();
    console.log(`${name}.png`);
  }
} finally {
  await browser.close().catch(() => {});
  server.close();
}

console.log(`Visual previews written to ${outDir}`);
