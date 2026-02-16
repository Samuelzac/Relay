# Self-Serve Streaming Platform (Test Bundle)

This folder contains:
- `api/` Cloudflare Worker backend (Stripe + IVS + seat limiting + broadcaster lock + expiry cron)
- `pages/` Static frontend pages (create, success, watch, broadcast, ended)

## Prereqs (Windows)
- Node.js LTS
- Wrangler CLI: `npm i -g wrangler`
- A Cloudflare account
- An AWS account with IVS permissions (Sydney region)
- A Stripe account (test mode)
- Neon Postgres + Cloudflare Hyperdrive binding

## 1) Database
Run `sql/001_init.sql`, `sql/002_test_streams.sql`, and `sql/003_reports.sql` against your Neon database.

## 2) Backend (Cloudflare Worker)
From the `api` folder:

1. Install deps:
   ```bash
   npm install
   ```

2. Edit `wrangler.jsonc`:
   - set `hyperdrive[].id` to your Hyperdrive ID
   - set `vars.APP_ORIGIN` to your Pages domain (or `http://127.0.0.1:8788` for local Pages testing)
   - keep AWS region `ap-southeast-2`

3. Set secrets (Wrangler):
   ```bash
   wrangler secret put STRIPE_SECRET_KEY
   wrangler secret put STRIPE_WEBHOOK_SECRET
   wrangler secret put AWS_ACCESS_KEY_ID
   wrangler secret put AWS_SECRET_ACCESS_KEY
   # 32-byte key, base64:
   wrangler secret put STREAMKEY_ENC_KEY_B64
   ```

   Tip to generate STREAMKEY_ENC_KEY_B64:
   - Use PowerShell:
     ```powershell
     $bytes = New-Object byte[] 32
     (New-Object System.Security.Cryptography.RNGCryptoServiceProvider).GetBytes($bytes)
     [Convert]::ToBase64String($bytes)
     ```

4. Run locally:
   ```bash
   npm run dev
   ```

## 3) Stripe webhook (local)
Use Stripe CLI to forward webhooks to your worker:
```bash
stripe listen --forward-to http://127.0.0.1:8787/api/stripe/webhook
```
Then set the printed webhook secret into `STRIPE_WEBHOOK_SECRET`.

## 4) Frontend (Cloudflare Pages)
The `pages/` folder is plain static HTML.

### Quick local test (no Pages)
Run a static server on `pages/` (any method). Example:
```bash
npx serve pages -l 8788
```

Then:
- Set `APP_ORIGIN` in `wrangler.jsonc` to `http://127.0.0.1:8788`
- In each HTML page, set `const API = "http://127.0.0.1:8787"` (wrangler dev)

### Deploy
- Create a Pages project pointing at `pages/`
- Update `API` constants to your Worker deployed URL
- Update HOME_URL / BRAND_NAME if you want

## URLs
- Create: `/index.html`
- Success: `/success/:eventId?st=...` (Stripe sends you here)
- Watch: `/watch/:eventId`
- Broadcast: `/broadcast/:eventId?key=SECRET`
- Ended: `/ended/:eventId`

## Notes
- Broadcaster override: opening the broadcast link on another device will take over within ~12 seconds.
- Viewer cap: enforced via a Durable Object (45s inactivity timeout).
- Expiry: cron marks events expired; backend denies new sessions once expired.


## 2-minute test stream
After payment, the Success page includes a **Start 2-minute test** button. It creates a temporary IVS channel and opens a test broadcast + test watch page. This does **not** start the real event timer.


## Reports + basic admin
Set `ADMIN_KEY` in Worker secrets. Visit `/admin?key=YOUR_ADMIN_KEY` to view reports and disable events.


## Website
Homepage is at `/`.
Event purchase / creation is at `/create`.
