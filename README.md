# Castlink

This folder contains:
- `api/` Cloudflare Worker backend (Stripe + IVS + seat limiting + broadcaster lock + expiry cron)
- `pages_out/pages/` static frontend pages (home, create, success, watch, broadcast, admin, FAQ, support, terms, privacy)

Production domains:
- Frontend: `https://castlink.stream`
- API Worker: `https://api.castlink.stream`
- NZ redirect/trust domain: `https://castlink.co.nz` -> `https://castlink.stream`

## Prereqs (Windows)
- Node.js LTS
- Wrangler CLI: `npm i -g wrangler`
- A Cloudflare account
- An AWS account with IVS permissions matching `api/wrangler.jsonc`
- A Stripe account (test mode)
- Neon Postgres + Cloudflare Hyperdrive binding

## 1) Database
Run `api/sql/001_init.sql` through `api/sql/010_test_streams.sql` against your Neon database, in order.

## 2) Backend (Cloudflare Worker)
From the `api` folder:

1. Install deps:
   ```bash
   npm install
   ```

2. Edit `wrangler.jsonc`:
   - set `hyperdrive[].id` to your Hyperdrive ID
   - set `vars.APP_ORIGIN` to `https://castlink.stream` for production
   - set `vars.EMAIL_FROM` to a verified sender such as `Castlink <support@castlink.stream>`

3. Set secrets (Wrangler):
   ```bash
   wrangler secret put STRIPE_SECRET_KEY
   wrangler secret put STRIPE_WEBHOOK_SECRET
   wrangler secret put AWS_ACCESS_KEY_ID
   wrangler secret put AWS_SECRET_ACCESS_KEY
   wrangler secret put ADMIN_KEY
   wrangler secret put POSTMARK_SERVER_TOKEN
   wrangler secret put IVS_PROXY_SECRET
   # 32-byte key, base64:
   wrangler secret put STREAMKEY_ENC_KEY_B64
   ```

   `IVS_PROXY_SECRET` must match the IVS proxy service's `PROXY_SECRET`.

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

Production webhook:
```txt
https://api.castlink.stream/api/stripe/webhook
```

## 4) Frontend (Cloudflare Pages)
The `pages_out/pages/` folder is plain static HTML.

### Quick local test (no Pages)
Run a static server on `pages_out/pages/` (any method). Example:
```bash
npx serve pages_out/pages -l 8788
```

Then:
- Set `APP_ORIGIN` in `wrangler.jsonc` to `http://127.0.0.1:8788`
- Use the admin page API override or `localStorage.RELAY_API_BASE` for local Worker testing.

### Deploy
- Create a Pages project pointing at `pages_out/pages/`
- Add `castlink.stream` as the primary custom domain
- Optionally add `www.castlink.stream`
- Add `api.castlink.stream` as the Worker custom domain
- Set Cloudflare Bulk Redirects or Redirect Rules for `castlink.co.nz` and `www.castlink.co.nz` to `https://castlink.stream`

## URLs
- Home: `/`
- Create: `/create`
- Success: `/success/:eventId?st=...` (Stripe sends you here)
- Watch: `/watch/:eventId`
- Broadcast: `/broadcast/:eventId?key=SECRET`
- Ended: `/ended/:eventId`
- Admin: `/admin`
- FAQ: `/faq`
- Support: `/support`

## Notes
- Broadcaster override: opening the broadcast link on another device will take over within ~12 seconds.
- Viewer cap: enforced via a Durable Object (45s inactivity timeout).
- Viewer upgrades: hosts can buy +100, +250, or +500 concurrent viewer capacity during a paid stream. Same watch link continues working.
- Expiry: cron marks events expired; backend denies new sessions once expired. Paid events must start within `PAID_UNUSED_EXPIRY_DAYS` days, currently 7.
- Quality: launch streams target 720p HD at up to 30fps with 128kbps stereo audio. Do not market the current configuration as 1080p.
- Stream inventory: pre-created IVS slots can be kept warm with `INVENTORY_MIN_HLS`, `INVENTORY_MIN_RTC`, and `INVENTORY_MIN_BOTH`. Paid events claim a matching available slot before falling back to live provisioning.
- Free tests: `/create#test` creates checkoutless test events through `/api/test-events`. Test events use the normal broadcast/watch flow, expire quickly, are rate-limited, and do not use paid inventory by default.

## Reports + basic admin
Set `ADMIN_KEY` in Worker secrets. Visit `/admin?key=YOUR_ADMIN_KEY` to view reports, usage, inventory, and event controls.


## Website
Homepage is at `/`.
Event purchase / creation is at `/create`.
