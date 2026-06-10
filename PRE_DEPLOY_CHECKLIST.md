# Castlink Pre-Deploy Checklist

Use this as the final keyboard checklist before production deploy.

For full command-by-command steps, use `LAUNCH_RUNBOOK.md`.

## Local Commands

```powershell
cd C:\Relay\api
npm.cmd install
npm.cmd run typecheck
npx.cmd wrangler whoami
```

```powershell
cd C:\Relay
git diff --check
node tools/local-qa.mjs
node tools/generate-visual-previews.mjs
start C:\Relay\visual-previews\index.html
```

## Database

Run these Neon migrations in order:

1. `api/sql/001_init.sql`
2. `api/sql/002_test_streams.sql`
3. `api/sql/003_reports.sql`
4. `api/sql/004_realtime.sql`
5. `api/sql/005_stream_inventory.sql`
6. `api/sql/006_launch_pricing_tiers.sql`
7. `api/sql/007_event_cleanup_tracking.sql`
8. `api/sql/008_stream_warning_emails.sql`
9. `api/sql/009_report_moderation.sql`
10. `api/sql/010_test_streams.sql`

## Cloudflare Worker Secrets

Set these in the Worker dashboard or with Wrangler:

```bash
wrangler secret put STRIPE_SECRET_KEY
wrangler secret put STRIPE_WEBHOOK_SECRET
wrangler secret put AWS_ACCESS_KEY_ID
wrangler secret put AWS_SECRET_ACCESS_KEY
wrangler secret put STREAMKEY_ENC_KEY_B64
wrangler secret put ADMIN_KEY
wrangler secret put POSTMARK_SERVER_TOKEN
wrangler secret put IVS_PROXY_SECRET
```

PowerShell/Wrangler version:

```powershell
cd C:\Relay\api
npx.cmd wrangler secret put STRIPE_SECRET_KEY
npx.cmd wrangler secret put STRIPE_WEBHOOK_SECRET
npx.cmd wrangler secret put AWS_ACCESS_KEY_ID
npx.cmd wrangler secret put AWS_SECRET_ACCESS_KEY
npx.cmd wrangler secret put STREAMKEY_ENC_KEY_B64
npx.cmd wrangler secret put ADMIN_KEY
npx.cmd wrangler secret put POSTMARK_SERVER_TOKEN
npx.cmd wrangler secret put IVS_PROXY_SECRET
npx.cmd wrangler secret list
```

`IVS_PROXY_SECRET` must match the IVS proxy service `PROXY_SECRET`.

## Worker Vars

Confirm these production values:

- `APP_ORIGIN=https://castlink.stream`
- `EMAIL_PROVIDER=postmark`
- `EMAIL_FROM=Castlink <support@castlink.stream>`
- `REPORT_ALERT_EMAIL=support@castlink.stream`
- `POSTMARK_MESSAGE_STREAM=outbound`
- `PAID_UNUSED_EXPIRY_DAYS=7`
- `STREAM_WARNING_MINUTES=10`
- `VIEWER_UPGRADE_100_NZD=39`
- `VIEWER_UPGRADE_250_NZD=79`
- `VIEWER_UPGRADE_500_NZD=149`
- `VIEWER_CAP_WARNING_PERCENT=90`
- `TEST_STREAM_MINUTES=3`
- `TEST_UNUSED_EXPIRY_MINUTES=15`
- `TEST_VIEWER_LIMIT=3`
- `TEST_STREAMS_PER_EMAIL_PER_DAY=2`
- `TEST_USE_INVENTORY=false`
- `DELETE_IVS_ON_EXPIRE=true`
- pricing vars are NZD dollar values, not cents
- inventory minimums are set for launch

## Domains

- Pages custom domain: `castlink.stream`
- Worker custom domain: `api.castlink.stream`
- Redirect `castlink.co.nz` to `https://castlink.stream`
- Redirect `www.castlink.co.nz` to `https://castlink.stream`
- Optional redirect `www.castlink.stream` to `https://castlink.stream`

## Email

- Verify `castlink.stream` or `support@castlink.stream` in Postmark.
- Confirm `POSTMARK_SERVER_TOKEN` is the Server API Token.
- Send a test event email.
- Submit a watch-page report and confirm the alert email arrives.
- Test the ending-soon email on a short event.

## Stripe

- Set webhook endpoint: `https://api.castlink.stream/api/stripe/webhook`
- Subscribe to `checkout.session.completed`.
- Test normal purchase.
- Test +1 hour extension.
- Test extension link from ending-soon email.

## Deploy

From `C:\Relay\api`:

```bash
npm install
npm run typecheck
wrangler deploy
```

PowerShell/Wrangler version:

```powershell
cd C:\Relay\api
npm.cmd install
npm.cmd run typecheck
npx.cmd wrangler deploy
```

Deploy `pages_out/pages` to Cloudflare Pages.

```powershell
cd C:\Relay
npx.cmd wrangler pages deploy pages_out/pages --project-name castlink
```

## Admin Launch Checks

```powershell
$adminKey="PASTE_ADMIN_KEY"
Invoke-RestMethod -Headers @{ "x-relay-admin-key"=$adminKey } https://api.castlink.stream/api/admin/launch-checks
```

Open admin:

```powershell
start https://castlink.stream/admin
```

## Smoke Tests

- `/` loads.
- `/create` loads pricing.
- Free 3-minute test stream creates without Stripe checkout.
- Create page and FAQ describe current quality as 720p HD target, not 1080p.
- Automated real-stream smoke test passes after Worker deploy:
  ```powershell
  node tools/smoke-test-stream.mjs --email you@example.com --mode rtc
  node tools/smoke-test-stream.mjs --email you@example.com --mode hls
  node tools/smoke-test-stream.mjs --email you@example.com --mode both
  ```
- HLS checkout works.
- WebRTC checkout works.
- Both checkout works.
- Success page shows links.
- Broadcast page can go live.
- Watch page receives stream.
- OBS token flow works.
- Admin can view events, usage, reports, and inventory.
- Admin can resend email.
- Admin can finish/disable event.
- Expired event cleanup runs or can be retried.
