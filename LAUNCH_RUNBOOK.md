# Castlink Launch Runbook

This is the keyboard runbook for deploying Castlink.

Assumptions:

- Workspace: `C:\Relay`
- Worker folder: `C:\Relay\api`
- Pages output folder: `C:\Relay\pages_out\pages`
- Production site: `https://castlink.stream`
- Production API: `https://api.castlink.stream`
- Production recording API: `https://recording.castlink.stream`
- Support sender: `support@castlink.stream`

Use PowerShell unless noted.

## 0. Local Preflight

From the repo root:

```powershell
cd C:\Relay
git status --short
```

Check the Worker:

```powershell
cd C:\Relay\api
npm.cmd install
npm.cmd run typecheck
```

Check static page scripts:

```powershell
cd C:\Relay
node -e "const fs=require('fs');const files=['pages_out/pages/admin/index.html','pages_out/pages/watch/index.html','pages_out/pages/broadcast/index.html','pages_out/pages/create/index.html','pages_out/pages/success/index.html','pages_out/pages/index.html','pages_out/pages/support/index.html','pages_out/pages/faq/index.html','pages_out/pages/terms/index.html','pages_out/pages/privacy/index.html','pages_out/pages/ended/index.html'];for(const file of files){const s=fs.readFileSync(file,'utf8');const scripts=[...s.matchAll(/<script(?![^>]*src)[^>]*>([\s\S]*?)<\/script>/gi)].map(m=>m[1]);for(const code of scripts)new Function(code);console.log(file,'ok')}"
```

Or run the bundled local QA script:

```powershell
cd C:\Relay
node tools/local-qa.mjs
```

Check whitespace:

```powershell
cd C:\Relay
git diff --check
```

Regenerate visual previews:

```powershell
cd C:\Relay
node tools/generate-visual-previews.mjs
start C:\Relay\visual-previews\index.html
```

## 1. Confirm Cloudflare Login

```powershell
cd C:\Relay\api
npx.cmd wrangler whoami
```

If not logged in:

```powershell
npx.cmd wrangler login
```

## 2. Database Migrations

Run these SQL files in Neon, in order:

```text
api/sql/001_init.sql
api/sql/002_test_streams.sql
api/sql/003_reports.sql
api/sql/004_realtime.sql
api/sql/005_stream_inventory.sql
api/sql/006_launch_pricing_tiers.sql
api/sql/007_event_cleanup_tracking.sql
api/sql/008_stream_warning_emails.sql
api/sql/009_report_moderation.sql
api/sql/010_test_streams.sql
api/sql/011_ready_emails.sql
api/sql/012_viewer_invites.sql
api/sql/013_recordings.sql
api/sql/014_ops_automation.sql
```

### Option A: Neon Console

1. Open Neon.
2. Open the Castlink production database.
3. Open SQL Editor.
4. Paste each file and run it in order.

To print a migration file for copy/paste:

```powershell
cd C:\Relay
Get-Content api\sql\005_stream_inventory.sql
```

### Option B: psql

If you have `psql` and the Neon connection string:

```powershell
$env:DATABASE_URL="postgresql://USER:PASSWORD@HOST/DB?sslmode=require"
psql $env:DATABASE_URL -f C:\Relay\api\sql\001_init.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\002_test_streams.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\003_reports.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\004_realtime.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\005_stream_inventory.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\006_launch_pricing_tiers.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\007_event_cleanup_tracking.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\008_stream_warning_emails.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\009_report_moderation.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\010_test_streams.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\011_ready_emails.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\012_viewer_invites.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\013_recordings.sql
psql $env:DATABASE_URL -f C:\Relay\api\sql\014_ops_automation.sql
```

Verify core tables/columns:

```powershell
psql $env:DATABASE_URL -c "select to_regclass('public.events') as events, to_regclass('public.stream_inventory') as stream_inventory, to_regclass('public.reports') as reports, to_regclass('public.ops_alerts') as ops_alerts, to_regclass('public.link_recovery_requests') as link_recovery_requests;"
psql $env:DATABASE_URL -c "select column_name from information_schema.columns where table_schema='public' and table_name='events' and column_name in ('disabled','cleanup_started_at','cleanup_completed_at','cleanup_error','warning_email_sent_at','warning_email_error','ready_email_attempts','viewer_invites_attempts','warning_email_attempts','recording_email_attempts') order by column_name;"
```

## 3. Hyperdrive

Confirm `api/wrangler.jsonc` has the correct Hyperdrive binding:

```powershell
cd C:\Relay\api
Select-String -Path wrangler.jsonc -Pattern "HYPERDRIVE|id"
```

If the Hyperdrive ID is wrong, update it in:

```text
C:\Relay\api\wrangler.jsonc
```

Cloudflare dashboard path:

```text
Workers & Pages -> Hyperdrive -> your database config -> copy ID
```

## 4. Postmark

In Postmark:

1. Verify `castlink.stream` or a sender signature for `support@castlink.stream`.
2. Add DNS records at your DNS provider.
3. Wait until Postmark shows the sender/domain as verified.
4. Copy the Server API Token.

Worker vars already expect:

```text
EMAIL_PROVIDER=postmark
EMAIL_FROM=Castlink <support@castlink.stream>
REPORT_ALERT_EMAIL=support@castlink.stream
POSTMARK_MESSAGE_STREAM=outbound
```

## 5. Generate Secrets

Generate stream-key encryption secret:

```powershell
node -e "console.log(Buffer.from(require('crypto').randomBytes(32)).toString('base64'))"
```

Generate admin key:

```powershell
node -e "console.log(require('crypto').randomBytes(24).toString('hex'))"
```

Generate IVS proxy secret if you need a new one:

```powershell
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

Keep these somewhere safe.

## 6. Set Worker Secrets

From the Worker folder:

```powershell
cd C:\Relay\api
```

Run each command and paste the secret value when prompted:

```powershell
npx.cmd wrangler secret put STRIPE_SECRET_KEY
npx.cmd wrangler secret put STRIPE_WEBHOOK_SECRET
npx.cmd wrangler secret put AWS_ACCESS_KEY_ID
npx.cmd wrangler secret put AWS_SECRET_ACCESS_KEY
npx.cmd wrangler secret put STREAMKEY_ENC_KEY_B64
npx.cmd wrangler secret put ADMIN_KEY
npx.cmd wrangler secret put POSTMARK_SERVER_TOKEN
npx.cmd wrangler secret put IVS_PROXY_SECRET
npx.cmd wrangler secret put RECORDING_WEBHOOK_SECRET
```

`IVS_PROXY_SECRET` must match the IVS proxy service `PROXY_SECRET`.
`RECORDING_WEBHOOK_SECRET` must match the recording Worker secret.

List known secret names:

```powershell
npx.cmd wrangler secret list
```

Set recording Worker secrets:

```powershell
cd C:\Relay\recording-worker
npx.cmd wrangler secret put AWS_ACCESS_KEY_ID
npx.cmd wrangler secret put AWS_SECRET_ACCESS_KEY
npx.cmd wrangler secret put POSTMARK_SERVER_TOKEN
npx.cmd wrangler secret put RECORDING_WEBHOOK_SECRET
npx.cmd wrangler secret list
```

Use the same `RECORDING_WEBHOOK_SECRET` value as the API Worker.

## 7. Confirm Worker Vars

Open:

```text
C:\Relay\api\wrangler.jsonc
```

Important production values:

```text
APP_ORIGIN=https://castlink.stream
BRAND_NAME=Castlink
EMAIL_PROVIDER=postmark
EMAIL_FROM=Castlink <support@castlink.stream>
REPORT_ALERT_EMAIL=support@castlink.stream
POSTMARK_MESSAGE_STREAM=outbound
AWS_REGION=ap-northeast-1
IVS_API_ENDPOINT=https://ivs.ap-northeast-1.amazonaws.com
IVS_REALTIME_API_ENDPOINT=https://ivsrealtime.ap-northeast-1.amazonaws.com
STREAM_WARNING_MINUTES=10
DELETE_IVS_ON_EXPIRE=true
HLS_OVERAGE_VIEWER_HOUR_NZD=0
WEBRTC_OVERAGE_PARTICIPANT_HOUR_NZD=0
EXTRA_STREAM_MINUTE_NZD=0
EXTENSION_1H_PRICE_NZD=49
TEST_STREAM_MINUTES=3
TEST_UNUSED_EXPIRY_MINUTES=15
TEST_VIEWER_LIMIT=3
TEST_STREAMS_PER_EMAIL_PER_DAY=2
TEST_USE_INVENTORY=false
IVS_PROXY_BASE=https://ivs-proxy.fly.dev
```

For launch inventory, consider changing:

```text
INVENTORY_MIN_HLS=2
INVENTORY_MIN_RTC=2
INVENTORY_MIN_BOTH=1
INVENTORY_FILL_MAX=1
```

Keep them at `0` if you want to fill inventory manually from `/admin`.

## 8. Deploy Worker

```powershell
cd C:\Relay\api
npm.cmd run typecheck
npx.cmd wrangler deploy
```

Deploy recording Worker:

```powershell
cd C:\Relay\recording-worker
npm.cmd install
npm.cmd run typecheck
npx.cmd wrangler deploy
```

Test Worker health:

```powershell
Invoke-WebRequest -UseBasicParsing https://api.castlink.stream/healthz
Invoke-WebRequest -UseBasicParsing https://api.castlink.stream/api/pricing
Invoke-WebRequest -UseBasicParsing https://recording.castlink.stream/healthz
```

Expected:

- `/healthz` returns `ok`
- `/api/pricing` returns JSON with `ok:true`
- recording `/healthz` returns `ok`

If the custom domain is not connected yet, use the temporary `workers.dev` URL printed by Wrangler.

## 9. Deploy Cloudflare Pages

### Option A: Cloudflare Dashboard

1. Cloudflare -> Workers & Pages -> Pages project.
2. Upload/deploy folder:

```text
C:\Relay\pages_out\pages
```

### Option B: Wrangler Pages Deploy

If your Pages project is already created:

```powershell
cd C:\Relay
npx.cmd wrangler pages deploy pages_out/pages --project-name castlink
```

If the Pages project has a different name, replace `castlink`.

Test Pages:

```powershell
Invoke-WebRequest -UseBasicParsing https://castlink.stream/
Invoke-WebRequest -UseBasicParsing https://castlink.stream/create/
Invoke-WebRequest -UseBasicParsing https://castlink.stream/admin/
```

## 10. Domains

In Cloudflare:

Pages custom domains:

```text
castlink.stream -> Pages project
www.castlink.stream -> optional redirect or Pages alias
```

Worker custom domain:

```text
api.castlink.stream -> stream-platform-api Worker
recording.castlink.stream -> castlink-recording-worker Worker
```

Redirect rules:

```text
castlink.co.nz -> https://castlink.stream
www.castlink.co.nz -> https://castlink.stream
www.castlink.stream -> https://castlink.stream
```

PowerShell checks:

```powershell
Resolve-DnsName castlink.stream
Resolve-DnsName api.castlink.stream
Resolve-DnsName recording.castlink.stream
Resolve-DnsName castlink.co.nz
Invoke-WebRequest -UseBasicParsing https://castlink.stream/
Invoke-WebRequest -UseBasicParsing https://api.castlink.stream/healthz
Invoke-WebRequest -UseBasicParsing https://recording.castlink.stream/healthz
```

## 11. Stripe Webhook

In Stripe dashboard:

1. Developers -> Webhooks.
2. Add endpoint:

```text
https://api.castlink.stream/api/stripe/webhook
```

3. Select event:

```text
checkout.session.completed
```

4. Copy the signing secret.
5. Set it:

```powershell
cd C:\Relay\api
npx.cmd wrangler secret put STRIPE_WEBHOOK_SECRET
```

Redeploy after changing secrets if needed:

```powershell
npx.cmd wrangler deploy
```

## 12. Run Admin Launch Checks

Open:

```powershell
start https://castlink.stream/admin
```

Enter `ADMIN_KEY`.

Click:

```text
Run checks
```

Expected:

- `0 failed`
- Some warnings are acceptable before inventory is filled.

If you want to call the endpoint directly:

```powershell
$adminKey="PASTE_ADMIN_KEY"
Invoke-RestMethod -Headers @{ "x-relay-admin-key"=$adminKey } https://api.castlink.stream/api/admin/launch-checks
```

## 13. Fill Pre-Made Inventory

Admin UI:

```text
/admin -> Fill inventory
```

Recommended first manual fill:

```text
HLS target: 2
WebRTC target: 2
Both target: 1
```

API commands:

```powershell
$adminKey="PASTE_ADMIN_KEY"
$headers=@{ "x-relay-admin-key"=$adminKey; "content-type"="application/json" }

Invoke-RestMethod -Method Post -Headers $headers -Body '{"mode":"hls","target_available":2}' https://api.castlink.stream/api/admin/inventory/fill
Invoke-RestMethod -Method Post -Headers $headers -Body '{"mode":"rtc","target_available":2}' https://api.castlink.stream/api/admin/inventory/fill
Invoke-RestMethod -Method Post -Headers $headers -Body '{"mode":"both","target_available":1}' https://api.castlink.stream/api/admin/inventory/fill
```

Check inventory:

```powershell
Invoke-RestMethod -Headers @{ "x-relay-admin-key"=$adminKey } https://api.castlink.stream/api/admin/inventory
```

## 14. End-To-End Test Purchases

Run at least one checkout for each mode:

```text
HLS
WebRTC
Both
```

Before paid checkout tests, run one free test stream:

```powershell
start https://castlink.stream/create#test
```

Confirm:

1. Test stream creates without Stripe checkout.
2. Success page opens with test wording.
3. Test event email arrives.
4. Broadcast/watch links work.
5. The stream expires after the configured test duration once started.
6. The test cannot buy a +1 hour extension.

Open:

```powershell
start https://castlink.stream/create
```

For each purchase:

1. Use your email.
2. Choose the mode.
3. Pay through Stripe.
4. Confirm redirect to `/success`.
5. Confirm event email arrives.
6. Open broadcast link.
7. Open watch link on another browser/device.
8. Click Preview.
9. Click Go Live.
10. Confirm watch page receives media.
11. Generate OBS token and confirm setup text appears.
12. Use Buy +1 hour during the live window.
13. Click Finish Stream.
14. Confirm admin shows event status and usage.

For HLS or Both purchases with recording enabled:

1. Confirm `https://recording.castlink.stream/healthz` returns `ok`.
2. Finish the stream and wait for IVS recording and MP4 conversion.
3. Open the recording email link or `/success/?event=EVENT_ID&key=BROADCAST_KEY&recording=1`.
4. Confirm the page calls `recording.castlink.stream`, shows recording status, and starts the MP4 download once ready.

Admin events:

```powershell
$adminKey="PASTE_ADMIN_KEY"
Invoke-RestMethod -Headers @{ "x-relay-admin-key"=$adminKey } https://api.castlink.stream/api/admin/events
```

## 15. Report Test

On a watch page:

1. Click `Report`.
2. Submit a report.
3. Confirm Postmark sends alert to `support@castlink.stream`.
4. Open admin and close the report.

API check:

```powershell
$adminKey="PASTE_ADMIN_KEY"
Invoke-RestMethod -Headers @{ "x-relay-admin-key"=$adminKey } https://api.castlink.stream/api/admin/reports
```

## 16. Ending-Soon Email Test

Use a short test event or temporarily lower a paid event expiry in Neon.

Example with `psql`:

```powershell
$env:DATABASE_URL="postgresql://USER:PASSWORD@HOST/DB?sslmode=require"
$eventId="PASTE_EVENT_ID"
psql $env:DATABASE_URL -c "update public.events set expires_at = now() + interval '9 minutes', warning_email_sent_at = null, warning_email_error = null where id = '$eventId';"
```

Wait for the Worker cron, or trigger scheduled behavior from Cloudflare dashboard if available.

Confirm:

- warning email arrives
- warning email link opens Stripe extension checkout
- extension returns to same broadcast page

## 17. Cleanup Test

Finish an event from broadcast page or admin.

Then check:

```powershell
$adminKey="PASTE_ADMIN_KEY"
Invoke-RestMethod -Headers @{ "x-relay-admin-key"=$adminKey } https://api.castlink.stream/api/admin/events
```

If cleanup did not complete, retry from admin or:

```powershell
$eventId="PASTE_EVENT_ID"
Invoke-RestMethod -Method Post -Headers @{ "x-relay-admin-key"=$adminKey } "https://api.castlink.stream/api/admin/events/$eventId/cleanup"
```

## 18. Common Fix Commands

Redeploy Worker:

```powershell
cd C:\Relay\api
npx.cmd wrangler deploy
```

Redeploy recording Worker:

```powershell
cd C:\Relay\recording-worker
npx.cmd wrangler deploy
```

Redeploy Pages:

```powershell
cd C:\Relay
npx.cmd wrangler pages deploy pages_out/pages --project-name castlink
```

Check Worker logs:

```powershell
cd C:\Relay\api
npx.cmd wrangler tail
```

Check pricing:

```powershell
Invoke-RestMethod https://api.castlink.stream/api/pricing
```

Check health:

```powershell
Invoke-WebRequest -UseBasicParsing https://api.castlink.stream/healthz
```

Open key pages:

```powershell
start https://castlink.stream/
start https://castlink.stream/create/
start https://castlink.stream/admin/
start https://castlink.stream/faq/
start https://castlink.stream/support/
```

## 19. Stop/Go Decision

Do not launch publicly until:

- Admin launch checks show `0 failed`.
- Stripe purchase works.
- Postmark event email arrives.
- Browser broadcast works.
- OBS token flow works.
- Watch page receives media.
- +1 hour extension works.
- Finish stream works.
- Report alert email works.
- Admin can disable an event.
- Cleanup completes or can be retried.
