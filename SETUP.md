# Castlink - end-to-end setup (keep your existing stack)

You already have:
- Cloudflare Pages (frontend) deployed
- Castlink Worker (`stream-platform-api`) deployed
- Hyperdrive connected to Neon Postgres
- Stripe Checkout working

This repo version finishes:
- Worker root handler (no more hang)
- WebRTC (Amazon IVS Real-Time stages) provisioning
- Optional HLS (Amazon IVS channels) provisioning for OBS / RTMPS
- Pricing split by delivery mode (WebRTC vs HLS vs Both)

---

## 1) Neon DB — run migrations

Run these SQL files in order (Neon SQL editor is fine):

1. `api/sql/001_init.sql` (if you haven't already)
2. `api/sql/004_realtime.sql`  ✅ adds:
   - `broadcast_key`
   - `rtc_stage_arn`, `rtc_stage_endpoints`
   - `rtc_enabled`, `hls_enabled`
3. `api/sql/005_stream_inventory.sql` adds:
   - pre-provisioned stream inventory
   - fast assignment of ready IVS resources after checkout
4. `api/sql/006_launch_pricing_tiers.sql` adds:
   - 1h, 2h, 3h, and 8h launch pricing tiers
5. `api/sql/007_event_cleanup_tracking.sql` adds:
   - cleanup tracking for expired stream resources
6. `api/sql/008_stream_warning_emails.sql` adds:
   - one-time stream ending warning email tracking
7. `api/sql/009_report_moderation.sql` adds:
   - report moderation fields and status tracking
8. `api/sql/010_test_streams.sql` adds:
   - checkoutless 3-minute test stream tracking and rate limiting

If your production DB already has the IVS columns, `004_realtime.sql` is safe (uses `add column if not exists`).

---

## 2) Cloudflare Worker secrets & vars

From `C:\Relay\api`:

### Secrets (must be set)
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

`STREAMKEY_ENC_KEY_B64` must be **32 bytes** (base64). Generate one:
```bash
node -e "console.log(Buffer.from(require('crypto').randomBytes(32)).toString('base64'))"
```

`IVS_PROXY_SECRET` must match the `PROXY_SECRET` set on the IVS proxy service.

### Vars (already in wrangler.jsonc, adjust as you like)
- `APP_ORIGIN` = `https://castlink.stream`
- `BRAND_NAME`
- `EMAIL_PROVIDER` = `postmark`
- `EMAIL_FROM` (verified sender, e.g. `Castlink <support@castlink.stream>`)
- `REPORT_ALERT_EMAIL` = `support@castlink.stream`
- `POSTMARK_MESSAGE_STREAM` = `outbound`
- `STREAM_WARNING_MINUTES` = `10`
- `TEST_STREAM_MINUTES` = `3`
- `TEST_UNUSED_EXPIRY_MINUTES` = `15`
- `TEST_VIEWER_LIMIT` = `3`
- `TEST_STREAMS_PER_EMAIL_PER_DAY` = `2`
- `TEST_USE_INVENTORY` = `false`
- `AWS_REGION` = `ap-northeast-1`
- `IVS_API_ENDPOINT` = `https://ivs.ap-northeast-1.amazonaws.com`
- `IVS_REALTIME_API_ENDPOINT` = `https://ivsrealtime.ap-northeast-1.amazonaws.com`
  - If you choose another IVS region later, update all three values together before deploying.
- Tier prices are NZD dollar amounts:
  - `HLS_1H_PRICE_NZD`
  - `HLS_3H_PRICE_NZD`
  - `HLS_8H_PRICE_NZD`
  - `WEBRTC_1H_PRICE_NZD`
  - `WEBRTC_2H_PRICE_NZD`
  - `BOTH_3H_PRICE_NZD`
  - `BOTH_8H_PRICE_NZD`
- Legacy fallback prices:
  - `STANDARD_PRICE_NZD`
  - `EXTENDED_PRICE_NZD`
- Mode add-ons are NZD dollar amounts:
  - `HLS_ADDON_NZD` (default 0)
  - `WEBRTC_ADDON_NZD`
  - `BOTH_ADDON_NZD`
- `DELETE_IVS_ON_EXPIRE` = `true` (cron deletes expired event channels/stages after stopping any active composition)
- `INVENTORY_MIN_HLS` = `0` (set to e.g. `3` to keep HLS slots warm)
- `INVENTORY_MIN_RTC` = `0`
- `INVENTORY_MIN_BOTH` = `0`
- `INVENTORY_FILL_MAX` = `2` (max slots to create per refill run)

Expired events are cleaned up by cron. The admin page can also retry cleanup for expired events.

---

## 3) Stripe webhook

In Stripe dashboard:
- Create a webhook endpoint pointing at:

`https://api.castlink.stream/api/stripe/webhook`

Events:
- `checkout.session.completed`

Copy the webhook signing secret into `STRIPE_WEBHOOK_SECRET` (wrangler secret).

---

## 4) Deploy the Worker

```bash
cd C:\Relay\api
npm install
wrangler deploy
```

Test:
- `GET /` should return `Castlink API OK`
- `GET /api/pricing` should return JSON

Production API URL:
- `https://api.castlink.stream`

---

## 5) Deploy Cloudflare Pages

The Pages build folder is `Relay/pages_out/pages`

If you already have Pages connected to a repo, just replace files and push.

Key pages:
- `https://castlink.stream`
- `/create`
- `/success/:eventId?st=...`
- `/broadcast/:eventId?key=...`  (private)
- `/watch/:eventId?key=...`      (share link)
- `/admin`                       (admin key required)

Custom domains:
- Primary Pages domain: `castlink.stream`
- Optional Pages alias: `www.castlink.stream`
- NZ redirect/trust domain: `castlink.co.nz` -> `https://castlink.stream`
- Optional NZ www redirect: `www.castlink.co.nz` -> `https://castlink.stream`

Use Cloudflare Bulk Redirects or Redirect Rules for domain-level redirects. Pages `_redirects` handles path routing inside the app, but Cloudflare docs do not support domain-level redirects in `_redirects`.

The admin page can view event usage, copy links, resend event emails, finish/disable events, review viewer reports, retry cleanup, and manage pre-made stream inventory.

---

## 6) How it works (flow)

1. User goes to `/create` and pays
2. Success page fetches:
   `GET /api/events/:id/links?st=...`
3. Success page shows:
   - Watch link (share)
   - Broadcast link (keep private)
4. Phone broadcaster opens broadcast link and taps **Go Live**
   - Worker provisions IVS Real-Time **stage** (WebRTC) if needed
   - Worker mints a **host** participant token
5. Viewers open watch link
   - Worker mints **viewer** participant tokens
   - Watch page joins stage and shows video

If the event was purchased with HLS:
- Broadcast can also provision HLS credentials later via:
  `POST /api/events/:id/hls/provision?key=...`
  (for OBS / RTMPS)

---

## 7) Where to find YOUR_EVENT_ID

It’s the UUID created in the database when you created the event.

Easiest:
- Create an event → Stripe success redirect lands you on `/success/<EVENT_ID>?st=...`
- That `<EVENT_ID>` in the URL is your event id.
