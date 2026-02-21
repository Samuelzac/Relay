# Relay — end-to-end setup (keep your existing stack)

You already have:
- Cloudflare Pages (frontend) deployed
- Cloudflare Worker `stream-platform-api` deployed
- Hyperdrive → Neon Postgres connected
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
```

`STREAMKEY_ENC_KEY_B64` must be **32 bytes** (base64). Generate one:
```bash
node -e "console.log(Buffer.from(require('crypto').randomBytes(32)).toString('base64'))"
```

### Vars (already in wrangler.jsonc, adjust as you like)
- `APP_ORIGIN` (your Pages URL)
- `AWS_REGION` = `ap-southeast-2`
- `IVS_API_ENDPOINT` = `https://ivs.ap-southeast-2.amazonaws.com`
- `IVS_REALTIME_API_ENDPOINT` = `https://ivsrealtime.ap-southeast-2.amazonaws.com`
- Tier prices:
  - `STANDARD_PRICE_NZD` (cents)
  - `EXTENDED_PRICE_NZD` (cents)
- Mode add-ons (cents):
  - `HLS_ADDON_NZD` (default 0)
  - `WEBRTC_ADDON_NZD`
  - `BOTH_ADDON_NZD`
- `DELETE_IVS_ON_EXPIRE` = `false` (set true if you want cron to delete channels/stages after expiry)

---

## 3) Stripe webhook

In Stripe dashboard:
- Create a webhook endpoint pointing at:

`https://stream-platform-api.<your-domain>/api/stripe/webhook`

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
- `GET /` should return `Relay API OK`
- `GET /api/pricing` should return JSON

---

## 5) Deploy Cloudflare Pages

The Pages build folder is `Relay/pages_out/pages`

If you already have Pages connected to a repo, just replace files and push.

Key pages:
- `/create`
- `/success/:eventId?st=...`
- `/broadcast/:eventId?key=...`  (private)
- `/watch/:eventId?key=...`      (share link)

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
