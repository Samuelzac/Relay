# Castlink Staging Setup

Staging is deliberately separate from production:

- Frontend: `https://staging.castlink.stream`
- API Worker: `https://api.staging.castlink.stream`
- Recording Worker: `https://recording.staging.castlink.stream`
- Pages project: `castlink-staging`
- API Worker name: `stream-platform-api-staging`
- Recording Worker name: `castlink-recording-worker-staging`
- Database: separate Neon/Postgres database
- Stripe: test mode keys and test webhook

## 1. Create the Staging Database

In Neon, create a separate staging database/project. Do not reuse production.

Copy the staging pooled connection string. It should look like:

```txt
postgresql://USER:PASSWORD@HOST/DB?sslmode=require
```

## 2. Run Migrations

If `psql` is installed:

```powershell
$env:DATABASE_URL = Read-Host "Paste staging Neon connection string"

Get-ChildItem C:\Relay\api\sql\*.sql |
  Sort-Object Name |
  ForEach-Object {
    psql $env:DATABASE_URL -v ON_ERROR_STOP=1 -f $_.FullName
  }
```

If `psql` is not installed, run the files in `C:\Relay\api\sql` in order through the Neon SQL editor.

## 3. Create Staging Hyperdrive

Current staging Hyperdrive ID:

```txt
ac0741140e02450caa6b243cb7e2fe51
```

The staging config files already point at this ID.

To recreate it later:

```powershell
$env:DATABASE_URL = Read-Host "Paste staging Neon connection string"

npx.cmd wrangler hyperdrive create castlink-staging-db `
  --connection-string $env:DATABASE_URL `
  --caching-disabled `
  --origin-connection-limit 5
```

Copy the Hyperdrive `id` from the output.

Replace `REPLACE_WITH_STAGING_HYPERDRIVE_ID` in:

- `C:\Relay\api\wrangler.staging.jsonc`
- `C:\Relay\recording-worker\wrangler.staging.jsonc`

## 4. Set API Worker Secrets

Use Stripe test-mode values for staging.

Preferred prompt-based helper:

```powershell
cd C:\Relay
.\tools\set-staging-secrets.ps1
```

Manual API commands:

```powershell
cd C:\Relay\api

npx.cmd wrangler secret put STRIPE_SECRET_KEY --config wrangler.staging.jsonc
npx.cmd wrangler secret put STRIPE_WEBHOOK_SECRET --config wrangler.staging.jsonc
npx.cmd wrangler secret put AWS_ACCESS_KEY_ID --config wrangler.staging.jsonc
npx.cmd wrangler secret put AWS_SECRET_ACCESS_KEY --config wrangler.staging.jsonc
npx.cmd wrangler secret put ADMIN_KEY --config wrangler.staging.jsonc
npx.cmd wrangler secret put POSTMARK_SERVER_TOKEN --config wrangler.staging.jsonc
npx.cmd wrangler secret put IVS_PROXY_SECRET --config wrangler.staging.jsonc
npx.cmd wrangler secret put RECORDING_WEBHOOK_SECRET --config wrangler.staging.jsonc
npx.cmd wrangler secret put STREAMKEY_ENC_KEY_B64 --config wrangler.staging.jsonc
```

Generate `STREAMKEY_ENC_KEY_B64`:

```powershell
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$bytes = New-Object byte[] 32
$rng.GetBytes($bytes)
[Convert]::ToBase64String($bytes)
```

## 5. Set Recording Worker Secrets

Use the same `RECORDING_WEBHOOK_SECRET` value as the API Worker.

```powershell
cd C:\Relay\recording-worker

npx.cmd wrangler secret put AWS_ACCESS_KEY_ID --config wrangler.staging.jsonc
npx.cmd wrangler secret put AWS_SECRET_ACCESS_KEY --config wrangler.staging.jsonc
npx.cmd wrangler secret put POSTMARK_SERVER_TOKEN --config wrangler.staging.jsonc
npx.cmd wrangler secret put RECORDING_WEBHOOK_SECRET --config wrangler.staging.jsonc
```

## 6. Deploy Staging Workers

```powershell
cd C:\Relay\api
npm.cmd run typecheck
npx.cmd wrangler deploy --config wrangler.staging.jsonc

cd C:\Relay\recording-worker
npm.cmd run typecheck
npx.cmd wrangler deploy --config wrangler.staging.jsonc
```

## 7. Deploy Staging Pages

The project already exists as `castlink-staging`.

```powershell
cd C:\Relay
node tools/local-qa.mjs
npx.cmd wrangler pages deploy pages_out/pages --project-name castlink-staging
```

## 8. Add Domains

In Cloudflare, add these custom domains:

- Pages project `castlink-staging`: `staging.castlink.stream`
- Worker `stream-platform-api-staging`: `api.staging.castlink.stream`
- Worker `castlink-recording-worker-staging`: `recording.staging.castlink.stream`

The staging worker configs also include custom-domain route entries for the API and recording domains.

## 9. Stripe Test Webhook

In Stripe test mode, create a webhook endpoint:

```txt
https://api.staging.castlink.stream/api/stripe/webhook
```

Subscribe to:

```txt
checkout.session.completed
```

Copy the test webhook signing secret into staging:

```powershell
cd C:\Relay\api
npx.cmd wrangler secret put STRIPE_WEBHOOK_SECRET --config wrangler.staging.jsonc
```

## 10. Smoke Test

Open:

```txt
https://staging.castlink.stream/create/
```

Use Stripe test card:

```txt
4242 4242 4242 4242
```

Confirm:

- payment confirms quickly
- success page shows links
- broadcast starts
- watch page plays
- recording email arrives
- recording download works
- admin shows staging-only events
