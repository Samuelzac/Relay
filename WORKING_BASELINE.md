# Working Baseline

Date: 2026-06-20 NZT

Production was confirmed working after rolling back the API Worker to:

- Worker version: `dafcea89-fdfd-4aaf-8bf8-393a1a99ebfa`
- Pages deployment: `https://62f6c702.castlink-efj.pages.dev`

Confirmed behavior:

- Stripe payment confirmation becomes paid quickly after checkout.
- Broadcast and watch links unlock again.

Rule for follow-up changes:

- Do not deploy the dirty local API folder directly.
- Reintroduce features in small deploys with a rollback point after each working stage.
