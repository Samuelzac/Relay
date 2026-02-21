import express from "express";
import crypto from "crypto";
import {
  IVSRealTimeClient,
  CreateStageCommand,
  CreateParticipantTokenCommand,
  DeleteStageCommand,
} from "@aws-sdk/client-ivs-realtime";

const app = express();
app.use(express.json({ limit: "1mb" }));

const REGION = process.env.AWS_REGION || "ap-southeast-2";
const PROXY_SECRET = process.env.PROXY_SECRET; // shared secret with Worker

if (!PROXY_SECRET) {
  throw new Error("Missing PROXY_SECRET");
}

function timingSafeEq(a, b) {
  const ba = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
}

function verify(req) {
  const sig = req.header("x-relay-sig") || "";
  const body = JSON.stringify(req.body || {});
  const expected = crypto.createHmac("sha256", PROXY_SECRET).update(body).digest("hex");
  return timingSafeEq(sig, expected);
}

const ENDPOINT =
  process.env.IVS_REALTIME_ENDPOINT ||
  process.env.IVS_REALTIME_API_ENDPOINT ||
  `https://ivsrealtime.${REGION}.api.aws`;

console.log("IVS REALTIME endpoint override:", ENDPOINT);

const client = new IVSRealTimeClient({
  region: REGION,
  endpoint: ENDPOINT,
});

app.post("/createStage", async (req, res) => {
  try {
    if (!verify(req)) return res.status(401).json({ error: "bad_sig" });
    const name = String(req.body?.name || "");
    if (!name) return res.status(400).json({ error: "missing_name" });

    const out = await client.send(new CreateStageCommand({ name }));
    return res.json(out);
  } catch (e) {
    return res.status(500).json({ error: "createStage_failed", detail: String(e?.message || e) });
  }
});

app.post("/createParticipantToken", async (req, res) => {
  try {
    if (!verify(req)) return res.status(401).json({ error: "bad_sig" });

    const stageArn = String(req.body?.stageArn || "");
    const userId = req.body?.userId ? String(req.body.userId) : undefined;
    const capabilities = Array.isArray(req.body?.capabilities) ? req.body.capabilities : undefined;

    if (!stageArn) return res.status(400).json({ error: "missing_stageArn" });

    const out = await client.send(
      new CreateParticipantTokenCommand({
        stageArn,
        userId,
        capabilities,
      })
    );
    return res.json(out);
  } catch (e) {
    return res.status(500).json({ error: "createToken_failed", detail: String(e?.message || e) });
  }
});

app.post("/deleteStage", async (req, res) => {
  try {
    if (!verify(req)) return res.status(401).json({ error: "bad_sig" });
    const stageArn = String(req.body?.stageArn || "");
    if (!stageArn) return res.status(400).json({ error: "missing_stageArn" });

    const out = await client.send(new DeleteStageCommand({ arn: stageArn }));
    return res.json(out);
  } catch (e) {
    return res.status(500).json({ error: "deleteStage_failed", detail: String(e?.message || e) });
  }
});

app.get("/healthz", (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`ivs-proxy listening on ${port}`));