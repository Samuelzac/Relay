// src/ivs.ts
import { AwsClient } from "aws4fetch";

export type IvsCreateResult = {
  channelArn: string;
  ingestEndpoint: string; // host only (no protocol)
  playbackUrl: string;
  streamKey: string; // returned once
};

function getIvsEndpoint(env: any) {
  // env.IVS_API_ENDPOINT should be like: https://ivs.ap-southeast-2.api.aws
  const base =
    (env.IVS_API_ENDPOINT as string) ||
    `https://ivs.${env.AWS_REGION || "ap-southeast-2"}.api.aws`;
  return base.replace(/\/+$/, ""); // no trailing slash
}

function assertAwsCreds(env: any) {
  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }
  if (!env.AWS_REGION) {
    // you do have it in wrangler.jsonc but keep it safe
    env.AWS_REGION = "ap-southeast-2";
  }
}

export async function createIvsChannel(env: any, eventId: string): Promise<IvsCreateResult> {
  assertAwsCreds(env);

  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region: env.AWS_REGION,
    service: "ivs",
  });

  const endpoint = getIvsEndpoint(env);

  const payload = {
    name: `relay-${eventId}`,
    type: "STANDARD",      // or "BASIC"
    latencyMode: "LOW",
    authorized: false,
    tags: { app: "relay", eventId },
  };

  const res = await aws.fetch(`${endpoint}/CreateChannel`, {
    method: "POST",
    headers: { "content-type": "application/x-amz-json-1.1" },
    body: JSON.stringify(payload),
  });

  const text = await res.text(); // ✅ consume body
  let data: any = {};
  try { data = text ? JSON.parse(text) : {}; } catch {}

  if (!res.ok) {
    const msg = data?.message || data?.__type || text || "(no body)";
    throw new Error(`IVS CreateChannel failed: ${res.status} ${msg}`);
  }

  // CreateChannel returns { channel: {...}, streamKey: {...} }
  const ch = data?.channel;
  const sk = data?.streamKey;

  if (!ch?.arn || !ch?.ingestEndpoint || !ch?.playbackUrl || !sk?.value) {
    throw new Error(`Unexpected IVS CreateChannel response: ${JSON.stringify(data)}`);
  }

  return {
    channelArn: ch.arn,
    ingestEndpoint: ch.ingestEndpoint,
    playbackUrl: ch.playbackUrl,
    streamKey: sk.value,
  };
}

export async function deleteIvsChannel(env: any, channelArn: string) {
  assertAwsCreds(env);

  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region: env.AWS_REGION,
    service: "ivs",
  });

  const endpoint = getIvsEndpoint(env);

  const payload = { arn: channelArn };

  const res = await aws.fetch(`${endpoint}/DeleteChannel`, {
    method: "POST",
    headers: { "content-type": "application/x-amz-json-1.1" },
    body: JSON.stringify(payload),
  });

  const text = await res.text(); // ✅ consume body
  let data: any = {};
  try { data = text ? JSON.parse(text) : {}; } catch {}

  if (!res.ok) {
    const msg = data?.message || data?.__type || text || "(no body)";
    throw new Error(`IVS DeleteChannel failed: ${res.status} ${msg}`);
  }

  return data;
}