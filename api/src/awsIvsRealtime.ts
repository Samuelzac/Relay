// src/awsIvsRealtime.ts
// Amazon IVS Real-Time (WebRTC) control-plane via REST (SigV4).
// Uses aws4fetch so it works inside Cloudflare Workers with nodejs_compat.

import { AwsClient } from "aws4fetch";

export type RealtimeStage = {
  stageArn: string;
  stageName?: string;
  endpoints?: Record<string, string>;
};

export type ParticipantToken = {
  participantId: string;
  token: string;
  userId?: string;
  expirationTime?: string;
};

function rtAws(env: any) {
  const region = env.AWS_REGION || "ap-southeast-2";
  const endpoint =
    env.IVS_REALTIME_API_ENDPOINT ||
    env.IVS_REALTIME_ENDPOINT ||
    `https://ivsrealtime.${region}.amazonaws.com`;

  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region,
    service: "ivsrealtime",
  });

  return { aws, endpoint };
}

async function postJson<T>(env: any, path: string, body: any): Promise<T> {
  const { aws, endpoint } = rtAws(env);

  const res = await aws.fetch(`${endpoint}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });

  const raw = await res.text().catch(() => "");
  let data: any = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch {
    data = null;
  }

  if (!res.ok) {
    const msg = data?.message || data?.Message || raw || `HTTP ${res.status}`;
    throw new Error(`IVSRealTime ${path} failed: ${res.status} ${msg}`);
  }

  return (data as T) ?? ({} as T);
}

export async function createStage(env: any, name: string): Promise<RealtimeStage> {
  const data = await postJson<any>(env, "/CreateStage", { name });
  const stage = data?.stage;
  return {
    stageArn: stage?.arn,
    stageName: stage?.name,
    endpoints: stage?.endpoints,
  };
}

export async function deleteStage(env: any, stageArn: string) {
  await postJson<any>(env, "/DeleteStage", { arn: stageArn });
}

export type TokenCapability = "PUBLISH" | "SUBSCRIBE";

export async function createParticipantToken(
  env: any,
  stageArn: string,
  userId: string,
  capabilities: TokenCapability[],
  durationSeconds?: number
): Promise<ParticipantToken> {
  const payload: any = { stageArn, userId, capabilities };
  if (durationSeconds) payload.duration = durationSeconds;

  const data = await postJson<any>(env, "/CreateParticipantToken", payload);
  const pt = data?.participantToken;

  return {
    participantId: pt?.participantId,
    token: pt?.token,
    userId: pt?.userId,
    expirationTime: pt?.expirationTime,
  };
}
