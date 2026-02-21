// src/awsIvsRealtime.ts
// Amazon IVS Real-Time (WebRTC) control-plane.
// Primary path: call Fly proxy (avoids Cloudflare Worker DNS/1016 issues).
// Fallback path: direct SigV4 to AWS via aws4fetch (kept for compatibility).

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

export type TokenCapability = "PUBLISH" | "SUBSCRIBE";

function hasProxy(env: any): boolean {
  return !!(env.IVS_PROXY_BASE && env.IVS_PROXY_SECRET);
}

function rtEndpoint(env: any): string {
  const region = env.AWS_REGION || "ap-southeast-2";
  return (
    env.IVS_REALTIME_API_ENDPOINT ||
    env.IVS_REALTIME_ENDPOINT ||
    `https://ivsrealtime.${region}.api.aws`
  );
}

function rtAws(env: any) {
  const region = env.AWS_REGION || "ap-southeast-2";
  const endpoint = rtEndpoint(env);

  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region,
    service: "ivsrealtime",
  });

  return { aws, endpoint };
}

async function hmacHex(secret: string, body: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function proxyPost<T>(env: any, route: string, payload: any): Promise<T> {
  const base = String(env.IVS_PROXY_BASE || "").replace(/\/$/, "");
  const secret = String(env.IVS_PROXY_SECRET || "");

  if (!base || !secret) {
    throw new Error("Missing IVS proxy env vars: IVS_PROXY_BASE / IVS_PROXY_SECRET");
  }

  const body = JSON.stringify(payload ?? {});
  const sig = await hmacHex(secret, body);

  const url = `${base}${route}`;
  console.log("IVS REALTIME via proxy:", url);

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-relay-sig": sig,
    },
    body,
  });

  const text = await res.text(); // always consume
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = {};
  }

  if (!res.ok) {
    const msg =
      (data?.error ? `${data.error}${data?.detail ? `: ${data.detail}` : ""}` : "") ||
      data?.detail ||
      text ||
      "(no body)";
    throw new Error(`IVS proxy ${route} failed: ${res.status} ${msg}`);
  }

  return data as T;
}

async function awsPostJson<T>(env: any, path: string, payload: any): Promise<T> {
  console.log("IVS REALTIME direct path:", path);

  const { aws, endpoint } = rtAws(env);
  console.log("IVS REALTIME direct endpoint:", endpoint);

  const url = `${endpoint.replace(/\/$/, "")}${path}`;
  const res = await aws.fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload ?? {}),
  });

  const text = await res.text(); // always consume
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = {};
  }

  if (!res.ok) {
    const msg = data?.message || data?.__type || text || "(no body)";
    throw new Error(`IVSRealTime ${path} failed: ${res.status} ${msg}`);
  }

  return data as T;
}

/**
 * Internal router: uses proxy when configured, otherwise falls back to direct AWS.
 * We keep the old AWS action paths so existing code stays intact.
 */
async function postJson<T>(env: any, actionPath: string, payload: any): Promise<T> {
  if (hasProxy(env)) {
    // Map AWS REST paths to proxy routes
    if (actionPath === "/CreateStage") {
      return await proxyPost<T>(env, "/createStage", { name: payload?.name });
    }
    if (actionPath === "/CreateParticipantToken") {
      // NOTE: proxy currently ignores "duration" if provided; safe to pass.
      return await proxyPost<T>(env, "/createParticipantToken", {
        stageArn: payload?.stageArn,
        userId: payload?.userId,
        capabilities: payload?.capabilities,
        duration: payload?.duration,
      });
    }
    if (actionPath === "/DeleteStage") {
      // Original payload is { arn }, proxy expects { stageArn }
      return await proxyPost<T>(env, "/deleteStage", { stageArn: payload?.arn });
    }

    throw new Error(`IVS proxy action not mapped: ${actionPath}`);
  }

  return await awsPostJson<T>(env, actionPath, payload);
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