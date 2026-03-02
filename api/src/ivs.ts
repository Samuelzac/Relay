// src/ivs.ts
// Helper for deleting IVS channels (REST+JSON API).

import { signAwsRestJsonRequest } from "./awsSigV4";

function apiBase(env: any) {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const raw = String(env.IVS_API_ENDPOINT || `https://ivs.${region}.amazonaws.com`);
  return raw.endsWith("/") ? raw.slice(0, -1) : raw;
}

async function awsRestJsonCall<T>(env: any, path: string, payload: Record<string, any>): Promise<T> {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const url = `${apiBase(env)}${path}`;
  const body = JSON.stringify(payload ?? {});

  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }

  const headers = await signAwsRestJsonRequest({
    method: "POST",
    url,
    body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region,
    service: "ivs",
    headers: { "content-type": "application/json", accept: "application/json" },
  });

  const res = await fetch(url, { method: "POST", headers, body });

  const text = await res.text();
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {}

  if (!res.ok) {
    const msg = data?.message || data?.__type || text || "(no body)";
    throw new Error(`IVS ${path} failed: ${res.status} ${msg}`);
  }
  return data as T;
}

export async function deleteIvsChannel(env: any, channelArn: string): Promise<void> {
  await awsRestJsonCall(env, "/DeleteChannel", { arn: channelArn });
}
