// src/awsIvs.ts
import { signAwsJsonRequest } from "./awsSigV4";

export type IvsChannelInfo = {
  channelArn: string;
  ingestEndpoint: string; // host only
  playbackUrl: string;
};

export type IvsStreamKeyInfo = {
  streamKeyArn: string;
  streamKeyValue: string; // only returned once by AWS
};

function apiUrl(env: any) {
  // You already have IVS_API_ENDPOINT in env (nice).
  // Ensure it ends with a trailing slash.
  const base =
    (env.IVS_API_ENDPOINT as string) ||
    `https://ivs.${env.AWS_REGION || "ap-southeast-2"}.amazonaws.com/`;
  return base.endsWith("/") ? base : base + "/";
}

async function awsJsonCall<T>(env: any, target: string, payload: Record<string, any>): Promise<T> {
  const region = env.AWS_REGION || "ap-southeast-2";
  const url = apiUrl(env);
  const body = JSON.stringify(payload ?? {});

  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }

  const headers = await signAwsJsonRequest({
    method: "POST",
    url,
    body,
    target,
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region,
    service: "ivs",
  });

  // Hard timeout so requests never hang
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 12_000);

  try {
    const res = await fetch(url, {
      method: "POST",
      headers,
      body,
      signal: ac.signal,
    });

    const text = await res.text();
    if (!res.ok) {
      throw new Error(`AWS IVS ${target} failed: ${res.status} ${text}`);
    }
    return JSON.parse(text) as T;
  } finally {
    clearTimeout(timer);
  }
}

export async function createChannel(env: any, name: string): Promise<IvsChannelInfo> {
  const resp = await awsJsonCall<any>(env, "IVS.CreateChannel", {
    name,
    type: "STANDARD",     // switch to "BASIC" if you want cheaper
    latencyMode: "LOW",
    authorized: false,
    tags: { app: "relay" },
  });

  const ch = resp?.channel;
  if (!ch?.arn || !ch?.ingestEndpoint || !ch?.playbackUrl) {
    throw new Error(`Unexpected CreateChannel response: ${JSON.stringify(resp)}`);
  }

  return {
    channelArn: ch.arn,
    ingestEndpoint: ch.ingestEndpoint,
    playbackUrl: ch.playbackUrl,
  };
}

export async function createStreamKey(env: any, channelArn: string): Promise<IvsStreamKeyInfo> {
  const resp = await awsJsonCall<any>(env, "IVS.CreateStreamKey", { channelArn });
  const sk = resp?.streamKey;

  if (!sk?.arn || !sk?.value) {
    throw new Error(`Unexpected CreateStreamKey response: ${JSON.stringify(resp)}`);
  }

  return {
    streamKeyArn: sk.arn,
    streamKeyValue: sk.value,
  };
}