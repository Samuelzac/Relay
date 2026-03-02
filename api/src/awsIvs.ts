// src/awsIvs.ts
// Amazon IVS (Low-Latency Streaming) uses a REST+JSON API.
// IMPORTANT: do NOT use x-amz-target here. Endpoints look like POST /CreateChannel.
//
// Docs: CreateChannel request syntax is POST /CreateChannel with Content-type: application/json
// https://docs.aws.amazon.com/ivs/latest/LowLatencyAPIReference/API_CreateChannel.html

import { signAwsRestJsonRequest } from "./awsSigV4";

export type IvsChannelInfo = {
  channelArn: string;
  ingestEndpoint: string; // host only (no scheme)
  playbackUrl: string;
};

export type IvsStreamKeyInfo = {
  streamKeyArn: string;
  streamKeyValue: string; // only returned once by AWS
};

function apiBase(env: any) {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const raw = String(env.IVS_API_ENDPOINT || `https://ivs.${region}.amazonaws.com`);
  // Normalize to NO trailing slash
  return raw.endsWith("/") ? raw.slice(0, -1) : raw;
}

async function awsRestJsonCall<T>(
  env: any,
  path: string, // e.g. "/CreateChannel"
  payload: Record<string, any>
): Promise<T> {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const url = `${apiBase(env)}${path}`;
  const body = JSON.stringify(payload ?? {});

  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }

  const signedHeaders = await signAwsRestJsonRequest({
    method: "POST",
    url,
    body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region,
    service: "ivs",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
    },
  });

  const res = await fetch(url, {
    method: "POST",
    headers: signedHeaders,
    body,
  });

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

export async function createChannel(env: any, name: string): Promise<IvsChannelInfo> {
  // NOTE: AWS returns an associated StreamKey too, but Relay's HLS path uses:
  // WebRTC ingest -> Realtime Composition -> IVS Channel (no RTMP ingest needed).
  const resp = await awsRestJsonCall<any>(env, "/CreateChannel", {
    name,
    type: "STANDARD",
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

export async function getChannel(env: any, channelArn: string): Promise<IvsChannelInfo> {
  const resp = await awsRestJsonCall<any>(env, "/GetChannel", { arn: channelArn });
  const ch = resp?.channel;
  if (!ch?.arn || !ch?.ingestEndpoint || !ch?.playbackUrl) {
    throw new Error(`Unexpected GetChannel response: ${JSON.stringify(resp)}`);
  }
  return {
    channelArn: ch.arn,
    ingestEndpoint: ch.ingestEndpoint,
    playbackUrl: ch.playbackUrl,
  };
}

export async function createStreamKey(env: any, channelArn: string): Promise<IvsStreamKeyInfo> {
  const resp = await awsRestJsonCall<any>(env, "/CreateStreamKey", { channelArn });
  const sk = resp?.streamKey;
  if (!sk?.arn || !sk?.value) {
    throw new Error(`Unexpected CreateStreamKey response: ${JSON.stringify(resp)}`);
  }
  return {
    streamKeyArn: sk.arn,
    streamKeyValue: sk.value,
  };
}
