// src/awsIvsRealtime.ts
import { signAwsRestJsonRequest } from "./awsSigV4";

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

export type EncoderConfiguration = {
  arn: string;
  name?: string;
};

export type Composition = {
  arn: string;
  state?: string;
  stageArn?: string;
};

function baseUrl(env: any) {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const raw = env.IVS_REALTIME_API_ENDPOINT;

  let base = String(raw || `https://ivsrealtime.${region}.amazonaws.com`);

  // HARDEN: never allow the old bad host
  if (base.includes(".api.aws")) {
    base = `https://ivsrealtime.${region}.amazonaws.com`;
  }

  // no trailing slash here; we append /CreateStage etc
  base = base.replace(/\/+$/, "");

  console.log(
    "IVS Realtime endpoint config:",
    JSON.stringify({
      aws_region: region,
      env_IVS_REALTIME_API_ENDPOINT: raw ?? null,
      using_endpoint_base: base,
    })
  );

  return { region, base };
}

function normalizeOpPath(opPath: string) {
  if (!opPath) return "/";
  return opPath.startsWith("/") ? opPath : `/${opPath}`;
}

async function callRt<T>(env: any, opPath: string, payload: any): Promise<T> {
  const { region, base } = baseUrl(env);

  const safeOpPath = normalizeOpPath(opPath);
  const url = `${base}${safeOpPath}`; // e.g. https://...amazonaws.com/CreateStage
  const body = JSON.stringify(payload ?? {});

  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }

  // IMPORTANT: For IVS Realtime, AWS is requiring SigV4 service scope "ivs"
  const signingService = "ivs";

  console.log(
    "IVS Realtime request:",
    JSON.stringify({
      url,
      opPath: safeOpPath,
      service: signingService,
      region,
      bodyBytes: body.length,
    })
  );

  const headers = await signAwsRestJsonRequest({
    method: "POST",
    url,
    body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region,
    service: signingService,
    headers: {
      "content-type": "application/json",
    },
  });

  const res = await fetch(url, {
    method: "POST",
    headers,
    body,
  });

  const text = await res.text();
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    // ignore non-json bodies
  }

  if (!res.ok) {
    const requestId =
      res.headers.get("x-amzn-requestid") ||
      res.headers.get("x-amz-request-id") ||
      res.headers.get("x-amz-id-2") ||
      null;

    console.log(
      "IVS Realtime ERROR:",
      JSON.stringify({
        url,
        opPath: safeOpPath,
        service: signingService,
        status: res.status,
        requestId,
        body: text || null,
      })
    );

    const msg = data?.message || data?.Message || data?.__type || text || "(no body)";
    throw new Error(
      `IVS Realtime ${safeOpPath} failed: ${res.status} ${msg}${requestId ? ` (requestId=${requestId})` : ""}`
    );
  }

  return data as T;
}

export async function createStage(env: any, name: string): Promise<RealtimeStage> {
  const resp = await callRt<any>(env, "/CreateStage", {
    name,
    tags: { app: "relay" },
  });

  const st = resp?.stage;
  if (!st?.arn) throw new Error(`Unexpected CreateStage response: ${JSON.stringify(resp)}`);

  return {
    stageArn: st.arn,
    stageName: st.name,
    endpoints: st.endpoints,
  };
}

export async function deleteStage(env: any, stageArn: string): Promise<void> {
  if (!stageArn) return;
  await callRt<any>(env, "/DeleteStage", { arn: stageArn });
}

export async function createParticipantToken(
  env: any,
  stageArn: string,
  userId: string,
  capabilities: TokenCapability[]
): Promise<ParticipantToken> {
  const resp = await callRt<any>(env, "/CreateParticipantToken", {
    stageArn,
    userId,
    capabilities,
  });

  const tok = resp?.participantToken;
  if (!tok?.token || !tok?.participantId) {
    throw new Error(`Unexpected CreateParticipantToken response: ${JSON.stringify(resp)}`);
  }

  return {
    participantId: tok.participantId,
    token: tok.token,
    userId: tok.userId,
    expirationTime: tok.expirationTime,
  };
}

export async function createEncoderConfiguration(env: any, name: string): Promise<EncoderConfiguration> {
  const resp = await callRt<any>(env, "/CreateEncoderConfiguration", {
    name,
    video: {
      width: 1280,
      height: 720,
      framerate: 30,
      bitrate: 2500000,
    },
    audio: {
      bitrate: 128000,
      channels: 2,
      sampleRate: 48000,
    },
  });

  const enc = resp?.encoderConfiguration;
  if (!enc?.arn) throw new Error(`Unexpected CreateEncoderConfiguration response: ${JSON.stringify(resp)}`);
  return { arn: enc.arn, name: enc.name };
}

/**
 * Starts a server-side composition that pushes the Stage into the given IVS Channel.
 * NOTE: StartComposition REQUIRES an idempotencyToken.
 */
export async function createComposition(
  env: any,
  stageArn: string,
  channelArn: string,
  encoderConfigurationArn: string,
  idempotencyToken?: string
): Promise<Composition> {
  const token = (idempotencyToken && String(idempotencyToken).trim()) || crypto.randomUUID();

  const resp = await callRt<any>(env, "/StartComposition", {
    stageArn,
    destinations: [
      {
        channel: {
          channelArn,
          encoderConfigurationArn,
        },
      },
    ],
    idempotencyToken: token,
  });

  const comp = resp?.composition;
  if (!comp?.arn) throw new Error(`Unexpected StartComposition response: ${JSON.stringify(resp)}`);

  return {
    arn: comp.arn,
    state: comp.state,
    stageArn: comp.stageArn,
  };
}

export async function stopComposition(env: any, compositionArn: string): Promise<void> {
  if (!compositionArn) return;
  await callRt<any>(env, "/StopComposition", { arn: compositionArn });
}