import { signAwsRestJsonRequest } from "./awsSigV4";

function region(env: any) {
  return String(env.AWS_REGION || "ap-northeast-1");
}

function endpoint(env: any) {
  const configured = String(env.MEDIACONVERT_ENDPOINT || "").trim();
  const base = configured || `https://mediaconvert.${region(env)}.amazonaws.com`;
  return base.replace(/\/+$/, "");
}

async function mediaConvertFetch(env: any, method: "GET" | "POST", path: string, payload?: any) {
  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }

  const url = `${endpoint(env)}${path}`;
  const body = payload === undefined ? "" : JSON.stringify(payload);
  const headers = await signAwsRestJsonRequest({
    method,
    url,
    body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region: region(env),
    service: "mediaconvert",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
  });

  const res = await fetch(url, {
    method,
    headers,
    body: body || undefined,
  });
  const text = await res.text();
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {}

  if (!res.ok) {
    const message = data?.message || data?.Message || data?.__type || text || "(no body)";
    throw new Error(`MediaConvert ${method} ${path} failed: ${res.status} ${message}`);
  }
  return data;
}

function s3Uri(bucket: string, keyOrPrefix: string) {
  return `s3://${bucket}/${String(keyOrPrefix || "").replace(/^\/+/, "")}`;
}

export function mediaConvertConfigured(env: any) {
  return !!String(env.MEDIACONVERT_ROLE_ARN || "").trim();
}

export function recordingMp4NameModifier(env: any) {
  const configured = String(env.RECORDING_MP4_NAME_MODIFIER || "").trim();
  return configured || "-castlink";
}

export function recordingMp4OutputKey(env: any, outputPrefix: string, manifestKey: string) {
  const cleanPrefix = String(outputPrefix || "mp4").replace(/^\/+|\/+$/g, "");
  const base = String(manifestKey || "recording")
    .split("/")
    .pop()!
    .replace(/\.[^.]+$/, "") || "recording";
  return `${cleanPrefix}/${base}${recordingMp4NameModifier(env)}.mp4`;
}

export async function createMp4Job(env: any, opts: {
  bucket: string;
  inputManifestKey: string;
  outputPrefix: string;
  eventId: string;
}) {
  const role = String(env.MEDIACONVERT_ROLE_ARN || "").trim();
  if (!role) throw new Error("MEDIACONVERT_ROLE_ARN is not configured");

  const nameModifier = recordingMp4NameModifier(env);
  const input = s3Uri(opts.bucket, opts.inputManifestKey);
  const destination = `${s3Uri(opts.bucket, opts.outputPrefix.replace(/^\/+|\/+$/g, ""))}/`;

  const payload = {
    Role: role,
    UserMetadata: {
      app: "castlink",
      event_id: opts.eventId,
    },
    Settings: {
      TimecodeConfig: { Source: "ZEROBASED" },
      Inputs: [
        {
          FileInput: input,
          AudioSelectors: {
            "Audio Selector 1": { DefaultSelection: "DEFAULT" },
          },
          VideoSelector: {},
          TimecodeSource: "ZEROBASED",
        },
      ],
      OutputGroups: [
        {
          Name: "MP4",
          OutputGroupSettings: {
            Type: "FILE_GROUP_SETTINGS",
            FileGroupSettings: { Destination: destination },
          },
          Outputs: [
            {
              NameModifier: nameModifier,
              ContainerSettings: {
                Container: "MP4",
                Mp4Settings: {},
              },
              VideoDescription: {
                CodecSettings: {
                  Codec: "H_264",
                  H264Settings: {
                    RateControlMode: "QVBR",
                    QvbrSettings: { QvbrQualityLevel: 7 },
                    MaxBitrate: 5000000,
                    QualityTuningLevel: "SINGLE_PASS",
                    GopSize: 2,
                    GopSizeUnits: "SECONDS",
                    NumberBFramesBetweenReferenceFrames: 2,
                  },
                },
              },
              AudioDescriptions: [
                {
                  AudioSourceName: "Audio Selector 1",
                  CodecSettings: {
                    Codec: "AAC",
                    AacSettings: {
                      Bitrate: 128000,
                      CodingMode: "CODING_MODE_2_0",
                      SampleRate: 48000,
                    },
                  },
                },
              ],
            },
          ],
        },
      ],
    },
  };

  const data = await mediaConvertFetch(env, "POST", "/2017-08-29/jobs", payload);
  return data?.Job || data?.job || data;
}

export async function getMp4Job(env: any, jobId: string) {
  const data = await mediaConvertFetch(env, "GET", `/2017-08-29/jobs/${encodeURIComponent(jobId)}`);
  return data?.Job || data?.job || data;
}
