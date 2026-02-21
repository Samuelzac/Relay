// src/awsSigV4.ts
// Minimal AWS SigV4 signer for AWS JSON RPC APIs (like IVS).
// Works in Cloudflare Workers (no AWS SDK required).

function toHex(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function sha256Hex(data: string): Promise<string> {
  const enc = new TextEncoder();
  const hash = await crypto.subtle.digest("SHA-256", enc.encode(data));
  return toHex(hash);
}

async function hmacSha256(key: ArrayBuffer, data: string): Promise<ArrayBuffer> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    key,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  return crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(data));
}

async function hmacSha256Bytes(keyBytes: Uint8Array, data: string): Promise<ArrayBuffer> {
  return hmacSha256(keyBytes.buffer, data);
}

function isoDate(now: Date) {
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

function isoDateTime(now: Date) {
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  const hh = String(now.getUTCHours()).padStart(2, "0");
  const mi = String(now.getUTCMinutes()).padStart(2, "0");
  const ss = String(now.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
}

async function deriveSigningKey(secretKey: string, date: string, region: string, service: string) {
  const enc = new TextEncoder();
  const kSecret = enc.encode("AWS4" + secretKey);
  const kDate = await hmacSha256Bytes(kSecret, date);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  const kSigning = await hmacSha256(kService, "aws4_request");
  return kSigning;
}

export async function signAwsJsonRequest(params: {
  method: "POST" | "GET";
  url: string;
  body: string;
  target: string; // e.g. "IVS.CreateChannel"
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  service: string; // "ivs"
  now?: Date;
}) {
  const { method, url, body, target, accessKeyId, secretAccessKey, region, service } = params;
  const now = params.now ?? new Date();

  const u = new URL(url);
  const host = u.host;

  const canonicalUri = u.pathname || "/";
  const canonicalQuery = "";

  const amzDate = isoDateTime(now);
  const dateStamp = isoDate(now);
  const payloadHash = await sha256Hex(body);

  const headers: Record<string, string> = {
    host,
    "content-type": "application/x-amz-json-1.0",
    "x-amz-date": amzDate,
    "x-amz-target": target,
  };

  const sortedHeaderKeys = Object.keys(headers).sort();
  const canonicalHeaders = sortedHeaderKeys.map((k) => `${k}:${headers[k].trim()}\n`).join("");
  const signedHeaders = sortedHeaderKeys.join(";");

  const canonicalRequest = [
    method,
    canonicalUri,
    canonicalQuery,
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");

  const algorithm = "AWS4-HMAC-SHA256";
  const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;
  const stringToSign = [
    algorithm,
    amzDate,
    credentialScope,
    await sha256Hex(canonicalRequest),
  ].join("\n");

  const signingKey = await deriveSigningKey(secretAccessKey, dateStamp, region, service);
  const signatureBytes = await hmacSha256(signingKey, stringToSign);
  const signature = toHex(signatureBytes);

  const authorizationHeader =
    `${algorithm} Credential=${accessKeyId}/${credentialScope}, ` +
    `SignedHeaders=${signedHeaders}, Signature=${signature}`;

  return {
    "Content-Type": headers["content-type"],
    "X-Amz-Date": headers["x-amz-date"],
    "X-Amz-Target": headers["x-amz-target"],
    Authorization: authorizationHeader,
    Host: host,
  };
}