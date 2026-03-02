// src/awsSigV4.ts
// Minimal AWS SigV4 signer for AWS REST+JSON APIs (and IVS JSON-RPC if you still use it).

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
  const enc = new TextEncoder();
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    key,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  return crypto.subtle.sign("HMAC", cryptoKey, enc.encode(data));
}

async function hmacSha256Str(key: ArrayBuffer, data: string): Promise<string> {
  return toHex(await hmacSha256(key, data));
}

function isoDate(now: Date): string {
  // YYYYMMDD
  const y = now.getUTCFullYear().toString().padStart(4, "0");
  const m = (now.getUTCMonth() + 1).toString().padStart(2, "0");
  const d = now.getUTCDate().toString().padStart(2, "0");
  return `${y}${m}${d}`;
}

function isoDateTime(now: Date): string {
  // YYYYMMDDTHHMMSSZ
  const ymd = isoDate(now);
  const hh = now.getUTCHours().toString().padStart(2, "0");
  const mm = now.getUTCMinutes().toString().padStart(2, "0");
  const ss = now.getUTCSeconds().toString().padStart(2, "0");
  return `${ymd}T${hh}${mm}${ss}Z`;
}

async function getSigningKey(secretAccessKey: string, dateStamp: string, region: string, service: string) {
  const enc = new TextEncoder();
  const kSecret = enc.encode("AWS4" + secretAccessKey);
  const kDate = await hmacSha256(kSecret, dateStamp);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  const kSigning = await hmacSha256(kService, "aws4_request");
  return kSigning;
}

/**
 * Existing helper (JSON-RPC style; used by IVS low-latency APIs in your repo).
 * This sets x-amz-target and content-type application/x-amz-json-1.0.
 */
export async function signAwsJsonRequest(params: {
  method: "POST" | "GET";
  url: string;
  body: string;
  target: string; // e.g. "IVS.CreateChannel"
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  service: string; // e.g. "ivs"
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

  const signingKey = await getSigningKey(secretAccessKey, dateStamp, region, service);
  const signature = await hmacSha256Str(signingKey, stringToSign);

  headers["authorization"] =
    `${algorithm} Credential=${accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  return headers;
}

/**
 * NEW: REST+JSON signer (what IVS Realtime uses: POST /CreateStage, etc.)
 * You provide the headers you intend to send (we'll add host/x-amz-date/x-amz-content-sha256).
 */
export async function signAwsRestJsonRequest(params: {
  method: "POST" | "GET";
  url: string;
  body: string;
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  service: string; // "ivsrealtime"
  headers?: Record<string, string>;
  now?: Date;
}) {
  const { method, url, body, accessKeyId, secretAccessKey, region, service } = params;
  const now = params.now ?? new Date();

  const u = new URL(url);
  const host = u.host;
  const canonicalUri = u.pathname || "/";
  const canonicalQuery = u.searchParams.toString(); // keep if any (usually none)

  const amzDate = isoDateTime(now);
  const dateStamp = isoDate(now);
  const payloadHash = await sha256Hex(body);

  const headers: Record<string, string> = {
    ...(params.headers || {}),
    host,
    "x-amz-date": amzDate,
    // Many AWS REST JSON services accept this and it improves compatibility.
    "x-amz-content-sha256": payloadHash,
  };

  // Normalize keys to lowercase for canonicalization
  const canon: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) canon[k.toLowerCase()] = String(v).trim();

  const sortedHeaderKeys = Object.keys(canon).sort();
  const canonicalHeaders = sortedHeaderKeys.map((k) => `${k}:${canon[k]}\n`).join("");
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

  const signingKey = await getSigningKey(secretAccessKey, dateStamp, region, service);
  const signature = await hmacSha256Str(signingKey, stringToSign);

  canon["authorization"] =
    `${algorithm} Credential=${accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  return canon;
}