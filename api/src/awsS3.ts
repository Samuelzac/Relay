function toHex(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function hmacSha256(key: BufferSource, data: string): Promise<ArrayBuffer> {
  const enc = new TextEncoder();
  const cryptoKey = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return crypto.subtle.sign("HMAC", cryptoKey, enc.encode(data));
}

async function hmacSha256Str(key: BufferSource, data: string): Promise<string> {
  return toHex(await hmacSha256(key, data));
}

async function sha256Hex(data: string): Promise<string> {
  const enc = new TextEncoder();
  return toHex(await crypto.subtle.digest("SHA-256", enc.encode(data)));
}

function isoDate(now: Date): string {
  return `${now.getUTCFullYear().toString().padStart(4, "0")}${(now.getUTCMonth() + 1).toString().padStart(2, "0")}${now.getUTCDate().toString().padStart(2, "0")}`;
}

function isoDateTime(now: Date): string {
  return `${isoDate(now)}T${now.getUTCHours().toString().padStart(2, "0")}${now.getUTCMinutes().toString().padStart(2, "0")}${now.getUTCSeconds().toString().padStart(2, "0")}Z`;
}

async function getSigningKey(secretAccessKey: string, dateStamp: string, region: string, service: string) {
  const enc = new TextEncoder();
  const kSecret = enc.encode("AWS4" + secretAccessKey);
  const kDate = await hmacSha256(kSecret, dateStamp);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  return hmacSha256(kService, "aws4_request");
}

function encodeS3Key(key: string) {
  return key.split("/").map((part) => encodeURIComponent(part)).join("/");
}

function encodeQuery(value: string) {
  return encodeURIComponent(value).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function xmlEscape(value: string) {
  return String(value).replace(/[&<>"']/g, (c) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&apos;" } as any)[c]);
}

function xmlUnescape(value: string) {
  return String(value).replace(/&(amp|lt|gt|quot|apos);/g, (_m, entity) => ({
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    apos: "'",
  } as any)[entity]);
}

async function signedS3Fetch(env: any, bucket: string, method: string, key: string, query: Record<string, string>, body = "") {
  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }

  const region = String(env.AWS_REGION || "ap-northeast-1");
  const now = new Date();
  const dateStamp = isoDate(now);
  const amzDate = isoDateTime(now);
  const host = `${bucket}.s3.${region}.amazonaws.com`;
  const payloadHash = await sha256Hex(body);
  const credentialScope = `${dateStamp}/${region}/s3/aws4_request`;

  const canonicalQuery = Object.keys(query)
    .sort()
    .map((k) => `${encodeQuery(k)}=${encodeQuery(query[k])}`)
    .join("&");
  const canonicalUri = key ? `/${encodeS3Key(key)}` : "/";
  const signedHeaders = "host;x-amz-content-sha256;x-amz-date";
  const canonicalHeaders = [
    `host:${host}`,
    `x-amz-content-sha256:${payloadHash}`,
    `x-amz-date:${amzDate}`,
    "",
  ].join("\n");
  const canonicalRequest = [method, canonicalUri, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash].join("\n");
  const stringToSign = ["AWS4-HMAC-SHA256", amzDate, credentialScope, await sha256Hex(canonicalRequest)].join("\n");
  const signingKey = await getSigningKey(String(env.AWS_SECRET_ACCESS_KEY).trim(), dateStamp, region, "s3");
  const signature = await hmacSha256Str(signingKey, stringToSign);
  const authorization = [
    `AWS4-HMAC-SHA256 Credential=${String(env.AWS_ACCESS_KEY_ID).trim()}/${credentialScope}`,
    `SignedHeaders=${signedHeaders}`,
    `Signature=${signature}`,
  ].join(", ");

  const res = await fetch(`https://${host}${canonicalUri}${canonicalQuery ? `?${canonicalQuery}` : ""}`, {
    method,
    headers: {
      authorization,
      "x-amz-content-sha256": payloadHash,
      "x-amz-date": amzDate,
      ...(body ? { "content-type": "application/xml" } : {}),
    },
    body: body || undefined,
  });
  if (!res.ok) {
    throw new Error(`s3_${method.toLowerCase()}_failed_${res.status}: ${(await res.text()).slice(0, 1000)}`);
  }
  return res;
}

export async function createSignedS3GetUrl(
  env: any,
  bucket: string,
  key: string,
  expiresSeconds = 900,
  opts: { filename?: string; contentType?: string } = {}
) {
  if (!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY) {
    throw new Error("Missing AWS secrets: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY");
  }

  const region = String(env.AWS_REGION || "ap-northeast-1");
  const now = new Date();
  const dateStamp = isoDate(now);
  const amzDate = isoDateTime(now);
  const boundedExpires = String(Math.max(60, Math.min(Number(expiresSeconds || 900), 3600)));
  const host = `${bucket}.s3.${region}.amazonaws.com`;
  const credentialScope = `${dateStamp}/${region}/s3/aws4_request`;
  const credential = `${String(env.AWS_ACCESS_KEY_ID).trim()}/${credentialScope}`;

  const query: Record<string, string> = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": credential,
    "X-Amz-Date": amzDate,
    "X-Amz-Expires": boundedExpires,
    "X-Amz-SignedHeaders": "host",
  };
  const filename = String(opts.filename || "").trim();
  if (filename) {
    const asciiFilename = filename.replace(/[^\x20-\x7E]/g, "").replace(/["\\]/g, "").trim() || "recording.mp4";
    query["response-content-disposition"] = `attachment; filename="${asciiFilename}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
  }
  const contentType = String(opts.contentType || "").trim();
  if (contentType) query["response-content-type"] = contentType;

  const canonicalQuery = Object.keys(query)
    .sort()
    .map((k) => `${encodeQuery(k)}=${encodeQuery(query[k])}`)
    .join("&");

  const canonicalUri = `/${encodeS3Key(key)}`;
  const canonicalRequest = ["GET", canonicalUri, canonicalQuery, `host:${host}\n`, "host", "UNSIGNED-PAYLOAD"].join("\n");
  const stringToSign = ["AWS4-HMAC-SHA256", amzDate, credentialScope, await sha256Hex(canonicalRequest)].join("\n");
  const signingKey = await getSigningKey(String(env.AWS_SECRET_ACCESS_KEY).trim(), dateStamp, region, "s3");
  const signature = await hmacSha256Str(signingKey, stringToSign);

  return `https://${host}${canonicalUri}?${canonicalQuery}&X-Amz-Signature=${signature}`;
}

export async function listS3Keys(env: any, bucket: string, prefix: string, maxKeys = 5000) {
  const keys: string[] = [];
  let continuationToken = "";
  while (keys.length < maxKeys) {
    const query: Record<string, string> = {
      "list-type": "2",
      "max-keys": String(Math.min(1000, maxKeys - keys.length)),
      prefix,
    };
    if (continuationToken) query["continuation-token"] = continuationToken;
    const xml = await (await signedS3Fetch(env, bucket, "GET", "", query)).text();
    for (const match of xml.matchAll(/<Key>([\s\S]*?)<\/Key>/g)) {
      keys.push(xmlUnescape(match[1]));
      if (keys.length >= maxKeys) break;
    }
    const truncated = /<IsTruncated>true<\/IsTruncated>/.test(xml);
    const next = xml.match(/<NextContinuationToken>([\s\S]*?)<\/NextContinuationToken>/);
    if (!truncated || !next) break;
    continuationToken = xmlUnescape(next[1]);
  }
  return keys;
}

export async function listS3Objects(env: any, bucket: string, prefix: string, maxKeys = 5000) {
  const objects: Array<{ key: string; lastModified: string | null }> = [];
  let continuationToken = "";
  while (objects.length < maxKeys) {
    const query: Record<string, string> = {
      "list-type": "2",
      "max-keys": String(Math.min(1000, maxKeys - objects.length)),
      prefix,
    };
    if (continuationToken) query["continuation-token"] = continuationToken;
    const xml = await (await signedS3Fetch(env, bucket, "GET", "", query)).text();
    for (const match of xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)) {
      const block = match[1];
      const keyMatch = block.match(/<Key>([\s\S]*?)<\/Key>/);
      if (!keyMatch) continue;
      const lastModifiedMatch = block.match(/<LastModified>([\s\S]*?)<\/LastModified>/);
      objects.push({
        key: xmlUnescape(keyMatch[1]),
        lastModified: lastModifiedMatch ? xmlUnescape(lastModifiedMatch[1]) : null,
      });
      if (objects.length >= maxKeys) break;
    }
    const truncated = /<IsTruncated>true<\/IsTruncated>/.test(xml);
    const next = xml.match(/<NextContinuationToken>([\s\S]*?)<\/NextContinuationToken>/);
    if (!truncated || !next) break;
    continuationToken = xmlUnescape(next[1]);
  }
  return objects;
}

export async function deleteS3Keys(env: any, bucket: string, keys: string[]) {
  const unique = [...new Set(keys.filter(Boolean))];
  let deleted = 0;
  for (let i = 0; i < unique.length; i += 1000) {
    const chunk = unique.slice(i, i + 1000);
    const body = `<Delete>${chunk.map((key) => `<Object><Key>${xmlEscape(key)}</Key></Object>`).join("")}<Quiet>true</Quiet></Delete>`;
    await signedS3Fetch(env, bucket, "POST", "", { delete: "" }, body);
    deleted += chunk.length;
  }
  return deleted;
}

export async function deleteS3RecordingObjects(env: any, bucket: string, prefix?: string | null, keys: string[] = []) {
  const listed = prefix ? await listS3Keys(env, bucket, prefix) : [];
  const deleted = await deleteS3Keys(env, bucket, [...listed, ...keys]);
  return { listed: listed.length, deleted };
}
