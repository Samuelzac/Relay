function toHex(buffer: ArrayBuffer): string {
  return [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function sha256Hex(data: string): Promise<string> {
  return toHex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(data)));
}

async function hmacSha256(key: BufferSource, data: string): Promise<ArrayBuffer> {
  const cryptoKey = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(data));
}

async function hmacSha256Str(key: BufferSource, data: string): Promise<string> {
  return toHex(await hmacSha256(key, data));
}

function amzDate(now: Date) {
  return `${now.getUTCFullYear().toString().padStart(4, "0")}${(now.getUTCMonth() + 1).toString().padStart(2, "0")}${now.getUTCDate().toString().padStart(2, "0")}T${now.getUTCHours().toString().padStart(2, "0")}${now.getUTCMinutes().toString().padStart(2, "0")}${now.getUTCSeconds().toString().padStart(2, "0")}Z`;
}

function dateStamp(now: Date) {
  return amzDate(now).slice(0, 8);
}

async function signingKey(secret: string, date: string, region: string, service: string) {
  const enc = new TextEncoder();
  const kDate = await hmacSha256(enc.encode("AWS4" + secret), date);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  return hmacSha256(kService, "aws4_request");
}

function encodeQuery(value: string) {
  return encodeURIComponent(value).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

export async function signAwsJsonRequest(opts: {
  method: string;
  url: string;
  body: string;
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  service: string;
  headers?: Record<string, string>;
}) {
  const now = new Date();
  const date = dateStamp(now);
  const stamp = amzDate(now);
  const u = new URL(opts.url);
  const payloadHash = await sha256Hex(opts.body || "");
  const headers: Record<string, string> = {
    host: u.host,
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": stamp,
    ...(opts.headers || {}),
  };
  const signedHeaderNames = Object.keys(headers).map((h) => h.toLowerCase()).sort();
  const canonicalHeaders = signedHeaderNames.map((h) => `${h}:${headers[h] || headers[Object.keys(headers).find((k) => k.toLowerCase() === h)!]}`).join("\n") + "\n";
  const queryPairs: Array<[string, string]> = [];
  u.searchParams.forEach((v, k) => queryPairs.push([k, v]));
  const canonicalQuery = queryPairs.sort().map(([k, v]) => `${encodeQuery(k)}=${encodeQuery(v)}`).join("&");
  const canonicalRequest = [opts.method, u.pathname || "/", canonicalQuery, canonicalHeaders, signedHeaderNames.join(";"), payloadHash].join("\n");
  const scope = `${date}/${opts.region}/${opts.service}/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", stamp, scope, await sha256Hex(canonicalRequest)].join("\n");
  const signature = await hmacSha256Str(await signingKey(opts.secretAccessKey, date, opts.region, opts.service), stringToSign);
  return {
    ...headers,
    authorization: `AWS4-HMAC-SHA256 Credential=${opts.accessKeyId}/${scope}, SignedHeaders=${signedHeaderNames.join(";")}, Signature=${signature}`,
  };
}

export async function presignS3GetUrl(env: any, bucket: string, key: string, expiresSeconds: number, filename: string) {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const now = new Date();
  const date = dateStamp(now);
  const stamp = amzDate(now);
  const host = `${bucket}.s3.${region}.amazonaws.com`;
  const scope = `${date}/${region}/s3/aws4_request`;
  const credential = `${String(env.AWS_ACCESS_KEY_ID).trim()}/${scope}`;
  const query: Record<string, string> = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": credential,
    "X-Amz-Date": stamp,
    "X-Amz-Expires": String(Math.max(60, Math.min(expiresSeconds, 3600))),
    "X-Amz-SignedHeaders": "host",
    "response-content-disposition": `attachment; filename="${filename.replace(/[^\x20-\x7E]/g, "").replace(/["\\]/g, "") || "recording.mp4"}"; filename*=UTF-8''${encodeURIComponent(filename)}`,
    "response-content-type": "video/mp4",
  };
  const canonicalQuery = Object.keys(query).sort().map((k) => `${encodeQuery(k)}=${encodeQuery(query[k])}`).join("&");
  const canonicalUri = `/${key.split("/").map(encodeURIComponent).join("/")}`;
  const canonicalRequest = ["GET", canonicalUri, canonicalQuery, `host:${host}\n`, "host", "UNSIGNED-PAYLOAD"].join("\n");
  const stringToSign = ["AWS4-HMAC-SHA256", stamp, scope, await sha256Hex(canonicalRequest)].join("\n");
  const signature = await hmacSha256Str(await signingKey(String(env.AWS_SECRET_ACCESS_KEY).trim(), date, region, "s3"), stringToSign);
  return `https://${host}${canonicalUri}?${canonicalQuery}&X-Amz-Signature=${signature}`;
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

function xmlEscape(value: string) {
  return String(value).replace(/[<>&'"]/g, (c) => ({
    "<": "&lt;",
    ">": "&gt;",
    "&": "&amp;",
    "'": "&apos;",
    '"': "&quot;",
  } as any)[c]);
}

async function signedS3Fetch(env: any, bucket: string, method: string, key: string, query: Record<string, string>, body = "") {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const host = `${bucket}.s3.${region}.amazonaws.com`;
  const canonicalUri = key ? `/${key.split("/").map(encodeURIComponent).join("/")}` : "/";
  const url = new URL(`https://${host}${canonicalUri}`);
  for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
  const headers = await signAwsJsonRequest({
    method,
    url: url.toString(),
    body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region,
    service: "s3",
    headers: body ? { "content-type": "application/xml" } : {},
  });
  const res = await fetch(url.toString(), { method, headers, body: body || undefined });
  if (!res.ok) throw new Error(`s3_${method.toLowerCase()}_failed_${res.status}: ${(await res.text()).slice(0, 1000)}`);
  return res;
}

export async function getS3Text(env: any, bucket: string, key: string) {
  return (await signedS3Fetch(env, bucket, "GET", key, {})).text();
}

export async function putS3Text(env: any, bucket: string, key: string, body: string, contentType = "application/vnd.apple.mpegurl") {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  const host = `${bucket}.s3.${region}.amazonaws.com`;
  const canonicalUri = `/${key.split("/").map(encodeURIComponent).join("/")}`;
  const url = new URL(`https://${host}${canonicalUri}`);
  const headers = await signAwsJsonRequest({
    method: "PUT",
    url: url.toString(),
    body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region,
    service: "s3",
    headers: { "content-type": contentType },
  });
  const res = await fetch(url.toString(), { method: "PUT", headers, body });
  if (!res.ok) throw new Error(`s3_put_failed_${res.status}: ${(await res.text()).slice(0, 1000)}`);
  return { key, bytes: new TextEncoder().encode(body).length };
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
  const listed = prefix ? (await listS3Objects(env, bucket, prefix, 10000)).map((obj) => obj.key) : [];
  const deleted = await deleteS3Keys(env, bucket, [...listed, ...keys]);
  return { listed: listed.length, deleted };
}
