function b64(bytes: ArrayBuffer) {
  return btoa(String.fromCharCode(...new Uint8Array(bytes)));
}
function unb64(s: string) {
  return Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
}

export function randomSecretUrlSafe(len = 48) {
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  return btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}

export async function sha256Hex(s: string) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export async function encryptString(plaintext: string, base64Key32: string) {
  const keyRaw = unb64(base64Key32);
  if (keyRaw.length !== 32) throw new Error("STREAMKEY_ENC_KEY_B64 must decode to 32 bytes");
  const key = await crypto.subtle.importKey("raw", keyRaw, "AES-GCM", false, ["encrypt"]);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, new TextEncoder().encode(plaintext));
  return `${b64(iv.buffer)}.${b64(ct)}`;
}

export async function decryptString(ciphertext: string, base64Key32: string) {
  const [ivB64, ctB64] = ciphertext.split(".");
  const keyRaw = unb64(base64Key32);
  if (keyRaw.length !== 32) throw new Error("STREAMKEY_ENC_KEY_B64 must decode to 32 bytes");
  const key = await crypto.subtle.importKey("raw", keyRaw, "AES-GCM", false, ["decrypt"]);
  const iv = unb64(ivB64);
  const ct = unb64(ctB64);
  const pt = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ct);
  return new TextDecoder().decode(pt);
}
