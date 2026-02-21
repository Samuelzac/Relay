import Stripe from "stripe";

export type StreamMode = "webrtc" | "hls" | "both";

export function stripeClient(env: any) {
  return new Stripe(env.STRIPE_SECRET_KEY, {
    apiVersion: "2024-06-20",
    httpClient: Stripe.createFetchHttpClient(),
  });
}

export function basePriceForTier(env: any, tier: number) {
  if (tier === 3) return Number(env.STANDARD_PRICE_NZD);
  if (tier === 8) return Number(env.EXTENDED_PRICE_NZD);
  throw new Error("Invalid tier");
}

export function addonPriceForMode(env: any, mode: StreamMode) {
  // Keep existing pricing working: HLS-only adds $0 by default.
  // Add-ons are configured via Worker vars.
  if (mode === "hls") return Number(env.HLS_ADDON_NZD ?? 0);
  if (mode === "webrtc") return Number(env.WEBRTC_ADDON_NZD ?? 0);
  if (mode === "both") return Number(env.BOTH_ADDON_NZD ?? 0);
  throw new Error("Invalid mode");
}

export function priceForTierAndMode(env: any, tier: number, mode: StreamMode) {
  return basePriceForTier(env, tier) + addonPriceForMode(env, mode);
}

export function hoursForTier(env: any, tier: number) {
  if (tier === 3) return Number(env.STANDARD_HOURS);
  if (tier === 8) return Number(env.EXTENDED_HOURS);
  throw new Error("Invalid tier");
}
