import Stripe from "stripe";

export function stripeClient(env: any) {
  return new Stripe(env.STRIPE_SECRET_KEY, {
    apiVersion: "2024-06-20",
    httpClient: Stripe.createFetchHttpClient()
  });
}

export function priceForTier(env: any, tier: number) {
  if (tier === 3) return Number(env.STANDARD_PRICE_NZD);
  if (tier === 8) return Number(env.EXTENDED_PRICE_NZD);
  throw new Error("Invalid tier");
}

export function hoursForTier(env: any, tier: number) {
  if (tier === 3) return Number(env.STANDARD_HOURS);
  if (tier === 8) return Number(env.EXTENDED_HOURS);
  throw new Error("Invalid tier");
}
