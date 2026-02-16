export class BroadcastLockDO {
  state: DurableObjectState;
  env: any;

  ownerSessionId: string | null = null;
  lastSeenMs = 0;

  constructor(state: DurableObjectState, env: any) {
    this.state = state;
    this.env = env;
  }

  json(data: any, status = 200) {
    return new Response(JSON.stringify(data), {
      status,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
    });
  }

  get ttlMs() {
    return 25_000; // 25s
  }

  isStale(now: number) {
    return !this.ownerSessionId || (now - this.lastSeenMs > this.ttlMs);
  }

  async fetch(req: Request) {
    const url = new URL(req.url);
    const path = url.pathname;
    const now = Date.now();

    if (req.method === "POST" && path.endsWith("/claim")) {
      const newId = crypto.randomUUID();
      this.ownerSessionId = newId;
      this.lastSeenMs = now;
      return this.json({ ok: true, broadcast_session_id: newId });
    }

    if (req.method === "POST" && path.endsWith("/heartbeat")) {
      const body = await req.json<any>().catch(() => ({}));
      const sid = body.broadcast_session_id;

      if (!sid) return this.json({ ok: false, still_owner: false, reason: "missing_session" }, 401);

      if (this.isStale(now)) {
        return this.json({ ok: false, still_owner: false, reason: "stale_lock" }, 409);
      }

      const stillOwner = sid === this.ownerSessionId;
      if (stillOwner) this.lastSeenMs = now;

      return this.json({ ok: true, still_owner: stillOwner });
    }

    if (req.method === "POST" && path.endsWith("/release")) {
      const body = await req.json<any>().catch(() => ({}));
      const sid = body.broadcast_session_id;

      if (sid && sid === this.ownerSessionId) {
        this.ownerSessionId = null;
        this.lastSeenMs = 0;
      }
      return this.json({ ok: true });
    }

    if (req.method === "GET" && path.endsWith("/status")) {
      return this.json({
        has_owner: !!this.ownerSessionId && !this.isStale(now),
        stale: this.isStale(now),
        owner_last_seen_ms_ago: this.ownerSessionId ? now - this.lastSeenMs : null
      });
    }

    return this.json({ error: "not_found" }, 404);
  }
}
