export class SeatsDO {
  state: DurableObjectState;
  env: any;

  // sessionId -> lastSeenMs
  sessions: Map<string, number> = new Map();

  constructor(state: DurableObjectState, env: any) {
    this.state = state;
    this.env = env;
  }

  cleanup(now: number, timeoutMs: number) {
    for (const [sid, last] of this.sessions.entries()) {
      if (now - last > timeoutMs) this.sessions.delete(sid);
    }
  }

  json(data: any, status = 200) {
    return new Response(JSON.stringify(data), {
      status,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
    });
  }

  async fetch(req: Request) {
    const url = new URL(req.url);
    const path = url.pathname;

    const limit = Number(url.searchParams.get("limit") ?? this.env.DEFAULT_VIEWER_LIMIT ?? 150);
    const timeoutMs = 45_000;
    const now = Date.now();
    this.cleanup(now, timeoutMs);

    if (req.method === "POST" && path.endsWith("/view-session")) {
      const sessionId = crypto.randomUUID();
      if (this.sessions.size >= limit) {
        return this.json({ granted: false, active: this.sessions.size, limit });
      }
      this.sessions.set(sessionId, now);
      return this.json({ granted: true, session_id: sessionId, active: this.sessions.size, limit });
    }

    if (req.method === "POST" && path.endsWith("/heartbeat")) {
      const body = await req.json<any>().catch(() => ({}));
      const sessionId = body.session_id;
      if (!sessionId || !this.sessions.has(sessionId)) {
        return this.json({ ok: false, reason: "invalid_session" }, 401);
      }
      this.sessions.set(sessionId, now);
      return this.json({ ok: true, active: this.sessions.size, limit });
    }

    if (req.method === "POST" && path.endsWith("/leave")) {
      const body = await req.json<any>().catch(() => ({}));
      const sessionId = body.session_id;
      if (sessionId) this.sessions.delete(sessionId);
      return this.json({ ok: true, active: this.sessions.size, limit });
    }

    if (req.method === "GET" && path.endsWith("/stats")) {
      return this.json({ active: this.sessions.size, limit });
    }

    return this.json({ error: "not_found" }, 404);
  }
}
