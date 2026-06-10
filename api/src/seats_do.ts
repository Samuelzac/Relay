export class SeatsDO {
  state: DurableObjectState;
  env: any;

  sessions: Map<string, { startedMs: number; lastSeenMs: number }> = new Map();
  initialized = false;
  totalViewerMs = 0;
  peakActive = 0;

  constructor(state: DurableObjectState, env: any) {
    this.state = state;
    this.env = env;
  }

  cleanup(now: number, timeoutMs: number) {
    for (const [sid, session] of this.sessions.entries()) {
      if (now - session.lastSeenMs > timeoutMs) {
        this.totalViewerMs += Math.max(0, session.lastSeenMs - session.startedMs);
        this.sessions.delete(sid);
      }
    }
  }

  async init() {
    if (this.initialized) return;
    this.totalViewerMs = Number((await this.state.storage.get("totalViewerMs")) || 0);
    this.peakActive = Number((await this.state.storage.get("peakActive")) || 0);
    const stored = await this.state.storage.get<Record<string, { startedMs: number; lastSeenMs: number }>>("sessions");
    if (stored) this.sessions = new Map(Object.entries(stored));
    this.initialized = true;
  }

  async persist() {
    await this.state.storage.put("totalViewerMs", this.totalViewerMs);
    await this.state.storage.put("peakActive", this.peakActive);
    await this.state.storage.put("sessions", Object.fromEntries(this.sessions.entries()));
  }

  usageMs(now: number) {
    let activeMs = 0;
    for (const session of this.sessions.values()) activeMs += Math.max(0, now - session.startedMs);
    return this.totalViewerMs + activeMs;
  }

  json(data: any, status = 200) {
    return new Response(JSON.stringify(data), {
      status,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
    });
  }

  async fetch(req: Request) {
    await this.init();
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
      this.sessions.set(sessionId, { startedMs: now, lastSeenMs: now });
      this.peakActive = Math.max(this.peakActive, this.sessions.size);
      await this.persist();
      return this.json({ granted: true, session_id: sessionId, active: this.sessions.size, limit });
    }

    if (req.method === "POST" && path.endsWith("/heartbeat")) {
      const body = await req.json<any>().catch(() => ({}));
      const sessionId = body.session_id;
      if (!sessionId || !this.sessions.has(sessionId)) {
        return this.json({ ok: false, reason: "invalid_session" }, 401);
      }
      const session = this.sessions.get(sessionId)!;
      session.lastSeenMs = now;
      this.sessions.set(sessionId, session);
      this.peakActive = Math.max(this.peakActive, this.sessions.size);
      await this.persist();
      return this.json({ ok: true, active: this.sessions.size, limit });
    }

    if (req.method === "POST" && path.endsWith("/leave")) {
      const body = await req.json<any>().catch(() => ({}));
      const sessionId = body.session_id;
      if (sessionId) {
        const session = this.sessions.get(sessionId);
        if (session) this.totalViewerMs += Math.max(0, now - session.startedMs);
        this.sessions.delete(sessionId);
        await this.persist();
      }
      return this.json({ ok: true, active: this.sessions.size, limit });
    }

    if (req.method === "GET" && path.endsWith("/stats")) {
      return this.json({
        active: this.sessions.size,
        limit,
        peak_active: this.peakActive,
        total_viewer_seconds: Math.ceil(this.usageMs(now) / 1000),
      });
    }

    return this.json({ error: "not_found" }, 404);
  }
}
