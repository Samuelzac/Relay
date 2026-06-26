window.RelayConfig = (() => {
  const canonicalHost = "castlink.stream";
  const canonicalApi = "https://api.castlink.stream";
  const canonicalRecordingApi = "https://castlink-recording-worker.kiwismurph.workers.dev";
  const stagingPagesHost = "castlink-staging.pages.dev";
  const stagingWorkerApi = "https://stream-platform-api-staging.kiwismurph.workers.dev";
  const stagingRecordingApi = "https://castlink-recording-worker-staging.kiwismurph.workers.dev";
  const fallbackWorker = canonicalApi;
  const fallbackRecordingWorker = canonicalRecordingApi;
  const aliasHosts = new Set(["castlink.co.nz", "www.castlink.co.nz", "www.castlink.stream"]);

  function fromMeta(name) {
    const el = document.querySelector(`meta[name="${name}"]`);
    return el && el.content ? el.content.trim() : "";
  }

  function fromStorage(key) {
    try {
      return localStorage.getItem(key) || "";
    } catch {
      return "";
    }
  }

  function likelyProductionApi() {
    const host = location.hostname;
    if (host === stagingPagesHost) return stagingWorkerApi;
    if (!host || host === "localhost" || host === "127.0.0.1" || host.endsWith(".pages.dev")) {
      return fallbackWorker;
    }
    if (host.startsWith("api.")) return location.origin;
    if (host === canonicalHost || aliasHosts.has(host)) return canonicalApi;
    const root = host.startsWith("www.") ? host.slice(4) : host;
    return `https://api.${root}`;
  }

  function likelyProductionRecordingApi() {
    const host = location.hostname;
    if (host === stagingPagesHost) return stagingRecordingApi;
    if (!host || host === "localhost" || host === "127.0.0.1" || host.endsWith(".pages.dev")) {
      return fallbackRecordingWorker;
    }
    if (host.startsWith("recording.")) return location.origin;
    if (host === canonicalHost || aliasHosts.has(host)) return canonicalRecordingApi;
    const root = host.startsWith("www.") ? host.slice(4) : host;
    return `https://recording.${root}`;
  }

  function cleanBase(value) {
    return String(value || "").replace(/\/+$/, "");
  }

  const apiBase = cleanBase(window.RELAY_API_BASE || fromMeta("relay-api-base") || fromStorage("RELAY_API_BASE") || likelyProductionApi());
  const recordingApiBase = cleanBase(
    window.RELAY_RECORDING_API_BASE ||
    fromMeta("relay-recording-api-base") ||
    fromStorage("RELAY_RECORDING_API_BASE") ||
    likelyProductionRecordingApi()
  );

  return {
    apiBase,
    recordingApiBase,
    brandName: window.RELAY_BRAND_NAME || "Castlink",
  };
})();
