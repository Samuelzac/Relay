window.RelayConfig = (() => {
  const canonicalHost = "castlink.stream";
  const canonicalApi = "https://api.castlink.stream";
  const fallbackWorker = canonicalApi;
  const aliasHosts = new Set(["castlink.co.nz", "www.castlink.co.nz", "www.castlink.stream"]);

  function fromMeta() {
    const el = document.querySelector('meta[name="relay-api-base"]');
    return el && el.content ? el.content.trim() : "";
  }

  function fromStorage() {
    try {
      return localStorage.getItem("RELAY_API_BASE") || "";
    } catch {
      return "";
    }
  }

  function likelyProductionApi() {
    const host = location.hostname;
    if (!host || host === "localhost" || host === "127.0.0.1" || host.endsWith(".pages.dev")) {
      return fallbackWorker;
    }
    if (host.startsWith("api.")) return location.origin;
    if (host === canonicalHost || aliasHosts.has(host)) return canonicalApi;
    const root = host.startsWith("www.") ? host.slice(4) : host;
    return `https://api.${root}`;
  }

  function cleanBase(value) {
    return String(value || "").replace(/\/+$/, "");
  }

  const apiBase = cleanBase(window.RELAY_API_BASE || fromMeta() || fromStorage() || likelyProductionApi());

  return {
    apiBase,
    brandName: window.RELAY_BRAND_NAME || "Castlink",
  };
})();
