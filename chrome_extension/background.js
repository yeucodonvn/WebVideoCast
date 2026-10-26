const SERVER_BASE = "http://localhost:30001";
const tabStreams = {}; // tabId -> Array of stream objects

const NOISE_PATTERNS = [
  "manifest.webmanifest",
  ".webmanifest",
  "ping.gif",
  "jwpltx.com",
  "google-analytics.com",
  "googletagmanager.com",
  "doubleclick.net",
  "googlesyndication.com",
  "facebook.com/tr",
  "clarity.ms",
  "sentry.io",
  "/discover.gif",
  "cast2tv.gif",
  "1s_blank.mp4",
  "/playergk/",
  "333xbet",
  "video888.mp4",
  "player-ads.xml",
  "decafeligiblyhad.com",
];

const IGNORE_EXTENSIONS = [
  ".ts", ".m4s", ".aac", ".mp3", ".jpg", ".jpeg", ".png", ".gif",
  ".webp", ".svg", ".ico", ".css", ".js", ".woff", ".woff2", ".ttf",
  ".webmanifest", ".json", ".xml"
];

function unwrapAndValidateUrl(rawUrl) {
  if (!rawUrl) return null;
  const low = rawUrl.toLowerCase();
  if (low.startsWith(SERVER_BASE) || low.includes(":30001/")) return null;

  // 1. If this is a JWPlayer ping.gif or tracker carrying ?mu=https://...index.m3u8, unwrap the real stream URL!
  try {
    const u = new URL(rawUrl);
    const mu = u.searchParams.get("mu") || u.searchParams.get("file") || u.searchParams.get("stream");
    if (mu && (mu.includes(".m3u8") || mu.includes(".mp4"))) {
      return {
        url: mu,
        kind: "video",
        vd: u.searchParams.get("vd") ? Number(u.searchParams.get("vd")) : 0,
        vw: u.searchParams.get("vw") ? Number(u.searchParams.get("vw")) : 0,
        vh: u.searchParams.get("vh") ? Number(u.searchParams.get("vh")) : 0,
        sbr: u.searchParams.get("sbr") ? Number(u.searchParams.get("sbr")) : 0,
      };
    }
  } catch (e) {}

  // 2. Check subtitle (.srt / .vtt)
  const cleanPath = low.split("?")[0].split("#")[0];
  if (cleanPath.endsWith(".vtt") || cleanPath.endsWith(".srt")) {
    return { url: rawUrl, kind: "subtitle" };
  }

  // 3. Block known noise domains & extensions
  for (const pat of NOISE_PATTERNS) {
    if (low.includes(pat)) return null;
  }
  for (const ext of IGNORE_EXTENSIONS) {
    if (cleanPath.endsWith(ext)) return null;
  }
  if (low.includes("seg-") || low.includes("chunk-") || low.includes("fragment-")) {
    return null;
  }

  // 4. Match real playable stream URLs
  if (
    cleanPath.endsWith(".m3u8") ||
    cleanPath.endsWith(".mp4") ||
    cleanPath.endsWith(".mkv") ||
    cleanPath.endsWith(".webm") ||
    cleanPath.endsWith(".mpd") ||
    low.includes(".m3u8?") ||
    low.includes(".mp4?") ||
    low.includes("/sora/")
  ) {
    return { url: rawUrl, kind: "video" };
  }

  return null;
}

const requestHeadersMap = {}; // requestId -> { referer, userAgent }

function reportDiscoveredVideo(tabId, streamUrl, referer, userAgent, hints = {}) {
  chrome.tabs.get(tabId, (tab) => {
    if (chrome.runtime.lastError || !tab) return;
    const list = tabStreams[tabId] || [];
    if (list.some((x) => x.url === streamUrl)) return;

    const item = {
      url: streamUrl,
      title: tab.title || "Web Video Stream",
      page_url: tab.url || referer,
      referer: referer || tab.url || "",
      user_agent: userAgent || "",
      duration_sec: hints.vd || 0,
      width: hints.vw || 0,
      height: hints.vh || 0,
      bitrate_kbps: hints.sbr || 0,
      time: new Date().toLocaleTimeString(),
    };
    list.unshift(item);
    tabStreams[tabId] = list.slice(0, 25);

    chrome.action.setBadgeText({
      tabId: tabId,
      text: String(tabStreams[tabId].length),
    });
    chrome.action.setBadgeBackgroundColor({
      tabId: tabId,
      color: "#ec4899",
    });

    fetch(`${SERVER_BASE}/api/sniff-report`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(item),
    }).catch(() => {});
  });
}

chrome.webRequest.onBeforeSendHeaders.addListener(
  (details) => {
    if (details.tabId < 0) return;

    let referer = details.initiator || "";
    let userAgent = "";
    for (const h of details.requestHeaders || []) {
      const name = h.name.toLowerCase();
      if (name === "referer") referer = h.value;
      if (name === "user-agent") userAgent = h.value;
    }
    requestHeadersMap[details.requestId] = { referer, userAgent };

    const parsed = unwrapAndValidateUrl(details.url);
    if (!parsed) return;

    if (parsed.kind === "subtitle") {
      chrome.tabs.get(details.tabId, (tab) => {
        if (chrome.runtime.lastError || !tab) return;
        fetch(`${SERVER_BASE}/api/sniff-subtitle`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            url: parsed.url,
            title: tab.title || "Subtitle",
            page_url: tab.url || referer,
            referer: referer || tab.url || "",
          }),
        }).catch(() => {});
      });
      return;
    }

    reportDiscoveredVideo(details.tabId, parsed.url, referer, userAgent, parsed);
  },
  { urls: ["<all_urls>"] },
  ["requestHeaders", "extraHeaders"]
);

// Universal Content-Type / MIME Sniffer: catches extensionless video URLs (like Abyss/Hydrax /sora/, googlevideo, etc.)!
chrome.webRequest.onHeadersReceived.addListener(
  (details) => {
    if (details.tabId < 0) return;
    const low = details.url.toLowerCase();
    if (low.startsWith(SERVER_BASE) || low.includes(":30001/")) return;
    for (const pat of NOISE_PATTERNS) {
      if (low.includes(pat)) return;
    }
    const cleanPath = low.split("?")[0].split("#")[0];
    if (cleanPath.endsWith(".ts") || cleanPath.endsWith(".m4s") || low.includes("seg-") || low.includes("chunk-")) {
      return;
    }

    let ctype = "";
    let clen = 0;
    let crange = "";
    for (const h of details.responseHeaders || []) {
      const name = h.name.toLowerCase();
      if (name === "content-type") ctype = (h.value || "").toLowerCase();
      if (name === "content-length") clen = parseInt(h.value || "0", 10);
      if (name === "content-range") crange = h.value || "";
    }

    const isVideoMime =
      ctype.startsWith("video/") ||
      ctype.includes("mpegurl") ||
      ctype.includes("dash+xml");

    if (isVideoMime) {
      if (!ctype.includes("mpegurl") && clen > 0 && clen < 50000 && !crange) return;
      const reqMeta = requestHeadersMap[details.requestId] || {};
      delete requestHeadersMap[details.requestId];
      reportDiscoveredVideo(details.tabId, details.url, reqMeta.referer || details.initiator || "", reqMeta.userAgent || "");
    }
  },
  { urls: ["<all_urls>"] },
  ["responseHeaders"]
);

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  const url = changeInfo.url || (changeInfo.status === "complete" && tab ? tab.url : "");
  if (!url) return;
  const m = url.match(/(?:youtube\.com\/(?:watch\?.*?v=|shorts\/)|youtu\.be\/)([A-Za-z0-9_-]{11})/);
  if (m) {
    const ytWatch = `https://www.youtube.com/watch?v=${m[1]}`;
    reportDiscoveredVideo(tabId, ytWatch, ytWatch, navigator.userAgent || "");
  }
});

chrome.tabs.onRemoved.addListener((tabId) => {
  delete tabStreams[tabId];
});

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === "get_tab_streams") {
    sendResponse({ streams: tabStreams[msg.tabId] || [] });
  }
  return true;
});
