const SERVER_BASE = "http://localhost:30001";

document.getElementById("open-studio").addEventListener("click", () => {
  chrome.tabs.create({ url: SERVER_BASE });
});

chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
  const tab = tabs[0];
  if (!tab) return;

  chrome.runtime.sendMessage({ action: "get_tab_streams", tabId: tab.id }, (resp) => {
    const listEl = document.getElementById("list");
    const streams = (resp && resp.streams) || [];

    if (!streams.length) {
      listEl.innerHTML = `
        <div class="item">
          <div class="meta">
            <strong>Chưa bắt được luồng .m3u8/.mp4 trực tiếp</strong>
            <small>Bấm nút bên dưới để bóc tách trang này bằng PC Studio</small>
          </div>
          <button id="sniff-page">🔍 Bóc tách</button>
        </div>
      `;
      document.getElementById("sniff-page").onclick = () => {
        fetch(`${SERVER_BASE}/api/extract`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: tab.url, deep_browser: true }),
        }).then((r) => r.json()).then((d) => {
          if (d.streams && d.streams[0]) {
            castStream(d.streams[0]);
          } else {
            chrome.tabs.create({ url: SERVER_BASE });
          }
        });
      };
      return;
    }

    streams.forEach((st) => {
      const div = document.createElement("div");
      div.className = "item";
      div.innerHTML = `
        <div class="meta">
          <strong>${st.title}</strong>
          <small>${st.url}</small>
        </div>
        <button>▶️ Phát TV</button>
      `;
      div.querySelector("button").onclick = () => castStream(st);
      listEl.appendChild(div);
    });
  });
});

function castStream(st) {
  fetch(`${SERVER_BASE}/api/cast`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url: st.url,
      title: st.title,
      referer: st.referer,
      user_agent: st.user_agent,
      use_proxy: true,
    }),
  }).then(() => {
    window.close();
  });
}
