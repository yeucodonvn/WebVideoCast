import asyncio
import base64
import io
import json
import os
import re
import socket
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ============================================================================
# 1. CAST2TV.APP NATIVE PROTOCOL & BASE36 CODE ENGINE (Reverse-Engineered)
# ============================================================================

BASE36_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"
PRIVATE_SUBNETS = [
    {"start": 3232235520, "end": 3232301055},  # 192.168.0.0 - 192.168.255.255
    {"start": 167772160, "end": 184549375},    # 10.0.0.0 - 10.255.255.255
    {"start": 2886729728, "end": 2887778303},  # 172.16.0.0 - 172.31.255.255
]

# 2x2 transparent GIF (Triggers instant auto-connect in cast2tv.app senders-available.ts line 604!)
GIF_2X2_BYTES = base64.b64decode(
    "R0lGODlhAgACAIAAAP///wAAACH5BAEAAAAALAAAAAACAAIAAAIChFEAOw=="
)

# 1x1 PNG icon for PC Sender
PNG_ICON_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)


def to_base36(val: int) -> str:
    if val <= 0:
        return "0"
    encoded = ""
    remainder = val
    while remainder > 0:
        remainder, i = divmod(remainder, 36)
        encoded = BASE36_CHARS[i] + encoded
    return encoded


def from_base36(value: str) -> int:
    decoded = 0
    for ch in value.lower().strip():
        idx = BASE36_CHARS.find(ch)
        if idx > -1:
            decoded = decoded * 36 + idx
    return decoded


def ip_to_int(ip: str) -> int:
    parts = [int(p) for p in ip.strip().split(".")]
    return (parts[0] << 24) + (parts[1] << 16) + (parts[2] << 8) + parts[3]


def int_to_ip(val: int) -> str:
    return f"{(val >> 24) & 0xFF}.{(val >> 16) & 0xFF}.{(val >> 8) & 0xFF}.{val & 0xFF}"


def ip_to_cast2tv_code(ip: str, port: int = 30001) -> Dict[str, str]:
    full_int = ip_to_int(ip)
    short_int = full_int
    for subnet in PRIVATE_SUBNETS:
        if subnet["start"] < short_int < subnet["end"]:
            short_int -= subnet["start"]
            break

    ip_code = to_base36(short_int).upper()
    full_ip_code = to_base36(full_int).lower()
    if port == 30001:
        code = ip_code
    elif port > 30000:
        code = f"{ip_code}.{to_base36(port - 30000).upper()}"
    else:
        code = f"{ip_code}.{to_base36(port).upper()}"

    return {
        "code": code,
        "ip": ip,
        "port": port,
        "wvcast_ssl_domain": f"{full_ip_code}.p.wvcast.com",
        "direct_tv_url": f"http://{ip}:{port}/tv",
        "receiver_url": f"http://{ip}:{port}/web-receiver/index.html",
    }


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def format_duration_hms(seconds: float) -> str:
    if not seconds or seconds <= 0:
        return ""
    total = int(round(seconds))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"00:{m:02d}:{s:02d}"


def format_resolution_badge(width: int, height: int) -> str:
    if not width or not height:
        return ""
    tag = f"{width}x{height}"
    if width >= 3800 or height >= 2100:
        return f"{tag} (4K UHD)"
    if width >= 2500 or height >= 1400:
        return f"{tag} (2K QHD)"
    if width >= 1900 or height >= 1000 or height >= 750:
        return f"{tag} (1080p FHD)"
    if width >= 1200 or height >= 700:
        return f"{tag} (720p HD)"
    if width >= 800 or height >= 480:
        return f"{tag} (480p)"
    return f"{tag} ({height}p)"


# ============================================================================
# 2. APPLICATION STATE & DUAL-PROTOCOL HUB (NATIVE WVC LONG-POLL + WEBSOCKET)
# ============================================================================

import sys
import threading
import webbrowser
from contextlib import asynccontextmanager

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PORT = int(os.environ.get("WVC_PORT", "30001"))
IS_FROZEN = getattr(sys, "frozen", False)
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", Path(__file__).parent)).resolve()
BASE_DIR = Path(sys.executable).parent.resolve() if IS_FROZEN else Path(__file__).parent.resolve()
STATIC_DIR = (BUNDLE_DIR / "static") if (BUNDLE_DIR / "static").exists() else (BASE_DIR / "static")
STATIC_DIR.mkdir(exist_ok=True)
if str(BUNDLE_DIR) not in os.environ.get("PATH", ""):
    os.environ["PATH"] = f"{BUNDLE_DIR}{os.pathsep}{os.environ.get('PATH', '')}"


@asynccontextmanager
async def _app_lifespan(app_instance: FastAPI):
    clock_task = asyncio.create_task(_realtime_media_clock_loop())
    try:
        yield
    finally:
        clock_task.cancel()
        await close_shared_http_client()


app = FastAPI(title="WebVideoCast PC Studio", version="3.0.0", lifespan=_app_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_private_network_headers(request: Request, call_next):
    if request.method == "OPTIONS":
        resp = Response(status_code=204)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, HEAD"
        resp.headers["Access-Control-Allow-Headers"] = "*"
        resp.headers["Access-Control-Allow-Private-Network"] = "true"
        return resp

    resp = await call_next(request)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    resp.headers["Access-Control-Allow-Private-Network"] = "true"
    # Allow segment caching on TV for smooth playback while keeping API/M3U8 responses fresh
    if request.url.path.startswith("/proxy/segment.ts"):
        resp.headers["Cache-Control"] = "public, max-age=3600"
    elif "Cache-Control" not in resp.headers:
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


from collections import OrderedDict

_SHARED_HTTP_CLIENT: Optional[httpx.AsyncClient] = None


def get_shared_http_client() -> httpx.AsyncClient:
    global _SHARED_HTTP_CLIENT
    if _SHARED_HTTP_CLIENT is None or _SHARED_HTTP_CLIENT.is_closed:
        _SHARED_HTTP_CLIENT = httpx.AsyncClient(
            verify=False,
            follow_redirects=True,
            timeout=httpx.Timeout(35.0, connect=8.0),
            limits=httpx.Limits(max_connections=120, max_keepalive_connections=50, keepalive_expiry=45.0),
        )
    return _SHARED_HTTP_CLIENT


async def close_shared_http_client():
    global _SHARED_HTTP_CLIENT
    if _SHARED_HTTP_CLIENT is not None and not _SHARED_HTTP_CLIENT.is_closed:
        try:
            await _SHARED_HTTP_CLIENT.aclose()
        except Exception:
            pass
    _SHARED_HTTP_CLIENT = None


DEFAULT_DISK_CACHE_DIR = BASE_DIR / "stream_cache"

BUFFER_PRESETS: Dict[str, Dict[str, Any]] = {
    "normal": {
        "mode": "normal",
        "enabled": True,
        "storage_type": "disk",  # "disk" (Ổ cứng - Không tốn RAM) | "ram" (Bộ nhớ RAM)
        "prefetch_segments": 5,
        "concurrency": 3,
        "max_ram_mb": 250,
        "mp4_readahead_mb": 16,
        "prebuffer_before_play": False,
        "hls_max_buffer_sec": 90,
    },
    "weak_net": {
        "mode": "weak_net",
        "enabled": True,
        "storage_type": "disk",
        "prefetch_segments": 15,
        "concurrency": 5,
        "max_ram_mb": 800,
        "mp4_readahead_mb": 48,
        "prebuffer_before_play": True,
        "hls_max_buffer_sec": 240,
    },
    "ultra": {
        "mode": "ultra",
        "enabled": True,
        "storage_type": "disk",
        "prefetch_segments": 30,
        "concurrency": 6,
        "max_ram_mb": 2048,
        "mp4_readahead_mb": 96,
        "prebuffer_before_play": True,
        "hls_max_buffer_sec": 480,
    },
}


class CastState:
    def __init__(self):
        # WebSocket TVs (/ws/tv)
        self.tv_clients: Dict[str, WebSocket] = {}
        # Native WVC Long-Poll TVs (/web-receiver-io/longPoll)
        self.wvc_queues: Dict[str, asyncio.Queue] = {}
        self.wvc_connect_queues: Dict[str, asyncio.Queue] = {}
        # Unified TV Info dictionary (cid -> info)
        self.tv_info: Dict[str, Dict[str, Any]] = {}

        self.pc_clients: Dict[str, WebSocket] = {}
        self.current_media: Dict[str, Any] = {
            "url": "",
            "original_url": "",
            "title": "Chưa phát video",
            "type": "auto",
            "mimeType": "video/mp4",
            "use_proxy": True,
            "headers": {},
            "subtitles": [],
            "playing": False,
            "currentTime": 0,
            "duration": 0,
            "volume": 1.0,
            "muted": False,
            "playbackRate": 1.0,
            "updatedAt": time.time(),
        }
        self.sniffed_history: List[Dict[str, Any]] = []
        self.sniffed_subtitles: List[Dict[str, Any]] = []
        self.probe_cache: Dict[str, Dict[str, Any]] = {}
        self.local_files: Dict[str, Path] = {}
        self.dlna_devices: List[Dict[str, Any]] = []
        # Multi-Episode Playlist & Custom YouTube/Music Loop Engine
        self.episodes: List[Dict[str, Any]] = []
        self.current_episode_index: int = -1
        self.auto_next_enabled: bool = True
        self.loop_mode: str = "all"  # "off" | "all" (lặp toàn bộ danh sách) | "one" (lặp 1 bài)
        self.skip_intro_sec: int = 0
        self.skip_outro_sec: int = 0
        self.sponsorblock_enabled: bool = True
        self.saved_playlists: Dict[str, List[Dict[str, Any]]] = {}
        self.last_yt_query: str = ""
        self.last_yt_results: List[Dict[str, Any]] = []
        self.preferred_height: int = 1080
        self.last_auto_next_ts: float = 0.0
        self.max_position_reached: float = 0.0
        self.sniffing_yt_urls: set = set()
        self.yt_stream_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self.virtual_m3u8_store: Dict[str, Any] = {}

        # Smart Disk / RAM Buffer & Multi-Thread HLS/MP4 Read-Ahead Engine (Chống Đứng Hình Khi Mạng Yếu)
        self.buffer_config: Dict[str, Any] = {
            **BUFFER_PRESETS["weak_net"],
            "disk_dir": str(DEFAULT_DISK_CACHE_DIR),
        }
        self.hls_segment_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.hls_cache_bytes: int = 0
        self.hls_inflight_tasks: Dict[str, asyncio.Task] = {}
        self.hls_next_map: Dict[str, Dict[str, Any]] = {}
        self.hls_prefetch_sem: asyncio.Semaphore = asyncio.Semaphore(5)
        self.hls_cache_hits: int = 0
        self.hls_cache_misses: int = 0
        self.hls_prefetched_count: int = 0
        self.mp4_buffered_bytes: int = 0

        # Subtitle Setup & Customization State (Font Size, Colors, Opacity, Outline, Position, Sync Offset)
        self.subtitle_style: Dict[str, Any] = {
            "textSize": 1.5,            # 1.5 = 150% (0.6 -> 3.0)
            "textColor": "#FFFF00",     # Vàng rạp phim (#FFFF00) hoặc Trắng (#FFFFFF)
            "backgroundColor": "#000000",
            "backgroundOpacity": 0.65,  # 0.0 (Trong suốt) -> 1.0 (Đen đặc)
            "edgeStyle": "outline",     # "outline" | "shadow" | "normal"
            "edgeColor": "#000000",
            "textFont": "sans-serif",   # "sans-serif" | "serif" | "monospace"
            "textStyle": "bold",        # "bold" | "normal" | "italic" | "bold_italic"
            "bottomPosition": 8,        # 2% -> 45% (cách đáy màn hình TV)
            "offsetSec": 0.0,           # Độ trễ phụ đề (+/- giây)
        }
        self.uploaded_subtitles: Dict[str, Dict[str, Any]] = {}

        self._load_playlists_from_disk()
        self.ensure_disk_cache_dir()
        self._cleanup_disk_cache_files()

    def get_disk_cache_dir(self) -> Path:
        raw_dir = (self.buffer_config.get("disk_dir") or "").strip()
        p = Path(raw_dir) if raw_dir else DEFAULT_DISK_CACHE_DIR
        return p

    def ensure_disk_cache_dir(self) -> Path:
        p = self.get_disk_cache_dir()
        try:
            p.mkdir(parents=True, exist_ok=True)
        except Exception:
            p = DEFAULT_DISK_CACHE_DIR
            p.mkdir(parents=True, exist_ok=True)
            self.buffer_config["disk_dir"] = str(p)
        return p

    def _cleanup_disk_cache_files(self):
        try:
            d = self.get_disk_cache_dir()
            if d.exists() and d.is_dir():
                for fp in d.iterdir():
                    if fp.is_file() and (fp.name.startswith("seg_") or fp.name.startswith("mp4chunk_")):
                        try:
                            fp.unlink(missing_ok=True)
                        except Exception:
                            pass
        except Exception:
            pass

    def build_wvc_subtitle_style_cmd(self) -> Dict[str, Any]:
        """
        Builds the exact Native WVC `subtitlesStyleSet` command (CommandReceiver.ts & Subtitles.ts).
        Note: Native WVC Subtitles.ts line 373 evaluates `(subtitleInfo.backgroundOpacity) ? ... : 0.75`,
        so if backgroundOpacity is 0.0 (transparent), we send 0.001 so JS treats it as truthy and renders 0% background!
        """
        st = self.subtitle_style
        bg_op = float(st.get("backgroundOpacity", 0.65))
        wvc_bg_op = 0.001 if bg_op <= 0.005 else min(1.0, max(0.001, bg_op))
        t_style = str(st.get("textStyle") or "bold").replace("bold_italic", "italic bold")
        return {
            "cmd": "subtitlesStyleSet",
            "backgroundColor": str(st.get("backgroundColor") or "#000000"),
            "backgroundOpacity": wvc_bg_op,
            "textFont": str(st.get("textFont") or "sans-serif"),
            "textColor": str(st.get("textColor") or "#FFFF00"),
            "textSize": float(st.get("textSize") or 1.5),
            "textStyle": t_style,
            "edgeColor": str(st.get("edgeColor") or "#000000"),
            "edgeStyle": str(st.get("edgeStyle") or "outline"),
        }

    def get_buffer_stats(self) -> Dict[str, Any]:
        hls_mb = round(self.hls_cache_bytes / (1024 * 1024), 2)
        mp4_mb = round(self.mp4_buffered_bytes / (1024 * 1024), 2)
        used_mb = round(hls_mb + mp4_mb, 1)
        total_req = self.hls_cache_hits + self.hls_cache_misses
        hit_rate = round((self.hls_cache_hits / total_req) * 100) if total_req > 0 else 100
        return {
            "mode": self.buffer_config.get("mode", "weak_net"),
            "storage_type": self.buffer_config.get("storage_type", "disk"),
            "disk_dir": str(self.get_disk_cache_dir()),
            "hls_cached_segments": len(self.hls_segment_cache),
            "cached_segments": len(self.hls_segment_cache),
            "inflight_tasks": len(self.hls_inflight_tasks),
            "hls_cache_mb": hls_mb,
            "mp4_buffered_mb": mp4_mb,
            "ram_used_mb": used_mb,
            "max_ram_mb": self.buffer_config.get("max_ram_mb", 800),
            "prefetch_segments": self.buffer_config.get("prefetch_segments", 15),
            "prefetched_total": self.hls_prefetched_count,
            "hls_cache_hits": self.hls_cache_hits,
            "hit_rate_pct": hit_rate,
        }

    def clear_ram_buffer(self):
        for t in list(self.hls_inflight_tasks.values()):
            if not t.done():
                t.cancel()
        self.hls_inflight_tasks.clear()
        for entry in list(self.hls_segment_cache.values()):
            if isinstance(entry, dict) and entry.get("path"):
                try:
                    Path(entry["path"]).unlink(missing_ok=True)
                except Exception:
                    pass
        self.hls_segment_cache.clear()
        self.hls_next_map.clear()
        self.hls_cache_bytes = 0
        self.mp4_buffered_bytes = 0
        self._cleanup_disk_cache_files()

    def _playlists_file(self) -> Path:
        return BASE_DIR / "playlists.json"

    def _load_playlists_from_disk(self):
        try:
            pf = self._playlists_file()
            if pf.exists():
                data = json.loads(pf.read_text(encoding="utf-8"))
                self.saved_playlists = data.get("saved_playlists") or {}
                self.loop_mode = data.get("loop_mode") or "all"
                self.auto_next_enabled = bool(data.get("auto_next_enabled", True))
                self.skip_intro_sec = max(0, int(data.get("skip_intro_sec") or 0))
                self.skip_outro_sec = max(0, int(data.get("skip_outro_sec") or 0))
                self.sponsorblock_enabled = bool(data.get("sponsorblock_enabled", True))
                self.last_yt_query = data.get("last_yt_query") or ""
                self.last_yt_results = data.get("last_yt_results") or []
                if isinstance(data.get("subtitle_style"), dict):
                    self.subtitle_style.update(data["subtitle_style"])
                    if int(self.subtitle_style.get("bottomPosition", 8)) > 60:
                        self.subtitle_style["bottomPosition"] = 100 - int(self.subtitle_style["bottomPosition"])
                if isinstance(data.get("buffer_config"), dict):
                    self.buffer_config.update(data["buffer_config"])
                    workers = max(1, min(8, int(self.buffer_config.get("concurrency") or self.buffer_config.get("parallel_workers") or 5)))
                    self.hls_prefetch_sem = asyncio.Semaphore(workers)
                if not self.episodes and data.get("last_queue"):
                    self.episodes = data["last_queue"]
                    self.current_episode_index = next((i for i, e in enumerate(self.episodes) if e.get("active")), 0) if self.episodes else -1
        except Exception:
            pass

    def save_playlists_to_disk(self):
        try:
            clean_q = [
                {k: v for k, v in ep.items() if k != "cached_streams"}
                for ep in self.episodes
            ]
            clean_saved = {
                name: [{k: v for k, v in ep.items() if k != "cached_streams"} for ep in items]
                for name, items in self.saved_playlists.items()
            }
            self._playlists_file().write_text(
                json.dumps(
                    {
                        "loop_mode": self.loop_mode,
                        "auto_next_enabled": self.auto_next_enabled,
                        "skip_intro_sec": self.skip_intro_sec,
                        "skip_outro_sec": self.skip_outro_sec,
                        "sponsorblock_enabled": self.sponsorblock_enabled,
                        "subtitle_style": self.subtitle_style,
                        "buffer_config": self.buffer_config,
                        "last_queue": clean_q,
                        "saved_playlists": clean_saved,
                        "last_yt_query": self.last_yt_query,
                        "last_yt_results": self.last_yt_results[:25],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass

    def get_active_tvs(self) -> List[Dict[str, Any]]:
        now = time.time()
        expired = [
            cid for cid, info in self.tv_info.items()
            if info.get("protocol") == "wvc-native" and (now - info.get("last_seen", now)) > 65
        ]
        for cid in expired:
            self.tv_info.pop(cid, None)
            self.wvc_queues.pop(cid, None)
        return list(self.tv_info.values())

    async def register_wvc_tv(self, client_id: str, ip: str, label: Optional[str] = None, from_longpoll: bool = False):
        is_new = client_id not in self.tv_info
        existing = self.tv_info.get(client_id, {})
        now = time.time()
        self.tv_info[client_id] = {
            "id": client_id,
            "ip": ip,
            "label": label or existing.get("label") or f"Smart TV WVC ({ip})",
            "protocol": "wvc-native",
            "connected_at": existing.get("connected_at") or time.strftime("%H:%M:%S"),
            "last_seen": now,
            "last_longpoll_ts": now if from_longpoll else existing.get("last_longpoll_ts", 0.0),
        }
        if client_id not in self.wvc_queues:
            self.wvc_queues[client_id] = asyncio.Queue()
            await self.wvc_queues[client_id].put({"cmd": "getDeviceInfo"})
            await self.wvc_queues[client_id].put({"cmd": "positionGet"})
        if is_new:
            await self.broadcast_to_pcs({"action": "tv_list", "tvs": self.get_active_tvs()})

    async def push_wvc_command(self, cmd_obj: Dict[str, Any]):
        for cid, q in list(self.wvc_queues.items()):
            await q.put(cmd_obj)

    async def broadcast_to_tvs(self, ws_message: Dict[str, Any], wvc_command: Optional[Dict[str, Any]] = None):
        dead = []
        for cid, ws in list(self.tv_clients.items()):
            try:
                await ws.send_json(ws_message)
            except Exception:
                dead.append(cid)
        for cid in dead:
            self.tv_clients.pop(cid, None)
            self.tv_info.pop(cid, None)

        if wvc_command:
            await self.push_wvc_command(wvc_command)

        await self.broadcast_to_pcs({"action": "tv_list", "tvs": self.get_active_tvs()})

    async def broadcast_to_pcs(self, message: Dict[str, Any]):
        dead = []
        for cid, ws in list(self.pc_clients.items()):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(cid)
        for cid in dead:
            self.pc_clients.pop(cid, None)


state = CastState()


async def trigger_auto_next_if_ready(reason: str = ""):
    """
    Unified Auto-Next & Loop Engine:
    - If `state.loop_mode == "one"`, replays the current track/episode (`state.current_episode_index`)!
    - If `state.auto_next_enabled`:
      - Advances to `state.current_episode_index + 1` if there is a next item.
      - Or if at the end of the playlist and `state.loop_mode == "all"`, wraps back to index `0` (Nghe/Xem đi nghe lại)!
    """
    if (time.time() - state.last_auto_next_ts) <= 12.0:
        return
    if not state.episodes:
        return

    target_idx = None
    if state.loop_mode == "one" and 0 <= state.current_episode_index < len(state.episodes):
        target_idx = state.current_episode_index
    elif state.auto_next_enabled:
        if 0 <= state.current_episode_index < len(state.episodes) - 1:
            target_idx = state.current_episode_index + 1
        elif state.loop_mode == "all" and len(state.episodes) > 0:
            target_idx = 0

    if target_idx is not None:
        state.last_auto_next_ts = time.time()
        print(f"[AUTO-NEXT / LOOP ({state.loop_mode})] {reason} -> playing item #{target_idx + 1}/{len(state.episodes)}!", flush=True)
        asyncio.create_task(play_episode_by_index(target_idx))


async def check_and_apply_skip_rules(cur: float, dur: float, from_hardware_pos: bool = False):
    """
    Real-Time Auto-Skip Intro / Outro & YouTube SponsorBlock Engine:
    1. Series Auto-Skip Intro (`state.skip_intro_sec > 0`):
       If playback starts in `[0.5s .. skip_intro_sec - 2.0s]` on a full episode (`dur > skip_intro_sec + 45s`)
       and hasn't skipped intro yet for this episode, seeks TV directly to `skip_intro_sec`!
    2. YouTube SponsorBlock (`state.sponsorblock_enabled == True`):
       Checks `state.current_media.get("skip_segments")` (fetched from `sponsor.ajay.app`).
       Whenever `seg["start"] <= cur < seg["end"] - 1.5`, automatically seeks TV to `seg["end"]`
       (or triggers Auto-Next if the segment reaches the end of the video)!
    3. Series Auto-Skip Outro (`state.skip_outro_sec > 0`):
       When `cur >= dur - effective_outro`, triggers `trigger_auto_next_if_ready` to jump to the next episode early!
    """
    now = time.time()
    skip_intro = int(state.skip_intro_sec or 0)
    skip_outro = int(state.skip_outro_sec or 0)

    # 1. Auto-Skip Intro on Smart TV if TV ignored initial loadMedia position and started at 0s
    if (
        from_hardware_pos
        and skip_intro > 0
        and dur > (skip_intro + 45)
        and 0.3 <= cur < (skip_intro - 2.0)
        and not state.current_media.get("intro_skipped")
        and (now - state.current_media.get("last_skip_seek_ts", 0.0)) > 4.0
    ):
        state.current_media["intro_skipped"] = True
        state.current_media["last_skip_seek_ts"] = now
        state.current_media["currentTime"] = float(skip_intro)
        print(f"[SKIP INTRO] Auto-jumping past Intro from {cur:.1f}s -> {skip_intro}s!", flush=True)
        await state.broadcast_to_tvs(
            ws_message={"action": "control", "command": "seek", "value": float(skip_intro)},
            wvc_command={"cmd": "positionSet", "position": int(skip_intro * 1000)},
        )
        return

    # 2. YouTube SponsorBlock segments (sponsor / intro / outro / selfpromo / music_offtopic)
    if state.sponsorblock_enabled:
        segments = state.current_media.get("skip_segments") or []
        for seg in segments:
            s_start = float(seg.get("start") or 0)
            s_end = float(seg.get("end") or 0)
            s_cat = seg.get("category") or "sponsor"
            if s_end > s_start + 1.5 and s_start <= cur < (s_end - 1.5):
                if (now - state.current_media.get("last_skip_seek_ts", 0.0)) <= 4.5:
                    break
                state.current_media["last_skip_seek_ts"] = now
                if dur > 20 and s_end >= (dur - 2.5):
                    print(f"[SPONSORBLOCK OUTRO ({s_cat})] Segment {s_start:.1f}s->{s_end:.1f}s reaches end -> Auto-Next!", flush=True)
                    await trigger_auto_next_if_ready(f"SponsorBlock {s_cat} reached end at {cur:.1f}s/{dur:.1f}s")
                    return
                state.current_media["currentTime"] = s_end
                print(f"[SPONSORBLOCK ({s_cat})] Auto-skipping {s_start:.1f}s -> {s_end:.1f}s!", flush=True)
                await state.broadcast_to_tvs(
                    ws_message={"action": "control", "command": "seek", "value": s_end},
                    wvc_command={"cmd": "positionSet", "position": int(s_end * 1000)},
                )
                return

    # 3. Auto-Skip Outro / End-of-Episode Auto-Next
    effective_outro = skip_outro if (skip_outro > 0 and dur > (skip_intro + skip_outro + 45)) else 0
    threshold = max(1.5, float(effective_outro))
    if dur > 20 and cur >= (dur - threshold):
        reason_str = (
            f"Skip Outro ({effective_outro}s before end at {cur:.1f}s/{dur:.1f}s)"
            if effective_outro > 0
            else f"Clock/Pos reached {cur:.1f}s/{dur:.1f}s"
        )
        await trigger_auto_next_if_ready(reason_str)


async def _realtime_media_clock_loop():
    """
    1-Second Real-Time Media Clock & WVC Hardware Position Poller:
    - Why did the TV only update every 60 seconds (`08:00 / 21:42` -> `09:00 / 21:42`) by default?
      Because the native Web Video Caster Receiver app (`cast2tv.app`) only pushes spontaneous
      `/web-receiver-io/position` heartbeats once per minute unless the sender sends `{"cmd": "positionGet"}`!
    - This loop ticks every 1.0 second:
      1. Smoothly increments `state.current_media["currentTime"]` by `1.0 * playbackRate` whenever `playing == True`
         and broadcasts `tv_status` to all connected PC Dashboards every second (`08:01`, `08:02`, `08:03`...).
      2. Every 5 seconds (or every 2 seconds during the final 30 seconds of an episode), pushes `{"cmd": "positionGet"}`
         to the connected WVC Smart TV so the TV reports its exact hardware millisecond position and triggers
         Auto-Next Episode right on time!
    """
    tick_count = 0
    while True:
        await asyncio.sleep(1.0)
        try:
            tick_count += 1
            if not state.current_media.get("playing") or not state.current_media.get("url"):
                continue

            rate = float(state.current_media.get("playbackRate") or 1.0)
            cur = float(state.current_media.get("currentTime") or 0.0) + (1.0 * rate)
            dur = float(state.current_media.get("duration") or 0.0)
            if dur > 0 and cur > dur:
                cur = dur

            state.current_media["currentTime"] = cur
            if cur > state.max_position_reached:
                state.max_position_reached = cur

            # Broadcast 1-second real-time clock update to all PC Dashboards
            if state.pc_clients:
                await state.broadcast_to_pcs({
                    "action": "tv_status",
                    "tv_id": "realtime-clock",
                    "status": {
                        "currentTime": cur,
                        "duration": dur,
                        "playing": True,
                        "playbackRate": rate,
                        "buffer_stats": state.get_buffer_stats(),
                    },
                })

            # Poll exact hardware position from WVC Receiver on TV via {"cmd": "positionGet"}
            eff_outro = state.skip_outro_sec if (state.skip_outro_sec > 0 and dur > (state.skip_intro_sec + state.skip_outro_sec + 45)) else 0
            near_end = (dur > 20 and (dur - cur) <= (30.0 + eff_outro)) or (state.skip_intro_sec > 0 and cur < state.skip_intro_sec)
            poll_interval = 2 if near_end else 5
            if tick_count % poll_interval == 0:
                for cid, q in list(state.wvc_queues.items()):
                    if q.empty():
                        await q.put({"cmd": "positionGet"})

            # Check Skip Intro / SponsorBlock / Skip Outro / Auto-Next
            await check_and_apply_skip_rules(cur, dur, from_hardware_pos=False)
        except Exception:
            pass


# ============================================================================
# 3. NATIVE WEB VIDEO CASTER TV RECEIVER PROTOCOL (/web-receiver-io/*)
# ============================================================================

@app.api_route("/web-receiver/discover.gif", methods=["GET", "HEAD"])
@app.api_route("/discover.gif", methods=["GET", "HEAD"])
@app.api_route("/web-receiver-io/cast2tv.gif", methods=["GET", "HEAD"])
async def cast2tv_discover_gif(request: Request):
    client_ip = request.client.host if request.client else "127.0.0.1"
    device_id = request.query_params.get("device") or request.query_params.get("deviceId")
    label = request.query_params.get("label")
    if client_ip not in ("127.0.0.1", "::1", get_local_ip()):
        cid = device_id or f"tv-{client_ip}"
        await state.register_wvc_tv(cid, client_ip, label)
    return Response(content=GIF_2X2_BYTES, media_type="image/gif")


@app.get("/static/pc-icon.png")
async def pc_icon_png():
    return Response(content=PNG_ICON_BYTES, media_type="image/png")


@app.post("/web-receiver-io/connect")
async def wvc_longpoll_connect(request: Request):
    client_ip = request.client.host if request.client else "TV"
    try:
        body = await request.json()
    except Exception:
        body = {}

    receiver = body.get("receiver") or {}
    cid = receiver.get("deviceId") or request.headers.get("client-id") or f"tv-{client_ip}"
    label = receiver.get("label") or f"Smart TV ({client_ip})"
    await state.register_wvc_tv(cid, client_ip, label, from_longpoll=False)

    # Check if this TV (or any entry with the same IP) is already actively polling /web-receiver-io/longPoll
    now = time.time()
    is_already_polling = any(
        info.get("ip") == client_ip and (now - info.get("last_longpoll_ts", 0.0)) < 25.0
        for info in state.tv_info.values()
    )
    if not is_already_polling:
        local_ip = get_local_ip()
        print(f"[AUTO-CONNECT TV] Instructing TV {client_ip} ({cid}) to connectToSender {local_ip}:{PORT}!", flush=True)
        return JSONResponse({
            "action": "connectToSender",
            "data": {
                "sender": {
                    "deviceId": f"{local_ip}:{PORT}",
                    "localIp": local_ip,
                    "localPort": PORT,
                    "label": f"WebVideoCast PC Studio ({local_ip})",
                    "icon": f"http://{local_ip}:{PORT}/static/pc-icon.png",
                }
            },
        })

    await asyncio.sleep(15.0)
    return Response(content="", status_code=200, media_type="text/plain")


@app.post("/web-receiver-io/request-connection")
async def wvc_request_connection(request: Request):
    client_ip = request.client.host if request.client else "TV"
    try:
        body = await request.json()
    except Exception:
        body = {}

    receiver = body.get("receiver") or {}
    cid = request.headers.get("client-id") or receiver.get("deviceId") or f"tv-{client_ip}"
    label = receiver.get("label") or f"Smart TV ({client_ip})"
    await state.register_wvc_tv(cid, client_ip, label, from_longpoll=True)
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/longPoll")
async def wvc_command_longpoll(request: Request):
    client_ip = request.client.host if request.client else "TV"
    try:
        body = await request.json()
    except Exception:
        body = {}

    receiver = body.get("receiver") or {}
    cid = request.headers.get("client-id") or receiver.get("deviceId") or f"tv-{client_ip}"
    label = receiver.get("label")
    await state.register_wvc_tv(cid, client_ip, label, from_longpoll=True)

    q = state.wvc_queues[cid]

    try:
        cmd = await asyncio.wait_for(q.get(), timeout=22.0)
        return JSONResponse(cmd)
    except asyncio.TimeoutError:
        return JSONResponse({})


@app.post("/web-receiver-io/deviceInfo")
async def wvc_device_info(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    try:
        info = await request.json()
    except Exception:
        info = {}

    brand = info.get("brand") or info.get("buildVariant") or "Smart TV"
    devs = info.get("device") or []
    model = devs[0] if devs else ""
    nice_label = f"{brand} {model} ({client_ip})".strip()
    await state.register_wvc_tv(cid, client_ip, nice_label)
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/state")
async def wvc_state_update(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    await state.register_wvc_tv(cid, client_ip)
    try:
        data = await request.json()
        print(f"[TV STATE] {client_ip}: {data}", flush=True)
        tv_state = (data.get("state") or "").lower()
        state.current_media["playing"] = tv_state in ("playing", "loaded", "buffering")
        dur = float(state.current_media.get("duration") or 0)
        max_pos = float(state.max_position_reached or state.current_media.get("currentTime") or 0)
        await state.broadcast_to_pcs({
            "action": "tv_status",
            "tv_id": cid,
            "status": {
                "currentTime": state.current_media.get("currentTime", 0),
                "duration": dur,
                "playing": state.current_media["playing"],
                "state": tv_state,
            },
        })
        # Auto-Next / Loop when TV transitions to idle/ended/completed near the end of a track/episode!
        eff_outro = state.skip_outro_sec if (state.skip_outro_sec > 0 and dur > (state.skip_intro_sec + state.skip_outro_sec + 45)) else 0
        if tv_state in ("idle", "ended", "completed", "stopped") and dur > 20 and max_pos >= (dur - max(15.0, float(eff_outro) + 5.0)):
            await trigger_auto_next_if_ready(f"TV state={tv_state} at {max_pos:.1f}s/{dur:.1f}s")
    except Exception:
        pass
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/position")
async def wvc_position_update(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    await state.register_wvc_tv(cid, client_ip)
    try:
        data = await request.json()
        raw_pos = float(data.get("position") or 0)
        raw_dur = float(data.get("duration") or 0)
        pos = raw_pos / 1000.0 if raw_dur > 100000 or raw_pos > 100000 else raw_pos
        dur = raw_dur / 1000.0 if raw_dur > 100000 else raw_dur
        print(f"[TV POSITION] {client_ip}: pos={pos:.1f}s / dur={dur:.1f}s", flush=True)
        state.current_media["currentTime"] = pos
        if dur > 0:
            state.current_media["duration"] = dur
            if not state.current_media.get("url"):
                state.current_media["url"] = "wvc-active-stream"
                state.current_media["playing"] = True
        if pos > state.max_position_reached:
            state.max_position_reached = pos
        await state.broadcast_to_pcs({
            "action": "tv_status",
            "tv_id": cid,
            "status": {
                "currentTime": pos,
                "duration": dur,
                "playing": state.current_media.get("playing", True),
            },
        })
        await check_and_apply_skip_rules(pos, dur, from_hardware_pos=True)
    except Exception:
        pass
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/error")
async def wvc_error_notification(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    try:
        data = await request.json()
        print(f"[TV ERROR] {client_ip}: {data}", flush=True)
    except Exception:
        pass
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/playNext")
async def wvc_play_next_notification(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    await state.register_wvc_tv(cid, client_ip)
    if state.episodes:
        nxt = (state.current_episode_index + 1) if state.current_episode_index < len(state.episodes) - 1 else (0 if state.loop_mode == "all" else -1)
        if nxt >= 0:
            state.last_auto_next_ts = time.time()
            asyncio.create_task(play_episode_by_index(nxt))
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/playPrevious")
async def wvc_play_prev_notification(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    await state.register_wvc_tv(cid, client_ip)
    if state.episodes:
        prv = (state.current_episode_index - 1) if state.current_episode_index > 0 else (len(state.episodes) - 1 if state.loop_mode == "all" else -1)
        if prv >= 0:
            state.last_auto_next_ts = time.time()
            asyncio.create_task(play_episode_by_index(prv))
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/media")
@app.post("/web-receiver-io/volume")
@app.post("/web-receiver-io/log")
async def wvc_misc_notifications(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    await state.register_wvc_tv(cid, client_ip)
    return JSONResponse({"status": "ok"})


@app.post("/web-receiver-io/disconnect")
async def wvc_disconnect(request: Request):
    client_ip = request.client.host if request.client else "TV"
    cid = request.headers.get("client-id") or f"tv-{client_ip}"
    state.tv_info.pop(cid, None)
    state.wvc_queues.pop(cid, None)
    await state.broadcast_to_pcs({"action": "tv_list", "tvs": state.get_active_tvs()})
    return JSONResponse({"status": "ok"})


@app.get("/web-receiver/index.html", response_class=HTMLResponse)
@app.get("/web-receiver/", response_class=HTMLResponse)
@app.get("/tv", response_class=HTMLResponse)
async def tv_receiver_page(request: Request, receiver: Optional[str] = None):
    tv_html_path = STATIC_DIR / "tv.html"
    return HTMLResponse(tv_html_path.read_text(encoding="utf-8"))


@app.get("/", response_class=HTMLResponse)
async def pc_dashboard_page():
    pc_html_path = STATIC_DIR / "index.html"
    return HTMLResponse(pc_html_path.read_text(encoding="utf-8"))


# ============================================================================
# 4. ANTI-CORS / HEADER-SPOOFING STREAM PROXY + FAKE PNG HLS HEADER STRIPPER
# ============================================================================

def encode_proxy_params(url: str, headers: Dict[str, str]) -> str:
    payload = json.dumps({"u": url, "h": headers}, ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def decode_proxy_params(token: str) -> Tuple[str, Dict[str, str]]:
    pad = "=" * ((4 - len(token) % 4) % 4)
    raw = base64.urlsafe_b64decode((token + pad).encode("ascii"))
    data = json.loads(raw.decode("utf-8"))
    return data["u"], data.get("h") or {}


def build_proxy_url(
    target_url: str,
    headers: Dict[str, str],
    host_base: str,
    is_hls: bool = False,
    is_segment: bool = False,
) -> str:
    token = encode_proxy_params(target_url, headers)
    if is_hls:
        suffix = "/stream.m3u8"
    elif is_segment:
        suffix = "/segment.ts"
    else:
        suffix = "/stream"
    return f"{host_base}/proxy{suffix}?token={urllib.parse.quote(token)}"


def strip_disguised_mpegts_header(data: bytes) -> bytes:
    """
    Many Vietnamese movie CDNs (e.g. streamvsmov.com / file-tiktok_1.png) prepend a fake
    PNG/JPG/GIF/BMP header (typically 64..2048 bytes) before the real 188-byte MPEG-TS packets!
    If we detect the 0x47 sync byte repeating at +188 and +376 within the first 4096 bytes,
    we slice off the fake image header so Smart TVs hardware-decode the MPEG-TS stream cleanly!
    """
    if len(data) < 564:
        return data
    if data[0] == 0x47 and data[188] == 0x47 and data[376] == 0x47:
        return data
    limit = min(4096, len(data) - 376)
    for i in range(1, limit):
        if data[i] == 0x47 and data[i + 188] == 0x47 and data[i + 376] == 0x47:
            return data[i:]
    return data


def decrypt_enc_aesgcm_m3u8_if_needed(text: str, stream_url: str, referer_url: str = "") -> str:
    """
    Universal Pure-Python Decryptor for `#ENC-AESGCM;iv=...` / `#EXT-X-B65:0-138` encrypted HLS playlists
    (used by embed14.streamc.xyz / streamc / rophim / cobephim Vietsub embeds):
    - Derives the 256-bit AES-GCM key via `HMAC-SHA256("stream-derive-v1", video_hash)`
    - Extracts `video_hash` automatically from the `/eyJ0Ijoi...` token in `stream_url` or `?hash=` in `referer_url`
    - Decrypts the Base64 ciphertext+tag into a standard `#EXTM3U` playlist with `#EXTINF` segments!
    """
    if "#ENC-AESGCM" not in text:
        return text
    try:
        import hashlib
        import hmac
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
        iv_m = re.search(r"#ENC-AESGCM;iv=([a-fA-F0-9]{24})", text)
        if not iv_m or len(lines) < 4:
            return text
        iv = bytes.fromhex(iv_m.group(1))
        b64_line = lines[-1]

        video_hash = ""
        for cand in (referer_url, stream_url):
            hm = re.search(r"[?&]hash=([a-fA-F0-9]{16,64})", cand or "")
            if hm:
                video_hash = hm.group(1)
                break
        if not video_hash:
            path_part = urllib.parse.urlparse(stream_url).path.lstrip("/").replace("/", "")
            if path_part.startswith("eyJ0Ijoi"):
                pad1 = "=" * ((4 - len(path_part) % 4) % 4)
                outer = json.loads(base64.urlsafe_b64decode(path_part + pad1).decode("utf-8", errors="replace"))
                tok_part = (outer.get("t") or "").split(".")[0]
                if tok_part:
                    pad2 = "=" * ((4 - len(tok_part) % 4) % 4)
                    inner = json.loads(base64.urlsafe_b64decode(tok_part + pad2).decode("utf-8", errors="replace"))
                    video_hash = inner.get("v") or ""
        if not video_hash:
            return text

        key = hmac.new(b"stream-derive-v1", video_hash.encode("utf-8"), hashlib.sha256).digest()
        combined = base64.b64decode(b64_line)
        pt = AESGCM(key).decrypt(iv, combined, None).decode("utf-8", errors="replace")
        if pt.lstrip().startswith("#EXTM3U"):
            return pt
    except Exception as e:
        print(f"[AESGCM M3U8 DECRYPT WARN] {stream_url[:80]}: {e}", flush=True)
    return text


def _evict_hls_cache_if_needed():
    max_bytes = max(32, int(state.buffer_config.get("max_ram_mb") or 800)) * 1024 * 1024
    while state.hls_segment_cache and state.hls_cache_bytes > max_bytes:
        _, old_entry = state.hls_segment_cache.popitem(last=False)
        if isinstance(old_entry, dict):
            old_sz = int(old_entry.get("size") or 0)
            old_path = old_entry.get("path")
            if old_path:
                try:
                    Path(old_path).unlink(missing_ok=True)
                except Exception:
                    pass
        else:
            old_sz = len(old_entry[0])
        state.hls_cache_bytes = max(0, state.hls_cache_bytes - old_sz)


def _write_bytes_to_file_sync(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(data)
    tmp_path.replace(path)


def _read_and_optional_delete_sync(path: Path, delete_after: bool = False) -> bytes:
    try:
        data = path.read_bytes()
    finally:
        if delete_after:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
    return data


async def _download_and_clean_segment(target_url: str, req_headers: Dict[str, str]) -> Tuple[int, bytes]:
    """
    Downloads an HLS segment (.ts / .m4s / disguised .png/.jpg) using the persistent HTTP connection pool
    with 3x automatic retry + DPI TLS split fallback when the network is weak or jittery.
    """
    client = await get_shared_http_client()
    last_status = 502
    raw_bytes = b""

    for attempt in range(3):
        try:
            resp = await client.get(
                target_url,
                headers=req_headers,
                timeout=httpx.Timeout(22.0, connect=7.0),
            )
            last_status = resp.status_code
            if resp.status_code in (200, 206) and resp.content:
                raw_bytes = resp.content
                break
            if resp.status_code in (403, 404, 429, 500, 502, 503, 504) and attempt < 2:
                await asyncio.sleep(0.35 * (attempt + 1))
                continue
            raw_bytes = resp.content
            break
        except Exception:
            if attempt == 1 and target_url.startswith("https://"):
                try:
                    st_dpi, _, b_dpi = await asyncio.to_thread(fetch_dpi_tls_split_sync, target_url, req_headers, 10.0)
                    if st_dpi in (200, 206) and b_dpi:
                        last_status = st_dpi
                        raw_bytes = b_dpi
                        break
                except Exception:
                    pass
            if attempt < 2:
                await asyncio.sleep(0.35 * (attempt + 1))

    clean_bytes = strip_disguised_mpegts_header(raw_bytes)
    return (200 if last_status in (200, 206) else last_status), clean_bytes


async def get_or_fetch_hls_segment(
    target_url: str,
    req_headers: Dict[str, str],
    is_prefetch: bool = False,
) -> Tuple[int, bytes]:
    """
    Smart Disk / RAM Cache + Single-Flight Request Deduplicator for HLS segments:
    - When `storage_type == "disk"` (default): stores pre-fetched `.ts` segments on the hard drive
      (`stream_cache/seg_<sha1>.ts`) so 0 MB of system RAM is consumed by cached segments!
    - When `storage_type == "ram"`: stores segments in RAM.
    - If `target_url` is currently being pre-fetched in the background: awaits the in-flight task
      instead of opening a duplicate network connection!
    """
    import hashlib

    cached = state.hls_segment_cache.get(target_url)
    if cached is not None:
        if isinstance(cached, dict) and cached.get("storage") == "disk":
            seg_p = Path(cached.get("path") or "")
            if seg_p.exists():
                state.hls_segment_cache.move_to_end(target_url)
                if is_prefetch:
                    return 200, b""
                state.hls_cache_hits += 1
                data = await asyncio.to_thread(_read_and_optional_delete_sync, seg_p, False)
                return 200, data
            else:
                # File was externally removed; clean up stale entry
                old_e = state.hls_segment_cache.pop(target_url, None)
                if old_e:
                    state.hls_cache_bytes = max(0, state.hls_cache_bytes - int(old_e.get("size") or 0))
        elif isinstance(cached, dict) and cached.get("storage") == "ram":
            state.hls_segment_cache.move_to_end(target_url)
            if not is_prefetch:
                state.hls_cache_hits += 1
            return 200, cached.get("data") or b""

    if not is_prefetch:
        state.hls_cache_misses += 1

    inflight = state.hls_inflight_tasks.get(target_url)
    if inflight is not None and not inflight.done():
        try:
            return await asyncio.shield(inflight)
        except Exception:
            pass

    task = asyncio.create_task(_download_and_clean_segment(target_url, dict(req_headers)))
    state.hls_inflight_tasks[target_url] = task
    try:
        status_code, clean_bytes = await asyncio.shield(task)
        if (
            status_code == 200
            and len(clean_bytes) > 188
            and state.buffer_config.get("enabled", True)
        ):
            if target_url in state.hls_segment_cache:
                old_e = state.hls_segment_cache.pop(target_url)
                old_sz = int(old_e.get("size") or 0) if isinstance(old_e, dict) else len(old_e[0])
                state.hls_cache_bytes = max(0, state.hls_cache_bytes - old_sz)
                if isinstance(old_e, dict) and old_e.get("path"):
                    try:
                        Path(old_e["path"]).unlink(missing_ok=True)
                    except Exception:
                        pass

            use_disk = state.buffer_config.get("storage_type", "disk") == "disk"
            seg_size = len(clean_bytes)
            if use_disk:
                seg_hash = hashlib.sha1(target_url.encode("utf-8")).hexdigest()
                seg_path = state.ensure_disk_cache_dir() / f"seg_{seg_hash}.ts"
                await asyncio.to_thread(_write_bytes_to_file_sync, seg_path, clean_bytes)
                state.hls_segment_cache[target_url] = {
                    "storage": "disk",
                    "path": str(seg_path),
                    "size": seg_size,
                    "ts": time.time(),
                }
            else:
                state.hls_segment_cache[target_url] = {
                    "storage": "ram",
                    "data": clean_bytes,
                    "size": seg_size,
                    "ts": time.time(),
                }

            state.hls_cache_bytes += seg_size
            _evict_hls_cache_if_needed()
            if is_prefetch:
                state.hls_prefetched_count += 1
                if use_disk:
                    # Drop reference to clean_bytes immediately on prefetch so RAM stays 0MB!
                    return status_code, b""
        return status_code, clean_bytes
    finally:
        if state.hls_inflight_tasks.get(target_url) is task:
            state.hls_inflight_tasks.pop(target_url, None)


async def _prefetch_single_segment_bg(seg_url: str, req_headers: Dict[str, str]):
    if not state.buffer_config.get("enabled", True):
        return
    if seg_url in state.hls_segment_cache or seg_url in state.hls_inflight_tasks:
        return
    try:
        async with state.hls_prefetch_sem:
            if seg_url in state.hls_segment_cache or seg_url in state.hls_inflight_tasks:
                return
            await get_or_fetch_hls_segment(seg_url, req_headers, is_prefetch=True)
    except Exception:
        pass


def trigger_hls_readahead(current_url: str):
    """
    When the TV requests segment N, immediately spawns parallel background tasks
    to download segments N+1 .. N+K into RAM before the TV needs them!
    """
    if not state.buffer_config.get("enabled", True):
        return
    prefetch_count = int(state.buffer_config.get("prefetch_segments") or 0)
    if prefetch_count <= 0:
        return
    nxt_info = state.hls_next_map.get(current_url)
    if not nxt_info:
        return
    next_urls = (nxt_info.get("next_urls") or [])[:prefetch_count]
    hdrs = nxt_info.get("headers") or {}
    for u in next_urls:
        if u not in state.hls_segment_cache and u not in state.hls_inflight_tasks:
            asyncio.create_task(_prefetch_single_segment_bg(u, hdrs))


def rewrite_m3u8_content(content: str, base_url: str, headers: Dict[str, str], host_base: str) -> str:
    lines = content.splitlines()
    out_lines = []
    expect_variant_m3u8 = False
    media_segments: List[Tuple[str, float]] = []
    pending_extinf_dur = 4.0

    def replace_uri_attr(match):
        uri = match.group(1)
        abs_uri = urllib.parse.urljoin(base_url, uri)
        is_m3u = ".m3u8" in abs_uri.lower()
        proxied = build_proxy_url(abs_uri, headers, host_base, is_hls=is_m3u, is_segment=False)
        return f'URI="{proxied}"'

    for line in lines:
        stripped = line.strip()
        if not stripped:
            out_lines.append(line)
            continue
        if stripped.startswith("#"):
            if stripped.startswith("#EXT-X-STREAM-INF"):
                expect_variant_m3u8 = True
            elif stripped.startswith("#EXTINF:"):
                m_dur = re.search(r"#EXTINF:\s*([\d.]+)", stripped)
                if m_dur:
                    try:
                        pending_extinf_dur = max(0.5, float(m_dur.group(1)))
                    except Exception:
                        pending_extinf_dur = 4.0
            if 'URI="' in stripped:
                line = re.sub(r'URI="([^"]+)"', replace_uri_attr, line)
            out_lines.append(line)
        else:
            abs_url = urllib.parse.urljoin(base_url, stripped)
            low_abs = abs_url.lower().split("?")[0]
            is_sub_m3u8 = expect_variant_m3u8 or low_abs.endswith(".m3u8") or low_abs.endswith(".m3u")
            expect_variant_m3u8 = False
            if not is_sub_m3u8:
                media_segments.append((abs_url, pending_extinf_dur))
                pending_extinf_dur = 4.0
            out_lines.append(
                build_proxy_url(
                    abs_url,
                    headers,
                    host_base,
                    is_hls=is_sub_m3u8,
                    is_segment=not is_sub_m3u8,
                )
            )

    # Register segment adjacency graph (`state.hls_next_map`) for Smart Read-Ahead Pre-fetching!
    if media_segments:
        seg_req_headers = {
            "User-Agent": headers.get(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ),
            "Accept": "*/*",
        }
        for k, v in (headers or {}).items():
            if v:
                seg_req_headers[k] = v
        if "Referer" in seg_req_headers and "Origin" not in seg_req_headers:
            parsed_ref = urllib.parse.urlparse(seg_req_headers["Referer"])
            if parsed_ref.scheme and parsed_ref.netloc:
                seg_req_headers["Origin"] = f"{parsed_ref.scheme}://{parsed_ref.netloc}"

        seg_urls = [u for (u, _) in media_segments]
        if len(state.hls_next_map) > 5000:
            state.hls_next_map.clear()
        for idx, s_url in enumerate(seg_urls):
            state.hls_next_map[s_url] = {
                "next_urls": seg_urls[idx + 1 : idx + 1 + 25],
                "headers": seg_req_headers,
            }

        # Warm up initial segments around current playback position immediately!
        if state.buffer_config.get("enabled", True):
            target_time = max(0.0, float(state.current_media.get("currentTime") or 0.0))
            start_idx = 0
            if target_time > 5.0:
                acc_t = 0.0
                for idx, (_, dur_s) in enumerate(media_segments):
                    if acc_t + dur_s >= target_time:
                        start_idx = max(0, idx - 1)
                        break
                    acc_t += dur_s
            warm_count = min(len(seg_urls) - start_idx, max(3, int(state.buffer_config.get("prefetch_segments", 8)) // 2))
            for u in seg_urls[start_idx : start_idx + warm_count]:
                if u not in state.hls_segment_cache and u not in state.hls_inflight_tasks:
                    asyncio.create_task(_prefetch_single_segment_bg(u, seg_req_headers))

    return "\n".join(out_lines)


@app.get("/proxy/segment.ts")
async def proxy_hls_segment(request: Request, token: str):
    """
    Smart RAM-Cached + Read-Ahead Pre-fetched HLS Segment Proxy (.ts / .m4s / disguised .png/.jpg/.html).
    - Serves immediately from RAM if already pre-fetched (0ms latency!).
    - Triggers parallel read-ahead of the next N segments (`state.buffer_config["prefetch_segments"]`).
    - Strips any anti-leech PNG/JPG header before 0x47 and returns exact Content-Length.
    """
    target_url, custom_headers = decode_proxy_params(token)
    req_headers = {
        "User-Agent": custom_headers.get(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ),
        "Accept": "*/*",
    }
    for k, v in custom_headers.items():
        if v:
            req_headers[k] = v

    if "Referer" in req_headers and "Origin" not in req_headers:
        parsed_ref = urllib.parse.urlparse(req_headers["Referer"])
        if parsed_ref.scheme and parsed_ref.netloc:
            req_headers["Origin"] = f"{parsed_ref.scheme}://{parsed_ref.netloc}"

    # Immediately trigger parallel read-ahead pre-fetching for upcoming segments!
    trigger_hls_readahead(target_url)

    status_code, clean_bytes = await get_or_fetch_hls_segment(target_url, req_headers, is_prefetch=False)
    return Response(
        content=clean_bytes,
        status_code=status_code,
        media_type="video/mp2t",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Private-Network": "true",
            "Content-Length": str(len(clean_bytes)),
            "Cache-Control": "public, max-age=3600",
        },
    )


@app.api_route("/proxy/virtual/{vid_id}.m3u8", methods=["GET", "HEAD"])
async def serve_virtual_m3u8(vid_id: str, request: Request):
    """
    Serves a decrypted in-memory #EXTM3U playlist (e.g. from streamfree.vip / hhpanda.st AES-GCM + PNG-prefixed segments)
    and rewrites all segment URLs through `/proxy/segment.ts` so the Smart TV receives 100% pure, unencrypted MPEG-TS!
    """
    v_item = state.virtual_m3u8_store.get(vid_id)
    if not v_item:
        raise HTTPException(status_code=404, detail="Virtual M3U8 not found or expired")
    host_base = f"http://{request.url.hostname or get_local_ip()}:{PORT}"
    rewritten = rewrite_m3u8_content(
        v_item["m3u8_text"],
        v_item["base_url"],
        v_item.get("headers") or {},
        host_base,
    )
    return Response(
        content=rewritten.encode("utf-8"),
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Private-Network": "true",
            "Cache-Control": "no-cache",
        },
    )


@app.api_route("/proxy/stream", methods=["GET", "HEAD"])
@app.api_route("/proxy/stream.m3u8", methods=["GET", "HEAD"])
async def proxy_stream(
    request: Request,
    token: Optional[str] = None,
    url: Optional[str] = None,
    referer: Optional[str] = None,
    user_agent: Optional[str] = None,
):
    if token:
        target_url, custom_headers = decode_proxy_params(token)
    elif url:
        target_url = url
        custom_headers = {}
        if referer:
            custom_headers["Referer"] = referer
        if user_agent:
            custom_headers["User-Agent"] = user_agent
    else:
        raise HTTPException(status_code=400, detail="Missing token or url")

    req_headers = {
        "User-Agent": custom_headers.get(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ),
        "Accept": "*/*",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    for k, v in custom_headers.items():
        if v:
            req_headers[k] = v

    # YouTube googlevideo.com signed URLs (especially ANDROID_VR / IOS / WEB) reject mismatched Referer/Origin headers
    if "googlevideo.com/" in target_url.lower():
        req_headers.pop("Referer", None)
        req_headers.pop("Origin", None)
        req_headers.pop("Cookie", None)

    host_base = f"http://{request.url.hostname or get_local_ip()}:{PORT}"
    url_path = urllib.parse.urlparse(target_url).path.lower()

    # If it's a .ts segment requested on /proxy/stream (e.g. from older cached playlist), delegate to segment handler
    if url_path.endswith(".ts") and token:
        return await proxy_hls_segment(request, token)

    is_m3u8_request = (
        request.url.path.endswith(".m3u8")
        or url_path.endswith(".m3u8")
        or url_path.endswith(".m3u")
        or ".m3u8" in target_url.lower()
        or "/eyj0ijoi" in url_path
    )

    if is_m3u8_request:
        if "Referer" in req_headers and "Origin" not in req_headers:
            parsed_ref = urllib.parse.urlparse(req_headers["Referer"])
            if parsed_ref.scheme and parsed_ref.netloc:
                req_headers["Origin"] = f"{parsed_ref.scheme}://{parsed_ref.netloc}"
        try:
            _, _, raw_bytes, final_url = await fetch_url_universal(target_url, headers=req_headers, timeout=20.0)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Upstream M3U8 fetch error: {e}")

        text = raw_bytes.decode("utf-8", errors="replace")
        text = decrypt_enc_aesgcm_m3u8_if_needed(text, final_url, req_headers.get("Referer", ""))
        if text.lstrip().startswith("#EXTM3U"):
            rewritten = rewrite_m3u8_content(text, final_url, custom_headers, host_base)
            encoded_m3u8 = rewritten.encode("utf-8")
            return Response(
                content=encoded_m3u8,
                media_type="application/vnd.apple.mpegurl",
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Private-Network": "true",
                    "Content-Length": str(len(encoded_m3u8)),
                },
            )
        clean_bytes = strip_disguised_mpegts_header(raw_bytes)
        return Response(
            content=clean_bytes,
            media_type="video/mp2t",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Private-Network": "true",
                "Content-Length": str(len(clean_bytes)),
            },
        )

    # Special Bounded-Chunk Streaming Engine + Read-Ahead RAM Queue for YouTube googlevideo.com!
    if "googlevideo.com/" in target_url.lower():
        client_range_hdr = request.headers.get("range") or ""
        start_byte = 0
        req_end_byte: Optional[int] = None
        m_rng = re.search(r"bytes=(\d+)-(\d*)", client_range_hdr)
        if m_rng:
            start_byte = int(m_rng.group(1))
            if m_rng.group(2):
                req_end_byte = int(m_rng.group(2))

        first_chunk_size = 2 * 1024 * 1024  # 2MB initial chunk for instant TV playback startup
        first_end = (
            min(start_byte + first_chunk_size - 1, req_end_byte)
            if req_end_byte is not None
            else (start_byte + first_chunk_size - 1)
        )
        yt_headers = dict(req_headers)
        yt_headers["Accept-Encoding"] = "identity"
        yt_headers["Range"] = f"bytes={start_byte}-{first_end}"

        shared_client = await get_shared_http_client()
        try:
            r_first = await shared_client.get(target_url, headers=yt_headers, timeout=httpx.Timeout(25.0, connect=8.0))
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"YouTube upstream error: {e}")

        if r_first.status_code not in (200, 206):
            raise HTTPException(status_code=r_first.status_code, detail="YouTube upstream returned error")

        first_bytes = r_first.content
        cr_hdr = r_first.headers.get("content-range") or ""
        m_tot = re.search(r"/(\d+)", cr_hdr)
        total_size = int(m_tot.group(1)) if m_tot else (start_byte + len(first_bytes))
        final_end = min(req_end_byte, total_size - 1) if req_end_byte is not None else (total_size - 1)
        total_to_send = max(0, final_end - start_byte + 1)
        media_type_yt = r_first.headers.get("content-type", "video/mp4")

        resp_headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Private-Network": "true",
            "Accept-Ranges": "bytes",
            "Content-Type": media_type_yt,
            "Content-Length": str(total_to_send),
        }
        out_status = 206 if client_range_hdr else 200
        if client_range_hdr:
            resp_headers["Content-Range"] = f"bytes {start_byte}-{final_end}/{total_size}"

        if request.method == "HEAD":
            return Response(content=b"", status_code=out_status, headers=resp_headers, media_type=media_type_yt)

        step_size = 3 * 1024 * 1024  # 3MB chunks
        readahead_mb = max(8, int(state.buffer_config.get("mp4_readahead_mb") or 48))
        max_q_items = max(2, readahead_mb // 3)
        use_disk_yt = state.buffer_config.get("storage_type", "disk") == "disk"
        disk_cache_dir_yt = state.ensure_disk_cache_dir() if use_disk_yt else None

        async def iter_yt_bounded_chunks():
            q: asyncio.Queue = asyncio.Queue(maxsize=max_q_items)
            stop_evt = asyncio.Event()

            async def _yt_producer():
                cur_pos = start_byte + len(first_bytes)
                try:
                    while cur_pos <= final_end and not stop_evt.is_set():
                        c_end = min(cur_pos + step_size - 1, final_end)
                        h_chunk = dict(req_headers)
                        h_chunk["Accept-Encoding"] = "identity"
                        h_chunk["Range"] = f"bytes={cur_pos}-{c_end}"
                        chunk_data = b""
                        for attempt in range(3):
                            try:
                                r_chunk = await shared_client.get(
                                    target_url,
                                    headers=h_chunk,
                                    timeout=httpx.Timeout(25.0, connect=8.0),
                                )
                                if r_chunk.status_code in (200, 206) and r_chunk.content:
                                    chunk_data = r_chunk.content
                                    break
                            except Exception:
                                pass
                            await asyncio.sleep(0.35 * (attempt + 1))
                        if not chunk_data:
                            break
                        c_len = len(chunk_data)
                        state.mp4_buffered_bytes += c_len
                        if use_disk_yt and disk_cache_dir_yt is not None:
                            chunk_file = disk_cache_dir_yt / f"mp4chunk_yt_{uuid.uuid4().hex[:12]}.bin"
                            await asyncio.to_thread(_write_bytes_to_file_sync, chunk_file, chunk_data)
                            chunk_data = b""  # Free RAM immediately
                            await q.put(("disk", chunk_file, c_len))
                        else:
                            await q.put(("ram", chunk_data, c_len))
                        cur_pos += c_len
                finally:
                    await q.put(None)

            prod_task = asyncio.create_task(_yt_producer())
            try:
                yield first_bytes
                while True:
                    item = await q.get()
                    if item is None:
                        break
                    kind, payload_item, c_len = item
                    state.mp4_buffered_bytes = max(0, state.mp4_buffered_bytes - c_len)
                    if kind == "disk":
                        data_out = await asyncio.to_thread(_read_and_optional_delete_sync, payload_item, True)
                        yield data_out
                    else:
                        yield payload_item
            finally:
                stop_evt.set()
                if not prod_task.done():
                    prod_task.cancel()
                while not q.empty():
                    try:
                        leftover = q.get_nowait()
                        if leftover:
                            l_kind, l_payload, l_len = leftover
                            state.mp4_buffered_bytes = max(0, state.mp4_buffered_bytes - l_len)
                            if l_kind == "disk" and isinstance(l_payload, Path):
                                try:
                                    l_payload.unlink(missing_ok=True)
                                except Exception:
                                    pass
                    except Exception:
                        break

        return StreamingResponse(
            iter_yt_bounded_chunks(),
            status_code=out_status,
            headers=resp_headers,
            media_type=media_type_yt,
        )

    # Direct MP4 / MKV / WebM / Extensionless Sora streaming with Read-Ahead RAM Buffer & Auto-Resume!
    client_range_hdr = request.headers.get("range") or ""
    start_byte_mp4 = 0
    end_byte_mp4: Optional[int] = None
    if client_range_hdr:
        req_headers["Range"] = client_range_hdr
        m_r = re.search(r"bytes=(\d+)-(\d*)", client_range_hdr)
        if m_r:
            start_byte_mp4 = int(m_r.group(1))
            if m_r.group(2):
                end_byte_mp4 = int(m_r.group(2))
    elif "/sora/" in target_url or "sssrr.org" in target_url or "trycloudflare.com" in target_url:
        req_headers["Range"] = "bytes=0-"
    req_headers["Accept-Encoding"] = "identity"

    client = httpx.AsyncClient(verify=False, follow_redirects=True, timeout=httpx.Timeout(60.0, connect=10.0))
    try:
        upstream_req = client.build_request("GET", target_url, headers=req_headers)
        upstream_resp = await client.send(upstream_req, stream=True)
        if upstream_resp.status_code in (403, 404) and "Range" not in req_headers:
            await upstream_resp.aclose()
            req_headers["Range"] = "bytes=0-"
            upstream_req = client.build_request("GET", target_url, headers=req_headers)
            upstream_resp = await client.send(upstream_req, stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Upstream stream error: {e}")

    resp_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Private-Network": "true",
        "Accept-Ranges": "bytes",
    }
    client_sent_range = "range" in request.headers
    for h in ["content-length", "content-range", "accept-ranges", "content-type"]:
        if h in upstream_resp.headers:
            if h == "content-range" and not client_sent_range:
                continue
            resp_headers[h] = upstream_resp.headers[h]

    out_status = upstream_resp.status_code
    if out_status == 206 and not client_sent_range:
        out_status = 200

    media_type_out = upstream_resp.headers.get("content-type", "video/mp4")
    if request.method == "HEAD":
        await upstream_resp.aclose()
        await client.aclose()
        return Response(content=b"", status_code=out_status, headers=resp_headers, media_type=media_type_out)

    # Calculate max queue depth for MP4 stream (512KB chunks on disk, 128KB in RAM)
    readahead_mb = max(4, int(state.buffer_config.get("mp4_readahead_mb") or 48))
    use_disk_mp4 = state.buffer_config.get("storage_type", "disk") == "disk"
    chunk_sz_mp4 = 524288 if use_disk_mp4 else 131072
    max_mp4_chunks = max(16, (readahead_mb * 1024 * 1024) // chunk_sz_mp4)
    expected_len = int(upstream_resp.headers.get("content-length") or 0)
    disk_cache_dir = state.ensure_disk_cache_dir() if use_disk_mp4 else None

    async def iter_upstream():
        q: asyncio.Queue = asyncio.Queue(maxsize=max_mp4_chunks)
        stop_evt = asyncio.Event()
        mp4_stream_id = uuid.uuid4().hex[:8]
        chunk_idx = 0

        async def _mp4_producer():
            nonlocal upstream_resp, chunk_idx
            bytes_read = 0
            retries = 0
            try:
                while not stop_evt.is_set():
                    try:
                        async for chunk in upstream_resp.aiter_raw(chunk_size=chunk_sz_mp4):
                            if stop_evt.is_set():
                                break
                            if not chunk:
                                continue
                            c_len = len(chunk)
                            bytes_read += c_len
                            state.mp4_buffered_bytes += c_len
                            if use_disk_mp4 and disk_cache_dir is not None:
                                chunk_idx += 1
                                c_file = disk_cache_dir / f"mp4chunk_{mp4_stream_id}_{chunk_idx}.bin"
                                try:
                                    await asyncio.to_thread(_write_bytes_to_file_sync, c_file, chunk)
                                    await q.put(("disk", c_file, c_len))
                                except Exception:
                                    await q.put(("ram", chunk, c_len))
                            else:
                                await q.put(("ram", chunk, c_len))
                        break  # Clean EOF
                    except Exception:
                        # Mid-stream network drop! Auto-reconnect using Range header if possible
                        retries += 1
                        if retries > 5 or (expected_len > 0 and bytes_read >= expected_len):
                            break
                        try:
                            await upstream_resp.aclose()
                        except Exception:
                            pass
                        await asyncio.sleep(0.5 * retries)
                        resume_offset = start_byte_mp4 + bytes_read
                        resume_hdrs = dict(req_headers)
                        resume_hdrs["Range"] = (
                            f"bytes={resume_offset}-{end_byte_mp4}"
                            if end_byte_mp4 is not None
                            else f"bytes={resume_offset}-"
                        )
                        try:
                            req_retry = client.build_request("GET", target_url, headers=resume_hdrs)
                            resp_retry = await client.send(req_retry, stream=True)
                            if resp_retry.status_code == 206:
                                upstream_resp = resp_retry
                                continue
                            await resp_retry.aclose()
                        except Exception:
                            pass
                        break
            finally:
                await q.put(None)
                try:
                    await upstream_resp.aclose()
                except Exception:
                    pass
                try:
                    await client.aclose()
                except Exception:
                    pass

        prod_task = asyncio.create_task(_mp4_producer())
        try:
            while True:
                item = await q.get()
                if item is None:
                    break
                item_kind, item_payload, item_len = item
                state.mp4_buffered_bytes = max(0, state.mp4_buffered_bytes - item_len)
                if item_kind == "disk":
                    data_bytes = await asyncio.to_thread(_read_and_optional_delete_sync, item_payload, True)
                    if data_bytes:
                        yield data_bytes
                else:
                    yield item_payload
        finally:
            stop_evt.set()
            if not prod_task.done():
                prod_task.cancel()
            while not q.empty():
                try:
                    leftover = q.get_nowait()
                    if leftover:
                        l_kind, l_payload, l_len = leftover
                        state.mp4_buffered_bytes = max(0, state.mp4_buffered_bytes - l_len)
                        if l_kind == "disk":
                            try:
                                l_payload.unlink(missing_ok=True)
                            except Exception:
                                pass
                except Exception:
                    break

    return StreamingResponse(
        iter_upstream(),
        status_code=out_status,
        headers=resp_headers,
        media_type=media_type_out,
    )


def _decode_subtitle_bytes(raw_bytes: bytes) -> str:
    if not raw_bytes:
        return ""
    for enc in ("utf-8-sig", "utf-16", "cp1258", "windows-1252"):
        try:
            return raw_bytes.decode(enc)
        except Exception:
            continue
    return raw_bytes.decode("utf-8", errors="replace")


def _parse_vtt_ts_to_sec(ts_str: str) -> float:
    s = ts_str.strip().replace(",", ".")
    parts = s.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
    except Exception:
        pass
    return 0.0


def _format_sec_to_vtt_ts(sec: float) -> str:
    sec = max(0.0, float(sec))
    total_ms = int(round(sec * 1000))
    hrs = total_ms // 3600000
    rem = total_ms % 3600000
    mins = rem // 60000
    rem = rem % 60000
    secs = rem // 1000
    ms = rem % 1000
    return f"{hrs:02d}:{mins:02d}:{secs:02d}.{ms:03d}"


def _convert_ass_to_vtt(text: str) -> str:
    lines = text.splitlines()
    in_events = False
    format_cols: List[str] = []
    cues: List[Tuple[float, float, str]] = []
    for ln in lines:
        s = ln.strip()
        if s.lower() == "[events]":
            in_events = True
            continue
        if in_events and s.startswith("[") and s.endswith("]"):
            break
        if not in_events:
            continue
        if s.lower().startswith("format:"):
            format_cols = [c.strip().lower() for c in s.split(":", 1)[1].split(",")]
        elif s.lower().startswith("dialogue:"):
            payload = s.split(":", 1)[1].lstrip()
            max_splits = max(9, len(format_cols) - 1) if format_cols else 9
            parts = payload.split(",", max_splits)
            if len(parts) < 4:
                continue
            start_idx = format_cols.index("start") if "start" in format_cols else 1
            end_idx = format_cols.index("end") if "end" in format_cols else 2
            text_idx = format_cols.index("text") if "text" in format_cols else len(parts) - 1
            if max(start_idx, end_idx, text_idx) >= len(parts):
                continue
            st_sec = _parse_vtt_ts_to_sec(parts[start_idx])
            en_sec = _parse_vtt_ts_to_sec(parts[end_idx])
            raw_txt = parts[text_idx]
            clean_txt = re.sub(r"\{[^}]*\}", "", raw_txt)
            clean_txt = clean_txt.replace("\\N", "\n").replace("\\n", "\n").replace("\\h", " ").strip()
            if clean_txt and en_sec > st_sec:
                cues.append((st_sec, en_sec, clean_txt))

    cues.sort(key=lambda x: x[0])
    out = ["WEBVTT", ""]
    for idx, (st_s, en_s, c_txt) in enumerate(cues, 1):
        out.append(str(idx))
        out.append(f"{_format_sec_to_vtt_ts(st_s)} --> {_format_sec_to_vtt_ts(en_s)}")
        out.append(c_txt)
        out.append("")
    return "\n".join(out)


def convert_and_style_subtitle_to_vtt(raw_bytes: bytes) -> str:
    """
    Universal Subtitle Normalizer & Live Styler (.SRT / .VTT / .ASS / .SSA -> UTF-8 WEBVTT):
    1. Auto-detects UTF-8 / UTF-16 / CP1258 Vietnamese encoding.
    2. Converts .ASS/.SSA or .SRT into clean WebVTT.
    3. Applies live `offsetSec` (+/- seconds subtitle sync) and `bottomPosition` (`line:XX%`)
       directly to cue timestamps so both Native WVC Receiver (`cast2tv.app`) AND Web TV (`/tv`)
       respect timing offset and vertical position immediately!
    """
    text = _decode_subtitle_bytes(raw_bytes).lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return "WEBVTT\n\n"

    if "[events]" in text.lower() and "dialogue:" in text.lower():
        text = _convert_ass_to_vtt(text)
    else:
        # Convert SRT comma timestamps (00:01:02,345 or 01:02,345) to VTT dot timestamps
        text = re.sub(r"(\d{1,2}:\d{2}:\d{2}),(\d{3})", r"\1.\2", text)
        text = re.sub(r"(?<!\d:)(\d{2}:\d{2}),(\d{3})", r"00:\1.\2", text)
        if not text.lstrip().startswith("WEBVTT"):
            text = "WEBVTT\n\n" + text.lstrip()

    offset_sec = float(state.subtitle_style.get("offsetSec") or 0.0)
    bottom_pos = int(state.subtitle_style.get("bottomPosition") or 8)
    # In WebVTT & WVC SubtitleParser.ts, line:90% means 90% from top (10% from bottom)
    vtt_line_pct = max(10, min(95, 100 - bottom_pos))

    ts_line_re = re.compile(
        r"^(\d{1,2}:\d{2}:\d{2}\.\d{3}|\d{2}:\d{2}\.\d{3})\s*-->\s*(\d{1,2}:\d{2}:\d{2}\.\d{3}|\d{2}:\d{2}\.\d{3})(.*)$"
    )
    out_lines = []
    for ln in text.split("\n"):
        m = ts_line_re.match(ln.strip())
        if m:
            st_s = _parse_vtt_ts_to_sec(m.group(1)) + offset_sec
            en_s = _parse_vtt_ts_to_sec(m.group(2)) + offset_sec
            rest = m.group(3) or ""
            if en_s <= 0:
                st_s, en_s = 0.0, 0.001
            else:
                st_s = max(0.0, st_s)
            if "line:" not in rest:
                rest = (rest.rstrip() + f" line:{vtt_line_pct}%")
            out_lines.append(f"{_format_sec_to_vtt_ts(st_s)} --> {_format_sec_to_vtt_ts(en_s)}{rest}")
        else:
            out_lines.append(ln)

    return "\n".join(out_lines)


@app.get("/proxy/subtitle.vtt")
@app.get("/proxy/subtitle")
async def proxy_subtitle(
    url: Optional[str] = None,
    id: Optional[str] = None,
    referer: Optional[str] = None,
):
    raw_bytes = b""
    if id:
        sub_item = state.uploaded_subtitles.get(id)
        if not sub_item:
            raise HTTPException(status_code=404, detail="Không tìm thấy phụ đề đã tải lên!")
        if sub_item.get("file_path") and Path(sub_item["file_path"]).exists():
            raw_bytes = Path(sub_item["file_path"]).read_bytes()
        else:
            raw_bytes = (sub_item.get("content") or "").encode("utf-8")
    elif url:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        }
        if referer:
            headers["Referer"] = referer
        try:
            client = await get_shared_http_client()
            r = await client.get(url, headers=headers, timeout=15.0)
            raw_bytes = r.content
        except Exception:
            try:
                _, _, raw_bytes, _ = await fetch_url_universal(url, headers=headers, timeout=15.0)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Lỗi tải phụ đề: {e}")
    else:
        raise HTTPException(status_code=400, detail="Thiếu url hoặc id phụ đề")

    vtt_text = convert_and_style_subtitle_to_vtt(raw_bytes)
    return Response(
        content=vtt_text.encode("utf-8"),
        media_type="text/vtt; charset=utf-8",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Private-Network": "true",
            "Cache-Control": "no-cache, no-store, must-revalidate",
        },
    )


# ============================================================================
# 5. LOCAL VIDEO FILE STREAMING + FFMPEG ZERO-REENCODE MKV->MP4 LIVE REMUXER
# ============================================================================

FFMPEG_CANDIDATES = [
    r"C:\Program Files\Jellyfin\Server\ffmpeg.exe",
    r"C:\Program Files\BlueStacks_nxt\ffmpeg.exe",
    "ffmpeg",
]


def get_ffmpeg_exe() -> Optional[str]:
    import shutil
    for cand in FFMPEG_CANDIDATES:
        if os.path.isabs(cand) and os.path.exists(cand):
            return cand
        found = shutil.which(cand)
        if found:
            return found
    return None


@app.post("/api/register-local-file")
async def register_local_file(payload: Dict[str, Any]):
    file_path = Path(payload.get("path", "").strip().strip('"'))
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail=f"Không tìm thấy file: {file_path}")

    file_id = uuid.uuid4().hex[:10]
    state.local_files[file_id] = file_path
    local_ip = get_local_ip()
    stream_url = f"http://{local_ip}:{PORT}/stream/local/{file_id}/video.mp4"

    # Auto-discover companion subtitle files (.srt / .vtt / .ass / .ssa) in the same folder!
    companion_subs: List[Dict[str, str]] = []
    try:
        stem_low = file_path.stem.lower()
        sub_exts = {".srt", ".vtt", ".ass", ".ssa"}
        for cand in sorted(file_path.parent.iterdir()):
            if not cand.is_file() or cand.suffix.lower() not in sub_exts:
                continue
            cand_stem = cand.stem.lower()
            if cand_stem == stem_low or cand_stem.startswith(stem_low + ".") or cand_stem.startswith(stem_low + "_"):
                sub_id = f"local_{uuid.uuid4().hex[:8]}"
                state.uploaded_subtitles[sub_id] = {
                    "id": sub_id,
                    "label": f"Phụ đề đi kèm ({cand.name})",
                    "file_path": str(cand),
                    "created_at": time.time(),
                }
                sub_url = f"http://{local_ip}:{PORT}/proxy/subtitle.vtt?id={sub_id}"
                companion_subs.append({
                    "id": sub_id,
                    "label": f"Phụ đề ({cand.name})",
                    "url": sub_url,
                })
    except Exception:
        pass

    return {
        "file_id": file_id,
        "name": file_path.name,
        "size": file_path.stat().st_size,
        "stream_url": stream_url,
        "subtitles": companion_subs,
    }


@app.get("/stream/remux/{file_id}/{filename}")
async def stream_remux_mkv_to_mp4(file_id: str, filename: str, request: Request, ss: float = 0.0):
    file_path = state.local_files.get(file_id)
    if not file_path or not file_path.exists():
        raise HTTPException(status_code=404, detail="File local không tồn tại")

    ffmpeg_exe = get_ffmpeg_exe()
    if not ffmpeg_exe:
        return await stream_local_file(file_id, filename, request)

    cmd = [ffmpeg_exe, "-hide_banner", "-loglevel", "error"]
    if ss > 0:
        cmd += ["-ss", str(ss)]
    cmd += [
        "-i", str(file_path),
        "-map", "0:v:0",
        "-map", "0:a:0?",
        "-sn",
        "-c:v", "copy",
        "-c:a", "aac",
        "-ac", "2",
        "-b:a", "192k",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof+faststart",
        "-f", "mp4",
        "pipe:1",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def iter_remuxed():
        try:
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        finally:
            if proc.returncode is None:
                try:
                    proc.kill()
                except Exception:
                    pass

    return StreamingResponse(
        iter_remuxed(),
        status_code=200,
        media_type="video/mp4",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Private-Network": "true",
            "Cache-Control": "no-store",
        },
    )


@app.get("/stream/local/{file_id}/{filename}")
async def stream_local_file(file_id: str, filename: str, request: Request):
    file_path = state.local_files.get(file_id)
    if not file_path or not file_path.exists():
        raise HTTPException(status_code=404, detail="File local không tồn tại")

    file_size = file_path.stat().st_size
    range_header = request.headers.get("range")

    ext = file_path.suffix.lower()
    mime_map = {
        ".mp4": "video/mp4",
        ".mkv": "video/mp4",
        ".webm": "video/webm",
        ".m3u8": "application/vnd.apple.mpegurl",
        ".ts": "video/mp2t",
        ".mp3": "audio/mpeg",
    }
    media_type = mime_map.get(ext, "video/mp4")

    if not range_header:
        return FileResponse(file_path, media_type=media_type, headers={"Accept-Ranges": "bytes"})

    m = re.match(r"bytes=(\d+)-(\d*)", range_header)
    if not m:
        return FileResponse(file_path, media_type=media_type)

    start = int(m.group(1))
    end = int(m.group(2)) if m.group(2) else file_size - 1
    end = min(end, file_size - 1)
    chunk_len = end - start + 1

    def file_iterator(path: Path, offset: int, length: int):
        with open(path, "rb") as f:
            f.seek(offset)
            remaining = length
            while remaining > 0:
                data = f.read(min(65536, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_len),
        "Access-Control-Allow-Origin": "*",
    }
    return StreamingResponse(file_iterator(file_path, start, chunk_len), status_code=206, headers=headers, media_type=media_type)


# ============================================================================
# 6. NOISE FILTER, TRACKER UNWRAPPER & DEEP STREAM METADATA PROBER (WVC ENGINE)
# ============================================================================

NOISE_SUBSTRINGS = [
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
    "discover.gif",
    "cast2tv.gif",
    "favicon.ico",
    "1s_blank.mp4",
    "/playergk/",
    "333xbet",
    "video888.mp4",
    "player-ads.xml",
    "decafeligiblyhad.com",
    "morphify.net",
    "fuckadblock",
    "popads",
]

DOH_CACHE: Dict[str, str] = {}


def resolve_doh_ip(host: str) -> str:
    """
    Resolves a hostname via Cloudflare 1.1.1.1 DNS-over-HTTPS (DoH) to bypass ISP DNS poisoning
    (where Vietnamese ISPs return 127.0.0.1 or 0.0.0.0 for movie/player domains like abyssplayer.com).
    """
    if not host:
        return ""
    if host in DOH_CACHE:
        return DOH_CACHE[host]
    try:
        ip = socket.gethostbyname(host)
        if ip and not ip.startswith("127.") and ip != "0.0.0.0":
            DOH_CACHE[host] = ip
            return ip
    except Exception:
        pass
    try:
        req = urllib.request.Request(
            f"https://1.1.1.1/dns-query?name={urllib.parse.quote(host)}&type=A",
            headers={"Accept": "application/dns-json", "User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
            for ans in data.get("Answer", []):
                if ans.get("type") == 1 and ans.get("data"):
                    DOH_CACHE[host] = ans["data"]
                    return ans["data"]
    except Exception:
        pass
    return host


def fetch_dpi_tls_split_sync(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 8.0) -> Tuple[int, Dict[str, str], bytes]:
    """
    Pure-Python ISP Deep Packet Inspection (DPI) & DNS Poisoning Bypass:
    1. Resolves clean IP via Cloudflare 1.1.1.1 DoH.
    2. Splits the TLS Handshake ClientHello across TWO valid TLS records ([0x16, 0x03, 0x01, ...])
       right in the middle of the SNI hostname with TCP_NODELAY so stateless ISP DPI boxes never see
       the complete SNI string, while Cloudflare/BoringSSL reassembles it in ~200ms!
    """
    import ssl
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else "")
    ip = resolve_doh_ip(host)

    last_err = None
    for attempt in range(2):
        s = None
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            s = socket.create_connection((ip, port), timeout=timeout)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            in_bio = ssl.MemoryBIO()
            out_bio = ssl.MemoryBIO()
            ssl_obj = ctx.wrap_bio(in_bio, out_bio, server_hostname=host)
            try:
                ssl_obj.do_handshake()
            except ssl.SSLWantReadError:
                pass
            ch = out_bio.read()
            pos = ch.find(host.encode("ascii", errors="ignore"))
            if pos != -1 and len(ch) > 10 and ch[0] == 0x16:
                rec_hdr = ch[:3]
                payload = ch[5:]
                split_at = max(1, (pos - 5) + 4)
                p1, p2 = payload[:split_at], payload[split_at:]
                tls_frag1 = rec_hdr + len(p1).to_bytes(2, "big") + p1
                tls_frag2 = rec_hdr + len(p2).to_bytes(2, "big") + p2
                s.send(tls_frag1)
                time.sleep(0.01)
                s.send(tls_frag2)
            else:
                s.send(ch)

            while True:
                try:
                    ssl_obj.do_handshake()
                    break
                except ssl.SSLWantReadError:
                    data = s.recv(65536)
                    if not data:
                        break
                    in_bio.write(data)
                    out_data = out_bio.read()
                    if out_data:
                        s.send(out_data)

            req_hdrs = {
                "Host": host,
                "User-Agent": (headers or {}).get(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                ),
                "Accept": "*/*",
                "Accept-Encoding": "identity",
                "Connection": "close",
            }
            if headers:
                for k, v in headers.items():
                    if k.lower() not in ("host", "accept-encoding", "connection") and v:
                        req_hdrs[k] = v

            req_lines = [f"GET {path} HTTP/1.1"] + [f"{k}: {v}" for k, v in req_hdrs.items()] + ["", ""]
            ssl_obj.write("\r\n".join(req_lines).encode("utf-8"))
            s.sendall(out_bio.read())

            resp_chunks = []
            while True:
                try:
                    dec = ssl_obj.read(65536)
                    if dec:
                        resp_chunks.append(dec)
                    else:
                        break
                except ssl.SSLWantReadError:
                    enc = s.recv(65536)
                    if not enc:
                        break
                    in_bio.write(enc)
                except Exception:
                    break
            s.close()
            raw = b"".join(resp_chunks)
            sep = raw.find(b"\r\n\r\n")
            if sep == -1:
                raise RuntimeError("Invalid HTTP response from DPI bypass")
            hdr_bytes, body = raw[:sep], raw[sep + 4:]
            hdr_lines = hdr_bytes.decode("latin1", errors="replace").split("\r\n")
            status = int(hdr_lines[0].split()[1]) if len(hdr_lines[0].split()) > 1 else 200
            resp_headers: Dict[str, str] = {}
            for line in hdr_lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    resp_headers[k.strip().lower()] = v.strip()
            if resp_headers.get("transfer-encoding", "").lower() == "chunked":
                dec_body = bytearray()
                idx = 0
                while idx < len(body):
                    rn = body.find(b"\r\n", idx)
                    if rn == -1:
                        break
                    clen = int(body[idx:rn].split(b";")[0], 16)
                    if clen == 0:
                        break
                    dec_body.extend(body[rn + 2 : rn + 2 + clen])
                    idx = rn + 2 + clen + 2
                body = bytes(dec_body)
            return status, resp_headers, body
        except Exception as e:
            last_err = e
            if s:
                try:
                    s.close()
                except Exception:
                    pass
    raise RuntimeError(f"DPI bypass failed for {url}: {last_err}")


async def fetch_url_universal(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0) -> Tuple[int, Dict[str, str], bytes, str]:
    """
    Tries standard async httpx first; if the domain is DNS-poisoned (127.0.0.1) or blocked by ISP DPI,
    automatically falls back to DoH + TLS Record Splitting (`fetch_dpi_tls_split_sync`)!
    Returns (status_code, headers_dict, body_bytes, final_url).
    """
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    need_dpi_bypass = False
    try:
        ip = socket.gethostbyname(host)
        if ip.startswith("127.") or ip == "0.0.0.0":
            need_dpi_bypass = True
    except Exception:
        need_dpi_bypass = True

    if not need_dpi_bypass:
        try:
            async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=timeout) as client:
                r = await client.get(url, headers=headers)
                return r.status_code, {k.lower(): v for k, v in r.headers.items()}, r.content, str(r.url)
        except Exception:
            need_dpi_bypass = True

    st, hdrs, body = await asyncio.to_thread(fetch_dpi_tls_split_sync, url, headers, timeout)
    return st, hdrs, body, url

NOISE_EXTENSIONS = (
    ".gif", ".png", ".jpg", ".jpeg", ".webp", ".svg", ".ico",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".webmanifest",
    ".json", ".xml", ".html", ".htm", ".php",
)


def unwrap_and_clean_stream_url(raw_url: str) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    1. If raw_url is a JWPlayer / analytics ping URL (e.g. https://prd.jwpltx.com/v1/jwplayer6/ping.gif?...&mu=https://s3.phim1280.tv/.../index.m3u8&vd=5903&vw=1920&vh=796),
       automatically unwraps the `mu=` parameter into the real video URL AND extracts duration/resolution hints!
    2. Filters out noise URLs (manifest.webmanifest, .gif, .png, .js, .css, trackers).
    """
    if not raw_url:
        return None, {}
    raw_url = raw_url.strip()
    low = raw_url.lower()

    hints: Dict[str, Any] = {}
    try:
        parsed = urllib.parse.urlparse(raw_url)
        qs = urllib.parse.parse_qs(parsed.query)
        # Check if real stream URL is embedded in query params (like JWPlayer `mu=`)
        for key in ("mu", "file", "stream", "video_url", "src"):
            if key in qs and qs[key]:
                cand = urllib.parse.unquote(qs[key][0]).strip()
                if any(ext in cand.lower() for ext in (".m3u8", ".mp4", ".mkv", ".webm", ".mpd")):
                    raw_url = cand
                    low = raw_url.lower()
                    if "vd" in qs and qs["vd"][0].isdigit():
                        hints["duration_sec"] = float(qs["vd"][0])
                    if "vw" in qs and qs["vw"][0].isdigit():
                        hints["width"] = int(qs["vw"][0])
                    if "vh" in qs and qs["vh"][0].isdigit():
                        hints["height"] = int(qs["vh"][0])
                    if "sbr" in qs and qs["sbr"][0].isdigit():
                        hints["bitrate_kbps"] = int(qs["sbr"][0])
                    break
    except Exception:
        pass

    clean_path = low.split("?")[0].split("#")[0]

    # Check if it's a subtitle URL
    if clean_path.endswith(".srt") or clean_path.endswith(".vtt"):
        return raw_url, {"is_subtitle": True}

    # Filter out noise substrings and non-video extensions
    for bad in NOISE_SUBSTRINGS:
        if bad in low:
            return None, {}

    if clean_path.endswith(NOISE_EXTENSIONS):
        return None, {}

    # Filter out individual HLS/DASH segments (.ts / .m4s) from the stream list
    if clean_path.endswith(".ts") or clean_path.endswith(".m4s") or "seg-" in low or "chunk-" in low:
        return None, {}

    return raw_url, hints


def detect_stream_type(url: str) -> str:
    u = url.lower().split("?")[0]
    if ".m3u8" in url.lower() or u.endswith(".m3u") or "/eyj0ijoi" in u:
        return "hls"
    if ".mpd" in url.lower():
        return "dash"
    if any(u.endswith(ext) for ext in [".mp4", ".webm", ".mkv", ".mov", ".ogg"]):
        return "mp4"
    return "auto"


def get_wvc_mime_type(stream_type: str, url: str) -> str:
    if stream_type == "hls" or ".m3u8" in url.lower():
        return "application/x-mpegURL"
    if stream_type == "dash" or ".mpd" in url.lower():
        return "application/dash+xml"
    if ".webm" in url.lower():
        return "video/webm"
    return "video/mp4"


def _probe_bytes_with_pyav(raw_bytes: bytes) -> Dict[str, Any]:
    """
    Fast in-memory PyAV probe on the first 64KB of a segment or MP4 header.
    Extracts exact width, height, video codec, and audio codec in ~0.05s!
    """
    info: Dict[str, Any] = {}
    try:
        import av
        clean = strip_disguised_mpegts_header(raw_bytes)
        with av.open(io.BytesIO(clean)) as container:
            if container.duration and container.duration > 0:
                info["duration_sec"] = float(container.duration) / 1_000_000.0
            v_stream = next((s for s in container.streams if s.type == "video"), None)
            a_stream = next((s for s in container.streams if s.type == "audio"), None)
            if v_stream:
                if not v_stream.width or not v_stream.height:
                    try:
                        for idx, pkt in enumerate(container.demux(v_stream)):
                            for frame in pkt.decode():
                                if frame.width and frame.height:
                                    info["width"] = int(frame.width)
                                    info["height"] = int(frame.height)
                                    break
                            if info.get("width") or idx >= 5:
                                break
                    except Exception:
                        pass
                if v_stream.width and v_stream.height and not info.get("width"):
                    info["width"] = int(v_stream.width)
                    info["height"] = int(v_stream.height)
                if v_stream.codec_context and v_stream.codec_context.name:
                    info["vcodec"] = v_stream.codec_context.name.upper()
            if a_stream and a_stream.codec_context and a_stream.codec_context.name:
                info["acodec"] = a_stream.codec_context.name.upper()
    except Exception:
        pass
    return info


async def probe_stream_metadata(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    hints: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Probes an .m3u8 or .mp4 stream URL just like Web Video Caster Mobile App:
    - Computes exact total duration (HH:MM:SS) by summing #EXTINF segments or MP4 container duration
    - Computes exact resolution (e.g. 1920x796 / 1080p FHD) via #EXT-X-STREAM-INF or 64KB PyAV segment probe
    - Extracts bitrate (kbps), codecs (H264/AAC), segment count, and embedded HLS subtitles!
    """
    if url in state.probe_cache:
        cached = dict(state.probe_cache[url])
        return cached

    hints = hints or {}
    req_headers = {
        "User-Agent": (headers or {}).get(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ),
        "Accept": "*/*",
    }
    if headers and headers.get("Referer"):
        req_headers["Referer"] = headers["Referer"]

    meta: Dict[str, Any] = {
        "duration_sec": float(hints.get("duration_sec") or 0),
        "duration_str": "",
        "width": int(hints.get("width") or 0),
        "height": int(hints.get("height") or 0),
        "resolution_str": "",
        "bitrate_kbps": int(hints.get("bitrate_kbps") or 0),
        "vcodec": "",
        "acodec": "",
        "segment_count": 0,
        "variant_type": "",
        "variants": [],
        "subtitles": [],
        "is_main_movie": False,
    }

    stream_type = detect_stream_type(url)

    try:
        async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            if stream_type == "hls":
                _, _, raw_m3u8_bytes, final_url = await fetch_url_universal(url, headers=req_headers, timeout=10.0)
                text = raw_m3u8_bytes.decode("utf-8", errors="replace")
                text = decrypt_enc_aesgcm_m3u8_if_needed(text, final_url, req_headers.get("Referer", ""))

                if "#EXT-X-STREAM-INF" in text:
                    meta["variant_type"] = "Master Playlist"
                    # Extract embedded HLS subtitles (#EXT-X-MEDIA:TYPE=SUBTITLES)
                    for sub_m in re.finditer(r'#EXT-X-MEDIA:[^\r\n]*TYPE=SUBTITLES[^\r\n]*', text):
                        line = sub_m.group(0)
                        uri_m = re.search(r'URI="([^"]+)"', line)
                        name_m = re.search(r'NAME="([^"]+)"', line)
                        lang_m = re.search(r'LANGUAGE="([^"]+)"', line)
                        if uri_m:
                            sub_url = urllib.parse.urljoin(final_url, uri_m.group(1))
                            meta["subtitles"].append({
                                "label": (name_m.group(1) if name_m else None) or (lang_m.group(1) if lang_m else "Sub HLS"),
                                "url": sub_url,
                            })

                    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                    for idx, ln in enumerate(lines):
                        if ln.startswith("#EXT-X-STREAM-INF:"):
                            res_m = re.search(r"RESOLUTION=(\d+)x(\d+)", ln)
                            bw_m = re.search(r"BANDWIDTH=(\d+)", ln)
                            cod_m = re.search(r'CODECS="([^"]+)"', ln)
                            w = int(res_m.group(1)) if res_m else 0
                            h = int(res_m.group(2)) if res_m else 0
                            bw = int(bw_m.group(1)) // 1000 if bw_m else 0
                            var_url = ""
                            if idx + 1 < len(lines) and not lines[idx + 1].startswith("#"):
                                var_url = urllib.parse.urljoin(final_url, lines[idx + 1])
                            if w > meta["width"]:
                                meta["width"] = w
                                meta["height"] = h
                            if bw > meta["bitrate_kbps"]:
                                meta["bitrate_kbps"] = bw
                            if cod_m and not meta["vcodec"]:
                                meta["vcodec"] = cod_m.group(1)
                            if var_url:
                                meta["variants"].append({
                                    "url": var_url,
                                    "width": w,
                                    "height": h,
                                    "bitrate_kbps": bw,
                                    "resolution_str": format_resolution_badge(w, h),
                                })

                    # Fetch first variant playlist to calculate exact #EXTINF duration!
                    if meta["variants"]:
                        best_var = sorted(meta["variants"], key=lambda x: (x["width"], x["bitrate_kbps"]), reverse=True)[0]
                        r_var = await client.get(best_var["url"], headers=req_headers)
                        var_text = r_var.text
                        var_final = str(r_var.url)
                        extinfs = [float(x) for x in re.findall(r"#EXTINF:\s*([\d\.]+)", var_text)]
                        if extinfs:
                            meta["duration_sec"] = sum(extinfs)
                            meta["segment_count"] = len(extinfs)
                        # If Master playlist didn't specify RESOLUTION=, probe first 64KB of first segment!
                        if not meta["width"]:
                            seg_lines = [ln.strip() for ln in var_text.splitlines() if ln.strip() and not ln.startswith("#")]
                            if seg_lines:
                                first_seg = urllib.parse.urljoin(var_final, seg_lines[0])
                                r_seg = await client.get(first_seg, headers={**req_headers, "Range": "bytes=0-65535"})
                                av_info = await asyncio.to_thread(_probe_bytes_with_pyav, r_seg.content)
                                meta["width"] = av_info.get("width") or meta["width"]
                                meta["height"] = av_info.get("height") or meta["height"]
                                meta["vcodec"] = av_info.get("vcodec") or meta["vcodec"]
                                meta["acodec"] = av_info.get("acodec") or meta["acodec"]

                elif "#EXTINF" in text:
                    meta["variant_type"] = "Media Playlist"
                    extinfs = [float(x) for x in re.findall(r"#EXTINF:\s*([\d\.]+)", text)]
                    if extinfs:
                        meta["duration_sec"] = sum(extinfs)
                        meta["segment_count"] = len(extinfs)

                    # Check bitrate from URL if present (e.g. /2000kb/hls/index.m3u8)
                    kb_m = re.search(r"/(\d{3,5})kb/", url, re.I)
                    if kb_m and not meta["bitrate_kbps"]:
                        meta["bitrate_kbps"] = int(kb_m.group(1))

                    # Probe first 64KB of first segment with PyAV if width/height not yet known!
                    if not meta["width"]:
                        seg_lines = [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]
                        if seg_lines:
                            first_seg = urllib.parse.urljoin(final_url, seg_lines[0])
                            r_seg = await client.get(first_seg, headers={**req_headers, "Range": "bytes=0-65535"})
                            av_info = await asyncio.to_thread(_probe_bytes_with_pyav, r_seg.content)
                            meta["width"] = av_info.get("width") or meta["width"]
                            meta["height"] = av_info.get("height") or meta["height"]
                            meta["vcodec"] = av_info.get("vcodec") or meta["vcodec"]
                            meta["acodec"] = av_info.get("acodec") or meta["acodec"]

            elif stream_type in ("mp4", "auto"):
                # If yt-dlp or caller already provided exact duration and resolution, skip downloading 1MB!
                if meta["duration_sec"] > 0 and meta["height"] > 0:
                    meta["vcodec"] = meta["vcodec"] or "H264"
                    meta["acodec"] = meta["acodec"] or "AAC"
                else:
                    # Request first 1MB (1048575 bytes) so full-length 90-min movies with ~650KB moov atoms
                    # (like Abyss/Hydrax sssrr.org/sora/... extensionless MP4s) parse 100% of duration + H264/AAC tracks!
                    r_head = await client.get(url, headers={**req_headers, "Range": "bytes=0-1048575"})
                    if r_head.status_code in (200, 206):
                        ctype = (r_head.headers.get("content-type") or "").lower()
                        if "mpegurl" in ctype or r_head.content.lstrip().startswith(b"#EXTM3U"):
                            stream_type = "hls"
                        else:
                            av_info = await asyncio.to_thread(_probe_bytes_with_pyav, r_head.content)
                            meta["duration_sec"] = av_info.get("duration_sec") or meta["duration_sec"]
                            meta["width"] = av_info.get("width") or meta["width"]
                            meta["height"] = av_info.get("height") or meta["height"]
                            meta["vcodec"] = av_info.get("vcodec") or meta["vcodec"]
                            meta["acodec"] = av_info.get("acodec") or meta["acodec"]
                            # Estimate bitrate from Content-Range total size if available
                            cr = r_head.headers.get("content-range") or ""
                            cr_m = re.search(r"/(\d+)$", cr)
                            if cr_m and meta["duration_sec"] > 0 and not meta["bitrate_kbps"]:
                                total_bytes = int(cr_m.group(1))
                                meta["bitrate_kbps"] = int((total_bytes * 8) / (meta["duration_sec"] * 1000))
    except Exception as e:
        print(f"[PROBE WARN] {url}: {e}", flush=True)

    meta["duration_str"] = format_duration_hms(meta["duration_sec"])
    meta["resolution_str"] = format_resolution_badge(meta["width"], meta["height"])
    meta["is_main_movie"] = meta["duration_sec"] >= 300.0

    # Build a rich summary badge string like Web Video Caster Mobile App!
    parts = []
    if meta["resolution_str"]:
        parts.append(meta["resolution_str"])
    else:
        parts.append("MP4" if stream_type == "auto" and meta["vcodec"] else stream_type.upper())
    if meta["duration_str"]:
        parts.append(f"⏱️ {meta['duration_str']}")
    if meta["bitrate_kbps"]:
        parts.append(f"{meta['bitrate_kbps']} kbps")
    if meta["vcodec"]:
        codec_lbl = f"{meta['vcodec']}/{meta['acodec']}" if meta["acodec"] else meta["vcodec"]
        parts.append(codec_lbl)
    meta["quality_badge"] = " · ".join(parts)

    if meta["duration_sec"] > 0 or meta["width"] > 0:
        state.probe_cache[url] = meta

    return meta


# ============================================================================
# 7. UNIVERSAL 4-LAYER WVC SNIFFER (HTML/Base64/Iframe + DPI Bypass + Runtime Player Hook + Interactive Browser)
# ============================================================================

async def inspect_embed_player_url(embed_url: str, parent_referer: str, server_label: str = "", depth: int = 0) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Tuple[str, str, str]]]:
    """
    Unrolls iframe/embed players (like v4.streamvsmov.com, tvhayd.pro/play?id=..., abyssplayer.com, etc.):
    - Automatically bypasses ISP DNS/DPI blocks via `fetch_url_universal`
    - Converts `/tvc?id=` ad-wrappers into `/play?id=`
    - Follows nested `<iframe src="...">` up to 2 levels deep
    - Returns (streams, subtitles, js_iframe_targets_for_playwright)
    """
    streams: List[Dict[str, Any]] = []
    subs: List[Dict[str, Any]] = []
    js_iframes: List[Tuple[str, str, str]] = []

    if "/tvc?id=" in embed_url:
        embed_url = embed_url.replace("/tvc?id=", "/play?id=")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Referer": parent_referer,
    }
    try:
        status, _, raw_body, final_embed = await fetch_url_universal(embed_url, headers=headers, timeout=12.0)
        html = raw_body.decode("utf-8", errors="replace")

        # 1. Extract title from embed if present
        name_m = re.search(r'<div class="name">([^<]+)</div>', html)
        embed_title = name_m.group(1).strip() if name_m else (server_label or "Embed Stream")

        # 2. Extract subtitles JSON array: `subtitles: [...]` or `tracks: [...]`
        sub_json_m = re.search(r"subtitles\s*:\s*(\[\s*\{.*?\}\s*\])", html, re.S)
        if sub_json_m:
            try:
                sub_items = json.loads(sub_json_m.group(1))
                for s in sub_items:
                    s_url = s.get("url") or s.get("file")
                    if s_url:
                        abs_sub = urllib.parse.urljoin(final_embed, s_url)
                        code = (s.get("code") or s.get("label") or s.get("name") or "sub").upper()
                        lang_label = "Tiếng Việt (.VTT)" if "VIE" in code or "VI" in code else (
                            "Tiếng Anh (.VTT)" if "ENG" in code or "EN" in code else f"Phụ đề {code}"
                        )
                        if server_label:
                            lang_label = f"{lang_label} [{server_label}]"
                        subs.append({
                            "label": lang_label,
                            "url": abs_sub,
                            "referer": final_embed,
                        })
            except Exception:
                pass

        # Also regex any direct .vtt / .srt URLs inside the embed HTML
        for sm in re.finditer(r'(["\'])([^\s"\'<>]+?\.(?:vtt|srt)(?:\?[^\s"\'<>]*)?)\1', html, re.I):
            abs_sub = urllib.parse.urljoin(final_embed, sm.group(2))
            if not any(x["url"] == abs_sub for x in subs):
                subs.append({
                    "label": f"Phụ đề rời ({abs_sub.split('/')[-1].split('?')[0]})",
                    "url": abs_sub,
                    "referer": final_embed,
                })

        # 3. Extract streamvsmov pattern: baseUrl + '/stream/' + videoHash + '/master.m3u8'
        base_m = re.search(r'const\s+baseUrl\s*=\s*["\']([^"\']+)["\']', html)
        hash_m = re.search(r'const\s+videoHash\s*=\s*["\']([^"\']+)["\']', html)
        if base_m and hash_m:
            m3u8_url = f"{base_m.group(1).rstrip('/')}/stream/{hash_m.group(1)}/master.m3u8"
            streams.append({
                "title": f"{embed_title} ({server_label})" if server_label else embed_title,
                "server_name": server_label or "Embed Server",
                "url": m3u8_url,
                "type": "hls",
                "headers": {"Referer": final_embed, "User-Agent": headers["User-Agent"]},
                "subtitles": subs,
                "source": f"Embed ({urllib.parse.urlparse(final_embed).hostname})",
            })

        # 4. Also check generic direct .m3u8 / .mp4 inside the embed HTML
        clean_html = html.replace(r"\/", "/")
        for m_url in re.findall(r'(https?://[^\s"\'<>\\]+?\.(?:m3u8|mp4)(?:\?[^\s"\'<>\\]*)?)', clean_html, re.I):
            cleaned, _ = unwrap_and_clean_stream_url(m_url)
            if cleaned and not any(x["url"] == cleaned for x in streams):
                streams.append({
                    "title": f"{embed_title} ({server_label})" if server_label else embed_title,
                    "server_name": server_label or "Embed Server",
                    "url": cleaned,
                    "type": detect_stream_type(cleaned),
                    "headers": {"Referer": final_embed, "User-Agent": headers["User-Agent"]},
                    "subtitles": subs,
                    "source": f"Embed ({urllib.parse.urlparse(final_embed).hostname})",
                })

        # 5. Check if this embed page wraps a deeper `<iframe src="...">` (e.g. /play?id=910945 -> https://abyssplayer.com/Poh-lIVIIo)
        if depth < 2:
            for ifr_m in re.finditer(r'<iframe[^>]+src=["\']([^"\']+)["\']', html, re.I):
                raw_ifr = ifr_m.group(1).strip()
                if not raw_ifr or raw_ifr.startswith("javascript:") or raw_ifr == "about:blank":
                    continue
                abs_ifr = urllib.parse.urljoin(final_embed, raw_ifr)
                if "/tvc?id=" in abs_ifr:
                    abs_ifr = abs_ifr.replace("/tvc?id=", "/play?id=")
                sub_streams, sub_subs, sub_js = await inspect_embed_player_url(abs_ifr, final_embed, server_label, depth + 1)
                streams.extend(sub_streams)
                for s in sub_subs:
                    if not any(x["url"] == s["url"] for x in subs):
                        subs.append(s)
                js_iframes.extend(sub_js)

        # 6. If this embed page uses encrypted JS player initialization (like Abyss `SoTrym` / `jwplayer` / `streamfree.vip` `#app` / `eval(function(p,a,c,k,e,d))`)
        #    and didn't yield direct static URLs yet, queue it as a direct Playwright JS target!
        if not streams and (
            any(kw in html for kw in ("SoTrym", "jwplayer", "videojs", "Artplayer", "DPlayer", "Clappr", "eval(function", 'id="app"'))
            or "streamfree" in final_embed.lower()
            or "/embed/" in final_embed.lower()
        ):
            js_iframes.append((final_embed, parent_referer, server_label or "Embed Player"))

    except Exception as e:
        print(f"[EMBED INSPECT WARN] {embed_url}: {e}", flush=True)
        # Even if static fetch failed, queue for Playwright with DPI bypass!
        js_iframes.append((embed_url, parent_referer, server_label or "Embed Player"))

    return streams, subs, js_iframes


def _try_decode_base64_link(val: str, base_url: str) -> Optional[str]:
    """
    Detects and decodes Base64-encoded URLs or paths in `data-link` / `data-src` attributes
    (e.g. `L3R2Yz9pZD05MTA5NDU=` -> `/tvc?id=910945` -> `https://tvhayd.pro/play?id=910945`).
    """
    val = val.strip()
    if not val:
        return None
    if val.startswith("http://") or val.startswith("https://") or val.startswith("/"):
        res = urllib.parse.urljoin(base_url, val)
        return res.replace("/tvc?id=", "/play?id=")
    if re.match(r"^[A-Za-z0-9+/=_-]{8,}$", val) and len(val) % 4 in (0, 2, 3):
        try:
            pad = "=" * ((4 - len(val) % 4) % 4)
            dec = base64.b64decode(val + pad).decode("utf-8", errors="strict").strip()
            if dec.startswith("http://") or dec.startswith("https://") or dec.startswith("/"):
                res = urllib.parse.urljoin(base_url, dec)
                return res.replace("/tvc?id=", "/play?id=")
        except Exception:
            pass
    return None


async def sniff_with_html_and_nextjs(url: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Tuple[str, str, str]], str]:
    """
    Universal Layer 1 HTML + Next.js SSR (`__next_f`) + Base64 `data-link` + Multi-Server Iframe Unroller.
    Discovers:
    - Direct m3u8/mp4 links
    - Multi-server sibling episodes (e.g. Thuyết Minh + Song ngữ / Vietsub)
    - Base64-encoded `data-link` / `/tvc?id=` -> `/play?id=` server buttons (e.g. tvhayd.pro)
    - Nested Iframe / Embed players (e.g. v4.streamvsmov.com, abyssplayer.com) and their .vtt/.srt subtitles!
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Referer": url,
    }
    _, _, raw_body, final_url = await fetch_url_universal(url, headers=headers, timeout=15.0)
    html = raw_body.decode("utf-8", errors="replace")

    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    page_title = html_unescape(title_match.group(1).strip()) if title_match else url
    clean_html = html.replace(r'\"', '"').replace(r"\/", "/")

    results: List[Dict[str, Any]] = []
    subtitles: List[Dict[str, Any]] = []
    js_iframes: List[Tuple[str, str, str]] = []
    seen_urls = set()
    seen_embeds = set()
    embed_tasks = []

    # 1. Extract any direct .srt / .vtt subtitles on the main page
    for sm in re.finditer(r'(https?://[^\s"\'<>\\]+?\.(?:vtt|srt)(?:\?[^\s"\'<>\\]*)?)', clean_html, re.I):
        s_url = sm.group(1)
        if not any(x["url"] == s_url for x in subtitles):
            subtitles.append({
                "label": f"Phụ đề ({s_url.split('/')[-1].split('?')[0]})",
                "url": s_url,
                "referer": final_url,
            })

    # 2. Universal scan for `data-link`, `data-src`, `data-embed`, `data-play`, `data-video` attributes (including Base64!)
    for tag_m in re.finditer(r'<([a-zA-Z0-9]+)[^>]*?(?:data-link|data-embed|data-play|data-video|data-src)=["\']([^"\']+)["\'][^>]*>(.*?)</\1>', html, re.I | re.S):
        raw_attr = tag_m.group(2)
        btn_text = re.sub(r"<[^>]+>", "", tag_m.group(3)).strip() or "Server"
        decoded_target = _try_decode_base64_link(raw_attr, final_url)
        if decoded_target and decoded_target not in seen_embeds:
            seen_embeds.add(decoded_target)
            embed_tasks.append(inspect_embed_player_url(decoded_target, final_url, btn_text))

    # Also scan `<iframe src="...">` on the main page
    for ifr_m in re.finditer(r'<iframe[^>]+src=["\']([^"\']+)["\']', html, re.I):
        raw_ifr = ifr_m.group(1).strip()
        if not raw_ifr or raw_ifr.startswith("javascript:") or raw_ifr == "about:blank":
            continue
        if any(bad in raw_ifr.lower() for bad in ("googletagmanager", "doubleclick", "facebook.com", "disqus", "darkbytes.xyz")):
            continue
        abs_ifr = urllib.parse.urljoin(final_url, raw_ifr).replace("/tvc?id=", "/play?id=")
        if abs_ifr not in seen_embeds:
            seen_embeds.add(abs_ifr)
            embed_tasks.append(inspect_embed_player_url(abs_ifr, final_url, "Main Player"))

    # 2.5. Universal Halim / WordPress `dox_ajax_player` Unroller (e.g. hhpanda.st / halimthemes):
    # Detects `<div id="halim-player-wrapper" data-post-id="59" data-ep="tap-164" data-sv="2">`
    # and `.get-eps[data-type]` server tabs (PRO, VIP 4K, TIK, etc.)
    halim_m = (
        re.search(
            r'id=["\']halim-player-wrapper["\'][^>]*data-post-id=["\'](\d+)["\'][^>]+data-ep=["\']([^"\']+)["\'][^>]+data-sv=["\'](\d+)["\']',
            html,
            re.I,
        )
        or re.search(
            r'data-post-id=["\'](\d+)["\'][^>]+data-ep=["\']([^"\']+)["\'][^>]+data-sv=["\'](\d+)["\']',
            html,
            re.I,
        )
    )
    if halim_m:
        h_post_id, h_ep, h_sv = halim_m.group(1), halim_m.group(2), halim_m.group(3)
        url_sv_m = re.search(r"/(tap-[^/]+?)-sv(\d+)\.html", final_url, re.I)
        if url_sv_m:
            h_ep, h_sv = url_sv_m.group(1), url_sv_m.group(2)
        srv_types = []
        for st_m in re.finditer(r'class=["\'][^"\']*get-eps[^"\']*["\'][^>]*data-type=["\']([^"\']+)["\'][^>]*>([^<]+)<', html, re.I):
            t_code, t_label = st_m.group(1).strip(), st_m.group(2).strip()
            if (t_code, t_label) not in srv_types:
                srv_types.append((t_code, t_label))
        if not srv_types:
            srv_types = [("pro", "PRO"), ("vip4k", "VIP 4K"), ("tiktik", "TIK")]
        else:
            # Prioritize PRO (1080p) and VIP 4K (4K UHD) first
            srv_types.sort(key=lambda x: 0 if x[0] == "pro" else (1 if x[0] == "vip4k" else 2))

        async def _fetch_halim_ajax_server(t_code: str, t_label: str):
            ajax_url = (
                f"{ urllib.parse.urlparse(final_url).scheme }://{ urllib.parse.urlparse(final_url).netloc }"
                f"/player/player.php?action=dox_ajax_player&post_id={h_post_id}&chapter_st={h_ep}&type={t_code}&sv={h_sv}"
            )
            try:
                _, _, ajax_body, _ = await fetch_url_universal(
                    ajax_url,
                    headers={**headers, "X-Requested-With": "XMLHttpRequest"},
                    timeout=10.0,
                )
                ajax_html = ajax_body.decode("utf-8", errors="replace").replace(r"\/", "/")
                ifr_match = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', ajax_html, re.I)
                if ifr_match:
                    ifr_src = urllib.parse.urljoin(final_url, ifr_match.group(1).strip())
                    return await inspect_embed_player_url(ifr_src, final_url, f"Server {t_label}")
            except Exception as e:
                print(f"[HALIM AJAX WARN] {ajax_url}: {e}", flush=True)
            return ([], [], [])

        for t_code, t_label in srv_types[:2]:
            embed_tasks.append(_fetch_halim_ajax_server(t_code, t_label))

    # 3. Extract Next.js episode objects:
    for ep_m in re.finditer(
        r'"id"\s*:\s*(\d+)\s*,\s*"name"\s*:\s*"([^"]+)"(?:\s*,\s*"server"\s*:\s*"([^"]+)")?\s*,\s*"slug"\s*:\s*"([^"]+)"\s*,\s*"type"\s*:\s*"([^"]+)"\s*,\s*"link"\s*:\s*"([^"]+)"',
        clean_html,
    ):
        ep_id, ep_name, ep_srv, ep_slug, ep_type, ep_link = ep_m.groups()
        srv_name = ep_srv or ep_name

        if ep_type.lower() == "embed" or ("/video/" in ep_link and not ep_link.endswith(".m3u8")):
            if ep_link not in seen_embeds:
                seen_embeds.add(ep_link)
                embed_tasks.append(inspect_embed_player_url(ep_link, final_url, srv_name))
        else:
            cleaned, hints = unwrap_and_clean_stream_url(ep_link)
            if cleaned and cleaned not in seen_urls:
                seen_urls.add(cleaned)
                results.append({
                    "title": f"{page_title} [{srv_name}]",
                    "server_name": srv_name,
                    "url": cleaned,
                    "type": detect_stream_type(cleaned),
                    "headers": {"Referer": final_url, "User-Agent": headers["User-Agent"]},
                    "subtitles": [],
                    "hints": hints,
                    "source": f"Server {srv_name}",
                })

    # 4. Check if the page lists other servers in `latestEpisodes`:
    parsed_main = urllib.parse.urlparse(final_url)
    base_movie_path = re.sub(r"/tap-[^/]+/?$", "", parsed_main.path)
    sibling_urls = []
    for sib_m in re.finditer(
        r'\{"id"\s*:\s*(\d+)\s*,\s*"name"\s*:\s*"([^"]+)"\s*,\s*"server"\s*:\s*"([^"]+)"\s*,\s*"movieId"\s*:\s*\d+',
        clean_html,
    ):
        sib_id, sib_name, sib_srv = sib_m.groups()
        sib_slug = f"tap-{sib_id}"
        if sib_slug not in final_url:
            sib_full_url = f"{parsed_main.scheme}://{parsed_main.netloc}{base_movie_path}/{sib_slug}"
            if not any(u == sib_full_url for _, u in sibling_urls):
                sibling_urls.append((f"{sib_srv} ({sib_name})", sib_full_url))

    async def _fetch_sibling_episode(srv_label: str, sib_url: str):
        try:
            _, _, sib_body, _ = await fetch_url_universal(sib_url, headers=headers, timeout=10.0)
            sib_clean = sib_body.decode("utf-8", errors="replace").replace(r'\"', '"').replace(r"\/", "/")
            for ep_m in re.finditer(
                r'"id"\s*:\s*(\d+)\s*,\s*"name"\s*:\s*"([^"]+)"(?:\s*,\s*"server"\s*:\s*"([^"]+)")?\s*,\s*"slug"\s*:\s*"([^"]+)"\s*,\s*"type"\s*:\s*"([^"]+)"\s*,\s*"link"\s*:\s*"([^"]+)"',
                sib_clean,
            ):
                _, _, _, _, ep_type, ep_link = ep_m.groups()
                if ep_type.lower() == "embed" or ("/video/" in ep_link and not ep_link.endswith(".m3u8")):
                    return await inspect_embed_player_url(ep_link, sib_url, srv_label)
                cleaned, hints = unwrap_and_clean_stream_url(ep_link)
                if cleaned:
                    return ([{
                        "title": f"{page_title} [{srv_label}]",
                        "server_name": srv_label,
                        "url": cleaned,
                        "type": detect_stream_type(cleaned),
                        "headers": {"Referer": sib_url, "User-Agent": headers["User-Agent"]},
                        "subtitles": [],
                        "hints": hints,
                        "source": f"Server {srv_label}",
                    }], [], [])
        except Exception:
            pass
        return ([], [], [])

    for srv_label, sib_url in sibling_urls[:3]:
        embed_tasks.append(_fetch_sibling_episode(srv_label, sib_url))

    # 5. Also scan generic direct .m3u8 / .mp4 links in the page HTML
    pattern = r'(https?://[^\s"\'<>\\]+?\.(?:m3u8|mp4|mkv|webm|mpd)(?:\?[^\s"\'<>\\]*)?)'
    for m_url in re.findall(pattern, clean_html, re.I):
        cleaned, hints = unwrap_and_clean_stream_url(m_url)
        if not cleaned or cleaned in seen_urls:
            continue
        seen_urls.add(cleaned)
        results.append({
            "title": page_title,
            "server_name": "Direct HTML",
            "url": cleaned,
            "type": detect_stream_type(cleaned),
            "headers": {"Referer": final_url, "User-Agent": headers["User-Agent"]},
            "subtitles": [],
            "hints": hints,
            "source": "HTML Hunter",
        })

    if embed_tasks:
        embed_outcomes = await asyncio.gather(*embed_tasks, return_exceptions=True)
        for out in embed_outcomes:
            if isinstance(out, tuple) and len(out) == 3:
                e_streams, e_subs, e_js = out
                for s in e_subs:
                    if not any(x["url"] == s["url"] for x in subtitles):
                        subtitles.append(s)
                for st in e_streams:
                    if st["url"] not in seen_urls:
                        seen_urls.add(st["url"])
                        if page_title and st.get("title") in ("Embed Stream", st.get("server_name")):
                            st["title"] = f"{page_title} [{st.get('server_name', 'Server')}]"
                        results.append(st)
                for j_target in e_js:
                    if j_target not in js_iframes:
                        js_iframes.append(j_target)

    # Attach discovered subtitles to streams that don't have their own yet
    for st in results:
        if not st.get("subtitles") and subtitles:
            st["subtitles"] = subtitles

    episodes = extract_episodes_universal(html, final_url)
    return results, subtitles, js_iframes, page_title, episodes


def extract_episodes_universal(html: str, page_url: str) -> List[Dict[str, Any]]:
    """
    Universal Multi-Episode Playlist Extractor across Vietnamese & international movie sites
    (e.g. tvhayd.pro `/xem-phim/tho-san-yeu-tinh-30605-112420`, cobephim.icu `/phim/.../tap-1`,
    rophim, phimmoi, kkphim, ophim, motchill, animevietsub, etc.).
    Returns an ordered list of episodes:
    [{"index": 0, "name": "Tập 1", "label": "1", "url": "https://...", "active": True}, ...]
    """
    parsed = urllib.parse.urlparse(page_url)
    cur_path = parsed.path.rstrip("/")

    # 1. Determine common movie prefix from current path:
    # e.g. /xem-phim/tho-san-yeu-tinh-30605-112420 -> /xem-phim/tho-san-yeu-tinh-30605-
    # e.g. /phim/tan-o-long-vien/tap-1 -> /phim/tan-o-long-vien/
    m_prefix = re.match(r"^(/xem-phim/.+?-\d+-)\d+$", cur_path)
    if m_prefix:
        prefix = m_prefix.group(1)
    else:
        m_tap = re.match(r"^(.+?/)(?:tap-[^/]+|ep-[^/]+|xem-phim-[^/]+|\d+)$", cur_path)
        prefix = m_tap.group(1) if m_tap else (cur_path.rsplit("/", 1)[0] + "/")

    episodes: List[Dict[str, Any]] = []
    seen_ep_urls = set()

    for m in re.finditer(r'<a([^>]+href=["\']([^"\']+)["\'][^>]*)>(.*?)</a>', html, re.I | re.S):
        attrs, raw_href, inner = m.group(1), m.group(2).strip(), m.group(3)
        if not raw_href or raw_href.startswith("javascript:") or raw_href.startswith("#"):
            continue
        abs_u = urllib.parse.urljoin(page_url, raw_href)
        u_parsed = urllib.parse.urlparse(abs_u)
        if u_parsed.netloc != parsed.netloc:
            continue
        u_path = u_parsed.path.rstrip("/")
        if not u_path.startswith(prefix):
            continue
        label = re.sub(r"<[^>]+>", "", inner).strip()
        if not label or len(label) > 35:
            continue
        # Must look like an episode label (number, Tập X, Full, X-End, etc.)
        if not (re.match(r"^(?:tập\s*|tap\s*|ep\s*|episode\s*)?\d+", label, re.I) or "full" in label.lower() or "end" in label.lower()):
            continue
        if abs_u not in seen_ep_urls:
            seen_ep_urls.add(abs_u)
            ep_name = label if label.lower().startswith("tập") else f"Tập {label}"
            episodes.append({
                "index": len(episodes),
                "name": ep_name,
                "label": label,
                "url": abs_u,
                "active": (u_path == cur_path) or ("active" in attrs.lower()),
            })

    # Ensure matching server suffix if page_url has `-sv\d+\.html` (e.g. hhpanda.st `-sv1.html` vs `-sv2.html`)
    sv_suffix_m = re.search(r"(-sv\d+\.html)$", cur_path, re.I)
    if sv_suffix_m and len(episodes) > 1:
        target_suffix = sv_suffix_m.group(1).lower()
        filtered_sv = [e for e in episodes if e["url"].lower().endswith(target_suffix)]
        if filtered_sv:
            episodes = filtered_sv

    # Sort descending episode lists (e.g. 164 -> 1 on hhpanda.st) into natural ascending order (1 -> 164)
    if len(episodes) >= 2:
        first_num_m = re.search(r"(\d+)", episodes[0]["label"])
        last_num_m = re.search(r"(\d+)", episodes[-1]["label"])
        if first_num_m and last_num_m and int(first_num_m.group(1)) > int(last_num_m.group(1)):
            episodes.reverse()
            for idx, ep in enumerate(episodes):
                ep["index"] = idx

    # Ensure exactly one active episode if current URL matches
    if episodes and not any(e["active"] for e in episodes):
        for e in episodes:
            if urllib.parse.urlparse(e["url"]).path.rstrip("/") == cur_path:
                e["active"] = True
                break
        else:
            episodes[0]["active"] = True

    return episodes


def build_local_folder_episodes(file_path: Path) -> List[Dict[str, Any]]:
    """
    When casting a local video file (e.g. `I:\\tv show\\goblin\\[K-Zone].Goblin.E02.HD.KITES.VN.mkv`),
    automatically scans the parent folder for all video files in natural episode order so the user gets
    a full Episode Playlist & Auto-Next Episode for local TV show folders too!
    """
    video_exts = {".mkv", ".mp4", ".webm", ".avi", ".mov", ".ts", ".m4v"}
    try:
        parent = file_path.parent
        siblings = [
            p for p in parent.iterdir()
            if p.is_file() and p.suffix.lower() in video_exts
        ]
        # Natural sort (so E2 comes before E10)
        siblings.sort(key=lambda p: [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", p.name)])
        if len(siblings) <= 1:
            return []
        eps: List[Dict[str, Any]] = []
        for idx, p in enumerate(siblings):
            m_ep = re.search(r"(?:[eE][pP]?|[tT]ập\s*|-\s*)(\d{1,3})", p.stem)
            lbl = f"Tập {int(m_ep.group(1))}" if m_ep else f"File {idx + 1}"
            eps.append({
                "index": idx,
                "name": f"{lbl} ({p.name})",
                "label": lbl,
                "url": str(p),
                "is_local": True,
                "active": (p.resolve() == file_path.resolve()),
            })
        return eps
    except Exception:
        return []


def html_unescape(text: str) -> str:
    import html as html_mod
    return html_mod.unescape(text)


def extract_youtube_watch_url(raw_url: str, page_url: str = "", referer: str = "") -> Optional[str]:
    """
    Extracts a canonical YouTube watch URL (`https://www.youtube.com/watch?v=VIDEO_ID`)
    from raw_url, page_url, or referer if any of them points to a YouTube video/short.
    """
    for candidate in (raw_url, page_url, referer):
        if not candidate:
            continue
        m = re.search(
            r"(?:youtube\.com/(?:watch\?.*?v=|shorts/|embed/|live/)|youtu\.be/)([A-Za-z0-9_-]{11})",
            candidate,
        )
        if m:
            return f"https://www.youtube.com/watch?v={m.group(1)}"
    return None


class _SilentYtdlpLogger:
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


async def sniff_with_ytdlp(url: str) -> List[Dict[str, Any]]:
    import yt_dlp

    def _extract():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "logger": _SilentYtdlpLogger(),
            "skip_download": True,
            "noplaylist": True,
            "socket_timeout": 10,
            "js_runtimes": {"node": {}},
            "extractor_args": {"youtube": {"player_client": ["android", "mweb", "web_embedded"]}},
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=False)

    info = await asyncio.to_thread(_extract)
    if not info:
        return []

    results = []
    title = info.get("title") or "Video Stream"
    thumbnail = info.get("thumbnail")
    is_yt = ("youtube.com" in url.lower() or "youtu.be" in url.lower())
    http_headers = dict(info.get("http_headers") or ({} if is_yt else {"Referer": url}))
    if is_yt:
        http_headers.pop("Referer", None)
    duration_sec = float(info.get("duration") or 0)

    subtitles = []
    for lang, subs in (info.get("subtitles") or {}).items():
        for s in subs:
            if s.get("url"):
                subtitles.append({"label": f"{lang} ({s.get('ext', 'vtt')})", "url": s["url"], "referer": url})

    formats = info.get("formats") or []
    muxed_formats = [
        f for f in formats
        if f.get("vcodec") not in (None, "none") and f.get("acodec") not in (None, "none")
    ]
    candidate_formats = muxed_formats if muxed_formats else [f for f in formats if f.get("vcodec") != "none"]
    formats_sorted = sorted(
        candidate_formats,
        key=lambda f: (
            1 if f.get("vcodec") not in (None, "none") and f.get("acodec") not in (None, "none") else 0,
            f.get("height") or 0,
            f.get("tbr") or 0,
        ),
        reverse=True,
    )

    seen_urls = set()
    if info.get("url") and info.get("vcodec") not in (None, "none") and info.get("acodec") not in (None, "none"):
        cleaned_u, _ = unwrap_and_clean_stream_url(info["url"])
        if cleaned_u:
            seen_urls.add(cleaned_u)
            s_type = "mp4" if "googlevideo.com" in cleaned_u or info.get("ext") == "mp4" else detect_stream_type(cleaned_u)
            results.append({
                "title": title,
                "url": cleaned_u,
                "type": s_type,
                "headers": http_headers,
                "thumbnail": thumbnail,
                "subtitles": subtitles,
                "hints": {
                    "duration_sec": duration_sec,
                    "width": info.get("width") or 0,
                    "height": info.get("height") or 0,
                },
                "source": "yt-dlp",
            })

    for fmt in formats_sorted:
        f_url = fmt.get("url")
        if not f_url:
            continue
        cleaned_u, _ = unwrap_and_clean_stream_url(f_url)
        if not cleaned_u or cleaned_u in seen_urls:
            continue
        seen_urls.add(cleaned_u)
        proto = fmt.get("protocol") or ""
        s_type = (
            "hls" if "m3u8" in proto or ".m3u8" in cleaned_u
            else ("mp4" if fmt.get("ext") == "mp4" or "googlevideo.com" in cleaned_u else detect_stream_type(cleaned_u))
        )
        fmt_hdrs = dict(fmt.get("http_headers") or http_headers)
        if "googlevideo.com" in cleaned_u:
            fmt_hdrs.pop("Referer", None)
        results.append({
            "title": title,
            "url": cleaned_u,
            "type": s_type,
            "headers": fmt_hdrs,
            "thumbnail": thumbnail,
            "subtitles": subtitles,
            "hints": {
                "duration_sec": duration_sec,
                "width": fmt.get("width") or 0,
                "height": fmt.get("height") or 0,
                "bitrate_kbps": int(fmt.get("tbr") or 0),
            },
            "source": "yt-dlp",
        })
        if len(results) >= 8:
            break

    if is_yt and results:
        yt_canon = extract_youtube_watch_url(url) or url
        state.yt_stream_cache[yt_canon] = (time.time(), results[0])

    return results


# Universal JS snippet evaluated across EVERY frame in Playwright (Headless or Interactive)
# Extracts streams from JWPlayer, Video.js, Artplayer, DPlayer, Clappr, Plyr, and HTML5 <video> tags!
WVC_UNIVERSAL_FRAME_HOOK_JS = """
() => {
    const out = { sources: [], tracks: [], clicked: false };
    try {
        // 1. Auto-remove or click through ad overlays
        const overlay = document.getElementById('overlay');
        if (overlay) {
            try { overlay.remove(); out.clicked = true; } catch (e) {}
        }
        // 2. JWPlayer API Hook (catches extensionless Abyss/Hydrax sssrr.org/sora/... 1080p/720p/360p!)
        if (typeof window.jwplayer === 'function') {
            try {
                const jw = window.jwplayer();
                if (jw && typeof jw.getPlaylist === 'function') {
                    const pl = jw.getPlaylist() || [];
                    const dur = (typeof jw.getDuration === 'function' ? jw.getDuration() : 0) || 0;
                    for (const item of pl) {
                        for (const s of (item.sources || [])) {
                            if (s.file && !s.file.startsWith('blob:')) {
                                out.sources.push({
                                    url: s.file,
                                    label: s.label || '',
                                    type: s.type || '',
                                    duration: dur,
                                    player: 'JWPlayer'
                                });
                            }
                        }
                        for (const t of (item.tracks || [])) {
                            if (t.file && (!t.kind || t.kind === 'captions' || t.kind === 'subtitles')) {
                                out.tracks.push({ url: t.file, label: t.label || 'Sub JWPlayer' });
                            }
                        }
                    }
                }
            } catch (e) {}
        }
        // 3. Video.js API Hook
        if (window.videojs && window.videojs.players) {
            try {
                for (const k of Object.keys(window.videojs.players)) {
                    const vp = window.videojs.players[k];
                    if (vp && typeof vp.currentSrc === 'function') {
                        const src = vp.currentSrc();
                        if (src && !src.startsWith('blob:')) {
                            out.sources.push({
                                url: src,
                                label: '',
                                type: vp.currentType ? vp.currentType() : '',
                                duration: (vp.duration ? vp.duration() : 0) || 0,
                                player: 'Video.js'
                            });
                        }
                    }
                }
            } catch (e) {}
        }
        // 4. Generic global player instances (Artplayer, DPlayer, Plyr, Clappr)
        for (const gName of ['player', 'art', 'dp', 'ap']) {
            try {
                const p = window[gName];
                if (p && p.url && typeof p.url === 'string' && !p.url.startsWith('blob:')) {
                    out.sources.push({ url: p.url, label: '', type: '', duration: p.duration || 0, player: gName });
                }
            } catch (e) {}
        }
        // 5. HTML5 <video>, <source>, and <track> DOM Hook
        document.querySelectorAll('video').forEach(v => {
            const src = v.currentSrc || v.src;
            if (src && !src.startsWith('blob:')) {
                out.sources.push({
                    url: src,
                    label: v.videoHeight ? `${v.videoHeight}p` : '',
                    type: '',
                    duration: (v.duration && isFinite(v.duration)) ? v.duration : 0,
                    width: v.videoWidth || 0,
                    height: v.videoHeight || 0,
                    player: 'HTML5 Video'
                });
            }
            v.querySelectorAll('source').forEach(s => {
                if (s.src && !s.src.startsWith('blob:')) {
                    out.sources.push({
                        url: s.src,
                        label: s.getAttribute('label') || s.getAttribute('size') || '',
                        type: s.getAttribute('type') || '',
                        duration: (v.duration && isFinite(v.duration)) ? v.duration : 0,
                        player: 'HTML5 Source'
                    });
                }
            });
            v.querySelectorAll('track').forEach(t => {
                if (t.src) {
                    out.tracks.push({ url: t.src, label: t.label || t.srclang || 'Sub HTML5' });
                }
            });
        });
        // 6. Decrypted in-memory HLS playlist (e.g. streamfree.vip / hhpanda.st AES-GCM -> pure tiktokcdn M3U8)
        if (window.__PURE_M3U8__) {
            out.pure_m3u8 = window.__PURE_M3U8__;
        }
    } catch (e) {}
    return out;
}
"""

# Universal DOM Sibling Clustering Episode Extractor:
# Works across 100% of movie sites (PHP/SSR, WordPress, Next.js App Router, React, Vue/Nuxt)
# without any site-specific CSS selectors! Groups all <a href> elements by their parent/grandparent
# container signature and scores clusters by episode label/URL pattern ratio.
UNIVERSAL_DOM_EPISODE_EXTRACTOR_JS = """
() => {
    const currentUrl = location.href.split('#')[0];
    const allLinks = Array.from(document.querySelectorAll('a[href]'));
    const groups = new Map();

    for (const a of allLinks) {
        const href = a.href;
        if (!href || href.startsWith('javascript:') || href.endsWith('#')) continue;
        let u;
        try { u = new URL(href); } catch(e) { continue; }
        if (u.hostname !== location.hostname) continue;

        const text = (a.innerText || a.textContent || '').replace(/\\s+/g, ' ').trim();
        if (!text || text.length > 40) continue;

        const p = a.parentElement;
        const gp = p ? p.parentElement : null;
        const key = (gp ? gp.tagName + '.' + gp.className : '') + '>' + (p ? p.tagName + '.' + p.className : '');
        if (!groups.has(key)) groups.set(key, []);
        const cls = (a.className + ' ' + (p ? p.className : '')).toLowerCase();
        const isActive = (
            href === currentUrl ||
            /\\b(active|on-air|current|selected|playing)\\b/.test(cls) ||
            a.getAttribute('aria-current') === 'page'
        );
        groups.get(key).push({
            url: href,
            name: text,
            active: isActive
        });
    }

    let bestGroup = [];
    let bestScore = 0;

    for (const [key, items] of groups.entries()) {
        const seen = new Set();
        const unique = [];
        for (const it of items) {
            if (!seen.has(it.url)) {
                seen.add(it.url);
                unique.push(it);
            }
        }
        if (unique.length < 2) continue;

        let epPatternCount = 0;
        for (const it of unique) {
            if (
                /^(tập|tap|ep|episode|phần|chương|chap)?\\s*\\d+/i.test(it.name) ||
                /\\b(full|end|cuối)\\b/i.test(it.name) ||
                /(tap|ep|episode)-?\\d+/i.test(it.url)
            ) {
                epPatternCount++;
            }
        }
        const ratio = epPatternCount / unique.length;
        if (ratio < 0.5) continue;

        const keyLow = key.toLowerCase();
        const containerBonus = (keyLow.includes('ep') || keyLow.includes('list') || keyLow.includes('chap')) ? 50 : 0;
        const score = epPatternCount * 10 + containerBonus;
        if (score > bestScore) {
            bestScore = score;
            bestGroup = unique;
        }
    }

    return bestGroup.map((ep, idx) => ({
        index: idx,
        name: /^\\d+$/.test(ep.name) ? `Tập ${ep.name}` : ep.name,
        label: ep.name,
        url: ep.url,
        active: Boolean(ep.active)
    }));
}
"""

WVC_STREAMFREE_DECRYPT_INIT_JS = r"""
(() => {
    try {
        window.__REMOTE_DEBUG_ENABLED__ = true;
        window.ucapi = 1;
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'platform', { get: () => 'iPhone' });
        const origJoin = Array.prototype.join;
        Array.prototype.join = function(sep) {
            const res = origJoin.apply(this, arguments);
            if (sep === '\n' && typeof res === 'string' && res.startsWith('#EXTM3U') && !res.includes('streamfree.vip/hls/')) {
                window.__PURE_M3U8__ = origJoin.call(
                    res.split('\n').filter(l => !l.startsWith('#EXT-X-KEY:')),
                    '\n'
                );
            }
            return res;
        };
    } catch (e) {}
})();
"""


def expand_dom_episodes_with_api_json(dom_eps: List[Dict[str, Any]], api_json_blobs: List[Any]) -> List[Dict[str, Any]]:
    """
    If a SPA site (like cobephim.icu / rophim) paginates episode buttons in the DOM (e.g. showing 1..100 in tab 1)
    while fetching the full episode list via JSON API (e.g. `/api/v1/episodes/by-idMovie/13746` with all 131 episodes),
    automatically matches the DOM episode URL template (`.../tap-{id}`) against the intercepted JSON API array
    and expands the full unpaginated episode list!
    """
    if not dom_eps or not api_json_blobs:
        return dom_eps
    first_url = dom_eps[0]["url"]
    active_urls = {e["url"] for e in dom_eps if e.get("active")}
    m_id = re.search(r"^(.*?(?:tap-|ep-|-)?)(\d+)$", first_url)
    if not m_id:
        return dom_eps
    url_prefix, sample_id_str = m_id.group(1), m_id.group(2)
    for blob in api_json_blobs:
        items = (
            blob if isinstance(blob, list)
            else (blob.get("data") or blob.get("episodes") or blob.get("items") if isinstance(blob, dict) else None)
        )
        if not isinstance(items, list) or len(items) <= len(dom_eps):
            continue
        matched_item = next((x for x in items if isinstance(x, dict) and str(x.get("id")) == sample_id_str), None)
        if matched_item:
            target_server = matched_item.get("server")
            server_items = [
                x for x in items
                if isinstance(x, dict) and x.get("server") == target_server and x.get("id")
            ]
            server_items.sort(
                key=lambda x: [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(x.get("name") or ""))]
            )
            if len(server_items) > len(dom_eps):
                expanded = []
                for idx, x in enumerate(server_items):
                    raw_name = str(x.get("name") or (idx + 1)).strip()
                    ep_name = raw_name if raw_name.lower().startswith("tập") else f"Tập {raw_name}"
                    ep_url = f"{url_prefix}{x['id']}"
                    expanded.append({
                        "index": idx,
                        "name": ep_name,
                        "label": raw_name,
                        "url": ep_url,
                        "active": ep_url in active_urls or (idx == 0 and not active_urls),
                    })
                return expanded
    return dom_eps


def _parse_label_resolution(label: str) -> Tuple[int, int]:
    if not label:
        return 0, 0
    m = re.search(r"(\d{3,4})\s*[pP]", str(label))
    if m:
        h = int(m.group(1))
        w = int(round(h * 16 / 9))
        return w, h
    return 0, 0


async def _launch_playwright_chromium(p, headless: bool = True, args: Optional[List[str]] = None):
    """
    Launches Playwright Chromium if installed; otherwise automatically falls back to system
    Google Chrome (`channel="chrome"`) or Microsoft Edge (`channel="msedge"`, pre-installed on Windows 10/11)
    so the standalone `.exe` works 100% out-of-the-box without requiring `playwright install chromium`!
    """
    launch_args = args or []
    for ch in (None, "chrome", "msedge"):
        try:
            if ch is None:
                return await p.chromium.launch(headless=headless, args=launch_args)
            return await p.chromium.launch(channel=ch, headless=headless, args=launch_args)
        except Exception:
            continue
    return await p.chromium.launch(headless=headless, args=launch_args)


async def sniff_with_playwright(
    url: str,
    custom_referer: Optional[str] = None,
    page_title_hint: Optional[str] = None,
    is_embed_iframe: bool = False,
    fast_episode_mode: bool = False,
    srv_label_hint: str = "",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Universal WVC Mobile App Headless Browser Engine:
    - Automatically routes ISP-blocked / DNS-poisoned domains (like abyssplayer.com) through DoH + TLS Record Splitting!
    - Blocks preroll ad domains (333xbet, player-ads.xml, 1s_blank.mp4, popads, abyss.to homepage redirect).
    - If `is_embed_iframe=True`, mounts the target URL inside a genuine `<iframe src="...">` wrapper on a routed `https://`
      parent page so embed players (like `streamfree.vip` / `abyssplayer.com`) pass `window.self !== window.top` and
      `location.protocol === "https:"` checks!
    - Captures decrypted in-memory `#EXTM3U` playlists (`window.__PURE_M3U8__`) for AES-GCM encrypted HLS providers!
    - Captures HTTP Responses by Content-Type (`video/*`, `application/x-mpegurl`, `application/vnd.apple.mpegurl`)
      so extensionless video URLs (like `sssrr.org/sora/...`) are NEVER missed!
    - Executes `WVC_UNIVERSAL_FRAME_HOOK_JS` across every frame (`page.frames`) to harvest `jwplayer().getPlaylist()`,
      `videojs`, and `<video>` DOM elements!
    - Executes `UNIVERSAL_DOM_EPISODE_EXTRACTOR_JS` + JSON API episode expansion so SPA/React/Next.js sites
      (like cobephim.icu, rophim, etc.) yield their full multi-episode playlist universally!
    """
    from playwright.async_api import async_playwright

    found: List[Dict[str, Any]] = []
    seen = set()
    subtitles: List[Dict[str, Any]] = []
    dom_episodes: List[Dict[str, Any]] = []
    api_json_blobs: List[Any] = []
    captured_pure_m3u8 = False

    async with async_playwright() as p:
        browser = await _launch_playwright_chromium(
            p,
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ignore_https_errors=True,
        )
        await context.add_init_script(WVC_STREAMFREE_DECRYPT_INIT_JS)
        page = await context.new_page()

        def add_candidate(raw_u: str, ref: str, ua: str, source_tag: str, extra_hints: Optional[Dict[str, Any]] = None, srv_label: str = ""):
            if not raw_u or raw_u.startswith("blob:") or raw_u.startswith("data:"):
                return
            if "streamfree.vip/hls/" in raw_u.lower():
                return
            cleaned, hints = unwrap_and_clean_stream_url(raw_u)
            if not cleaned:
                return
            if hints.get("is_subtitle"):
                if not any(x["url"] == cleaned for x in subtitles):
                    subtitles.append({
                        "label": f"Sub ({cleaned.split('/')[-1].split('?')[0]})",
                        "url": cleaned,
                        "referer": ref or url,
                    })
                return
            if extra_hints:
                for k, v in extra_hints.items():
                    if v and not hints.get(k):
                        hints[k] = v
            if cleaned not in seen:
                seen.add(cleaned)
                s_type = detect_stream_type(cleaned)
                if s_type == "auto" and extra_hints and extra_hints.get("force_type"):
                    s_type = extra_hints["force_type"]
                eff_srv = srv_label or srv_label_hint
                title_str = page_title_hint or url
                if eff_srv:
                    title_str = f"{title_str} [{eff_srv}]"
                found.append({
                    "title": title_str,
                    "server_name": eff_srv or source_tag,
                    "url": cleaned,
                    "type": s_type,
                    "headers": {
                        "Referer": ref or custom_referer or url,
                        "User-Agent": ua or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    },
                    "subtitles": subtitles,
                    "hints": hints,
                    "source": source_tag,
                })

        wrapper_origin_url = ""
        if is_embed_iframe:
            ref_parsed = urllib.parse.urlparse(custom_referer or url)
            wrapper_origin_url = f"https://{ref_parsed.netloc or 'hhpanda.st'}/__wvc_embed__"

        async def wrapper_only_route(route):
            if page.is_closed():
                return
            try:
                await route.fulfill(
                    status=200,
                    content_type="text/html; charset=utf-8",
                    body=(
                        f'<!DOCTYPE html><html><body style="margin:0;background:#000;">'
                        f'<iframe id="sf" src="{url}" width="800" height="600" allow="autoplay; fullscreen"></iframe>'
                        f'</body></html>'
                    ),
                )
            except Exception:
                pass

        async def universal_route_interceptor(route):
            if page.is_closed():
                return
            try:
                req = route.request
                r_url = req.url
                low_u = r_url.lower()
                if wrapper_origin_url and r_url == wrapper_origin_url:
                    await wrapper_only_route(route)
                    return
                # 1. Block gambling ads, VAST prerolls, popup scripts & embed anti-direct redirects (`abyss.to`)
                if any(bad in low_u for bad in ("333xbet", "video888.mp4", "player-ads.xml", "1s_blank.mp4", "decafeligiblyhad.com", "fuckadblock", "popads", "abyss.to", "darkbytes.xyz")):
                    await route.abort()
                    return
                # 2. Auto-rewrite /tvc?id= ad wrapper to /play?id=
                if "/tvc?id=" in r_url:
                    new_u = r_url.replace("/tvc?id=", "/play?id=")
                    await route.continue_(url=new_u)
                    return
                # 3. Check if the domain is DNS-poisoned (127.0.0.1) or ISP-blocked (e.g. abyssplayer.com)
                parsed = urllib.parse.urlparse(r_url)
                host = parsed.hostname or ""
                is_poisoned = False
                if host and req.resource_type in ("document", "iframe", "script", "stylesheet", "xhr", "fetch", "other"):
                    try:
                        ip = socket.gethostbyname(host)
                        if ip.startswith("127.") or ip == "0.0.0.0":
                            is_poisoned = True
                    except Exception:
                        is_poisoned = True
                if is_poisoned:
                    try:
                        st, hdrs, body = await asyncio.to_thread(fetch_dpi_tls_split_sync, r_url, dict(req.headers), 8.0)
                        if page.is_closed():
                            return
                        clean_hdrs = {
                            k: v for k, v in hdrs.items()
                            if k.lower() not in ("transfer-encoding", "content-length", "content-encoding", "connection")
                        }
                        await route.fulfill(status=st, headers=clean_hdrs, body=body)
                        return
                    except Exception:
                        if page.is_closed():
                            return
                if not page.is_closed():
                    await route.continue_()
            except Exception:
                pass

        # IMPORTANT: ServiceWorker-based decryptors (like streamfree.vip) require ServiceWorkers to not be
        # intercepted by wildcard `page.route("**/*")`, so we only route `wrapper_origin_url` for them!
        if is_embed_iframe and "streamfree" in url.lower():
            await page.route(wrapper_origin_url, wrapper_only_route)
        else:
            await page.route("**/*", universal_route_interceptor)

        def handle_request(req):
            r_url = req.url
            cleaned, hints = unwrap_and_clean_stream_url(r_url)
            if not cleaned:
                return
            if hints.get("is_subtitle") or any(ext in cleaned.lower() for ext in [".m3u8", ".mp4", ".mpd", ".mkv", "/sora/"]):
                add_candidate(
                    r_url,
                    req.headers.get("referer", custom_referer or url),
                    req.headers.get("user-agent", ""),
                    "WVC Network Sniffer",
                )

        async def _capture_json_resp(resp):
            try:
                data = await resp.json()
                api_json_blobs.append(data)
            except Exception:
                pass

        def handle_response(resp):
            try:
                r_url = resp.url
                ctype = (resp.headers.get("content-type") or "").lower()
                if "json" in ctype and not is_embed_iframe and resp.status == 200:
                    asyncio.create_task(_capture_json_resp(resp))
                req = resp.request
                is_video_mime = (
                    ctype.startswith("video/")
                    or "mpegurl" in ctype
                    or "dash+xml" in ctype
                    or req.resource_type == "media"
                )
                if is_video_mime and resp.status in (200, 206):
                    clen = int(resp.headers.get("content-length") or 0)
                    if "mpegurl" not in ctype and clen and clen < 50000 and "bytes" not in (resp.headers.get("content-range") or ""):
                        return
                    force_t = "hls" if "mpegurl" in ctype else ("mp4" if "mp4" in ctype else "auto")
                    add_candidate(
                        r_url,
                        req.headers.get("referer", custom_referer or url),
                        req.headers.get("user-agent", ""),
                        "WVC MIME Sniffer",
                        extra_hints={"force_type": force_t},
                    )
            except Exception:
                pass

        page.on("request", handle_request)
        page.on("response", handle_response)

        try:
            if is_embed_iframe:
                await page.goto(wrapper_origin_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(4200)
            else:
                await page.goto(url, referer=custom_referer or url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(2500)
                try:
                    raw_dom_eps = await page.evaluate(UNIVERSAL_DOM_EPISODE_EXTRACTOR_JS)
                    if isinstance(raw_dom_eps, list) and raw_dom_eps:
                        dom_episodes = expand_dom_episodes_with_api_json(raw_dom_eps, api_json_blobs)
                except Exception:
                    pass

            if not fast_episode_mode:
                # Harvest from all frames (Attempt 1 + click play/overlay if needed)
                for _ in range(3):
                    for frame in page.frames:
                        try:
                            f_url = frame.url if (frame.url and frame.url.startswith("http")) else url
                            res = await frame.evaluate(WVC_UNIVERSAL_FRAME_HOOK_JS)
                            if isinstance(res, dict):
                                pure_m3u8 = res.get("pure_m3u8")
                                if pure_m3u8 and isinstance(pure_m3u8, str) and "#EXTINF" in pure_m3u8 and not captured_pure_m3u8:
                                    captured_pure_m3u8 = True
                                    vid_id = uuid.uuid4().hex[:10]
                                    state.virtual_m3u8_store[vid_id] = {
                                        "m3u8_text": pure_m3u8,
                                        "base_url": f_url,
                                        "headers": {
                                            "Referer": f_url,
                                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                                        },
                                    }
                                    virt_url = f"http://{get_local_ip()}:{PORT}/proxy/virtual/{vid_id}.m3u8"
                                    add_candidate(
                                        virt_url,
                                        f_url,
                                        "",
                                        "WVC AES-GCM Decryptor",
                                        extra_hints={"force_type": "hls"},
                                        srv_label=srv_label_hint or "Decrypted HLS",
                                    )
                                for t in res.get("tracks") or []:
                                    if t.get("url"):
                                        abs_t = urllib.parse.urljoin(f_url, t["url"])
                                        if not any(x["url"] == abs_t for x in subtitles):
                                            subtitles.append({
                                                "label": t.get("label") or "Sub Player",
                                                "url": abs_t,
                                                "referer": f_url,
                                            })
                                for s in res.get("sources") or []:
                                    s_url = s.get("url")
                                    if s_url:
                                        abs_s = urllib.parse.urljoin(f_url, s_url)
                                        lbl = s.get("label") or ""
                                        w, h = _parse_label_resolution(lbl)
                                        if not w and s.get("width"):
                                            w, h = int(s["width"]), int(s.get("height") or 0)
                                        f_type = "mp4" if "mp4" in str(s.get("type", "")).lower() or "/sora/" in abs_s else detect_stream_type(abs_s)
                                        add_candidate(
                                            abs_s,
                                            f_url,
                                            "",
                                            f"WVC {s.get('player', 'Player')} Hook",
                                            extra_hints={
                                                "width": w,
                                                "height": h,
                                                "duration_sec": float(s.get("duration") or 0),
                                                "force_type": f_type,
                                            },
                                            srv_label=lbl or srv_label_hint,
                                        )
                        except Exception:
                            pass
                    if found:
                        break
                    try:
                        await page.mouse.click(400, 300)
                    except Exception:
                        pass
                    await page.wait_for_timeout(1800)

            page_title = page_title_hint or await page.title()
            if page_title and "abyss" not in page_title.lower():
                for item in found:
                    if item["title"].startswith("http"):
                        item["title"] = page_title
        except Exception as e:
            print(f"[PLAYWRIGHT WARN] {url}: {e}", flush=True)
        finally:
            try:
                await page.unroute_all(behavior="ignoreErrors")
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass

    return found, subtitles, dom_episodes


@app.post("/api/browser/open")
async def open_interactive_wvc_browser(payload: Dict[str, Any]):
    """
    Opens a visible, interactive WVC Chromium Browser Window on the user's PC (just like the WVC Mobile App browser!):
    - Blocks gambling ads, VAST prerolls, and popups (`window.open` locked)
    - Automatically bypasses ISP DNS/DPI blocks (e.g. abyssplayer.com)
    - Continuously runs the 4-Layer WVC Hook across all frames and HTTP responses
    - Whenever a video plays or loads in the browser, it immediately beams the stream (with 1080p/720p/360p & duration)
      to the PC Studio Dashboard in real time!
    """
    target_url = (payload.get("url") or "https://google.com").strip()
    if not target_url.startswith("http"):
        target_url = "https://" + target_url

    async def _run_interactive_browser(start_url: str):
        from playwright.async_api import async_playwright
        seen_interactive = set()
        try:
            async with async_playwright() as p:
                browser = await _launch_playwright_chromium(
                    p,
                    headless=False,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--start-maximized",
                        "--new-window",
                    ],
                )
                context = await browser.new_context(
                    viewport=None,
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    ignore_https_errors=True,
                )
                await context.add_init_script("window.open = function() { return null; };")
                await context.add_init_script(WVC_STREAMFREE_DECRYPT_INIT_JS)
                page = await context.new_page()
                # Block popup tabs from gambling ad scripts AFTER main `page` is created!
                context.on("page", lambda new_p: asyncio.create_task(new_p.close()) if new_p != page else None)
                try:
                    await page.bring_to_front()
                except Exception:
                    pass

                async def register_interactive_stream(raw_u: str, ref: str, title_str: str, hints: Dict[str, Any]):
                    cleaned, u_hints = unwrap_and_clean_stream_url(raw_u)
                    if not cleaned:
                        return
                    if u_hints.get("is_subtitle"):
                        await register_sniffed_subtitle({
                            "label": f"Sub ({cleaned.split('/')[-1].split('?')[0]})",
                            "url": cleaned,
                            "referer": ref,
                        })
                        return
                    if cleaned in seen_interactive:
                        return
                    seen_interactive.add(cleaned)
                    merged_hints = {**u_hints, **hints}
                    headers = {"Referer": ref, "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0 Safari/537.36"}
                    meta = await probe_stream_metadata(cleaned, headers, merged_hints)
                    # Filter out ultra-short preroll clips (< 5s)
                    if 0 < meta.get("duration_sec", 0) < 5:
                        return
                    entry = {
                        "id": uuid.uuid4().hex[:8],
                        "title": title_str or "WVC Interactive Stream",
                        "url": cleaned,
                        "type": "mp4" if "/sora/" in cleaned else detect_stream_type(cleaned),
                        "headers": headers,
                        "quality": meta.get("quality_badge") or "STREAM",
                        "duration_sec": meta.get("duration_sec", 0),
                        "duration_str": meta.get("duration_str", ""),
                        "width": meta.get("width", 0),
                        "height": meta.get("height", 0),
                        "resolution_str": meta.get("resolution_str", ""),
                        "bitrate_kbps": meta.get("bitrate_kbps", 0),
                        "is_main_movie": meta.get("is_main_movie", False),
                        "timestamp": int(time.time()),
                    }
                    state.sniffed_history.insert(0, entry)
                    state.sniffed_history = state.sniffed_history[:40]
                    await state.broadcast_to_pcs({"action": "sniffed_stream", "stream": entry})

                async def interactive_route(route):
                    if page.is_closed():
                        return
                    try:
                        req = route.request
                        r_url = req.url
                        low_u = r_url.lower()
                        if any(bad in low_u for bad in ("333xbet", "video888.mp4", "player-ads.xml", "1s_blank.mp4", "decafeligiblyhad.com", "fuckadblock", "popads", "darkbytes.xyz")):
                            await route.abort()
                            return
                        if "/tvc?id=" in r_url:
                            await route.continue_(url=r_url.replace("/tvc?id=", "/play?id="))
                            return
                        parsed = urllib.parse.urlparse(r_url)
                        host = parsed.hostname or ""
                        is_poisoned = False
                        if host and req.resource_type in ("document", "iframe", "script", "stylesheet", "xhr", "fetch", "other"):
                            try:
                                ip = socket.gethostbyname(host)
                                if ip.startswith("127.") or ip == "0.0.0.0":
                                    is_poisoned = True
                            except Exception:
                                is_poisoned = True
                        if is_poisoned:
                            try:
                                st, hdrs, body = await asyncio.to_thread(fetch_dpi_tls_split_sync, r_url, dict(req.headers), 8.0)
                                if page.is_closed():
                                    return
                                clean_hdrs = {
                                    k: v for k, v in hdrs.items()
                                    if k.lower() not in ("transfer-encoding", "content-length", "content-encoding", "connection")
                                }
                                await route.fulfill(status=st, headers=clean_hdrs, body=body)
                                return
                            except Exception:
                                if page.is_closed():
                                    return
                        if not page.is_closed():
                            await route.continue_()
                    except Exception:
                        pass

                await page.route("**/*", interactive_route)

                interactive_json_blobs: List[Any] = []

                async def _cap_interactive_json(resp):
                    try:
                        data = await resp.json()
                        interactive_json_blobs.append(data)
                        if len(interactive_json_blobs) > 30:
                            interactive_json_blobs.pop(0)
                    except Exception:
                        pass

                def on_resp(resp):
                    try:
                        r_url = resp.url
                        ctype = (resp.headers.get("content-type") or "").lower()
                        if "json" in ctype and resp.status == 200:
                            asyncio.create_task(_cap_interactive_json(resp))
                        req = resp.request
                        if (ctype.startswith("video/") or "mpegurl" in ctype or "dash+xml" in ctype or req.resource_type == "media") and resp.status in (200, 206):
                            asyncio.create_task(
                                register_interactive_stream(r_url, req.headers.get("referer", page.url), page.url, {})
                            )
                    except Exception:
                        pass

                page.on("response", on_resp)
                await page.goto(start_url, wait_until="domcontentloaded", timeout=30000)
                last_ep_page_url = ""

                # Poll frames every 2.5s until the user closes the browser window
                while not page.is_closed():
                    try:
                        p_title = await page.title()
                        cur_p_url = page.url
                        yt_w = extract_youtube_watch_url(cur_p_url)
                        if yt_w and yt_w not in seen_interactive:
                            seen_interactive.add(yt_w)
                            asyncio.create_task(
                                receive_sniffed_stream_from_extension({
                                    "url": yt_w,
                                    "page_url": yt_w,
                                    "referer": yt_w,
                                    "title": p_title,
                                })
                            )
                        if cur_p_url and cur_p_url != last_ep_page_url and "youtube.com" not in cur_p_url.lower():
                            try:
                                raw_eps = await page.evaluate(UNIVERSAL_DOM_EPISODE_EXTRACTOR_JS)
                                if isinstance(raw_eps, list) and len(raw_eps) >= 2:
                                    last_ep_page_url = cur_p_url
                                    expanded_eps = expand_dom_episodes_with_api_json(raw_eps, interactive_json_blobs)
                                    state.episodes = expanded_eps
                                    state.current_episode_index = next((i for i, e in enumerate(expanded_eps) if e.get("active")), 0)
                                    await _broadcast_playlist_state()
                            except Exception:
                                pass
                        for frame in page.frames:
                            try:
                                f_url = frame.url or page.url
                                res = await frame.evaluate(WVC_UNIVERSAL_FRAME_HOOK_JS)
                                if isinstance(res, dict):
                                    pure_m3u8 = res.get("pure_m3u8")
                                    if pure_m3u8 and isinstance(pure_m3u8, str) and "#EXTINF" in pure_m3u8:
                                        m_sig = f"pure:{hash(pure_m3u8[:200])}"
                                        if m_sig not in seen_interactive:
                                            seen_interactive.add(m_sig)
                                            vid_id = uuid.uuid4().hex[:10]
                                            state.virtual_m3u8_store[vid_id] = {
                                                "m3u8_text": pure_m3u8,
                                                "base_url": f_url,
                                                "headers": {
                                                    "Referer": f_url,
                                                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                                                },
                                            }
                                            virt_url = f"http://{get_local_ip()}:{PORT}/proxy/virtual/{vid_id}.m3u8"
                                            await register_interactive_stream(virt_url, f_url, f"{p_title} [Decrypted HLS]", {})
                                    for t in res.get("tracks") or []:
                                        if t.get("url"):
                                            await register_sniffed_subtitle({
                                                "label": t.get("label") or "Sub Player",
                                                "url": urllib.parse.urljoin(f_url, t["url"]),
                                                "referer": f_url,
                                            })
                                    for s in res.get("sources") or []:
                                        if s.get("url"):
                                            abs_s = urllib.parse.urljoin(f_url, s["url"])
                                            lbl = s.get("label") or ""
                                            w, h = _parse_label_resolution(lbl)
                                            await register_interactive_stream(
                                                abs_s,
                                                f_url,
                                                f"{p_title} [{lbl}]" if lbl else p_title,
                                                {"width": w, "height": h, "duration_sec": float(s.get("duration") or 0)},
                                            )
                            except Exception:
                                pass
                    except Exception:
                        break
                    await asyncio.sleep(2.5)
        except Exception as e:
            print(f"[INTERACTIVE BROWSER] Closed or error: {e}", flush=True)

    asyncio.create_task(_run_interactive_browser(target_url))
    return {"status": "opened", "url": target_url}


async def enrich_and_expand_streams(raw_streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Probes all candidate streams in parallel for exact Duration (HH:MM:SS), Resolution (WxH),
    Bitrate (kbps), Codecs, and expands Master M3U8 variants so the user sees both Master + specific resolutions!
    Sorts main full-length movies (>= 5 min) to the top and demotes short ads/clips!
    """
    if not raw_streams:
        return []

    probe_tasks = [
        probe_stream_metadata(st["url"], st.get("headers"), st.get("hints"))
        for st in raw_streams
    ]
    probed_list = await asyncio.gather(*probe_tasks, return_exceptions=True)

    enriched: List[Dict[str, Any]] = []
    seen_urls = set()
    seen_sora_paths = set()

    for st, meta in zip(raw_streams, probed_list):
        if isinstance(meta, Exception) or not isinstance(meta, dict):
            meta = {
                "duration_sec": 0,
                "duration_str": "",
                "width": 0,
                "height": 0,
                "resolution_str": "",
                "bitrate_kbps": 0,
                "quality_badge": st.get("type", "STREAM").upper(),
                "variants": [],
                "subtitles": [],
                "is_main_movie": False,
            }

        merged_subs = list(st.get("subtitles") or [])
        for ms in meta.get("subtitles") or []:
            if not any(x["url"] == ms["url"] for x in merged_subs):
                merged_subs.append(ms)

        item = {
            **st,
            "duration_sec": meta.get("duration_sec", 0),
            "duration_str": meta.get("duration_str", ""),
            "width": meta.get("width", 0),
            "height": meta.get("height", 0),
            "resolution_str": meta.get("resolution_str", ""),
            "bitrate_kbps": meta.get("bitrate_kbps", 0),
            "vcodec": meta.get("vcodec", ""),
            "acodec": meta.get("acodec", ""),
            "segment_count": meta.get("segment_count", 0),
            "variant_type": meta.get("variant_type", ""),
            "quality": meta.get("quality_badge") or st.get("quality") or "STREAM",
            "is_main_movie": meta.get("is_main_movie", False),
            "subtitles": merged_subs,
        }
        u_path = urllib.parse.urlparse(item["url"]).path
        if "/sora/" in u_path:
            if u_path in seen_sora_paths:
                continue
            seen_sora_paths.add(u_path)
        if item["url"] not in seen_urls:
            seen_urls.add(item["url"])
            enriched.append(item)

        # Also add individual resolution variants from Master M3U8 if they aren't already listed
        for var in meta.get("variants") or []:
            v_url = var.get("url")
            if v_url and v_url not in seen_urls:
                seen_urls.add(v_url)
                v_res = var.get("resolution_str") or format_resolution_badge(var.get("width", 0), var.get("height", 0))
                v_bw = var.get("bitrate_kbps", 0)
                v_parts = [p for p in [v_res or "HLS Variant", f"⏱️ {item['duration_str']}" if item["duration_str"] else "", f"{v_bw} kbps" if v_bw else ""] if p]
                enriched.append({
                    **item,
                    "url": v_url,
                    "variant_type": "Direct Variant",
                    "width": var.get("width") or item["width"],
                    "height": var.get("height") or item["height"],
                    "resolution_str": v_res or item["resolution_str"],
                    "bitrate_kbps": v_bw or item["bitrate_kbps"],
                    "quality": " · ".join(v_parts),
                })

    # Sort: Main full-length movie first, then highest resolution, then duration
    enriched.sort(
        key=lambda x: (
            1 if x.get("is_main_movie") else 0,
            x.get("width") or 0,
            x.get("duration_sec") or 0,
            x.get("bitrate_kbps") or 0,
        ),
        reverse=True,
    )
    return enriched


@app.post("/api/extract")
async def extract_video_streams(payload: Dict[str, Any]):
    raw_input_url = (payload.get("url") or "").strip()
    deep_browser = bool(payload.get("deep_browser", True))
    custom_referer = (payload.get("referer") or "").strip()
    update_playlist = bool(payload.get("update_playlist", True))

    if not raw_input_url:
        raise HTTPException(status_code=400, detail="Vui lòng nhập URL video hoặc trang web!")

    url, hints = unwrap_and_clean_stream_url(raw_input_url)
    if not url:
        url = raw_input_url

    results: List[Dict[str, Any]] = []
    all_subtitles: List[Dict[str, Any]] = []
    js_iframes: List[Tuple[str, str, str]] = []
    episodes: List[Dict[str, Any]] = []
    page_title = url

    low = url.lower().split("?")[0]
    if any(low.endswith(ext) for ext in [".m3u8", ".mp4", ".webm", ".mkv", ".mpd"]) or "/sora/" in low:
        headers = {"Referer": custom_referer or url}
        raw_list = [{
            "title": urllib.parse.unquote(low.split("/")[-1]) or "Direct Video Stream",
            "url": url,
            "type": "mp4" if "/sora/" in low else detect_stream_type(url),
            "headers": headers,
            "subtitles": [],
            "hints": hints,
            "source": "Direct Link",
        }]
        enriched = await enrich_and_expand_streams(raw_list)
        return {"streams": enriched, "subtitles": [], "episodes": []}

    is_yt_url = ("youtube.com" in url.lower() or "youtu.be" in url.lower())
    if is_yt_url:
        try:
            done_ytdlp = await sniff_with_ytdlp(url)
        except Exception as e:
            done_ytdlp = e
        done_html = None
    else:
        html_task = asyncio.create_task(sniff_with_html_and_nextjs(url))
        ytdlp_task = asyncio.create_task(sniff_with_ytdlp(url))
        done_html, done_ytdlp = await asyncio.gather(html_task, ytdlp_task, return_exceptions=True)

    if isinstance(done_html, tuple) and len(done_html) == 5:
        h_streams, h_subs, h_js_iframes, page_title, episodes = done_html
        results.extend(h_streams)
        all_subtitles.extend(h_subs)
        js_iframes.extend(h_js_iframes)

    if isinstance(done_ytdlp, list):
        existing_urls = {r["url"] for r in results}
        for item in done_ytdlp:
            if item["url"] not in existing_urls:
                results.append(item)

    # If HTML unrolling discovered encrypted JS player iframes (like abyssplayer.com/Poh-lIVIIo or streamfree.vip)
    # OR if no streams were found yet, run our Universal 4-Layer WVC Playwright Hook!
    if js_iframes:
        pw_tasks = [
            sniff_with_playwright(
                ifr_url,
                custom_referer=ifr_ref,
                page_title_hint=page_title,
                is_embed_iframe=True,
                srv_label_hint=ifr_label,
            )
            for ifr_url, ifr_ref, ifr_label in js_iframes[:3]
        ]
        pw_outcomes = await asyncio.gather(*pw_tasks, return_exceptions=True)
        existing_urls = {r["url"] for r in results}
        for out in pw_outcomes:
            if isinstance(out, tuple) and len(out) >= 2:
                pw_streams, pw_subs = out[0], out[1]
                for item in pw_streams:
                    if item["url"] not in existing_urls:
                        existing_urls.add(item["url"])
                        results.append(item)
                for s in pw_subs:
                    if not any(x["url"] == s["url"] for x in all_subtitles):
                        all_subtitles.append(s)

    # Universal SPA / React / Next.js Episode & Stream Hydration Fallback:
    # If static HTML returned 0 streams OR 0 episodes on a movie page (like cobephim.icu / rophim where
    # <div id="episodes-list"> is hydrated client-side via API), run Playwright Universal DOM Sibling Clustering!
    is_yt_url = ("youtube.com" in url.lower() or "youtu.be" in url.lower())
    need_pw_streams = (not results and deep_browser and not js_iframes and not is_yt_url)
    need_pw_episodes = (update_playlist and not episodes and not is_yt_url)
    if need_pw_streams or need_pw_episodes:
        try:
            pw_streams, pw_subs, pw_eps = await sniff_with_playwright(
                url,
                page_title_hint=page_title,
                fast_episode_mode=bool(results) and not need_pw_streams,
            )
            if pw_eps and not episodes:
                episodes = pw_eps
            existing_urls = {r["url"] for r in results}
            for item in pw_streams:
                if item["url"] not in existing_urls:
                    existing_urls.add(item["url"])
                    results.append(item)
            for s in pw_subs:
                if not any(x["url"] == s["url"] for x in all_subtitles):
                    all_subtitles.append(s)
        except Exception:
            pass

    enriched = await enrich_and_expand_streams(results)
    for st in enriched:
        for s in st.get("subtitles") or []:
            if not any(x["url"] == s["url"] for x in all_subtitles):
                all_subtitles.append(s)

    # Record discovered subtitles into global state as well
    for s in all_subtitles:
        await register_sniffed_subtitle(s)

    # Update global episode playlist if this extraction is for the active session
    if update_playlist and episodes:
        state.episodes = episodes
        active_idx = next((i for i, e in enumerate(episodes) if e.get("active")), 0)
        state.current_episode_index = active_idx
        if enriched:
            state.episodes[active_idx]["cached_streams"] = enriched
        state.save_playlists_to_disk()
        await state.broadcast_to_pcs({
            "action": "episodes_updated",
            "episodes": state.episodes,
            "current_episode_index": state.current_episode_index,
            "auto_next_enabled": state.auto_next_enabled,
            "loop_mode": state.loop_mode,
            "skip_intro_sec": state.skip_intro_sec,
            "skip_outro_sec": state.skip_outro_sec,
            "sponsorblock_enabled": state.sponsorblock_enabled,
            "saved_playlists": list(state.saved_playlists.keys()),
        })

    return {
        "streams": enriched,
        "subtitles": all_subtitles,
        "episodes": state.episodes if update_playlist else episodes,
        "current_episode_index": state.current_episode_index,
        "auto_next_enabled": state.auto_next_enabled,
        "loop_mode": state.loop_mode,
        "skip_intro_sec": state.skip_intro_sec,
        "skip_outro_sec": state.skip_outro_sec,
        "sponsorblock_enabled": state.sponsorblock_enabled,
        "saved_playlists": list(state.saved_playlists.keys()),
    }


# ============================================================================
# 8. CASTING, SUBTITLE BEAMING, MULTI-EPISODE AUTO-NEXT, YOUTUBE & CUSTOM PLAYLIST API
# ============================================================================

async def _broadcast_playlist_state():
    for idx, ep in enumerate(state.episodes):
        ep["index"] = idx
        ep["active"] = (idx == state.current_episode_index)
    state.save_playlists_to_disk()
    await state.broadcast_to_pcs({
        "action": "episodes_updated",
        "episodes": state.episodes,
        "current_episode_index": state.current_episode_index,
        "auto_next_enabled": state.auto_next_enabled,
        "loop_mode": state.loop_mode,
        "skip_intro_sec": state.skip_intro_sec,
        "skip_outro_sec": state.skip_outro_sec,
        "sponsorblock_enabled": state.sponsorblock_enabled,
        "saved_playlists": list(state.saved_playlists.keys()),
    })


@app.post("/api/youtube/search")
async def api_youtube_search(payload: Dict[str, Any]):
    """
    Dedicated YouTube Search & Playlist Parser:
    - Accepts a keyword query (e.g. `"nhạc lofi chill"`, `"phim kiếm hiệp tập 1"`) OR a YouTube Video/Playlist URL!
    - Uses `yt_dlp` with `extract_flat=True` for fast ~1.5s search results with thumbnails, duration, and channel names!
    """
    import yt_dlp

    query = (payload.get("query") or "").strip()
    max_results = int(payload.get("max_results") or 12)
    if not query:
        raise HTTPException(status_code=400, detail="Vui lòng nhập từ khóa tìm kiếm hoặc link YouTube!")

    def _run_yt_search():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
            "socket_timeout": 10,
        }
        target = query if query.startswith("http://") or query.startswith("https://") else f"ytsearch{max_results}:{query}"
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(target, download=False)

    try:
        info = await asyncio.to_thread(_run_yt_search)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi tìm kiếm YouTube: {e}")

    entries = info.get("entries") if isinstance(info, dict) and "entries" in info else ([info] if isinstance(info, dict) else [])
    results = []
    for item in entries or []:
        if not isinstance(item, dict):
            continue
        vid_id = item.get("id") or ""
        raw_url = item.get("url") or item.get("webpage_url") or ""
        if vid_id and not raw_url.startswith("http"):
            raw_url = f"https://www.youtube.com/watch?v={vid_id}"
        if not raw_url:
            continue
        title = item.get("title") or "YouTube Video"
        if title in ("[Deleted video]", "[Private video]"):
            continue
        dur_sec = float(item.get("duration") or 0)
        thumbs = item.get("thumbnails") or []
        thumb_url = (
            item.get("thumbnail")
            or (thumbs[-1].get("url") if thumbs and isinstance(thumbs[-1], dict) else "")
            or (f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg" if vid_id else "")
        )
        results.append({
            "id": vid_id or uuid.uuid4().hex[:8],
            "title": title,
            "url": raw_url,
            "channel": item.get("channel") or item.get("uploader") or "YouTube",
            "duration_sec": dur_sec,
            "duration_str": format_duration_hms(dur_sec) if dur_sec > 0 else "LIVE / Video",
            "thumbnail": thumb_url,
            "view_count": item.get("view_count") or 0,
        })

    state.last_yt_query = query
    state.last_yt_results = results
    state.save_playlists_to_disk()
    await state.broadcast_to_pcs({
        "action": "yt_search_updated",
        "query": query,
        "results": results,
    })

    return {"status": "ok", "results": results, "query": query}


@app.post("/api/playlist/add")
async def api_playlist_add(payload: Dict[str, Any]):
    """
    Adds one or more videos/songs/episodes to the active playlist (`state.episodes`).
    Supports `play_now=True` (casts the newly added item immediately) and `replace=True` (replaces queue).
    """
    items = payload.get("items") or []
    if not items and payload.get("url"):
        items = [payload]
    if not items:
        raise HTTPException(status_code=400, detail="Không có video/bài hát nào để thêm vào danh sách!")

    replace = bool(payload.get("replace", False))
    play_now = bool(payload.get("play_now", False))

    if replace:
        state.episodes = []
        state.current_episode_index = -1

    first_added_idx = len(state.episodes)
    for it in items:
        u = (it.get("url") or "").strip()
        if not u:
            continue
        name = (it.get("name") or it.get("title") or u).strip()
        state.episodes.append({
            "index": len(state.episodes),
            "name": name,
            "label": name,
            "url": u,
            "duration_str": it.get("duration_str") or "",
            "channel": it.get("channel") or "",
            "thumbnail": it.get("thumbnail") or "",
            "active": False,
        })

    if state.current_episode_index < 0 and state.episodes:
        state.current_episode_index = 0

    await _broadcast_playlist_state()

    # Pre-fetch first added item in background if not playing immediately
    if play_now and 0 <= first_added_idx < len(state.episodes):
        state.last_auto_next_ts = time.time()
        await play_episode_by_index(first_added_idx)
    elif 0 <= first_added_idx < len(state.episodes):
        asyncio.create_task(_prefetch_episode_in_background(first_added_idx))

    return {
        "status": "ok",
        "episodes": state.episodes,
        "current_episode_index": state.current_episode_index,
        "loop_mode": state.loop_mode,
    }


@app.post("/api/playlist/remove")
async def api_playlist_remove(payload: Dict[str, Any]):
    idx = int(payload.get("index", -1))
    if 0 <= idx < len(state.episodes):
        state.episodes.pop(idx)
        if not state.episodes:
            state.current_episode_index = -1
        elif idx < state.current_episode_index:
            state.current_episode_index -= 1
        elif idx == state.current_episode_index and state.current_episode_index >= len(state.episodes):
            state.current_episode_index = len(state.episodes) - 1
        await _broadcast_playlist_state()
    return {"status": "ok", "episodes": state.episodes, "current_episode_index": state.current_episode_index}


@app.post("/api/playlist/move")
async def api_playlist_move(payload: Dict[str, Any]):
    idx = int(payload.get("index", -1))
    delta = int(payload.get("delta", 0))
    new_idx = idx + delta
    if 0 <= idx < len(state.episodes) and 0 <= new_idx < len(state.episodes):
        state.episodes[idx], state.episodes[new_idx] = state.episodes[new_idx], state.episodes[idx]
        if state.current_episode_index == idx:
            state.current_episode_index = new_idx
        elif state.current_episode_index == new_idx:
            state.current_episode_index = idx
        await _broadcast_playlist_state()
    return {"status": "ok", "episodes": state.episodes, "current_episode_index": state.current_episode_index}


@app.post("/api/playlist/clear")
async def api_playlist_clear():
    state.episodes = []
    state.current_episode_index = -1
    await _broadcast_playlist_state()
    return {"status": "ok"}


@app.post("/api/playlist/loop")
async def api_playlist_loop(payload: Dict[str, Any]):
    mode = (payload.get("loop_mode") or "all").lower()
    if mode not in ("off", "all", "one"):
        mode = "all"
    state.loop_mode = mode
    await _broadcast_playlist_state()
    return {"status": "ok", "loop_mode": state.loop_mode}


@app.post("/api/playlist/save")
async def api_playlist_save(payload: Dict[str, Any]):
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Vui lòng nhập tên danh sách phát để lưu!")
    if not state.episodes:
        raise HTTPException(status_code=400, detail="Danh sách phát hiện tại đang trống!")
    state.saved_playlists[name] = [
        {k: v for k, v in ep.items() if k != "cached_streams"}
        for ep in state.episodes
    ]
    await _broadcast_playlist_state()
    return {"status": "ok", "saved_playlists": list(state.saved_playlists.keys())}


@app.post("/api/playlist/load")
async def api_playlist_load(payload: Dict[str, Any]):
    name = (payload.get("name") or "").strip()
    play_now = bool(payload.get("play_now", False))
    if name not in state.saved_playlists:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy danh sách '{name}'!")
    state.episodes = [dict(ep) for ep in state.saved_playlists[name]]
    state.current_episode_index = 0 if state.episodes else -1
    await _broadcast_playlist_state()
    if play_now and state.episodes:
        state.last_auto_next_ts = time.time()
        await play_episode_by_index(0)
    return {"status": "ok", "episodes": state.episodes}


@app.post("/api/playlist/delete-saved")
async def api_playlist_delete_saved(payload: Dict[str, Any]):
    name = (payload.get("name") or "").strip()
    state.saved_playlists.pop(name, None)
    await _broadcast_playlist_state()
    return {"status": "ok", "saved_playlists": list(state.saved_playlists.keys())}


async def _prefetch_episode_in_background(ep_index: int):
    """
    Pre-extracts the next episode's video streams in the background while the current episode plays!
    When the current episode ends (or user presses Next), the next episode casts in 0.1s flat!
    """
    if not (0 <= ep_index < len(state.episodes)):
        return
    ep = state.episodes[ep_index]
    if ep.get("is_local") or ep.get("cached_streams"):
        return
    try:
        print(f"[PREFETCH] Pre-extracting next episode #{ep_index + 1}: {ep.get('name')} ({ep.get('url')})...", flush=True)
        res = await extract_video_streams({"url": ep["url"], "deep_browser": True, "update_playlist": False})
        streams = res.get("streams") or []
        if streams:
            ep["cached_streams"] = streams
            print(f"[PREFETCH OK] Cached {len(streams)} streams for {ep.get('name')}!", flush=True)
    except Exception as e:
        print(f"[PREFETCH WARN] {ep.get('name')}: {e}", flush=True)


async def play_episode_by_index(ep_index: int) -> Dict[str, Any]:
    """
    Switches to episode `ep_index` in `state.episodes`, extracts its streams (or uses pre-fetched cache),
    selects the stream matching `state.preferred_height`, and casts it directly to the TV!
    """
    if not (0 <= ep_index < len(state.episodes)):
        raise HTTPException(status_code=404, detail="Không có tập phim ở vị trí này!")

    state.current_episode_index = ep_index
    await _broadcast_playlist_state()
    ep = state.episodes[ep_index]

    if ep.get("is_local"):
        return await cast_to_tv({
            "url": ep["url"],
            "title": ep["name"],
            "use_proxy": True,
            "from_episode_playlist": True,
        })

    streams = ep.get("cached_streams")
    if not streams:
        res = await extract_video_streams({"url": ep["url"], "deep_browser": True, "update_playlist": False})
        streams = res.get("streams") or []
        ep["cached_streams"] = streams

    if not streams:
        raise HTTPException(status_code=404, detail=f"Không bóc tách được luồng video cho {ep.get('name')}!")

    # Pick the stream closest to user's preferred height (default 1080p) among main movies
    pref_h = state.preferred_height or 1080
    best_st = sorted(
        streams,
        key=lambda s: (
            1 if s.get("is_main_movie") else 0,
            -abs((s.get("height") or 1080) - pref_h),
            s.get("width") or 0,
        ),
        reverse=True,
    )[0]

    return await cast_to_tv({
        "url": best_st["url"],
        "title": ep.get("name") or best_st.get("title"),
        "type": best_st.get("type"),
        "headers": best_st.get("headers"),
        "subtitles": best_st.get("subtitles"),
        "duration_sec": best_st.get("duration_sec"),
        "height": best_st.get("height"),
        "use_proxy": True,
        "from_episode_playlist": True,
    })


@app.post("/api/episodes/play")
async def api_play_episode(payload: Dict[str, Any]):
    if "index" in payload and payload["index"] is not None:
        target_idx = int(payload["index"])
    elif "delta" in payload and payload["delta"] is not None:
        target_idx = state.current_episode_index + int(payload["delta"])
        if state.episodes and state.loop_mode == "all":
            target_idx = target_idx % len(state.episodes)
    else:
        raise HTTPException(status_code=400, detail="Thiếu index hoặc delta!")
    state.last_auto_next_ts = time.time()
    return await play_episode_by_index(target_idx)


@app.post("/api/episodes/auto-next")
async def api_toggle_auto_next(payload: Dict[str, Any]):
    state.auto_next_enabled = bool(payload.get("enabled", True))
    await _broadcast_playlist_state()
    return {"status": "ok", "auto_next_enabled": state.auto_next_enabled}


@app.post("/api/playlist/skip-config")
async def api_update_skip_config(payload: Dict[str, Any]):
    """
    Updates Auto-Skip Intro (`skip_intro_sec`), Auto-Skip Outro (`skip_outro_sec`),
    and YouTube SponsorBlock (`sponsorblock_enabled`).
    If the TV is currently playing inside the newly configured Intro window, immediately seeks past it!
    """
    if "skip_intro_sec" in payload and payload["skip_intro_sec"] is not None:
        state.skip_intro_sec = max(0, min(1800, int(float(payload["skip_intro_sec"]))))
    if "skip_outro_sec" in payload and payload["skip_outro_sec"] is not None:
        state.skip_outro_sec = max(0, min(1800, int(float(payload["skip_outro_sec"]))))
    if "sponsorblock_enabled" in payload and payload["sponsorblock_enabled"] is not None:
        state.sponsorblock_enabled = bool(payload["sponsorblock_enabled"])

    await _broadcast_playlist_state()

    # If a video is currently playing inside the intro window, apply skip immediately!
    cur_pos = float(state.current_media.get("currentTime") or 0)
    cur_dur = float(state.current_media.get("duration") or 0)
    if state.current_media.get("playing") and cur_pos > 0:
        await check_and_apply_skip_rules(cur_pos, cur_dur)

    return {
        "status": "ok",
        "skip_intro_sec": state.skip_intro_sec,
        "skip_outro_sec": state.skip_outro_sec,
        "sponsorblock_enabled": state.sponsorblock_enabled,
    }


async def fetch_youtube_sponsorblock_segments(yt_watch_url: str) -> List[Dict[str, Any]]:
    """
    Queries the official SponsorBlock API (`https://sponsor.ajay.app/api/skipSegments`)
    for community-submitted Intro, Outro, Sponsor, Self-Promo, Interaction, and Non-Music segments!
    """
    m = re.search(r"v=([A-Za-z0-9_-]{11})", yt_watch_url or "")
    if not m:
        return []
    vid_id = m.group(1)
    categories = json.dumps(["sponsor", "intro", "outro", "selfpromo", "interaction", "music_offtopic"])
    api_url = f"https://sponsor.ajay.app/api/skipSegments?videoID={vid_id}&categories={urllib.parse.quote(categories)}"
    try:
        async with httpx.AsyncClient(timeout=4.0, verify=False) as client:
            r = await client.get(api_url)
            if r.status_code == 200:
                items = r.json()
                segs = []
                for it in items:
                    seg = it.get("segment")
                    if isinstance(seg, list) and len(seg) == 2:
                        segs.append({
                            "start": float(seg[0]),
                            "end": float(seg[1]),
                            "category": it.get("category") or "sponsor",
                            "UUID": it.get("UUID") or "",
                        })
                return segs
    except Exception:
        pass
    return []


@app.get("/api/info")
async def get_server_info():
    local_ip = get_local_ip()
    code_info = ip_to_cast2tv_code(local_ip, PORT)
    return {
        **code_info,
        "hostname": socket.gethostname(),
        "connected_tvs": state.get_active_tvs(),
        "current_media": state.current_media,
        "sniffed_history": state.sniffed_history[:30],
        "sniffed_subtitles": state.sniffed_subtitles[:20],
        "dlna_devices": state.dlna_devices,
        "episodes": state.episodes,
        "current_episode_index": state.current_episode_index,
        "auto_next_enabled": state.auto_next_enabled,
        "loop_mode": state.loop_mode,
        "skip_intro_sec": state.skip_intro_sec,
        "skip_outro_sec": state.skip_outro_sec,
        "sponsorblock_enabled": state.sponsorblock_enabled,
        "saved_playlists": list(state.saved_playlists.keys()),
        "last_yt_query": state.last_yt_query,
        "last_yt_results": state.last_yt_results,
        "subtitle_style": state.subtitle_style,
        "buffer_config": state.buffer_config,
        "buffer_stats": state.get_buffer_stats(),
    }


@app.get("/api/export-share-zip")
async def export_clean_share_zip():
    import zipfile
    share_files = [
        "server.py",
        "requirements.txt",
        "1_CAI_DAT_LAN_DAU.bat",
        "2_CHAY_WEBVIDEOCAST_STUDIO.bat",
        "run_cast_studio.bat",
        "HUONG_DAN_SU_DUNG.txt",
        "static/index.html",
        "static/tv.html",
        "chrome_extension/manifest.json",
        "chrome_extension/background.js",
        "chrome_extension/popup.html",
        "chrome_extension/popup.js",
    ]
    out_zip = BASE_DIR / "WebVideoCast_Studio_v3.0_Share.zip"
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in share_files:
            fp = BASE_DIR / rel
            if fp.exists():
                zf.write(fp, arcname=f"WebVideoCast_Studio_v3.0/{rel}")
    return FileResponse(
        path=str(out_zip),
        media_type="application/zip",
        filename="WebVideoCast_Studio_v3.0_Share.zip",
    )


async def _delayed_push_wvc_subtitle_style(delay_sec: float = 1.6):
    await asyncio.sleep(delay_sec)
    style_cmd = state.build_wvc_subtitle_style_cmd()
    for cid, q in list(state.wvc_queues.items()):
        try:
            await q.put(style_cmd)
        except Exception:
            pass


async def _prewarm_hls_playlist_bg(raw_m3u8_url: str, custom_headers: Dict[str, str], host_base: str):
    """
    Fetches and rewrites the HLS playlist in the background right when `/api/cast` is triggered
    so the Smart RAM Read-Ahead Engine starts downloading the first `.ts` segments immediately!
    """
    try:
        req_headers = {
            "User-Agent": custom_headers.get(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ),
            "Accept": "*/*",
        }
        for k, v in (custom_headers or {}).items():
            if v:
                req_headers[k] = v
        if "Referer" in req_headers and "Origin" not in req_headers:
            parsed_ref = urllib.parse.urlparse(req_headers["Referer"])
            if parsed_ref.scheme and parsed_ref.netloc:
                req_headers["Origin"] = f"{parsed_ref.scheme}://{parsed_ref.netloc}"

        _, _, raw_bytes, final_url = await fetch_url_universal(raw_m3u8_url, headers=req_headers, timeout=15.0)
        text = raw_bytes.decode("utf-8", errors="replace")
        text = decrypt_enc_aesgcm_m3u8_if_needed(text, final_url, req_headers.get("Referer", ""))
        if text.lstrip().startswith("#EXTM3U"):
            # If it's a master playlist pointing to a variant .m3u8, follow the first variant too!
            rewrite_m3u8_content(text, final_url, custom_headers, host_base)
            if "#EXT-X-STREAM-INF" in text:
                for ln in text.splitlines():
                    s = ln.strip()
                    if s and not s.startswith("#"):
                        var_url = urllib.parse.urljoin(final_url, s)
                        _, _, v_bytes, v_final = await fetch_url_universal(var_url, headers=req_headers, timeout=15.0)
                        v_txt = v_bytes.decode("utf-8", errors="replace")
                        v_txt = decrypt_enc_aesgcm_m3u8_if_needed(v_txt, v_final, req_headers.get("Referer", ""))
                        if v_txt.lstrip().startswith("#EXTM3U"):
                            rewrite_m3u8_content(v_txt, v_final, custom_headers, host_base)
                        break
    except Exception:
        pass


@app.post("/api/cast")
async def cast_to_tv(payload: Dict[str, Any]):
    raw_url = (payload.get("url") or "").strip().strip('"')
    if not raw_url:
        raise HTTPException(status_code=400, detail="Thiếu URL video để phát!")

    # 1. Auto-detect local Windows file path (e.g. I:\tv show\...)
    if re.match(r"^[a-zA-Z]:[\\/]", raw_url) or os.path.exists(raw_url):
        p_obj = Path(raw_url)
        local_info = await register_local_file({"path": raw_url})
        raw_url = local_info["stream_url"]
        if not payload.get("title") or payload.get("title") == payload.get("url"):
            payload["title"] = local_info["name"]
        if not payload.get("subtitles") and local_info.get("subtitles"):
            payload["subtitles"] = local_info["subtitles"]
        if not payload.get("from_episode_playlist"):
            folder_eps = build_local_folder_episodes(p_obj)
            if folder_eps:
                state.episodes = folder_eps
                state.current_episode_index = next((i for i, e in enumerate(folder_eps) if e.get("active")), 0)
                await _broadcast_playlist_state()
    else:
        # 2. Auto-unwrap if user clicked an old JWPlayer ping.gif?...&mu=https://...index.m3u8 link!
        cleaned_url, _ = unwrap_and_clean_stream_url(raw_url)
        if not cleaned_url:
            raise HTTPException(
                status_code=400,
                detail="Link này là file rác/tracker (manifest.webmanifest / ping.gif), không phải luồng video!",
            )
        raw_url = cleaned_url

    # 3. Auto-resolve YouTube watch/short URLs or browser UMP googlevideo chunks via yt-dlp!
    yt_watch = extract_youtube_watch_url(
        raw_url,
        payload.get("page_url") or "",
        payload.get("referer") or (payload.get("headers") or {}).get("Referer") or "",
    )
    is_broken_ump = "googlevideo.com/videoplayback" in raw_url.lower() and ("ump=1" in raw_url.lower() or "srfvp=1" in raw_url.lower())
    if yt_watch and ("youtube.com/" in raw_url.lower() or "youtu.be/" in raw_url.lower() or is_broken_ump):
        cached_yt = state.yt_stream_cache.get(yt_watch)
        best_yt = None
        if cached_yt and (time.time() - cached_yt[0]) < 1800:
            best_yt = cached_yt[1]
        else:
            yt_streams = await sniff_with_ytdlp(yt_watch)
            if yt_streams:
                best_yt = yt_streams[0]
        if not best_yt:
            raise HTTPException(status_code=400, detail=f"Không thể bóc tách luồng phát YouTube cho {yt_watch}!")
        raw_url = best_yt["url"]
        payload["type"] = best_yt.get("type") or "mp4"
        payload["headers"] = dict(best_yt.get("headers") or {})
        payload["referer"] = ""
        hints_yt = best_yt.get("hints") or {}
        if not payload.get("duration_sec") and hints_yt.get("duration_sec"):
            payload["duration_sec"] = hints_yt["duration_sec"]
        if not payload.get("height") and hints_yt.get("height"):
            payload["height"] = hints_yt["height"]
        if not payload.get("title") or payload.get("title") in (yt_watch, "Tab Stream", "Web Video Stream"):
            payload["title"] = best_yt.get("title") or payload.get("title")
    elif is_broken_ump:
        raise HTTPException(
            status_code=400,
            detail="Đây là gói dữ liệu UMP nội bộ của trình duyệt YouTube (không phát trực tiếp được). Hãy tìm hoặc dán link YouTube vào khung YouTube để phát!",
        )

    if payload.get("height"):
        state.preferred_height = int(payload["height"])

    # Clear old stream's RAM cache if switching to a different video
    if raw_url != state.current_media.get("original_url"):
        state.clear_ram_buffer()

    title = payload.get("title") or "Web Video Stream"
    use_proxy = bool(payload.get("use_proxy", True))
    headers = dict(payload.get("headers") or {})
    if payload.get("referer"):
        headers["Referer"] = payload["referer"]
    if payload.get("user_agent"):
        headers["User-Agent"] = payload["user_agent"]
    if payload.get("cookie"):
        headers["Cookie"] = payload["cookie"]

    if "googlevideo.com/" in raw_url.lower():
        headers.pop("Referer", None)
        headers.pop("Origin", None)
        headers.pop("Cookie", None)

    local_ip = get_local_ip()
    host_base = f"http://{local_ip}:{PORT}"

    stream_type = "mp4" if "googlevideo.com/" in raw_url.lower() else (payload.get("type") or detect_stream_type(raw_url))
    is_hls = stream_type == "hls" or ".m3u8" in raw_url.lower()

    if use_proxy and not raw_url.startswith(host_base):
        play_url = build_proxy_url(raw_url, headers, host_base, is_hls=is_hls)
    else:
        play_url = raw_url

    raw_subs = payload.get("subtitles") or []
    processed_subs = []
    ver_ts = int(time.time() * 1000)
    for sub in raw_subs:
        s_url = (sub.get("url") or "").strip()
        if not s_url:
            continue
        sub_ref = sub.get("referer") or headers.get("Referer", "")
        if s_url.startswith(host_base):
            sep = "&" if "?" in s_url else "?"
            proxied_sub = re.sub(r"[?&]v=\d+", "", s_url)
            proxied_sub = f"{proxied_sub}{sep}v={ver_ts}"
        else:
            proxied_sub = f"{host_base}/proxy/subtitle.vtt?url={urllib.parse.quote(s_url)}&referer={urllib.parse.quote(sub_ref)}&v={ver_ts}"
        processed_subs.append({
            "label": sub.get("label") or "Tiếng Việt",
            "url": proxied_sub,
            "raw_url": s_url,
            "referer": sub_ref,
        })

    mime_type = get_wvc_mime_type(stream_type, raw_url)
    first_sub_url = processed_subs[0]["url"] if processed_subs else None

    dur_sec = float(payload.get("duration_sec") or 0)
    initial_start_sec = float(payload.get("start_time") or 0)
    if initial_start_sec <= 0 and state.skip_intro_sec > 0 and (dur_sec == 0 or dur_sec > state.skip_intro_sec + 45):
        initial_start_sec = float(state.skip_intro_sec)

    sb_segments: List[Dict[str, Any]] = []
    if yt_watch and state.sponsorblock_enabled:
        sb_segments = await fetch_youtube_sponsorblock_segments(yt_watch)
        for seg in sb_segments:
            if seg["start"] <= initial_start_sec < seg["end"] - 1.5:
                initial_start_sec = seg["end"]

    state.max_position_reached = initial_start_sec
    state.current_media = {
        "url": play_url,
        "original_url": raw_url,
        "title": title,
        "type": stream_type,
        "mimeType": mime_type,
        "use_proxy": use_proxy,
        "headers": headers,
        "subtitles": processed_subs,
        "playing": True,
        "currentTime": initial_start_sec,
        "duration": dur_sec,
        "skip_segments": sb_segments,
        "volume": state.current_media.get("volume", 1.0),
        "muted": False,
        "playbackRate": 1.0,
        "updatedAt": time.time(),
    }

    # Pre-warm HLS RAM Buffer before/while TV loads the media!
    if is_hls and not raw_url.startswith(host_base) and state.buffer_config.get("enabled", True):
        asyncio.create_task(_prewarm_hls_playlist_bg(raw_url, headers, host_base))

    has_next = (0 <= state.current_episode_index < (len(state.episodes) - 1)) or (state.loop_mode == "all" and len(state.episodes) > 1)
    has_prev = (state.current_episode_index > 0) or (state.loop_mode == "all" and len(state.episodes) > 1)

    wvc_load_cmd = {
        "cmd": "loadMedia",
        "media": "video",
        "url": play_url,
        "mimeType": mime_type,
        "position": int(state.current_media["currentTime"] * 1000),
        "poster": payload.get("poster"),
        "subtitle": first_sub_url,
        "corsOn": False,
        "corsServer": None,
        "headers": {} if play_url.startswith(host_base) else headers,
        "hasNext": has_next,
        "hasPrevious": has_prev,
    }

    await state.broadcast_to_tvs(
        ws_message={
            "action": "load",
            "media": state.current_media,
            "subtitle_style": state.subtitle_style,
            "buffer_config": state.buffer_config,
        },
        wvc_command=wvc_load_cmd,
    )
    # Push custom subtitle styling right after loadMedia + delayed push once WVC player mounts!
    style_cmd = state.build_wvc_subtitle_style_cmd()
    for cid, q in list(state.wvc_queues.items()):
        await q.put(style_cmd)
    asyncio.create_task(_delayed_push_wvc_subtitle_style(1.8))

    await state.broadcast_to_pcs({
        "action": "media_updated",
        "media": state.current_media,
    })

    # Pre-extract next episode in the background so Auto-Next is instantaneous!
    if has_next:
        asyncio.create_task(_prefetch_episode_in_background(state.current_episode_index + 1))

    dlna_location = payload.get("dlna_location")
    dlna_result = None
    if dlna_location:
        dlna_result = await dlna_play_url(dlna_location, play_url, title)

    return {
        "status": "ok",
        "media": state.current_media,
        "connected_tvs": len(state.get_active_tvs()),
        "dlna_result": dlna_result,
    }


@app.post("/api/subtitles/cast")
async def cast_subtitle_to_tv(payload: Dict[str, Any]):
    """
    Beams a .SRT, .VTT, or .ASS subtitle track directly onto the currently playing Smart TV
    using Native WVC's `subtitlesSet` + `subtitlesStyleSet` commands AND WebSocket `subtitle` event!
    """
    s_url = (payload.get("url") or "").strip()
    sub_id = (payload.get("id") or "").strip()
    local_ip = get_local_ip()
    host_base = f"http://{local_ip}:{PORT}"
    ver_ts = int(time.time() * 1000)

    if sub_id and not s_url:
        s_url = f"{host_base}/proxy/subtitle.vtt?id={urllib.parse.quote(sub_id)}"

    if not s_url:
        raise HTTPException(status_code=400, detail="Thiếu URL hoặc ID phụ đề (.srt / .vtt / .ass)!")

    referer = (payload.get("referer") or state.current_media.get("headers", {}).get("Referer") or "").strip()
    label = payload.get("label") or "Tiếng Việt (.VTT)"

    if s_url.startswith(host_base):
        clean_local = re.sub(r"[?&]v=\d+", "", s_url)
        sep = "&" if "?" in clean_local else "?"
        proxied_sub = f"{clean_local}{sep}v={ver_ts}"
    else:
        proxied_sub = f"{host_base}/proxy/subtitle.vtt?url={urllib.parse.quote(s_url)}&referer={urllib.parse.quote(referer)}&v={ver_ts}"

    sub_obj = {"label": label, "url": proxied_sub, "raw_url": s_url, "referer": referer}
    existing = [
        x for x in state.current_media.get("subtitles", [])
        if x.get("raw_url") != s_url and re.sub(r"[?&]v=\d+", "", x.get("url") or "") != re.sub(r"[?&]v=\d+", "", proxied_sub)
    ]
    state.current_media["subtitles"] = [sub_obj] + existing

    # 1. Push subtitlesSet to Native WVC TV and WebSocket TV
    await state.broadcast_to_tvs(
        ws_message={"action": "subtitle", "subtitle": sub_obj, "style": state.subtitle_style},
        wvc_command={"cmd": "subtitlesSet", "url": proxied_sub},
    )
    # 2. Immediately push subtitlesStyleSet so font size/color/opacity/outline apply on Native WVC TV
    style_cmd = state.build_wvc_subtitle_style_cmd()
    for cid, q in list(state.wvc_queues.items()):
        await q.put(style_cmd)

    await state.broadcast_to_pcs({
        "action": "media_updated",
        "media": state.current_media,
    })
    return {"status": "ok", "subtitle": sub_obj, "style": state.subtitle_style}


@app.post("/api/subtitles/style")
async def update_subtitle_style(payload: Dict[str, Any]):
    """
    Updates subtitle style (font size, text color, background color & opacity, outline/shadow,
    font family, bold/italic, vertical bottom position %, and timing offset +/- seconds) and
    applies it LIVE on both Native WVC Receiver (`cast2tv.app`) and Web TV (`/tv`)!
    """
    prev_offset = float(state.subtitle_style.get("offsetSec") or 0.0)
    prev_bottom = int(state.subtitle_style.get("bottomPosition") or 8)

    if payload.get("reset"):
        state.subtitle_style = {
            "textSize": 1.5,
            "textColor": "#FFFF00",
            "backgroundColor": "#000000",
            "backgroundOpacity": 0.65,
            "edgeStyle": "outline",
            "edgeColor": "#000000",
            "textFont": "sans-serif",
            "textStyle": "bold",
            "bottomPosition": 8,
            "offsetSec": 0.0,
        }
    else:
        if "textSize" in payload and payload["textSize"] is not None:
            state.subtitle_style["textSize"] = max(0.5, min(4.0, round(float(payload["textSize"]), 2)))
        if "textColor" in payload and payload["textColor"]:
            state.subtitle_style["textColor"] = str(payload["textColor"]).strip()
        if "backgroundColor" in payload and payload["backgroundColor"]:
            state.subtitle_style["backgroundColor"] = str(payload["backgroundColor"]).strip()
        if "backgroundOpacity" in payload and payload["backgroundOpacity"] is not None:
            state.subtitle_style["backgroundOpacity"] = max(0.0, min(1.0, round(float(payload["backgroundOpacity"]), 2)))
        if "edgeStyle" in payload and payload["edgeStyle"] in ("outline", "shadow", "normal"):
            state.subtitle_style["edgeStyle"] = payload["edgeStyle"]
        if "edgeColor" in payload and payload["edgeColor"]:
            state.subtitle_style["edgeColor"] = str(payload["edgeColor"]).strip()
        if "textFont" in payload and payload["textFont"]:
            state.subtitle_style["textFont"] = str(payload["textFont"]).strip()
        if "textStyle" in payload and payload["textStyle"] in ("normal", "bold", "italic", "bold_italic"):
            state.subtitle_style["textStyle"] = payload["textStyle"]
        if "bottomPosition" in payload and payload["bottomPosition"] is not None:
            state.subtitle_style["bottomPosition"] = max(2, min(60, int(float(payload["bottomPosition"]))))
        if "offsetSec" in payload and payload["offsetSec"] is not None:
            state.subtitle_style["offsetSec"] = max(-300.0, min(300.0, round(float(payload["offsetSec"]), 2)))
        if "offsetDelta" in payload and payload["offsetDelta"] is not None:
            new_off = float(state.subtitle_style.get("offsetSec") or 0.0) + float(payload["offsetDelta"])
            state.subtitle_style["offsetSec"] = max(-300.0, min(300.0, round(new_off, 2)))

    state.save_playlists_to_disk()

    wvc_style_cmd = state.build_wvc_subtitle_style_cmd()
    await state.broadcast_to_tvs(
        ws_message={"action": "subtitle_style", "style": state.subtitle_style},
        wvc_command=wvc_style_cmd,
    )

    # If timing offset (`offsetSec`) or vertical position (`bottomPosition`) changed while a subtitle is active,
    # re-push `subtitlesSet` with a fresh `&v=...` cache-buster so Native WVC TV reloads the shifted VTT cues!
    new_offset = float(state.subtitle_style.get("offsetSec") or 0.0)
    new_bottom = int(state.subtitle_style.get("bottomPosition") or 8)
    active_subs = state.current_media.get("subtitles") or []
    if active_subs and (abs(new_offset - prev_offset) > 0.001 or new_bottom != prev_bottom):
        top_sub = active_subs[0]
        base_sub_url = re.sub(r"[?&]v=\d+", "", top_sub.get("url") or "")
        if base_sub_url:
            sep = "&" if "?" in base_sub_url else "?"
            fresh_url = f"{base_sub_url}{sep}v={int(time.time() * 1000)}"
            top_sub["url"] = fresh_url
            for cid, q in list(state.wvc_queues.items()):
                await q.put({"cmd": "subtitlesSet", "url": fresh_url})
                await q.put(wvc_style_cmd)

    await state.broadcast_to_pcs({
        "action": "subtitle_style",
        "style": state.subtitle_style,
    })
    return {"status": "ok", "style": state.subtitle_style, "wvc_cmd": wvc_style_cmd}


@app.post("/api/subtitles/upload")
async def upload_subtitle_file(payload: Dict[str, Any]):
    """
    Uploads a local .SRT / .VTT / .ASS / .SSA subtitle file from PC or Mobile browser,
    converts it to UTF-8 WebVTT on the fly, and immediately casts it to the TV!
    """
    filename = (payload.get("filename") or "Phu_de_tai_len.srt").strip()
    content_b64 = payload.get("content_base64") or ""
    raw_text = payload.get("content") or ""

    if content_b64:
        try:
            raw_bytes = base64.b64decode(content_b64)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Lỗi giải mã file phụ đề: {e}")
    elif raw_text:
        raw_bytes = raw_text.encode("utf-8")
    else:
        raise HTTPException(status_code=400, detail="File phụ đề trống!")

    sub_id = f"up_{uuid.uuid4().hex[:8]}"
    decoded_text = _decode_subtitle_bytes(raw_bytes)
    state.uploaded_subtitles[sub_id] = {
        "id": sub_id,
        "label": filename,
        "content": decoded_text,
        "created_at": time.time(),
    }

    local_ip = get_local_ip()
    host_base = f"http://{local_ip}:{PORT}"
    proxied_url = f"{host_base}/proxy/subtitle.vtt?id={sub_id}&v={int(time.time() * 1000)}"
    label = f"📁 {filename}"

    sub_entry = await register_sniffed_subtitle({
        "url": proxied_url,
        "label": label,
    })

    if payload.get("auto_cast", True):
        await cast_subtitle_to_tv({"url": proxied_url, "label": label})

    return {
        "status": "ok",
        "id": sub_id,
        "url": proxied_url,
        "label": label,
        "subtitle": sub_entry,
    }


@app.post("/api/subtitles/stop")
async def stop_subtitle_on_tv():
    """
    Turns off subtitles on both Native WVC Receiver (`{"cmd": "subtitlesStop"}`) and Web TV (`/tv`).
    """
    await state.broadcast_to_tvs(
        ws_message={"action": "subtitle_stop"},
        wvc_command={"cmd": "subtitlesStop"},
    )
    return {"status": "ok"}


@app.post("/api/buffer/config")
async def update_buffer_config(payload: Dict[str, Any]):
    """
    Updates the Smart Stream Disk/RAM Buffer & Read-Ahead Pre-fetcher mode (`normal` | `weak_net` | `ultra`)
    or custom parameters and pushes updated buffer settings to connected TVs & PC Dashboards.
    """
    prev_storage = state.buffer_config.get("storage_type", "disk")
    prev_disk_dir = state.buffer_config.get("disk_dir", str(DEFAULT_DISK_CACHE_DIR))

    mode = (payload.get("mode") or "").strip().lower()
    if mode in BUFFER_PRESETS:
        state.buffer_config.update(BUFFER_PRESETS[mode])
        # Keep the user's chosen storage_type and disk_dir when switching speed presets
        state.buffer_config["storage_type"] = prev_storage
        state.buffer_config["disk_dir"] = prev_disk_dir

    if "storage_type" in payload and payload["storage_type"] is not None:
        st = str(payload["storage_type"]).strip().lower()
        if st in ("disk", "ram"):
            state.buffer_config["storage_type"] = st

    if "disk_dir" in payload and payload["disk_dir"] is not None:
        dd = str(payload["disk_dir"]).strip()
        state.buffer_config["disk_dir"] = dd if dd else str(DEFAULT_DISK_CACHE_DIR)

    if (
        state.buffer_config.get("storage_type") != prev_storage
        or state.buffer_config.get("disk_dir") != prev_disk_dir
    ):
        state.clear_ram_buffer()
        state.ensure_disk_cache_dir()

    for k in ("enabled", "prebuffer_before_play"):
        if k in payload and payload[k] is not None:
            state.buffer_config[k] = bool(payload[k])

    for k, bounds in (
        ("prefetch_segments", (0, 60)),
        ("max_ram_mb", (32, 8192)),
        ("mp4_readahead_mb", (4, 512)),
        ("hls_max_buffer_sec", (30, 900)),
        ("concurrency", (1, 8)),
    ):
        if k in payload and payload[k] is not None:
            lo, hi = bounds
            state.buffer_config[k] = max(lo, min(hi, int(payload[k])))

    state.hls_prefetch_sem = asyncio.Semaphore(int(state.buffer_config.get("concurrency") or 4))
    _evict_hls_cache_if_needed()
    state.save_playlists_to_disk()

    stats = state.get_buffer_stats()
    await state.broadcast_to_tvs(
        ws_message={"action": "buffer_config", "config": state.buffer_config},
        wvc_command=None,
    )
    await state.broadcast_to_pcs({
        "action": "buffer_config",
        "config": state.buffer_config,
        "stats": stats,
    })
    return {"status": "ok", "config": state.buffer_config, "stats": stats}


@app.get("/api/buffer/stats")
async def get_buffer_stats_api():
    return {
        "status": "ok",
        "config": state.buffer_config,
        "stats": state.get_buffer_stats(),
    }


@app.post("/api/buffer/clear")
async def clear_buffer_api():
    state.clear_ram_buffer()
    stats = state.get_buffer_stats()
    await state.broadcast_to_pcs({
        "action": "buffer_config",
        "config": state.buffer_config,
        "stats": stats,
    })
    return {"status": "ok", "stats": stats}


@app.post("/api/control")
async def remote_control_tv(payload: Dict[str, Any]):
    cmd = payload.get("command")
    value = payload.get("value")

    wvc_cmd = None
    if cmd == "play":
        state.current_media["playing"] = True
        wvc_cmd = {"cmd": "play"}
    elif cmd == "pause":
        state.current_media["playing"] = False
        wvc_cmd = {"cmd": "pause"}
    elif cmd == "seek" and value is not None:
        state.current_media["currentTime"] = float(value)
        wvc_cmd = {"cmd": "positionSet", "position": int(float(value) * 1000)}
    elif cmd == "seek_rel" and value is not None:
        new_pos = max(0.0, float(state.current_media.get("currentTime") or 0) + float(value))
        state.current_media["currentTime"] = new_pos
        wvc_cmd = {"cmd": "positionSet", "position": int(new_pos * 1000)}
    elif cmd == "volume" and value is not None:
        state.current_media["volume"] = float(value)
        wvc_cmd = {"cmd": "volumeSet", "volume": float(value)}
    elif cmd == "mute" and value is not None:
        state.current_media["muted"] = bool(value)
        wvc_cmd = {"cmd": "volumeSet", "volume": 0.0 if value else float(state.current_media.get("volume", 1.0))}
    elif cmd == "rate" and value is not None:
        state.current_media["playbackRate"] = float(value)
        wvc_cmd = {"cmd": "speedSet", "speed": float(value)}
    elif cmd == "stop":
        state.current_media["playing"] = False
        state.current_media["url"] = ""
        wvc_cmd = {"cmd": "stop"}

    await state.broadcast_to_tvs(
        ws_message={"action": "control", "command": cmd, "value": value},
        wvc_command=wvc_cmd,
    )
    return {"status": "ok", "command": cmd, "value": value}


async def register_sniffed_subtitle(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_url = (payload.get("url") or "").strip()
    if not raw_url:
        return None
    sub_entry = {
        "id": uuid.uuid4().hex[:8],
        "url": raw_url,
        "label": payload.get("label") or payload.get("title") or f"Phụ đề ({raw_url.split('/')[-1].split('?')[0]})",
        "referer": payload.get("referer") or payload.get("page_url") or "",
        "timestamp": time.strftime("%H:%M:%S"),
    }
    state.sniffed_subtitles = [x for x in state.sniffed_subtitles if x["url"] != raw_url]
    state.sniffed_subtitles.insert(0, sub_entry)
    state.sniffed_subtitles = state.sniffed_subtitles[:30]
    await state.broadcast_to_pcs({
        "action": "sniffed_subtitle",
        "subtitle": sub_entry,
    })
    return sub_entry


@app.post("/api/sniff-subtitle")
async def api_receive_sniffed_subtitle(payload: Dict[str, Any]):
    entry = await register_sniffed_subtitle(payload)
    if not entry:
        return {"status": "ignored"}
    return {"status": "ok", "subtitle": entry}


@app.post("/api/sniff-report")
async def receive_sniffed_stream_from_extension(payload: Dict[str, Any]):
    raw_url = (payload.get("url") or "").strip()
    if not raw_url:
        return {"status": "ignored"}

    # Intercept YouTube googlevideo.com/videoplayback chunks or youtube.com tabs from Chrome Extension!
    if "googlevideo.com/videoplayback" in raw_url.lower() or "youtube.com/" in raw_url.lower() or "youtu.be/" in raw_url.lower():
        yt_watch = extract_youtube_watch_url(
            raw_url,
            payload.get("page_url") or "",
            payload.get("referer") or "",
        )
        if not yt_watch:
            return {"status": "ignored_googlevideo_chunk"}
        if any(x.get("url") == yt_watch or x.get("page_url") == yt_watch for x in state.sniffed_history) or yt_watch in state.sniffing_yt_urls:
            return {"status": "already_sniffed_youtube"}
        state.sniffing_yt_urls.add(yt_watch)
        try:
            yt_streams = await sniff_with_ytdlp(yt_watch)
            if not yt_streams:
                return {"status": "ytdlp_empty"}
            best_yt = yt_streams[0]
            hints_yt = best_yt.get("hints") or {}
            dur_sec = float(hints_yt.get("duration_sec") or 0)
            w = int(hints_yt.get("width") or 0)
            h = int(hints_yt.get("height") or 0)
            res_str = format_resolution_badge(w, h)
            entry = {
                "id": uuid.uuid4().hex[:8],
                "url": yt_watch,
                "display_url": yt_watch,
                "title": best_yt.get("title") or payload.get("title") or "YouTube Video",
                "page_url": yt_watch,
                "type": "mp4",
                "quality": res_str or "YouTube MP4",
                "duration_sec": dur_sec,
                "duration_str": format_duration_hms(dur_sec),
                "width": w,
                "height": h,
                "resolution_str": res_str,
                "bitrate_kbps": int(hints_yt.get("bitrate_kbps") or 0),
                "is_main_movie": True,
                "headers": best_yt.get("headers") or {},
                "subtitles": best_yt.get("subtitles") or [],
                "timestamp": time.strftime("%H:%M:%S"),
            }
            state.sniffed_history = [
                x for x in state.sniffed_history
                if x.get("url") != yt_watch and "googlevideo.com/videoplayback" not in (x.get("url") or "").lower()
            ]
            state.sniffed_history.insert(0, entry)
            state.sniffed_history = state.sniffed_history[:50]
            await state.broadcast_to_pcs({"action": "sniffed_stream", "stream": entry})
            return {"status": "ok_youtube", "entry": entry}
        finally:
            state.sniffing_yt_urls.discard(yt_watch)

    cleaned_url, hints = unwrap_and_clean_stream_url(raw_url)
    if not cleaned_url:
        return {"status": "filtered_noise"}

    if hints.get("is_subtitle"):
        sub_entry = await register_sniffed_subtitle({**payload, "url": cleaned_url})
        return {"status": "ok_subtitle", "subtitle": sub_entry}

    # Merge any telemetry passed directly from extension
    for k in ("duration_sec", "width", "height", "bitrate_kbps"):
        if payload.get(k) and not hints.get(k):
            hints[k] = payload[k]

    headers = {
        "Referer": payload.get("referer") or payload.get("page_url") or "",
        "User-Agent": payload.get("user_agent") or "",
    }

    # Probe exact duration & resolution for the sniffed stream!
    meta = await probe_stream_metadata(cleaned_url, headers, hints)

    entry = {
        "id": uuid.uuid4().hex[:8],
        "url": cleaned_url,
        "title": payload.get("title") or "Tab Stream",
        "page_url": payload.get("page_url") or "",
        "type": detect_stream_type(cleaned_url),
        "quality": meta.get("quality_badge") or detect_stream_type(cleaned_url).upper(),
        "duration_sec": meta.get("duration_sec", 0),
        "duration_str": meta.get("duration_str", ""),
        "width": meta.get("width", 0),
        "height": meta.get("height", 0),
        "resolution_str": meta.get("resolution_str", ""),
        "bitrate_kbps": meta.get("bitrate_kbps", 0),
        "is_main_movie": meta.get("is_main_movie", False),
        "headers": headers,
        "subtitles": meta.get("subtitles", []),
        "timestamp": time.strftime("%H:%M:%S"),
    }

    state.sniffed_history = [x for x in state.sniffed_history if x["url"] != cleaned_url]
    state.sniffed_history.insert(0, entry)
    state.sniffed_history = state.sniffed_history[:50]

    await state.broadcast_to_pcs({
        "action": "sniffed_stream",
        "stream": entry,
    })

    if payload.get("auto_cast"):
        await cast_to_tv({
            "url": entry["url"],
            "title": entry["title"],
            "headers": entry["headers"],
            "subtitles": entry["subtitles"],
            "use_proxy": True,
        })

    return {"status": "ok", "entry": entry}


@app.post("/api/sniff-clear")
async def clear_sniffed_history():
    state.sniffed_history.clear()
    state.sniffed_subtitles.clear()
    await state.broadcast_to_pcs({"action": "sniff_cleared"})
    return {"status": "ok"}


@app.post("/api/sniff-remove")
async def remove_sniffed_item(payload: Dict[str, Any]):
    raw_url = payload.get("url") or ""
    state.sniffed_history = [s for s in state.sniffed_history if s.get("url") != raw_url]
    state.sniffed_subtitles = [s for s in state.sniffed_subtitles if s.get("url") != raw_url]
    await state.broadcast_to_pcs({
        "action": "sniff_removed",
        "url": raw_url,
    })
    return {"status": "ok", "url": raw_url}


@app.post("/api/youtube/clear")
async def api_youtube_clear():
    state.last_yt_query = ""
    state.last_yt_results = []
    state.save_playlists_to_disk()
    await state.broadcast_to_pcs({
        "action": "yt_search_updated",
        "results": [],
        "query": "",
    })
    return {"status": "ok"}


# ============================================================================
# 9. DLNA / UPNP AUTO-DISCOVERY & SOAP AVTRANSPORT PUSH
# ============================================================================

def _ssdp_discover_sync(timeout: float = 2.5) -> List[Dict[str, str]]:
    msg = (
        "M-SEARCH * HTTP/1.1\r\n"
        "HOST: 239.255.255.250:1900\r\n"
        'MAN: "ssdp:discover"\r\n'
        "MX: 2\r\n"
        "ST: urn:schemas-upnp-org:service:AVTransport:1\r\n\r\n"
    ).encode("ascii")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.settimeout(timeout)
    found = {}
    try:
        sock.sendto(msg, ("239.255.255.250", 1900))
        while True:
            data, addr = sock.recvfrom(4096)
            text = data.decode("utf-8", errors="ignore")
            loc_match = re.search(r"LOCATION:\s*(http[^\r\n]+)", text, re.I)
            srv_match = re.search(r"SERVER:\s*([^\r\n]+)", text, re.I)
            if loc_match:
                loc = loc_match.group(1).strip()
                found[loc] = {
                    "ip": addr[0],
                    "location": loc,
                    "server": srv_match.group(1).strip() if srv_match else "DLNA MediaRenderer",
                }
    except Exception:
        pass
    finally:
        sock.close()
    return list(found.values())


async def resolve_dlna_device(item: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=4.0, verify=False) as client:
            r = await client.get(item["location"])
            xml_text = r.text
        root = ET.fromstring(xml_text)
        ns = {"upnp": "urn:schemas-upnp-org:device-1-0"}
        friendly = root.find(".//upnp:friendlyName", ns)
        name = friendly.text if friendly is not None else f"TV ({item['ip']})"

        control_url = None
        for service in root.findall(".//{urn:schemas-upnp-org:device-1-0}service"):
            st = service.find("{urn:schemas-upnp-org:device-1-0}serviceType")
            cu = service.find("{urn:schemas-upnp-org:device-1-0}controlURL")
            if st is not None and st.text and "AVTransport" in st.text and cu is not None and cu.text:
                control_url = urllib.parse.urljoin(item["location"], cu.text.strip())
                break

        if control_url:
            return {
                "ip": item["ip"],
                "name": name,
                "location": item["location"],
                "control_url": control_url,
            }
    except Exception:
        pass
    return None


@app.get("/api/dlna/scan")
async def scan_dlna_tvs():
    raw_items = await asyncio.to_thread(_ssdp_discover_sync, 2.2)
    resolved = await asyncio.gather(*(resolve_dlna_device(x) for x in raw_items))
    state.dlna_devices = [d for d in resolved if d is not None]
    return {"devices": state.dlna_devices}


async def dlna_play_url(control_url: str, stream_url: str, title: str = "WebVideoCast Stream") -> Dict[str, Any]:
    escaped_url = (
        stream_url.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    set_uri_body = f"""<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:SetAVTransportURI xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
      <InstanceID>0</InstanceID>
      <CurrentURI>{escaped_url}</CurrentURI>
      <CurrentURIMetaData></CurrentURIMetaData>
    </u:SetAVTransportURI>
  </s:Body>
</s:Envelope>"""

    play_body = """<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:Play xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
      <InstanceID>0</InstanceID>
      <Speed>1</Speed>
    </u:Play>
  </s:Body>
</s:Envelope>"""

    async with httpx.AsyncClient(timeout=8.0, verify=False) as client:
        r1 = await client.post(
            control_url,
            content=set_uri_body.encode("utf-8"),
            headers={
                "Content-Type": 'text/xml; charset="utf-8"',
                "SOAPAction": '"urn:schemas-upnp-org:service:AVTransport:1#SetAVTransportURI"',
            },
        )
        r2 = await client.post(
            control_url,
            content=play_body.encode("utf-8"),
            headers={
                "Content-Type": 'text/xml; charset="utf-8"',
                "SOAPAction": '"urn:schemas-upnp-org:service:AVTransport:1#Play"',
            },
        )
    return {"set_uri_status": r1.status_code, "play_status": r2.status_code}


# ============================================================================
# 10. WEBSOCKET ENDPOINTS FOR TV RECEIVER & PC CONTROL DASHBOARD
# ============================================================================

@app.websocket("/ws/tv")
async def ws_tv_endpoint(websocket: WebSocket):
    await websocket.accept()
    cid = uuid.uuid4().hex[:8]
    client_ip = websocket.client.host if websocket.client else "TV"
    state.tv_clients[cid] = websocket
    state.tv_info[cid] = {
        "id": cid,
        "ip": client_ip,
        "label": f"Smart TV Web ({client_ip})",
        "protocol": "websocket",
        "connected_at": time.strftime("%H:%M:%S"),
        "last_seen": time.time(),
    }

    await state.broadcast_to_pcs({"action": "tv_list", "tvs": state.get_active_tvs()})

    local_ip = get_local_ip()
    await websocket.send_json({
        "action": "init",
        "server": ip_to_cast2tv_code(local_ip, PORT),
        "media": state.current_media if state.current_media.get("url") else None,
        "subtitle_style": state.subtitle_style,
        "buffer_config": state.buffer_config,
    })

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("action")
            if msg_type == "hello":
                state.tv_info[cid]["label"] = data.get("label") or f"Smart TV ({client_ip})"
                await state.broadcast_to_pcs({"action": "tv_list", "tvs": state.get_active_tvs()})
            elif msg_type == "status":
                for k in ["currentTime", "duration", "playing", "volume", "muted", "playbackRate"]:
                    if k in data:
                        state.current_media[k] = data[k]
                await state.broadcast_to_pcs({
                    "action": "tv_status",
                    "tv_id": cid,
                    "status": {**data, "buffer_stats": state.get_buffer_stats()},
                })
            elif msg_type == "ended":
                await trigger_auto_next_if_ready("WS TV ended")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        state.tv_clients.pop(cid, None)
        state.tv_info.pop(cid, None)
        await state.broadcast_to_pcs({"action": "tv_list", "tvs": state.get_active_tvs()})


@app.websocket("/ws/pc")
async def ws_pc_endpoint(websocket: WebSocket):
    await websocket.accept()
    cid = uuid.uuid4().hex[:8]
    state.pc_clients[cid] = websocket

    local_ip = get_local_ip()
    await websocket.send_json({
        "action": "init",
        "server": ip_to_cast2tv_code(local_ip, PORT),
        "tvs": state.get_active_tvs(),
        "media": state.current_media,
        "sniffed_history": state.sniffed_history[:30],
        "sniffed_subtitles": state.sniffed_subtitles[:20],
        "episodes": state.episodes,
        "current_episode_index": state.current_episode_index,
        "auto_next_enabled": state.auto_next_enabled,
        "loop_mode": state.loop_mode,
        "skip_intro_sec": state.skip_intro_sec,
        "skip_outro_sec": state.skip_outro_sec,
        "sponsorblock_enabled": state.sponsorblock_enabled,
        "saved_playlists": list(state.saved_playlists.keys()),
        "last_yt_query": state.last_yt_query,
        "last_yt_results": state.last_yt_results,
        "subtitle_style": state.subtitle_style,
        "buffer_config": state.buffer_config,
        "buffer_stats": state.get_buffer_stats(),
    })

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        state.pc_clients.pop(cid, None)


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

if __name__ == "__main__":
    import uvicorn
    local_ip = get_local_ip()
    info = ip_to_cast2tv_code(local_ip, PORT)
    print("=" * 68)
    print("  WEBVIDEOCAST PC STUDIO v3.0 - DEEP PROBER & NOISE FILTER")
    print("=" * 68)
    print(f"  [PC Dashboard]   : http://localhost:{PORT}")
    print(f"  [Mobile Remote]  : http://{local_ip}:{PORT}")
    print(f"  [TV Direct Link] : {info['direct_tv_url']}")
    print(f"  [cast2tv.app]    : Mo http://cast2tv.app tren TV -> Nhap ma: {info['code']}")
    print("=" * 68)
    if IS_FROZEN or os.environ.get("WVC_AUTO_OPEN") == "1":
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
