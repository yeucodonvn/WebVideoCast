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

PORT = int(os.environ.get("WVC_PORT", "30001"))
BASE_DIR = Path(__file__).parent.resolve()
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

app = FastAPI(title="WebVideoCast PC Studio", version="3.0.0")

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
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


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
        self.saved_playlists: Dict[str, List[Dict[str, Any]]] = {}
        self.last_yt_query: str = ""
        self.last_yt_results: List[Dict[str, Any]] = []
        self.preferred_height: int = 1080
        self.last_auto_next_ts: float = 0.0
        self.max_position_reached: float = 0.0
        self.sniffing_yt_urls: set = set()
        self.yt_stream_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._load_playlists_from_disk()

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
                self.last_yt_query = data.get("last_yt_query") or ""
                self.last_yt_results = data.get("last_yt_results") or []
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
                    },
                })

            # Poll exact hardware position from WVC Receiver on TV via {"cmd": "positionGet"}
            near_end = (dur > 20 and (dur - cur) <= 30.0)
            poll_interval = 2 if near_end else 5
            if tick_count % poll_interval == 0:
                for cid, q in list(state.wvc_queues.items()):
                    if q.empty():
                        await q.put({"cmd": "positionGet"})

            # Trigger Auto-Next / Loop if clock reaches the end of the track/episode
            if dur > 20 and cur >= (dur - 1.2):
                await trigger_auto_next_if_ready(f"Clock reached {cur:.1f}s/{dur:.1f}s")
        except Exception:
            pass


@app.on_event("startup")
async def _start_background_clock():
    asyncio.create_task(_realtime_media_clock_loop())



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
        if tv_state in ("idle", "ended", "completed", "stopped") and dur > 20 and max_pos >= (dur - 15.0):
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
        # Also trigger Auto-Next / Loop if position reaches within 1.5s of total duration!
        if dur > 20 and pos >= (dur - 1.5):
            await trigger_auto_next_if_ready(f"Position reached {pos:.1f}s/{dur:.1f}s")
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


def rewrite_m3u8_content(content: str, base_url: str, headers: Dict[str, str], host_base: str) -> str:
    lines = content.splitlines()
    out_lines = []
    expect_variant_m3u8 = False

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
            if 'URI="' in stripped:
                line = re.sub(r'URI="([^"]+)"', replace_uri_attr, line)
            out_lines.append(line)
        else:
            abs_url = urllib.parse.urljoin(base_url, stripped)
            low_abs = abs_url.lower().split("?")[0]
            is_sub_m3u8 = expect_variant_m3u8 or low_abs.endswith(".m3u8") or low_abs.endswith(".m3u")
            expect_variant_m3u8 = False
            out_lines.append(
                build_proxy_url(
                    abs_url,
                    headers,
                    host_base,
                    is_hls=is_sub_m3u8,
                    is_segment=not is_sub_m3u8,
                )
            )

    return "\n".join(out_lines)


@app.get("/proxy/segment.ts")
async def proxy_hls_segment(request: Request, token: str):
    """
    Dedicated, zero-crash HLS segment proxy (.ts / .m4s / disguised .png/.jpg/.html segments).
    Reads the segment cleanly, strips any anti-leech PNG/JPG header before 0x47, and returns
    an exact Content-Length Response so Uvicorn NEVER throws 'Response content shorter than Content-Length'!
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

    async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=httpx.Timeout(25.0, connect=8.0)) as client:
        resp = await client.get(target_url, headers=req_headers)
        raw_bytes = resp.content

    clean_bytes = strip_disguised_mpegts_header(raw_bytes)
    return Response(
        content=clean_bytes,
        status_code=200 if resp.status_code in (200, 206) else resp.status_code,
        media_type="video/mp2t",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Private-Network": "true",
            "Content-Length": str(len(clean_bytes)),
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
    )

    if is_m3u8_request:
        if "Referer" in req_headers and "Origin" not in req_headers:
            parsed_ref = urllib.parse.urlparse(req_headers["Referer"])
            if parsed_ref.scheme and parsed_ref.netloc:
                req_headers["Origin"] = f"{parsed_ref.scheme}://{parsed_ref.netloc}"
        async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=httpx.Timeout(20.0, connect=8.0)) as client:
            try:
                resp = await client.get(target_url, headers=req_headers)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Upstream M3U8 fetch error: {e}")

        raw_bytes = resp.content
        final_url = str(resp.url)
        text = raw_bytes.decode("utf-8", errors="replace")
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

    # Special Bounded-Chunk Streaming Engine for YouTube googlevideo.com!
    # YouTube's googlevideo.com (c=ANDROID_VR) rejects open-ended `Range: bytes=0-` or un-ranged GETs with 403 Forbidden,
    # and strictly requires bounded byte-range requests (`Range: bytes=START-END`).
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

        client = httpx.AsyncClient(verify=False, follow_redirects=True, timeout=httpx.Timeout(30.0, connect=10.0))
        try:
            r_first = await client.get(target_url, headers=yt_headers)
        except Exception as e:
            await client.aclose()
            raise HTTPException(status_code=502, detail=f"YouTube upstream error: {e}")

        if r_first.status_code not in (200, 206):
            await client.aclose()
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
            await client.aclose()
            return Response(content=b"", status_code=out_status, headers=resp_headers, media_type=media_type_yt)

        async def iter_yt_bounded_chunks():
            try:
                yield first_bytes
                cur_pos = start_byte + len(first_bytes)
                step_size = 4 * 1024 * 1024  # 4MB subsequent chunks
                while cur_pos <= final_end:
                    c_end = min(cur_pos + step_size - 1, final_end)
                    h_chunk = dict(req_headers)
                    h_chunk["Accept-Encoding"] = "identity"
                    h_chunk["Range"] = f"bytes={cur_pos}-{c_end}"
                    r_chunk = await client.get(target_url, headers=h_chunk)
                    if r_chunk.status_code not in (200, 206) or not r_chunk.content:
                        break
                    yield r_chunk.content
                    cur_pos += len(r_chunk.content)
            finally:
                await client.aclose()

        return StreamingResponse(
            iter_yt_bounded_chunks(),
            status_code=out_status,
            headers=resp_headers,
            media_type=media_type_yt,
        )

    # Direct MP4 / MKV / WebM / Extensionless Sora streaming with Byte-Range support
    if "range" in request.headers:
        req_headers["Range"] = request.headers["range"]
    elif "/sora/" in target_url or "sssrr.org" in target_url or "trycloudflare.com" in target_url:
        # Hydrax / Abyss / Sora servers return 404 Not Found if Range header is omitted!
        req_headers["Range"] = "bytes=0-"
    # Disable gzip/br compression on binary streams so Content-Length never mismatches!
    req_headers["Accept-Encoding"] = "identity"

    client = httpx.AsyncClient(verify=False, follow_redirects=True, timeout=httpx.Timeout(60.0, connect=10.0))
    try:
        upstream_req = client.build_request("GET", target_url, headers=req_headers)
        upstream_resp = await client.send(upstream_req, stream=True)
        # Universal fallback: if an upstream video CDN returns 403/404 when no Range header was sent, retry with Range: bytes=0-
        if upstream_resp.status_code in (403, 404) and "Range" not in req_headers:
            await upstream_resp.aclose()
            req_headers["Range"] = "bytes=0-"
            upstream_req = client.build_request("GET", target_url, headers=req_headers)
            upstream_resp = await client.send(upstream_req, stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Upstream stream error: {e}")

    async def iter_upstream():
        try:
            async for chunk in upstream_resp.aiter_raw(chunk_size=65536):
                yield chunk
        finally:
            await upstream_resp.aclose()
            await client.aclose()

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

    return StreamingResponse(
        iter_upstream(),
        status_code=out_status,
        headers=resp_headers,
        media_type=media_type_out,
    )


@app.get("/proxy/subtitle.vtt")
@app.get("/proxy/subtitle")
async def proxy_subtitle(url: str, referer: Optional[str] = None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0 Safari/537.36"
    }
    if referer:
        headers["Referer"] = referer
    async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=15.0) as client:
        r = await client.get(url, headers=headers)
        text = r.text.lstrip("\ufeff")

    if not text.startswith("WEBVTT"):
        vtt_body = re.sub(r"(\d{2}:\d{2}:\d{2}),(\d{3})", r"\1.\2", text)
        text = "WEBVTT\n\n" + vtt_body

    return Response(
        content=text,
        media_type="text/vtt; charset=utf-8",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Private-Network": "true",
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

    return {
        "file_id": file_id,
        "name": file_path.name,
        "size": file_path.stat().st_size,
        "stream_url": stream_url,
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
    if ".m3u8" in url.lower() or u.endswith(".m3u"):
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
                if v_stream.width and v_stream.height:
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
                r = await client.get(url, headers=req_headers)
                text = r.text
                final_url = str(r.url)

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

        # 6. If this embed page uses encrypted JS player initialization (like Abyss `SoTrym` / `jwplayer` / `eval(function(p,a,c,k,e,d))`)
        #    and didn't yield direct static URLs yet, queue it as a direct Playwright JS target!
        if not streams and any(kw in html for kw in ("SoTrym", "jwplayer", "videojs", "Artplayer", "DPlayer", "Clappr", "eval(function")):
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
        if any(bad in raw_ifr.lower() for bad in ("googletagmanager", "doubleclick", "facebook.com", "disqus")):
            continue
        abs_ifr = urllib.parse.urljoin(final_url, raw_ifr).replace("/tvc?id=", "/play?id=")
        if abs_ifr not in seen_embeds:
            seen_embeds.add(abs_ifr)
            embed_tasks.append(inspect_embed_player_url(abs_ifr, final_url, "Main Player"))

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


async def sniff_with_ytdlp(url: str) -> List[Dict[str, Any]]:
    import yt_dlp

    def _extract():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
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


async def sniff_with_playwright(
    url: str,
    custom_referer: Optional[str] = None,
    page_title_hint: Optional[str] = None,
    is_embed_iframe: bool = False,
    fast_episode_mode: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Universal WVC Mobile App Headless Browser Engine:
    - Automatically routes ISP-blocked / DNS-poisoned domains (like abyssplayer.com) through DoH + TLS Record Splitting!
    - Blocks preroll ad domains (333xbet, player-ads.xml, 1s_blank.mp4, popads, abyss.to homepage redirect).
    - If `is_embed_iframe=True`, mounts the target URL inside a genuine `<iframe src="...">` wrapper so embed players
      that check `if (top.location == self.location)` never redirect to their homepage!
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

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        def add_candidate(raw_u: str, ref: str, ua: str, source_tag: str, extra_hints: Optional[Dict[str, Any]] = None, srv_label: str = ""):
            if not raw_u or raw_u.startswith("blob:") or raw_u.startswith("data:"):
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
                title_str = page_title_hint or url
                if srv_label:
                    title_str = f"{title_str} [{srv_label}]"
                found.append({
                    "title": title_str,
                    "server_name": srv_label or source_tag,
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

        async def universal_route_interceptor(route):
            req = route.request
            r_url = req.url
            low_u = r_url.lower()
            # 1. Block gambling ads, VAST prerolls, popup scripts & embed anti-direct redirects (`abyss.to`)
            if any(bad in low_u for bad in ("333xbet", "video888.mp4", "player-ads.xml", "1s_blank.mp4", "decafeligiblyhad.com", "fuckadblock", "popads", "abyss.to")):
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
            if host:
                try:
                    ip = socket.gethostbyname(host)
                    if ip.startswith("127.") or ip == "0.0.0.0":
                        is_poisoned = True
                except Exception:
                    is_poisoned = True
            if is_poisoned:
                try:
                    st, hdrs, body = await asyncio.to_thread(fetch_dpi_tls_split_sync, r_url, dict(req.headers), 8.0)
                    clean_hdrs = {
                        k: v for k, v in hdrs.items()
                        if k.lower() not in ("transfer-encoding", "content-length", "content-encoding", "connection")
                    }
                    await route.fulfill(status=st, headers=clean_hdrs, body=body)
                    return
                except Exception:
                    pass
            await route.continue_()

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
                await page.set_content(
                    f'<!DOCTYPE html><html><body style="margin:0;background:#000;">'
                    f'<iframe src="{url}" width="800" height="600" referrerpolicy="origin" allow="autoplay; fullscreen"></iframe>'
                    f'</body></html>'
                )
                await page.wait_for_timeout(2800)
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
                for _ in range(2):
                    for frame in page.frames:
                        try:
                            f_url = frame.url if (frame.url and frame.url.startswith("http")) else url
                            res = await frame.evaluate(WVC_UNIVERSAL_FRAME_HOOK_JS)
                            if isinstance(res, dict):
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
                                            srv_label=lbl,
                                        )
                        except Exception:
                            pass
                    if found:
                        break
                    try:
                        await page.mouse.click(400, 300)
                    except Exception:
                        pass
                    await page.wait_for_timeout(2000)

            page_title = page_title_hint or await page.title()
            if page_title and "abyss" not in page_title.lower():
                for item in found:
                    if item["title"].startswith("http"):
                        item["title"] = page_title
        except Exception as e:
            print(f"[PLAYWRIGHT WARN] {url}: {e}", flush=True)
        finally:
            await browser.close()

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
                browser = await p.chromium.launch(
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
                    req = route.request
                    r_url = req.url
                    low_u = r_url.lower()
                    if any(bad in low_u for bad in ("333xbet", "video888.mp4", "player-ads.xml", "1s_blank.mp4", "decafeligiblyhad.com", "fuckadblock", "popads")):
                        await route.abort()
                        return
                    if "/tvc?id=" in r_url:
                        await route.continue_(url=r_url.replace("/tvc?id=", "/play?id="))
                        return
                    parsed = urllib.parse.urlparse(r_url)
                    host = parsed.hostname or ""
                    is_poisoned = False
                    if host:
                        try:
                            ip = socket.gethostbyname(host)
                            if ip.startswith("127.") or ip == "0.0.0.0":
                                is_poisoned = True
                        except Exception:
                            is_poisoned = True
                    if is_poisoned:
                        try:
                            st, hdrs, body = await asyncio.to_thread(fetch_dpi_tls_split_sync, r_url, dict(req.headers), 8.0)
                            clean_hdrs = {
                                k: v for k, v in hdrs.items()
                                if k.lower() not in ("transfer-encoding", "content-length", "content-encoding", "connection")
                            }
                            await route.fulfill(status=st, headers=clean_hdrs, body=body)
                            return
                        except Exception:
                            pass
                    await route.continue_()

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

    # If HTML unrolling discovered encrypted JS player iframes (like abyssplayer.com/Poh-lIVIIo)
    # OR if no streams were found yet, run our Universal 4-Layer WVC Playwright Hook!
    if js_iframes:
        pw_tasks = [
            sniff_with_playwright(ifr_url, custom_referer=ifr_ref, page_title_hint=page_title, is_embed_iframe=True)
            for ifr_url, ifr_ref, _ in js_iframes[:3]
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
            "saved_playlists": list(state.saved_playlists.keys()),
        })

    return {
        "streams": enriched,
        "subtitles": all_subtitles,
        "episodes": state.episodes if update_playlist else episodes,
        "current_episode_index": state.current_episode_index,
        "auto_next_enabled": state.auto_next_enabled,
        "loop_mode": state.loop_mode,
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
        "saved_playlists": list(state.saved_playlists.keys()),
        "last_yt_query": state.last_yt_query,
        "last_yt_results": state.last_yt_results,
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
    for sub in raw_subs:
        s_url = (sub.get("url") or "").strip()
        if not s_url:
            continue
        sub_ref = sub.get("referer") or headers.get("Referer", "")
        if s_url.startswith(host_base):
            proxied_sub = s_url
        else:
            proxied_sub = f"{host_base}/proxy/subtitle.vtt?url={urllib.parse.quote(s_url)}&referer={urllib.parse.quote(sub_ref)}"
        processed_subs.append({
            "label": sub.get("label") or "Tiếng Việt",
            "url": proxied_sub,
            "raw_url": s_url,
        })

    mime_type = get_wvc_mime_type(stream_type, raw_url)
    first_sub_url = processed_subs[0]["url"] if processed_subs else None

    state.max_position_reached = 0.0
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
        "currentTime": float(payload.get("start_time") or 0),
        "duration": float(payload.get("duration_sec") or 0),
        "volume": state.current_media.get("volume", 1.0),
        "muted": False,
        "playbackRate": 1.0,
        "updatedAt": time.time(),
    }

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
        ws_message={"action": "load", "media": state.current_media},
        wvc_command=wvc_load_cmd,
    )
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
    Beams a .SRT or .VTT subtitle track directly onto the currently playing Smart TV
    using Native WVC's `subtitlesSet` command (auto-converted to UTF-8 WebVTT via /proxy/subtitle.vtt)!
    """
    s_url = (payload.get("url") or "").strip()
    if not s_url:
        raise HTTPException(status_code=400, detail="Thiếu URL phụ đề (.srt / .vtt)!")

    referer = (payload.get("referer") or state.current_media.get("headers", {}).get("Referer") or "").strip()
    label = payload.get("label") or "Tiếng Việt (.VTT)"
    local_ip = get_local_ip()
    host_base = f"http://{local_ip}:{PORT}"

    if s_url.startswith(host_base):
        proxied_sub = s_url
    else:
        proxied_sub = f"{host_base}/proxy/subtitle.vtt?url={urllib.parse.quote(s_url)}&referer={urllib.parse.quote(referer)}"

    sub_obj = {"label": label, "url": proxied_sub, "raw_url": s_url}
    state.current_media["subtitles"] = [sub_obj] + [
        x for x in state.current_media.get("subtitles", []) if x.get("url") != proxied_sub
    ]

    # Native WVC subtitlesSet command (CommandReceiver.ts line 365)
    await state.broadcast_to_tvs(
        ws_message={"action": "subtitle", "subtitle": sub_obj},
        wvc_command={"cmd": "subtitlesSet", "url": proxied_sub},
    )
    return {"status": "ok", "subtitle": sub_obj}


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
                    "status": data,
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
        "saved_playlists": list(state.saved_playlists.keys()),
        "last_yt_query": state.last_yt_query,
        "last_yt_results": state.last_yt_results,
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
    print(f"  [TV Direct Link] : {info['direct_tv_url']}")
    print(f"  [cast2tv.app]    : Mo http://cast2tv.app tren TV -> Nhap ma: {info['code']}")
    print("=" * 68)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
