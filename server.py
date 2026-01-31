import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
import ssl
import mimetypes
import hashlib
import uuid
import shutil
import subprocess
import base64
import math
import random
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from secrets import token_hex
from threading import Event, Thread

ROOT = Path(__file__).parent.resolve()
STATIC_DIR = ROOT / "public"
DATA_DIR = ROOT / "data"
CONFIG_PATH = DATA_DIR / "config.json"
TOKEN_PATH = DATA_DIR / "tokens.json"
SCHEDULE_PATH = DATA_DIR / "schedule.json"
STATE_PATH = DATA_DIR / "upload_state.json"
DOWNLOAD_STATE_PATH = DATA_DIR / "download_state.json"
LOG_PATH = DATA_DIR / "upload.log"
DAILY_LOG_DIR = DATA_DIR / "daily_logs"
CYCLE_STATE_PATH = DATA_DIR / "auto_state.json"
COVER_DIR = DATA_DIR / "covers"
VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".m4v", ".flv", ".wmv", ".webm"}
DEFAULT_GOODS_ID = "861017472489"
RETRYABLE_ERROR_CODES = {"50000", "50002", "52001", "52002", "52101", "52102", "52103", "70031"}
# 容忍分段触发窗口（秒）。之前只有 2 分钟，容易错过；改为 30 分钟。
AUTO_RUN_WINDOW_SECONDS = 1800
DEFAULT_HTTP_TIMEOUT = int(os.getenv("PDD_HTTP_TIMEOUT", "30"))
UPLOAD_HTTP_TIMEOUT = int(os.getenv("PDD_UPLOAD_TIMEOUT", "120"))
MAX_UPLOAD_BYTES = int(os.getenv("PDD_MAX_UPLOAD_MB", "200")) * 1024 * 1024

DEFAULT_CONFIG = {
    "clientId": "",
    "clientSecret": "",
    "redirectUri": "",
    "authBase": "https://mms.pinduoduo.com/open.html",
    "goodsId": DEFAULT_GOODS_ID,
    "productGoodsMap": {},
    "productGoodsMapByShop": {},
    "videoDesc": "",
    "requireAuth": True,
    "downloadEnabled": True,
    "downloadTime": "08:30",
    "downloadRemoteRoot": "",
    "downloadLocalRoot": "video",
    "baiduCliPath": "BaiduPCS-Go.exe",
    "feishuWebhook": "",
    "autoRunEnabled": False,
    "autoRunTime": "09:00",
    "autoRunShops": [],
    "asrEnabled": False,
    "dashscopeApiKey": "",
    "dashscopeAsrModel": "qwen3-asr-flash",
    "asrContext": "",
    "titleEnabled": True,
    "titleModel": "qwen-turbo",
    "titlePrompt": "Summarize the transcript into one complete Chinese title sentence, length {min_len}-{max_len}.",
    "titleTags": [
        "美妆好物大盘点",
        "省到家的护发好物清单",
        "头皮护理好物推荐",
        "干枯毛躁护发好物",
    ],
    "hotTitleTags": [],
    "publishTimeSlots": [],
    "publishRatio": [],
    "titleMinLen": 10,
    "titleMaxLen": 20,
    "asrMaxSeconds": 60,
}

DEFAULT_SCHEDULE = {
    "shops": {
        "拼多多旗舰店": {
            "start_time": "09:00",
            "interval_seconds": 300,
            "daily_limit": 50,
            "enabled": True,
        }
    },
    "video_root": "video",
    "time_zone": "Asia/Shanghai",
}


log_buffer = []
upload_state = {"tasks": []}
download_state = {"files": {}, "auto_runs": {}}
progress_state = {
    "download": {"current": 0, "total": 0, "file": ""},
    "upload": {"current": 0, "total": 0, "file": ""},
}
stop_event = Event()
upload_pause_event = Event()
download_pause_event = Event()
upload_pause_event.set()
download_pause_event.set()


class PauseError(RuntimeError):
    pass
ffmpeg_cache = {"ts": 0, "info": None}
last_auth_warn_ts = 0
log_clear_ts = 0
auto_state = {"running": False, "last_run": ""}


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_LOG_DIR.mkdir(parents=True, exist_ok=True)
    COVER_DIR.mkdir(parents=True, exist_ok=True)
    if not SCHEDULE_PATH.exists():
        save_json(SCHEDULE_PATH, DEFAULT_SCHEDULE)
    if not DOWNLOAD_STATE_PATH.exists():
        save_json(DOWNLOAD_STATE_PATH, {"files": {}, "auto_runs": {}})
    if not STATE_PATH.exists():
        save_json(STATE_PATH, {"tasks": []})
    if not LOG_PATH.exists():
        LOG_PATH.write_text("", encoding="utf-8")


def load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_default_shop_name() -> str:
    schedule = normalize_schedule(load_json(SCHEDULE_PATH, DEFAULT_SCHEDULE))
    shops = schedule.get("shops") or {}
    if shops:
        return next(iter(shops.keys()))
    return "拼多多旗舰店"


def normalize_tokens(tokens: dict) -> tuple[dict, bool]:
    changed = False
    if not isinstance(tokens, dict):
        tokens = {}
        changed = True
    shops = tokens.get("shops")
    if not isinstance(shops, dict):
        shops = {}
        last_auth = tokens.get("lastAuth") or {}
        last_state = tokens.get("lastAuthState", "")
        if last_auth:
            default_shop = get_default_shop_name()
            shops[default_shop] = {"lastAuth": last_auth, "lastAuthState": last_state}
            tokens["lastAuthShop"] = tokens.get("lastAuthShop") or default_shop
        tokens["shops"] = shops
        changed = True
    else:
        last_auth = tokens.get("lastAuth") or {}
        last_state = tokens.get("lastAuthState", "")
        last_shop = tokens.get("lastAuthShop")
        if last_shop and last_auth:
            shop_entry = shops.get(last_shop) or {}
            existing_token = shop_entry.get("lastAuth", {})
            if existing_token.get("access_token") != last_auth.get("access_token"):
                shop_entry["lastAuth"] = last_auth
                if last_state:
                    shop_entry["lastAuthState"] = last_state
                shops[last_shop] = shop_entry
                tokens["shops"] = shops
                changed = True
        elif last_state and last_shop:
            shop_entry = shops.get(last_shop) or {}
            if shop_entry.get("lastAuthState") != last_state:
                shop_entry["lastAuthState"] = last_state
                shops[last_shop] = shop_entry
                tokens["shops"] = shops
                changed = True
    return tokens, changed


def load_tokens():
    tokens = load_json(TOKEN_PATH, {})
    tokens, changed = normalize_tokens(tokens)
    if changed:
        save_json(TOKEN_PATH, tokens)
    return tokens


def find_shop_by_state(tokens: dict, state: str) -> str:
    shops = tokens.get("shops") or {}
    if state:
        for shop_name, info in shops.items():
            if info.get("lastAuthState") == state:
                return shop_name
    return tokens.get("lastAuthShop") or get_default_shop_name()


def get_access_token(tokens: dict, shop: str) -> str:
    shops = tokens.get("shops") or {}
    shop_info = shops.get(shop) or {}
    return (
        shop_info.get("lastAuth", {}).get("access_token")
        or tokens.get("lastAuth", {}).get("access_token")
        or tokens.get("access_token")
        or ""
    )


def normalize_remote_root(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "pan.baidu.com" in raw or "path=" in raw:
        match = re.search(r"(?:[?#&]|^)path=([^&#]+)", raw)
        if match:
            try:
                raw = urllib.parse.unquote(match.group(1))
            except Exception:
                raw = match.group(1)
        else:
            try:
                parsed = urllib.parse.urlparse(raw)
                if parsed.path and parsed.path != "/":
                    raw = parsed.path
            except Exception:
                pass
    if raw and not raw.startswith("/"):
        raw = f"/{raw}"
    return raw


def normalize_schedule(schedule: dict) -> dict:
    shops = schedule.get("shops") or {}
    if "拼多多旗舰店" not in shops and "QJD" in shops:
        shops["拼多多旗舰店"] = shops.pop("QJD")
        schedule["shops"] = shops
        try:
            save_json(SCHEDULE_PATH, schedule)
        except Exception:
            pass
    return schedule


def sanitize_text(text: str) -> str:
    cleaned = re.sub(r"\s+", "", text or "")
    cleaned = re.sub(r"[^\w\u4e00-\u9fff]+", "", cleaned)
    return cleaned


def generate_title_from_transcript(transcript: str, product_name: str, cfg: dict) -> str:
    banned = {
        "最便宜",
        "绝对",
        "国家级",
        "最高级",
        "第一",
        "唯一",
        "100%有效",
        "包治",
        "根治",
        "包退",
        "零风险",
        "全网最低",
    }
    base = sanitize_text(transcript)
    min_len = int(cfg.get("titleMinLen", 10))
    max_len = int(cfg.get("titleMaxLen", 20))
    if not base:
        base = sanitize_text(product_name) or "真实体验分享"
    if len(base) < min_len:
        base = (base + "使用体验分享")[:max_len]
    title = base[:max_len]
    for word in banned:
        if word in title:
            title = title.replace(word, "")
    title = title[:max_len]
    if len(title) < min_len:
        title = (sanitize_text(product_name) + "使用体验")[:max_len] if product_name else (title + "体验分享")[:max_len]
    return title


def normalize_title_tags(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        raw = [str(item).strip() for item in value]
    else:
        raw = re.split(r"[,\n，]+", str(value))
        raw = [item.strip() for item in raw]
    cleaned = []
    for item in raw:
        if not item:
            continue
        if item.startswith("#"):
            item = item[1:].strip()
        if item:
            cleaned.append(item)
    return cleaned


def parse_product_map(raw_map) -> dict:
    product_map = {}
    if isinstance(raw_map, dict):
        for key, value in raw_map.items():
            name = str(key).strip()
            if not name:
                continue
            goods_id = str(value).strip()
            if not goods_id:
                continue
            product_map[name] = goods_id
    return product_map


def parse_product_tags_map(raw_map) -> dict:
    tag_map = {}
    if isinstance(raw_map, dict):
        for key, value in raw_map.items():
            name = str(key).strip()
            if not name:
                continue
            tags = normalize_title_tags(value)
            if tags:
                tag_map[name] = tags
    return tag_map


def parse_time_slots(raw_list) -> list[dict]:
    slots = []
    for item in raw_list or []:
        text = str(item or "").strip()
        if not text:
            continue
        try:
            h, m = [int(x) for x in text.split(":")]
            seconds = h * 3600 + m * 60
        except Exception:
            continue
        slots.append({"label": f"{h:02d}:{m:02d}", "seconds": seconds})
    return slots


def get_first_publish_slot_time(config) -> str:
    slots = parse_time_slots(config.get("publishTimeSlots", []))
    if not slots:
        return ""
    return slots[0].get("label", "")



def parse_ratio_list(raw_list) -> list[int]:
    ratios = []
    for item in raw_list or []:
        try:
            val = int(item)
        except Exception:
            continue
        if val > 0:
            ratios.append(val)
    return ratios


def extract_shop_from_path(path_str: str, today_str: str) -> str:
    try:
        parts = Path(path_str).parts
    except Exception:
        parts = []
    if today_str in parts:
        idx = parts.index(today_str)
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return ""


def get_time_slot_index(current_seconds: int, slots: list[dict]) -> int:
    if not slots:
        return -1
    idx = -1
    for i, slot in enumerate(slots):
        if current_seconds >= slot.get("seconds", 0):
            idx = i
    return idx



def append_title_tags(title: str, tags) -> str:
    if not title:
        return title
    normalized = normalize_title_tags(tags)
    if not normalized:
        return title
    count = 2 if len(normalized) >= 2 else len(normalized)
    picks = random.sample(normalized, count)
    suffix = " ".join([f"#{tag}" for tag in picks])
    if title.endswith(" "):
        return f"{title}{suffix}"
    return f"{title} {suffix}"


def append_combined_tags(title: str, hot_tags, product_tags) -> str:
    combined = []
    if hot_tags:
        combined.extend(random.sample(hot_tags, min(2, len(hot_tags))))
    if product_tags:
        combined.extend(random.sample(product_tags, min(2, len(product_tags))))
    if not combined:
        return title
    suffix = " ".join([f"#{tag}" for tag in combined])
    if title.endswith(" "):
        return f"{title}{suffix}"
    return f"{title} {suffix}"


def wait_for_resume(pause_event: Event) -> bool:
    while not pause_event.wait(1):
        if stop_event.is_set():
            return False
    return True


def wait_with_pause(total_seconds: int) -> bool:
    elapsed = 0
    while elapsed < total_seconds:
        if stop_event.is_set():
            return False
        if not download_pause_event.is_set() or not upload_pause_event.is_set():
            time.sleep(1)
            continue
        time.sleep(1)
        elapsed += 1
    return True


def fill_title_prompt(template: str, transcript: str, product_name: str, min_len: int, max_len: int) -> str:
    prompt = template or ""
    prompt = prompt.replace("{transcript}", transcript or "")
    prompt = prompt.replace("{product_name}", product_name or "")
    prompt = prompt.replace("{min_len}", str(min_len))
    prompt = prompt.replace("{max_len}", str(max_len))
    return prompt.strip()


def normalize_title_length(title: str, min_len: int, max_len: int) -> str:
    text = (title or "").strip()
    if not text:
        return ""
    if max_len and len(text) > max_len:
        punctuations = ("。", "！", "？", "；", ".", "!", "?", ";")
        cut_at = -1
        for p in punctuations:
            idx = text.rfind(p, 0, max_len)
            if idx > cut_at:
                cut_at = idx
        if cut_at >= 0 and cut_at + 1 >= min_len:
            return text[: cut_at + 1].strip()
    return text


def call_dashscope_title_llm(transcript: str, product_name: str, cfg: dict) -> str:
    api_key = (cfg.get("dashscopeApiKey") or "").strip()
    model = (cfg.get("titleModel") or "qwen-turbo").strip()
    min_len = int(cfg.get("titleMinLen", 10))
    max_len = int(cfg.get("titleMaxLen", 20))
    prompt = fill_title_prompt(cfg.get("titlePrompt") or "", transcript, product_name, min_len, max_len)
    if not prompt:
        prompt = "请将下面口播文案总结成一个完整的中文标题句子，长度控制在10-20字以内，保留关键信息，不要截断句子。"
    user_text = f"口播文案：{transcript}\n产品名：{product_name or ''}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": user_text},
        ],
        "stream": False,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        data=data,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    ctx = get_ssl_context()
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"DashScope Title HTTP {exc.code}: {detail}") from exc
    parsed = json.loads(body)
    if isinstance(parsed, dict) and parsed.get("error"):
        raise RuntimeError(f"Title LLM 请求失败: {parsed.get('error')}")
    text = ""
    if isinstance(parsed, dict):
        choices = parsed.get("choices") or []
        if isinstance(choices, list) and choices:
            message = choices[0].get("message") if isinstance(choices[0], dict) else None
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    text = content
                elif isinstance(content, list):
                    text = "".join(
                        [
                            item.get("text", "")
                            for item in content
                            if isinstance(item, dict) and item.get("text")
                        ]
                    )
    text = normalize_title_length(text, min_len, max_len)
    if not text:
        raise RuntimeError(f"Title LLM 未返回文本: {body}")
    return text


def extract_audio_for_asr(video_path: Path, max_seconds: int = 60) -> Path:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("未找到 ffmpeg，无法做语音转写")
    output_path = DATA_DIR / f"asr_{video_path.stem}_{uuid.uuid4().hex[:8]}.wav"
    cmd = [
        ffmpeg,
        "-y",
        "-i",
        str(video_path),
        "-t",
        str(max_seconds),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-vn",
        str(output_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, errors="ignore")
    if result.returncode != 0 or not output_path.exists():
        err = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"音频抽取失败: {err or 'ffmpeg 执行失败'}")
    return output_path





def extract_cover(video_path: Path) -> Path | None:
    COVER_DIR.mkdir(parents=True, exist_ok=True)
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        append_log("ffmpeg not found, skip cover extraction", level="warn")
        return None
    out_path = COVER_DIR / f"{video_path.stem}.jpg"
    cmd = [
        ffmpeg,
        "-y",
        "-i",
        str(video_path),
        "-vf",
        "select=eq(n\,0)",
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(out_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, errors="ignore")
    if result.returncode != 0 or not out_path.exists():
        err = (result.stderr or result.stdout or "").strip()
        append_log(f"cover extract failed: {err or 'ffmpeg error'}", level="warn")
        return None
    return out_path
def call_dashscope_asr(audio_path: Path, cfg: dict) -> str:
    api_key = (cfg.get("dashscopeApiKey") or "").strip()
    model = (cfg.get("dashscopeAsrModel") or "qwen3-asr-flash").strip()
    asr_context = (cfg.get("asrContext") or "").strip()
    if not api_key:
        raise RuntimeError("缺少 DashScope API Key")
    audio_bytes = audio_path.read_bytes()
    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
    data_uri = f"data:audio/wav;base64,{audio_b64}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": [{"text": asr_context}]},
            {
                "role": "user",
                "content": [{"type": "input_audio", "input_audio": {"data": data_uri}}],
            },
        ],
        "stream": False,
        "asr_options": {"enable_itn": False},
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        data=data,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    ctx = get_ssl_context()
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"DashScope ASR HTTP {exc.code}: {detail}") from exc
    parsed = json.loads(body)
    text = ""
    if isinstance(parsed, dict):
        if parsed.get("error"):
            raise RuntimeError(f"ASR 返回错误: {parsed.get('error')}")
        choices = parsed.get("choices") or []
        if isinstance(choices, list) and choices:
            message = choices[0].get("message") if isinstance(choices[0], dict) else None
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    text = content
                elif isinstance(content, list):
                    text = "".join(
                        [
                            item.get("text", "")
                            for item in content
                            if isinstance(item, dict) and item.get("text")
                        ]
                    )
    if not text:
        raise RuntimeError(f"ASR 未返回文本: {body}")
    return text


def decode_output(data: bytes) -> str:
    if not data:
        return ""
    for enc in ("utf-8", "gbk", "cp936"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode(errors="ignore")


def resolve_local_path(value, default_relative: str) -> Path:
    raw = str(value or default_relative).strip()
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT / path
    return path


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # keep console clean
        sys.stdout.write("%s - %s\n" % (self.log_date_time_string(), fmt % args))

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/config":
            return self.send_json(load_json(CONFIG_PATH, DEFAULT_CONFIG))
        if parsed.path == "/api/auth/url":
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            if not config.get("clientId") or not config.get("redirectUri"):
                return self.send_json(
                    {"error": "缺少 clientId 或 redirectUri，请先保存配置"},
                    status=HTTPStatus.BAD_REQUEST,
                )
            params = urllib.parse.parse_qs(parsed.query or "")
            shop = (params.get("shop") or [""])[0].strip() or get_default_shop_name()
            state = token_hex(12)
            auth_url = self.build_auth_url(config, state)
            tokens = load_tokens()
            tokens.setdefault("shops", {})
            tokens["shops"].setdefault(shop, {})["lastAuthState"] = state
            tokens["lastAuthShop"] = shop
            tokens["lastAuthState"] = state
            save_json(TOKEN_PATH, tokens)
            return self.send_json({"url": auth_url, "state": state, "shop": shop})
        if parsed.path == "/api/tokens":
            tokens = load_tokens()
            return self.send_json(
                {
                    "shops": tokens.get("shops", {}),
                    "lastAuthShop": tokens.get("lastAuthShop"),
                    "lastAuth": tokens.get("lastAuth", {}),
                    "lastAuthState": tokens.get("lastAuthState", ""),
                    "defaultShop": get_default_shop_name(),
                }
            )
        if parsed.path == "/api/auto/start":
            params = urllib.parse.parse_qs(parsed.query or "")
            shop_param = params.get("shops") or params.get("shop") or []
            selected_shops = [s.strip() for s in ",".join(shop_param).split(",") if s.strip()]
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            run_time = get_first_publish_slot_time(config)
            if not run_time:
                return self.send_json(
                    {"error": "请先配置发布时段，再启用自动执行"},
                    status=HTTPStatus.BAD_REQUEST,
                )
            config["autoRunEnabled"] = True
            save_json(CONFIG_PATH, config)
            start_auto_cycle(selected_shops)
            return self.send_json(
                {
                    "ok": True,
                    "enabled": True,
                    "time": run_time,
                    "running": auto_state.get("running", False),
                }
            )
        if parsed.path == "/api/schedule":
            return self.send_json(load_json(SCHEDULE_PATH, DEFAULT_SCHEDULE))
        if parsed.path == "/api/upload/status":
            return self.send_json(upload_state)
        if parsed.path == "/api/pause/status":
            return self.send_json(
                {
                    "uploadPaused": not upload_pause_event.is_set(),
                    "downloadPaused": not download_pause_event.is_set(),
                }
            )
        if parsed.path == "/api/logs":
            return self.send_json({"logs": tail_logs()})
        if parsed.path == "/api/system/ffmpeg":
            return self.send_json(get_ffmpeg_info())
        if parsed.path == "/api/baidu/status":
            return self.send_json(get_baidu_cli_status())
        if parsed.path == "/api/progress":
            return self.send_json(progress_state)
        if parsed.path == "/api/auto/status":
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            return self.send_json(
                {
                    "enabled": bool(config.get("autoRunEnabled")),
                    "time": get_first_publish_slot_time(config),
                    "running": bool(auto_state.get("running")),
                    "lastRun": auto_state.get("last_run", ""),
                }
            )
        if parsed.path == "/api/stats/today":
            return self.send_json(get_today_stats())
        if parsed.path == "/auth/callback":
            return self.handle_callback(parsed)

        return self.serve_static(parsed.path)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            body = {}

        if parsed.path == "/api/config":
            raw_map = body.get("productGoodsMap") or {}
            product_map = parse_product_map(raw_map)
            raw_map_by_shop = body.get("productGoodsMapByShop") or {}
            product_map_by_shop = {}
            if isinstance(raw_map_by_shop, dict):
                for shop_name, shop_map in raw_map_by_shop.items():
                    shop_key = str(shop_name).strip()
                    if not shop_key:
                        continue
                    parsed_map = parse_product_map(shop_map)
                    if parsed_map:
                        product_map_by_shop[shop_key] = parsed_map
            raw_tags_by_shop = body.get("productTitleTagsByShop") or {}
            product_title_tags_by_shop = {}
            if isinstance(raw_tags_by_shop, dict):
                for shop_name, shop_tags in raw_tags_by_shop.items():
                    shop_key = str(shop_name).strip()
                    if not shop_key:
                        continue
                    parsed_tags = parse_product_tags_map(shop_tags)
                    if parsed_tags:
                        product_title_tags_by_shop[shop_key] = parsed_tags
            hot_title_tags = normalize_title_tags(body.get("hotTitleTags", []))
            raw_slots = body.get("publishTimeSlots", [])
            slot_list = parse_time_slots(raw_slots)
            publish_time_slots = [s.get("label") for s in slot_list]
            publish_ratio = parse_ratio_list(body.get("publishRatio", []))
            next_conf = {
                "clientId": body.get("clientId", ""),
                "clientSecret": body.get("clientSecret", ""),
                "redirectUri": body.get("redirectUri", ""),
                "authBase": body.get("authBase", DEFAULT_CONFIG["authBase"]),
                "goodsId": str(body.get("goodsId", DEFAULT_GOODS_ID)).strip() or DEFAULT_GOODS_ID,
                "productGoodsMap": product_map,
                "productGoodsMapByShop": product_map_by_shop,
                "productTitleTagsByShop": product_title_tags_by_shop,
                "hotTitleTags": hot_title_tags,
                "videoDesc": body.get("videoDesc", ""),
                "requireAuth": bool(body.get("requireAuth", True)),
                "downloadEnabled": bool(body.get("downloadEnabled", True)),
                "downloadTime": body.get("downloadTime", DEFAULT_CONFIG["downloadTime"]),
                "downloadRemoteRoot": normalize_remote_root(body.get("downloadRemoteRoot", "")),
                "downloadLocalRoot": body.get("downloadLocalRoot", DEFAULT_CONFIG["downloadLocalRoot"]),
                "baiduCliPath": body.get("baiduCliPath", DEFAULT_CONFIG["baiduCliPath"]),
                "feishuWebhook": body.get("feishuWebhook", ""),
                "autoRunEnabled": bool(body.get("autoRunEnabled", False)),
                "asrEnabled": bool(body.get("asrEnabled", False)),
                "dashscopeApiKey": body.get("dashscopeApiKey", ""),
                "dashscopeAsrModel": body.get("dashscopeAsrModel", DEFAULT_CONFIG["dashscopeAsrModel"]),
                "asrContext": body.get("asrContext", ""),
                "titleEnabled": True,
                "titleModel": body.get("titleModel", DEFAULT_CONFIG["titleModel"]),
                "titlePrompt": body.get("titlePrompt", DEFAULT_CONFIG["titlePrompt"]),
                "publishTimeSlots": publish_time_slots,
                "publishRatio": publish_ratio,
                "titleMinLen": int(body.get("titleMinLen", DEFAULT_CONFIG["titleMinLen"])),
                "titleMaxLen": int(body.get("titleMaxLen", DEFAULT_CONFIG["titleMaxLen"])),
                "asrMaxSeconds": int(body.get("asrMaxSeconds", DEFAULT_CONFIG["asrMaxSeconds"])),
            }
            save_json(CONFIG_PATH, next_conf)
            append_log("配置已保存")
            return self.send_json({"ok": True, "config": next_conf})

        if parsed.path == "/api/baidu/logout":
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            cli_path = resolve_baidu_cli(config)
            if not cli_path:
                return self.send_json({"error": "BaiduPCS-Go not found"}, status=HTTPStatus.BAD_REQUEST)
            result = subprocess.run([cli_path, "logout"], capture_output=True, cwd=ROOT)
            output = (decode_output(result.stdout) or decode_output(result.stderr) or "").strip()
            if result.returncode != 0:
                return self.send_json({"error": output or "logout failed"}, status=HTTPStatus.BAD_REQUEST)
            append_log("BaiduPCS-Go logout ok")
            return self.send_json({"ok": True, "message": output or "ok"})

        if parsed.path == "/api/schedule":
            schedule = load_json(SCHEDULE_PATH, DEFAULT_SCHEDULE)
            schedule["video_root"] = body.get(
                "video_root", schedule.get("video_root", DEFAULT_SCHEDULE["video_root"])
            )
            schedule["time_zone"] = body.get("time_zone", schedule.get("time_zone", "Asia/Shanghai"))
            shops = body.get("shops") or schedule.get("shops", {})
            schedule["shops"] = shops
            save_json(SCHEDULE_PATH, schedule)
            append_log("上传计划已保存")
            return self.send_json({"ok": True, "schedule": schedule})

        if parsed.path == "/api/oauth/exchange":
            code = body.get("code")
            state = body.get("state")
            shop = (body.get("shop") or "").strip()
            if not code:
                return self.send_json({"error": "缺少 code"}, status=HTTPStatus.BAD_REQUEST)
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            if not config.get("clientId") or not config.get("clientSecret") or not config.get("redirectUri"):
                return self.send_json(
                    {"error": "缺少 clientId/clientSecret/redirectUri"},
                    status=HTTPStatus.BAD_REQUEST,
                )
            tokens = load_tokens()
            if not shop and state:
                for shop_name, info in tokens.get("shops", {}).items():
                    if info.get("lastAuthState") == state:
                        shop = shop_name
                        break
            if not shop:
                shop = tokens.get("lastAuthShop") or get_default_shop_name()
            shop_info = tokens.get("shops", {}).get(shop, {})
            if shop_info.get("lastAuthState") and state and shop_info.get("lastAuthState") != state:
                append_log("state 与店铺不匹配，仍尝试换取 token", level="error")
            try:
                token_resp = self.exchange_token(
                    client_id=config["clientId"],
                    client_secret=config["clientSecret"],
                    redirect_uri=config["redirectUri"],
                    code=code,
                )
                tokens.setdefault("shops", {})
                tokens["shops"].setdefault(shop, {})["lastAuth"] = {
                    **token_resp,
                    "receivedAt": self.date_time_string(),
                    "state": state,
                }
                tokens["shops"][shop]["lastAuth"] = add_refresh_meta(tokens["shops"][shop]["lastAuth"])
                if state:
                    tokens["shops"][shop]["lastAuthState"] = state
                tokens["lastAuthShop"] = shop
                tokens["lastAuth"] = tokens["shops"][shop]["lastAuth"]
                if state:
                    tokens["lastAuthState"] = state
                save_json(TOKEN_PATH, tokens)
                append_log(f"[{shop}] manual code token exchange ok")
                return self.send_json({"ok": True, "shop": shop, "token": tokens["shops"][shop]["lastAuth"]})
            except Exception as exc:  # noqa: BLE001
                append_log(f"手动 code 换取失败: {exc}", level="error")
                return self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_GATEWAY)

        if parsed.path == "/api/upload/pause":
            upload_pause_event.clear()
            append_log("上传已暂停")
            return self.send_json({"ok": True})
        if parsed.path == "/api/upload/resume":
            upload_pause_event.set()
            append_log("上传已恢复")
            return self.send_json({"ok": True})
        if parsed.path == "/api/upload/scan":
            if not upload_pause_event.is_set():
                return self.send_json({"error": "upload paused"}, status=HTTPStatus.CONFLICT)
            selected_shops = body.get("shops") or []
            if isinstance(selected_shops, str):
                selected_shops = [s.strip() for s in selected_shops.split(",") if s.strip()]
            ignore_slot = bool(body.get("ignoreSlot"))
            if ignore_slot:
                append_log("manual upload scan (ignore slots)")
            else:
                append_log("manual upload scan")
            Thread(
                target=run_scan_once,
                kwargs={"manual": True, "shops": selected_shops, "ignore_slot": ignore_slot},
                daemon=True,
            ).start()
            return self.send_json({"ok": True, "message": "scan started"})
        if parsed.path == "/api/upload/reset":
            upload_state["tasks"] = []
            persist_state()
            append_log("上传记录已清空")
            return self.send_json({"ok": True})
        if parsed.path == "/api/tokens/clear":
            shop = (body.get("shop") or "").strip()
            if not shop:
                return self.send_json({"error": "missing shop"}, status=HTTPStatus.BAD_REQUEST)
            tokens = load_tokens()
            shops = tokens.get("shops", {})
            if shop in shops:
                shops[shop].pop("lastAuth", None)
                shops[shop].pop("lastAuthState", None)
            if tokens.get("lastAuthShop") == shop:
                tokens["lastAuthShop"] = ""
                tokens["lastAuth"] = {}
                tokens["lastAuthState"] = ""
            tokens["shops"] = shops
            save_json(TOKEN_PATH, tokens)
            append_log(f"[{shop}] auth cleared")
            return self.send_json({"ok": True})


        if parsed.path == "/api/feishu/test":
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            webhook = (config.get("feishuWebhook") or "").strip()
            if not webhook:
                return self.send_json({"error": "missing feishu webhook"}, status=HTTPStatus.BAD_REQUEST)
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            send_feishu_message(f"测试消息：{now} 本地客户端通知正常", config)
            return self.send_json({"ok": True})

        if parsed.path == "/api/download/pause":
            download_pause_event.clear()
            append_log("下载已暂停")
            return self.send_json({"ok": True})
        if parsed.path == "/api/download/resume":
            download_pause_event.set()
            append_log("下载已恢复")
            return self.send_json({"ok": True})
        if parsed.path == "/api/download/manual":
            if not download_pause_event.is_set():
                return self.send_json({"error": "download paused"}, status=HTTPStatus.CONFLICT)
            append_log("开始手动下载")
            Thread(target=run_download_once, kwargs={"manual": True}, daemon=True).start()
            return self.send_json({"ok": True, "message": "已触发手动下载"})

        if parsed.path == "/api/logs/clear":
            global log_clear_ts
            log_clear_ts = time.time()
            log_buffer.clear()
            return self.send_json({"ok": True})

        if parsed.path == "/api/auto/start":
            selected_shops = body.get("shops") or []
            if isinstance(selected_shops, str):
                selected_shops = [s.strip() for s in selected_shops.split(",") if s.strip()]
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            run_time = get_first_publish_slot_time(config)
            if not run_time:
                return self.send_json(
                    {"error": "请先配置发布时段，再启用自动执行"},
                    status=HTTPStatus.BAD_REQUEST,
                )
            config["autoRunEnabled"] = True
            save_json(CONFIG_PATH, config)
            start_auto_cycle(selected_shops)
            return self.send_json(
                {
                    "ok": True,
                    "enabled": True,
                    "time": run_time,
                    "running": auto_state.get("running", False),
                }
            )

        self.send_error(HTTPStatus.NOT_FOUND)

    def handle_callback(self, parsed):
        query = urllib.parse.parse_qs(parsed.query)
        code = query.get("code", [None])[0]
        state = query.get("state", [None])[0]
        if not code:
            return self.send_html(
                HTTPStatus.BAD_REQUEST, self.render_message("缺少授权 code，无法换取 token")
            )

        config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        if not config.get("clientId") or not config.get("clientSecret") or not config.get("redirectUri"):
            return self.send_html(
                HTTPStatus.BAD_REQUEST,
                self.render_message("未配置 clientId/clientSecret/redirectUri，请先保存配置"),
            )

        tokens = load_tokens()
        shops = tokens.get("shops", {})
        state_shop = None
        if state:
            for shop_name, info in shops.items():
                if info.get("lastAuthState") == state:
                    state_shop = shop_name
                    break
            if state_shop is None:
                return self.send_html(HTTPStatus.BAD_REQUEST, self.render_message("state 未匹配到店铺"))
        shop = state_shop or tokens.get("lastAuthShop") or get_default_shop_name()

        try:
            token_resp = self.exchange_token(
                client_id=config["clientId"],
                client_secret=config["clientSecret"],
                redirect_uri=config["redirectUri"],
                code=code,
            )
            tokens.setdefault("shops", {})
            tokens["shops"].setdefault(shop, {})["lastAuth"] = {
                **token_resp,
                "receivedAt": self.date_time_string(),
                "state": state,
            }
            tokens["shops"][shop]["lastAuth"] = add_refresh_meta(tokens["shops"][shop]["lastAuth"])
            if state:
                tokens["shops"][shop]["lastAuthState"] = state
            tokens["lastAuthShop"] = shop
            tokens["lastAuth"] = tokens["shops"][shop]["lastAuth"]
            if state:
                tokens["lastAuthState"] = state
            save_json(TOKEN_PATH, tokens)
            return self.send_html(
                HTTPStatus.OK, self.render_message("授权成功，已换取 token", token_resp)
            )
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"token exchange failed: {exc}\n")
            return self.send_html(
                HTTPStatus.BAD_GATEWAY, self.render_message(f"换取 token 失败：{exc}")
            )

    def build_auth_url(self, config, state):
        base = config.get("authBase") or DEFAULT_CONFIG["authBase"]
        url = urllib.parse.urlparse(base)
        qs = urllib.parse.parse_qsl(url.query, keep_blank_values=True)
        params = dict(qs)
        params.update(
            {
                "response_type": "code",
                "client_id": config["clientId"],
                "redirect_uri": config["redirectUri"],
                "state": state,
                "view": "web",
            }
        )
        new_query = urllib.parse.urlencode(params)
        return urllib.parse.urlunparse(
            (url.scheme, url.netloc, url.path, url.params, new_query, url.fragment)
        )

    def exchange_token(self, *, client_id, client_secret, redirect_uri, code):
        payload_dict = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }
        payload = json.dumps(payload_dict).encode("utf-8")
        req = urllib.request.Request(
            "https://open-api.pinduoduo.com/oauth/token",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        ctx = get_ssl_context()
        try:
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8")
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"请求失败: {exc.reason}") from exc
        parsed = json.loads(body)
        if "error_response" in parsed:
            err = parsed["error_response"]
            raise RuntimeError(f"{err.get('error_code')}:{err.get('error_msg')}")
        if "error" in parsed:
            raise RuntimeError(parsed.get("error_description") or parsed["error"])
        if not parsed.get("access_token"):
            raise RuntimeError("token 响应缺少 access_token")
        return parsed

    def serve_static(self, path):
        target = path.lstrip("/") or "index.html"
        safe = os.path.normpath(target)
        full = STATIC_DIR / safe
        if not str(full.resolve()).startswith(str(STATIC_DIR)):
            return self.send_error(HTTPStatus.FORBIDDEN)
        if full.is_dir():
            full = full / "index.html"
        if full.exists():
            self.path = "/" + full.relative_to(STATIC_DIR).as_posix()
            return super().do_GET()
        return self.send_error(HTTPStatus.NOT_FOUND)

    def send_json(self, payload, status=HTTPStatus.OK):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_html(self, status, html):
        data = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def render_message(self, message, data=None):
        body = (
            f"<pre>{self.escape_html(json.dumps(data, ensure_ascii=False, indent=2))}</pre>"
            if data is not None
            else ""
        )
        return f"""<!doctype html>
        <html lang="zh-CN">
          <head>
            <meta charset="utf-8" />
            <title>授权结果</title>
            <style>
              body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; padding: 32px; line-height: 1.6; }}
              .box {{ max-width: 720px; margin: 0 auto; padding: 24px; border-radius: 12px; background: #f7f7fa; }}
              pre {{ background: #111827; color: #e5e7eb; padding: 16px; border-radius: 8px; overflow: auto; }}
              a {{ color: #2563eb; text-decoration: none; }}
            </style>
          </head>
          <body>
            <div class="box">
              <h2>提示</h2>
              <p>{self.escape_html(message)}</p>
              {body}
              <p><a href="/">返回首页</a></p>
            </div>
          </body>
        </html>"""

    @staticmethod
    def escape_html(text: str) -> str:
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )


def run():
    ensure_dirs()
    os.chdir(STATIC_DIR)
    load_state()
    load_download_state()
    load_auto_state()
    progress_state["download"] = {"current": 0, "total": 0, "file": ""}
    progress_state["upload"] = {"current": 0, "total": 0, "file": ""}
    port = int(os.environ.get("PORT", "3000"))
    server = ThreadingHTTPServer(("", port), Handler)
    start_refresh_worker()
    start_upload_scheduler()
    start_download_scheduler()
    start_auto_scheduler()
    print(f"PDD helper (Python) running at http://localhost:{port}")
    server.serve_forever()


def add_refresh_meta(token_payload: dict) -> dict:
    expires_in = token_payload.get("expires_in") or token_payload.get("expires_in", 0)
    try:
        expires_in = int(expires_in)
    except Exception:
        expires_in = 0
    now = time.time()
    margin = 300  # refresh 5 minutes before expiry
    next_refresh_ts = now + max(expires_in - margin, 60)
    token_payload["nextRefreshAt"] = next_refresh_ts
    token_payload["nextRefreshAtIso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(next_refresh_ts))
    token_payload["expiresAtIso"] = (
        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + expires_in)) if expires_in else None
    )
    return token_payload


def start_refresh_worker():
    thread = Thread(target=refresh_loop, daemon=True)
    thread.start()




def auto_run_scheduler_loop():
    while not stop_event.wait(30):
        config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        if not config.get("autoRunEnabled"):
            continue
        if auto_state.get("running"):
            continue
        if not upload_pause_event.is_set() or not download_pause_event.is_set():
            continue
        now_struct = time.localtime()
        today_str = time.strftime("%Y%m%d", now_struct)
        current_seconds = now_struct.tm_hour * 3600 + now_struct.tm_min * 60 + now_struct.tm_sec
        slots = parse_time_slots(config.get("publishTimeSlots", []))
        if slots:
            slot_index = get_time_slot_index(current_seconds, slots)
            if slot_index < 0:
                continue
            start_seconds = slots[slot_index].get("seconds", 0)
            slot_label = slots[slot_index].get("label", "")
            if current_seconds < start_seconds:
                continue
            if current_seconds > start_seconds + AUTO_RUN_WINDOW_SECONDS:
                continue
            auto_runs = upload_state.setdefault("auto_runs", {})
            slot_key = f"slot:{slot_label}"
            if auto_runs.get(slot_key) == today_str:
                continue
            auto_runs[slot_key] = today_str
            persist_state()
        else:
            try:
                run_time = get_first_publish_slot_time(config)
                if not run_time:
                    continue
                h, m = [int(x) for x in run_time.split(":")]
                start_seconds = h * 3600 + m * 60
            except Exception:
                continue
            if current_seconds < start_seconds:
                continue
            if current_seconds > start_seconds + AUTO_RUN_WINDOW_SECONDS:
                continue
            if auto_state.get("last_run") == today_str:
                continue
        selected_shops = [str(s).strip() for s in (config.get("autoRunShops") or []) if str(s).strip()]
        start_auto_cycle(selected_shops)


def start_auto_scheduler():
    thread = Thread(target=auto_run_scheduler_loop, daemon=True)
    thread.start()
def start_upload_scheduler():
    thread = Thread(target=upload_scheduler_loop, daemon=True)
    thread.start()


def start_download_scheduler():
    thread = Thread(target=download_scheduler_loop, daemon=True)
    thread.start()


def refresh_loop():
    while not stop_event.wait(60):
        tokens = load_tokens()
        shops = tokens.get("shops", {})
        if not shops:
            continue
        config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        if not config.get("clientId") or not config.get("clientSecret"):
            continue
        now = time.time()
        changed = False
        for shop, info in shops.items():
            last = info.get("lastAuth") or {}
            refresh_token = last.get("refresh_token")
            if not refresh_token:
                continue
            next_refresh = last.get("nextRefreshAt") or 0
            if now < next_refresh:
                continue
            try:
                resp = refresh_token_call(
                    client_id=config["clientId"],
                    client_secret=config["clientSecret"],
                    refresh_token=refresh_token,
                )
                info["lastAuth"] = add_refresh_meta(
                    {
                        **last,
                        **resp,
                        "refreshedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                    }
                )
                changed = True
                sys.stdout.write(f"{time.asctime()} - [{shop}] token auto-refreshed\n")
            except Exception as exc:  # noqa: BLE001
                sys.stderr.write(f"{time.asctime()} - [{shop}] token auto-refresh failed: {exc}\n")
                info.setdefault("lastAuth", {})
                info["lastAuth"]["nextRefreshAt"] = now + 120
                info["lastAuth"]["nextRefreshAtIso"] = time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 120)
                )
                changed = True
        if changed:
            save_json(TOKEN_PATH, tokens)


def refresh_token_call(*, client_id, client_secret, refresh_token):
    payload_dict = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    payload = json.dumps(payload_dict).encode("utf-8")
    req = urllib.request.Request(
        "https://open-api.pinduoduo.com/oauth/token",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    ctx = get_ssl_context()
    with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
        body = resp.read().decode("utf-8")
        parsed = json.loads(body)
        if "error_response" in parsed:
            err = parsed["error_response"]
            raise RuntimeError(f"{err.get('error_code')}:{err.get('error_msg')}")
        if "error" in parsed:
            raise RuntimeError(parsed.get("error_description") or parsed["error"])
        if not parsed.get("access_token"):
            raise RuntimeError("refresh 响应缺少 access_token")
        return parsed


def load_state():
    global upload_state
    upload_state = load_json(STATE_PATH, {"tasks": [], "auto_runs": {}})
    upload_state.setdefault("tasks", [])
    upload_state.setdefault("auto_runs", {})
    upload_state.setdefault("notify", {})


def persist_state():
    save_json(STATE_PATH, upload_state)


def load_download_state():
    global download_state
    download_state = load_json(DOWNLOAD_STATE_PATH, {"files": {}, "auto_runs": {}})
    download_state.setdefault("files", {})
    download_state.setdefault("auto_runs", {})
    download_state.setdefault("notify", {})


def persist_download_state():
    save_json(DOWNLOAD_STATE_PATH, download_state)

def send_feishu_message(text: str, config: dict | None = None) -> bool:
    cfg = config or load_json(CONFIG_PATH, DEFAULT_CONFIG)
    webhook = (cfg.get("feishuWebhook") or "").strip()
    if not webhook:
        return False
    payload = {"msg_type": "text", "content": {"text": text}}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    try:
        req = urllib.request.Request(webhook, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception as exc:  # noqa: BLE001
        append_log(f"feishu notify failed: {exc}", level="error")
        return False


def notify_once(store: dict, key: str, text: str, config: dict | None = None) -> bool:
    if store.get(key):
        return False
    if send_feishu_message(text, config):
        store[key] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return True
    return False


def load_auto_state():
    global auto_state
    auto_state = load_json(CYCLE_STATE_PATH, {"running": False, "last_run": ""})
    auto_state.setdefault("shops", [])
    # Always reset running flag on restart to avoid stale UI state.
    auto_state["running"] = False
    persist_auto_state()


def persist_auto_state():
    save_json(CYCLE_STATE_PATH, auto_state)


def append_log(message, level="info"):
    entry = {"ts": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "level": level, "message": message}
    log_buffer.append(entry)
    if len(log_buffer) > 200:
        del log_buffer[0 : len(log_buffer) - 200]
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass
    try:
        date_str = time.strftime("%Y%m%d", time.localtime())
        daily_path = DAILY_LOG_DIR / f"{date_str}.txt"
        line = f"[{entry['ts']}] {entry['level'].upper()}: {entry['message']}\n"
        with daily_path.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    sys.stdout.write(f"[{entry['ts']}] {level.upper()}: {message}\n")


def upload_scheduler_loop():
    while not stop_event.wait(30):
        config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        if config.get("autoRunEnabled"):
            continue
        if not upload_pause_event.is_set():
            continue
        run_scan_once(manual=False)


def download_scheduler_loop():
    while not stop_event.wait(60):
        config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        if not download_pause_event.is_set():
            continue
        run_download_once(manual=False)


def start_auto_cycle(selected_shops: list | None = None):
    if auto_state.get("running"):
        return
    auto_state["running"] = True
    auto_state["last_run"] = time.strftime("%Y%m%d", time.localtime())
    auto_state["shops"] = [str(s).strip() for s in (selected_shops or []) if str(s).strip()]
    persist_auto_state()

    def _run():

        try:
            append_log("auto cycle start")
            today_str = time.strftime("%Y%m%d", time.localtime())
            auto_runs = download_state.setdefault("auto_runs", {})
            if auto_runs.get("download") == today_str:
                append_log("download already done today, skip")
                download_ok = True
            else:
                download_ok = run_download_once(manual=True)
                if not download_ok:
                    append_log("auto download failed, wait next run")
                    return
            append_log("download ready, start upload")
            run_scan_once(manual=True, shops=auto_state.get("shops"))
            append_log("auto cycle done")
        finally:
            auto_state["running"] = False
            persist_auto_state()


    Thread(target=_run, daemon=True).start()


def resolve_baidu_cli(config: dict) -> str:
    candidates = []
    custom = (config.get("baiduCliPath") or "").strip()
    if custom:
        p = Path(custom)
        if not p.is_absolute():
            p = ROOT / p
        candidates.append(p)
    # default: repo 根目录放置 BaiduPCS-Go.exe
    candidates.append(ROOT / "BaiduPCS-Go.exe")
    if os.name == "nt":
        found = shutil.which("BaiduPCS-Go.exe")
        if found:
            candidates.append(Path(found))
    else:
        found = shutil.which("BaiduPCS-Go")
        if found:
            candidates.append(Path(found))
    for candidate in candidates:
        try:
            if candidate and candidate.exists():
                return str(candidate)
        except Exception:
            continue
    return ""


def get_baidu_cli_status() -> dict:
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    cli_path = resolve_baidu_cli(config)
    if not cli_path:
        return {"available": False, "logged_in": False, "message": "未找到 BaiduPCS-Go"}
    cmd_variants = [[cli_path, "who"], [cli_path, "user"], [cli_path, "account"]]
    output = ""
    last_err = ""
    for cmd in cmd_variants:
        result = subprocess.run(cmd, capture_output=True, cwd=ROOT)
        out = decode_output(result.stdout) or decode_output(result.stderr)
        out = (out or "").strip()
        if result.returncode == 0 and out:
            output = out
            break
        if out:
            last_err = out
    if not output:
        return {
            "available": True,
            "logged_in": None,
            "message": last_err or "无法读取登录状态",
        }
    lower = output.lower()
    if "not login" in lower or "not logged" in lower or "未登录" in output or "not logged in" in lower:
        logged_in = False
    else:
        uid_match = re.search(r"uid:\s*(\d+)", output, re.IGNORECASE)
        if uid_match:
            logged_in = uid_match.group(1) != "0"
        elif "当前账号" in output or "用户名" in output:
            logged_in = True
        else:
            logged_in = True
    return {"available": True, "logged_in": logged_in, "message": output}


def parse_baidu_list(raw: str):
    raw = raw.strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(data, dict):
        for key in ("data", "list", "files", "result"):
            if key in data and isinstance(data[key], list):
                return data[key]
        if "files" in data and isinstance(data["files"], list):
            return data["files"]
    if isinstance(data, list):
        return data
    return []


def parse_baidu_text_list(raw: str, remote_dir: str):
    entries = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("当前目录") or line.startswith("----") or line.startswith("总:"):
            continue
        match = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+(.+)$", line)
        if not match:
            continue
        name = match.group(1).strip()
        if not name:
            continue
        is_dir = name.endswith("/")
        clean_name = name.rstrip("/")
        path = f"{remote_dir.rstrip('/')}/{clean_name}" if remote_dir else clean_name
        entries.append(
            {
                "path": path,
                "name": clean_name,
                "isdir": is_dir,
                "size": 0,
                "mtime": 0,
            }
        )
    return entries


def list_baidu_dir(cli_path: str, remote_dir: str, recursive: bool = False):
    cmd = [cli_path, "ls", "--json"]
    if recursive:
        cmd.append("--recursive")
    cmd.append(remote_dir)
    try:
        result = subprocess.run(cmd, capture_output=True, cwd=ROOT, timeout=DEFAULT_HTTP_TIMEOUT)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"ls 超时: {remote_dir}") from exc
    if result.returncode == 0:
        raw = decode_output(result.stdout)
        parsed = parse_baidu_list(raw)
        if parsed:
            return parsed
        if "当前目录" in raw:
            return parse_baidu_text_list(raw, remote_dir)
    # fallback: non-json output or failure
    cmd = [cli_path, "ls", remote_dir]
    try:
        result = subprocess.run(cmd, capture_output=True, cwd=ROOT, timeout=DEFAULT_HTTP_TIMEOUT)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"ls 超时: {remote_dir}") from exc
    if result.returncode != 0:
        raw_err = decode_output(result.stderr) or decode_output(result.stdout)
        raise RuntimeError((raw_err or "").strip() or "ls 失败")
    raw = decode_output(result.stdout)
    return parse_baidu_text_list(raw, remote_dir)


def normalize_remote_entry(entry: dict):
    path = entry.get("path") or entry.get("path_lower") or entry.get("Path") or ""
    name = entry.get("name") or entry.get("filename") or ""
    is_dir = entry.get("isdir") or entry.get("is_dir") or entry.get("isDir") or False
    size = entry.get("size") or entry.get("Size") or 0
    mtime = entry.get("mtime") or entry.get("server_mtime") or entry.get("modify_time") or 0
    if not path and name:
        path = name
    return {
        "path": str(path),
        "name": str(name or Path(path).name),
        "is_dir": bool(is_dir),
        "size": int(size) if str(size).isdigit() else size,
        "mtime": mtime,
    }


def collect_remote_files(cli_path: str, remote_dir: str):
    entries = list_baidu_dir(cli_path, remote_dir, recursive=True)
    files = []
    dirs = []
    if entries:
        for raw in entries:
            entry = normalize_remote_entry(raw)
            if not entry["path"]:
                continue
            if entry["is_dir"]:
                dirs.append(entry["path"])
            else:
                files.append(entry)
        if files:
            return files
    # fallback: manual recursion when recursive listing is empty or only dirs
    if not entries:
        entries = list_baidu_dir(cli_path, remote_dir, recursive=False)
        dirs = []
        for raw in entries:
            entry = normalize_remote_entry(raw)
            if not entry["path"]:
                continue
            if entry["is_dir"]:
                dirs.append(entry["path"])
            else:
                files.append(entry)
        if files:
            return files
    for path in dirs:
        files.extend(collect_remote_files(cli_path, path))
    return files


def download_remote_file(cli_path: str, remote_path: str, local_root: Path):
    local_root.mkdir(parents=True, exist_ok=True)
    local_file = local_root / Path(remote_path).name
    cmd_variants = [
        [cli_path, "download", remote_path, "--saveto", str(local_root), "--ow"],
        [cli_path, "download", "--saveto", str(local_root), "--ow", remote_path],
    ]
    last_err = ""
    for cmd in cmd_variants:
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT)
        except PauseError:
            append_log("Download paused")
            return
        except Exception as exc:
            last_err = str(exc)
            continue
        code = None
        start_ts = time.monotonic()
        last_heartbeat = start_ts
        last_size = local_file.stat().st_size if local_file.exists() else 0
        no_change_count = 0
        while code is None:
            if not download_pause_event.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()
                raise PauseError("download paused")
            code = proc.poll()
            now = time.monotonic()
            if code is None and now - last_heartbeat >= 20:
                current_size = local_file.stat().st_size if local_file.exists() else 0
                delta_bytes = current_size - last_size
                elapsed = now - start_ts
                interval = now - last_heartbeat if now > last_heartbeat else 20
                avg_speed = current_size / elapsed if elapsed > 0 else 0
                inst_speed = delta_bytes / interval if interval > 0 else 0
                delta_mb = delta_bytes / (1024 * 1024)
                size_mb = current_size / (1024 * 1024)
                avg_mb = avg_speed / (1024 * 1024)
                inst_mb = inst_speed / (1024 * 1024)
                status = "无变化" if delta_bytes <= 0 else f"+{delta_mb:.2f}MB"
                append_log(
                    f"下载心跳: {remote_path} 大小={size_mb:.2f}MB {status} 平均={avg_mb:.2f}MB/s 速率={inst_mb:.2f}MB/s"
                )
                if delta_bytes <= 0:
                    no_change_count += 1
                    if no_change_count >= 3:
                        append_log(f"下载停滞: {remote_path} 超过60s无变化", level="error")
                else:
                    no_change_count = 0
                last_size = current_size
                last_heartbeat = now
            if code is None:
                time.sleep(0.5)
        if code == 0:
            return
        last_err = f"download exit={code}"
    raise RuntimeError(last_err or "download 失败")


def run_download_once(manual: bool = False) -> bool:
    if manual:
        append_log("manual download trigger")
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    progress_state["download"] = {"current": 0, "total": 0, "file": ""}
    if not wait_for_resume(download_pause_event):
        return False
    if not config.get("downloadEnabled", True) and not manual:
        return False
    remote_root = normalize_remote_root(config.get("downloadRemoteRoot"))
    if not remote_root:
        if manual:
            append_log("download remote root not set", level="error")
        return False
    remote_root = remote_root.rstrip("/")
    remote_video_dir = f"{remote_root}/video" if remote_root else "/video"
    local_root = resolve_local_path(config.get("downloadLocalRoot"), DEFAULT_CONFIG["downloadLocalRoot"])
    now_struct = time.localtime()
    today_str = time.strftime("%Y%m%d", now_struct)
    current_seconds = now_struct.tm_hour * 3600 + now_struct.tm_min * 60 + now_struct.tm_sec

    if not manual:
        start_time = config.get("downloadTime", "08:30")
        try:
            h, m = [int(x) for x in start_time.split(":")]
            start_seconds = h * 3600 + m * 60
        except Exception:
            start_seconds = 8 * 3600 + 30 * 60
        if current_seconds < start_seconds:
            return False
        if current_seconds > start_seconds + AUTO_RUN_WINDOW_SECONDS:
            return False
        auto_runs = download_state.setdefault("auto_runs", {})
        if auto_runs.get("download") == today_str:
            return False
        auto_runs["download"] = today_str
        persist_download_state()
        append_log(f"download window reached {start_time}, auto start")

    cli_path = resolve_baidu_cli(config)
    if not cli_path:
        append_log("BaiduPCS-Go not found", level="error")
        return False
    remote_date_dir = f"{remote_video_dir}/{today_str}"
    append_log(f"scan remote dir: {remote_date_dir}")
    try:
        remote_files = collect_remote_files(cli_path, remote_date_dir)
    except Exception as exc:
        append_log(f"remote scan failed: {exc}", level="error")
        return False
    if not remote_files:
        append_log(f"remote dir empty or missing: {remote_date_dir}")
        notify_store = download_state.setdefault("notify", {})
        key = f"download_empty_{today_str}"
        if notify_once(notify_store, key, f"下载无文件：{today_str} 远端目录为空或不存在。", config):
            persist_download_state()
        return False

    files_state = download_state.setdefault("files", {})
    new_files = [entry for entry in remote_files if entry.get("path")]
    downloaded_any = False
    success_count = 0
    per_shop = {}
    progress_state["download"]["total"] = len(new_files)
    for entry in new_files:
        remote_path = entry["path"]
        rel_path = remote_path[len(remote_video_dir) + 1 :] if remote_path.startswith(remote_video_dir) else remote_path
        target_dir = local_root / Path(rel_path).parent
        try:
            append_log(f"download start {remote_path}")
            started = time.time()
            download_remote_file(cli_path, remote_path, target_dir)
            elapsed = time.time() - started
            files_state[remote_path] = {
                "size": entry.get("size", 0),
                "mtime": entry.get("mtime", 0),
                "downloaded_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "local_dir": str(target_dir),
            }
            persist_download_state()
            append_log(f"download done {remote_path} elapsed {elapsed:.1f}s")
            downloaded_any = True
            success_count += 1
            shop_name = extract_shop_from_path(remote_path, today_str)
            if shop_name:
                per_shop[shop_name] = per_shop.get(shop_name, 0) + 1
        except Exception as exc:
            append_log(f"download failed {remote_path}: {exc}", level="error")
        progress_state["download"]["current"] += 1
        progress_state["download"]["file"] = remote_path
    progress_state["download"]["file"] = ""
    if new_files:
        detail = ""
        if per_shop:
            detail = "；店铺：" + "，".join([f"{k} {v}" for k, v in per_shop.items()])
        send_feishu_message(f"下载完成：成功 {success_count}/{len(new_files)} 个{detail}", config)
    return downloaded_any


def run_scan_once(manual: bool = False, shops: list | None = None, ignore_slot: bool = False):
    if manual:
            append_log("上传触发")
    schedule = normalize_schedule(load_json(SCHEDULE_PATH, DEFAULT_SCHEDULE))
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    progress_state["upload"] = {"current": 0, "total": 0, "file": ""}
    if not wait_for_resume(upload_pause_event):
        return
    require_auth = config.get("requireAuth", True)
    tokens = load_tokens() if require_auth else {}
    selected_shops = {str(s).strip() for s in (shops or []) if str(s).strip()}
    shops_cfg = schedule.get("shops", {})
    if selected_shops:
        missing = selected_shops - set(shops_cfg.keys())
        if missing and manual:
            append_log(f"店铺不存在: {', '.join(sorted(missing))}")
    video_root = resolve_local_path(schedule.get("video_root"), DEFAULT_SCHEDULE["video_root"])
    if manual:
        append_log(f"扫描根目录: {video_root}")
    now_struct = time.localtime()
    today_str = time.strftime("%Y%m%d", now_struct)
    current_seconds = now_struct.tm_hour * 3600 + now_struct.tm_min * 60 + now_struct.tm_sec
    auto_runs = upload_state.setdefault("auto_runs", {})
    slots = parse_time_slots(config.get("publishTimeSlots", []))
    ratios = parse_ratio_list(config.get("publishRatio", []))
    use_slot_ratio = bool(slots and ratios and len(slots) == len(ratios) and not ignore_slot)
    slot_index = get_time_slot_index(current_seconds, slots) if use_slot_ratio else -1

    notify_store = upload_state.setdefault("notify", {})
    if use_slot_ratio and slot_index >= 0:
        slot_label = slots[slot_index]["label"]
        key = f"slot_start_{today_str}_{slot_index}"
        if not notify_store.get(key):
            shop_names = [s for s in shops_cfg.keys() if (not selected_shops or s in selected_shops)]
            parts = []
            total_done = 0
            for s in shop_names:
                done_cnt = len([t for t in upload_state.get("tasks", []) if t.get("shop") == s and t.get("date") == today_str and t.get("status") == "done"])
                parts.append(f"{s} {done_cnt}")
                total_done += done_cnt
            text_msg = f"分段上传开始 {slot_label}，今日已成功 {total_done} 个；店铺：" + ("，".join(parts) if parts else "无")
            if notify_once(notify_store, key, text_msg, config):
                persist_state()


    for shop, cfg in shops_cfg.items():
        if selected_shops and shop not in selected_shops:
            continue
        if require_auth:
            access_token = get_access_token(tokens, shop)
            if not access_token:
                global last_auth_warn_ts
                now = time.time()
                if manual or now - last_auth_warn_ts > 120:
                    append_log(f"[{shop}] 缺少 access_token，已跳过", level="error")
                    last_auth_warn_ts = now
                continue
        if not cfg.get("enabled", True):
            if manual:
                append_log(f"[{shop}] 未授权，已跳过")
            continue
        start_time = cfg.get("start_time", "09:00")
        try:
            h, m = [int(x) for x in start_time.split(":")]
            start_seconds = h * 3600 + m * 60
        except Exception:
            start_seconds = 9 * 3600
        if use_slot_ratio and slot_index >= 0:
            start_seconds = slots[slot_index].get('seconds', start_seconds)
        if not manual and current_seconds < start_seconds:
            if manual:
                append_log(f"[{shop}] 今日已执行过 {start_time}，跳过")
            continue
        if not manual:
            if current_seconds > start_seconds + AUTO_RUN_WINDOW_SECONDS:
                continue
            run_key = f"{shop}:{slot_index}" if use_slot_ratio and slot_index >= 0 else shop
            if auto_runs.get(run_key) == today_str:
                continue
            auto_runs[run_key] = today_str
            persist_state()
            append_log(f"[{shop}] ????????????")
        daily_limit = int(cfg.get("daily_limit", 50))
        interval_seconds = int(cfg.get("interval_seconds", 300))
        if use_slot_ratio and slot_index < 0:
            if manual:
                append_log(f"[{shop}] 当前时间未到首个发布时段，跳过")
            continue


        shop_dir = video_root / today_str / shop
        if not shop_dir.exists():
            if manual:
                append_log(f"[{shop}] ??????{shop_dir}")
            key = f"upload_no_files_{today_str}_{shop}"
            if notify_once(notify_store, key, f"[{today_str}] shop={shop} no_dir", config):
                persist_state()
            continue
        product_dirs = sorted([p for p in shop_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
        if not product_dirs:
            if manual:
                append_log(f"[{shop}] 目录为空：{shop_dir}")
            continue
        files = []
        for product_dir in product_dirs:
            product_name = product_dir.name
            for file_path in sorted(product_dir.iterdir(), key=lambda p: p.name):
                if file_path.is_file() and is_video_file(file_path):
                    rel_path = f"{product_name}/{file_path.name}"
                    files.append((rel_path, file_path, product_name))
        if not files:
            if manual:
                append_log(f"[{shop}] ?????????{shop_dir}")
            key = f"upload_no_files_{today_str}_{shop}"
            if notify_once(notify_store, key, f"[{today_str}] shop={shop} no_video", config):
                persist_state()
            continue
        if manual:
            append_log(f"[{shop}] 发现视频 {len(files)} 个，目录 {len(product_dirs)} 个")

        upload_state.setdefault("tasks", [])
        today_tasks = [t for t in upload_state["tasks"] if t.get("shop") == shop and t.get("date") == today_str]
        done_tasks = [t for t in today_tasks if t.get("status") == "done"]
        failed_tasks = [t for t in today_tasks if t.get("status") == "failed"]
        processing_tasks = [t for t in today_tasks if t.get("status") == "processing"]
        if manual:
            append_log(
                f"[{shop}] 今日完成 {len(done_tasks)} 个，失败 {len(failed_tasks)} 个，进行中 {len(processing_tasks)} 个"
            )
        if processing_tasks:
            append_log(f"[{shop}] 今日已处理完毕（可能重复或已完成），跳过")
            continue
        completed_count = len([t for t in today_tasks if t.get("status") == "done"])
        if not manual and completed_count >= daily_limit:
            if manual:
                append_log(f"[{shop}] 已达每日上限 {daily_limit}，跳过")
            continue

        if not manual and today_tasks:
            last_task = max(today_tasks, key=lambda t: t.get("ended_at", ""))
            if last_task.get("ended_at"):
                try:
                    last_ts = time.strptime(last_task["ended_at"], "%Y-%m-%d %H:%M:%S")
                    last_seconds = time.mktime(last_ts)
                    if time.time() - last_seconds < interval_seconds:
                        if manual:
                            append_log(f"[{shop}] 上传间隔 {interval_seconds}s，等待后继续")
                        continue
                except Exception:
                    pass

        processed_files = {
            t.get("rel_path") or t.get("filename") for t in today_tasks if t.get("status") in ("done", "processing")
        }
        attempted_files = set(processed_files)
        processed_names = {Path(name).name for name in processed_files if name}
        attempted_names = set(processed_names)
        allowed_rels = None
        if use_slot_ratio:
            product_files = {}
            for rel_path, file_path, product_name in files:
                product_files.setdefault(product_name, []).append((rel_path, file_path, product_name))
            slot_label = slots[slot_index]["label"]
            allowed_rels = set()
            sum_ratio = sum(ratios) or 1
            for product, items in product_files.items():
                total = len(items)
                remaining_quota = total
                quotas = []
                for r in ratios:
                    if remaining_quota <= 0:
                        quotas.append(0)
                        continue
                    q = int(math.ceil(total * r / sum_ratio))
                    if q > remaining_quota:
                        q = remaining_quota
                    quotas.append(q)
                    remaining_quota -= q
                allowed_count = quotas[slot_index]
                used = len([t for t in today_tasks if t.get("product") == product and t.get("slot") == slot_index and t.get("status") in ("done", "processing")])
                to_pick = max(0, allowed_count - used)
                if to_pick <= 0:
                    continue
                for rel_path, file_path, product_name in items:
                    if rel_path in attempted_files or file_path.name in attempted_names:
                        continue
                    allowed_rels.add(rel_path)
                    to_pick -= 1
                    if to_pick == 0:
                        break
            if manual:
                append_log(f"[{shop}] 当前发布时段 {slot_label}，本轮可发布 {len(allowed_rels)} 个")
            progress_state["upload"]["total"] += len(allowed_rels)
            if len(allowed_rels) == 0:
                key = f"upload_no_slot_files_{today_str}_{shop}_{slot_index}"
                if notify_once(notify_store, key, f"[{today_str}] shop={shop} slot={slot_label} no_upload", config):
                    persist_state()
        else:
            remaining = 0
            for rel_path, file_path, product_name in files:
                if rel_path not in attempted_files and file_path.name not in attempted_names:
                    remaining += 1
            progress_state["upload"]["total"] += remaining

        while True:
            candidate = None
            for rel_path, file_path, product_name in files:
                if rel_path not in attempted_files and file_path.name not in attempted_names:
                    if allowed_rels is not None and rel_path not in allowed_rels:
                        continue
                    candidate = (rel_path, file_path, product_name)
                    break
            if not candidate:
                append_log(f"[{shop}] ???????????")
                # 重新统计当前店铺、当前分段的成功/失败，避免历史或其他分段干扰
                tasks_all = upload_state.get("tasks", [])
                def match_slot(task):
                    if not use_slot_ratio or slot_index < 0:
                        return True
                    return task.get("slot") == slot_index or task.get("slot_time") == slots[slot_index]["label"]
                done_cnt = len([t for t in tasks_all if t.get("shop") == shop and t.get("date") == today_str and t.get("status") == "done" and match_slot(t)])
                failed_cnt = len([t for t in tasks_all if t.get("shop") == shop and t.get("date") == today_str and t.get("status") == "failed" and match_slot(t)])
                key = f"upload_shop_done_{today_str}_{shop}_{slot_index if use_slot_ratio else 'all'}"
                slot_label = slots[slot_index]["label"] if use_slot_ratio and slot_index >= 0 else "all"
                if notify_once(notify_store, key, f"[{today_str}] shop={shop} slot={slot_label} upload_done success={done_cnt} failed={failed_cnt}", config):
                    persist_state()
                break

            rel_path, candidate_path, product_name = candidate
            progress_state["upload"]["current"] += 1
            progress_state["upload"]["file"] = rel_path
            attempted_files.add(rel_path)
            attempted_names.add(candidate_path.name)

            task = {
                "id": str(uuid.uuid4()),
                "shop": shop,
                "date": today_str,
                "filename": rel_path,
                "rel_path": rel_path,
                "product": product_name,
                "path": str(candidate_path),
                "status": "processing",
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "message": "",
            }
            if use_slot_ratio and slot_index >= 0:
                task["slot"] = slot_index
                task["slot_time"] = slots[slot_index]["label"]
            upload_state["tasks"].append(task)
            persist_state()
            append_log(f"[{shop}] 开始上传 {rel_path}")
            try:
                result = upload_video_file(candidate_path, product_name=product_name, shop=shop)
                task["status"] = "done"
                if isinstance(result, dict):
                    task["vid"] = result.get("vid")
                    task["video_id"] = result.get("video_id") or result.get("vid")
                    task["cover_url"] = result.get("cover_url")
                    task["title"] = result.get("title") or ""
                    if task["title"]:
                        task["message"] = f"自动标题：{task['title']}"
                    else:
                        task["message"] = "上传完成"
                    append_log(
                        f"[{shop}] 上传完成 {rel_path} vid={result.get('vid')} video_id={result.get('video_id')}"
                    )
                else:
                    task["video_id"] = result
                    task["message"] = "上传完成"
                append_log(f"[{shop}] 上传完成 {rel_path} video_id={result}")
            except Exception as exc:  # noqa: BLE001
                task["status"] = "failed"
                task["message"] = str(exc)
                append_log(f"[{shop}] 上传失败 {rel_path}: {exc}", level="error")
            task["ended_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            persist_state()

            if not manual:
                break

    progress_state["upload"]["file"] = ""


def pdd_upload_parts(
    file_path: Path,
    *,
    upload_sign: str,
    client_id: str,
    client_secret: str,
    access_token: str,
    part_size: int | None = None,
) -> bool:
    size = file_path.stat().st_size
    chunk_size = int(part_size or (19 * 1024 * 1024))
    if chunk_size > 20 * 1024 * 1024:
        chunk_size = 20 * 1024 * 1024
    if chunk_size <= 0:
        chunk_size = 19 * 1024 * 1024
    total_parts = (size + chunk_size - 1) // chunk_size
    append_log(
        f"准备上传文件: {file_path.name}, size={size} bytes, mime={mimetypes.guess_type(str(file_path))[0] or 'video/mp4'}"
    )
    append_log(f"开始分片上传，共 {total_parts} 片")
    with file_path.open("rb") as f:
        for idx in range(1, total_parts + 1):
            data = f.read(chunk_size)
            if not data:
                break
            append_log(f"上传分片 {idx}/{total_parts} size={len(data)}")
            call_pdd_api(
                client_id=client_id,
                client_secret=client_secret,
                access_token=access_token,
                type_name="pdd.live.video.mall.upload.part",
                extra_params={"upload_sign": upload_sign, "part_num": str(idx)},
                files={"part_file": (file_path.name, data, "application/octet-stream")},
            )
    return True

def upload_video_file(file_path: Path, product_name: str | None = None, shop: str | None = None):
    tokens = load_tokens()
    shop_name = shop or tokens.get("lastAuthShop") or get_default_shop_name()
    access_token = get_access_token(tokens, shop_name)
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    client_id = config.get("clientId") or config.get("client_id")
    client_secret = config.get("clientSecret") or config.get("client_secret")
    product_map_by_shop = config.get("productGoodsMapByShop") if isinstance(config.get("productGoodsMapByShop"), dict) else {}
    product_map = config.get("productGoodsMap") if isinstance(config.get("productGoodsMap"), dict) else {}
    if product_map_by_shop:
        product_map = product_map_by_shop.get(shop_name, {}) or product_map
    if not product_map:
        product_map = config.get("productGoodsMap") if isinstance(config.get("productGoodsMap"), dict) else {}
    if product_name:
        mapped_id = product_map.get(product_name)
        if mapped_id:
            goods_id = mapped_id
        else:
            goods_id = config.get("goodsId", DEFAULT_GOODS_ID)
            append_log(
                f"[{shop_name}] 未找到商品ID映射，product={product_name} 使用默认 goods_id={goods_id}"
            )
    else:
        goods_id = config.get("goodsId", DEFAULT_GOODS_ID)
    if not (client_id and client_secret and access_token):
        append_log(
            f"缺少 client_id/client_secret/access_token: client_id={bool(client_id)} client_secret={bool(client_secret)} access_token={bool(access_token)}",
            level="error",
        )
        raise RuntimeError("缺少 client_id/client_secret/access_token")

    mime_type, _ = mimetypes.guess_type(str(file_path))
    if not mime_type:
        mime_type = "video/mp4"

    init_payload = {
        "content_type": mime_type,
    }
    init_resp = call_pdd_api(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        type_name="pdd.live.video.mall.upload.part.init",
        extra_params=init_payload,
        files=None,
    )
    if not isinstance(init_resp, dict):
        raise RuntimeError(f"init 返回异常: {init_resp}")
    init_info = init_resp.get("response") if isinstance(init_resp.get("response"), dict) else init_resp
    upload_sign = init_info.get("upload_sign")
    if not upload_sign:
        raise RuntimeError(f"init 缺少 upload_sign: {init_info}")

    upload_complete = pdd_upload_parts(
        file_path,
        upload_sign=upload_sign,
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        part_size=init_info.get("part_size"),
    )
    if not upload_complete:
        raise RuntimeError("chunk upload failed")

    complete_resp = call_pdd_api(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        type_name="pdd.live.video.mall.upload.part.complete",
        extra_params={"upload_sign": upload_sign},
        files=None,
    )
    if not isinstance(complete_resp, dict):
        raise RuntimeError(f"complete response error: {complete_resp}")
    upload_info = complete_resp.get("response") if isinstance(complete_resp.get("response"), dict) else complete_resp
    vid = upload_info.get("vid") or upload_info.get("video_id") or ""
    if vid:
        append_log(f"complete vid={vid}")

    cover_path = extract_cover(file_path)
    cover_url = None
    if cover_path:
        cover_url = upload_image_file(
            cover_path, client_id=client_id, client_secret=client_secret, access_token=access_token
        )

    title = ""
    hot_tags = normalize_title_tags(config.get("hotTitleTags", []))
    tags_by_shop = config.get("productTitleTagsByShop") or {}
    tag_source = None
    if product_name:
        tag_source = (tags_by_shop.get(shop_name, {}) or {}).get(product_name)
    try:
        transcript = ""
        if config.get("asrEnabled"):
            transcript = call_dashscope_asr(
                extract_audio_for_asr(file_path, int(config.get("asrMaxSeconds", 60))), config
            )
        if not (config.get("dashscopeApiKey") and config.get("titleModel")):
            raise RuntimeError("DashScope API Key 或模型未配置，无法生成口播标题")
        title = call_dashscope_title_llm(transcript, product_name or "", config)
        if not (title or "").strip():
            raise RuntimeError("口播标题生成为空")
        title = append_combined_tags(title, hot_tags, tag_source)
    except Exception as exc:  # noqa: BLE001
        append_log(f"title generation failed: {exc}", level="error")
        raise

    desc = (title or config.get("videoDesc") or "").strip()

    publish_resp = publish_video(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        vid=vid,
        cover_url=cover_url,
        goods_id=goods_id,
        desc=desc,
    )
    if isinstance(publish_resp, dict):
        publish_resp["title"] = title
        publish_resp["cover_url"] = cover_url
    return publish_resp


def upload_image_file(image_path: Path, *, client_id, client_secret, access_token):
    mime_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"
    data = image_path.read_bytes()
    append_log(f"upload cover image: {image_path.name}, size={len(data)} bytes, mime={mime_type}")
    resp = call_pdd_api(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        type_name="pdd.live.img.mall.upload",
        extra_params={},
        files={"file": (image_path.name, data, mime_type)},
    )
    url = resp.get("url") or resp.get("response", {}).get("url")
    if not url:
        raise RuntimeError(f"upload cover image missing url: {resp}")
    return url

def publish_video(*, client_id, client_secret, access_token, vid, cover_url, goods_id, desc=""):
    payload = {"cover": cover_url, "goods_id": int(goods_id), "vid": vid, "desc": desc or ""}
    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if desc:
        append_log(f"publish desc={desc}")
    append_log(f"publish request={payload_str}")
    resp = call_pdd_api(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        type_name="pdd.live.video.mall.create",
        extra_params={"request": payload_str, "version": "V1"},
        files=None,
        base_url="https://gw-api.pinduoduo.com/api/router",
    )
    error_code = str(resp.get("error_code") or "")
    success_flag = resp.get("success")
    result = resp.get("result") or {}
    video_id = result.get("video_id") or resp.get("video_id") or ""
    if success_flag is True and video_id:
        if error_code and error_code not in {"0", "1000000"}:
            append_log(f"publish success but error_code={error_code}: {json.dumps(resp, ensure_ascii=False)}")
        return video_id
    if error_code and error_code != "0":
        append_log(f"publish failed: {json.dumps(resp, ensure_ascii=False)}", level="error")
        error_msg = resp.get("error_msg") or resp.get("msg") or resp.get("error_desc") or "unknown"
        raise RuntimeError(f"{error_code}:{error_msg}")
    if success_flag is False:
        append_log(f"publish failed: {json.dumps(resp, ensure_ascii=False)}", level="error")
        raise RuntimeError(resp.get("error_msg") or resp.get("msg") or "publish failed")
    if not video_id:
        raise RuntimeError(f"publish missing video_id: {resp}")
    return video_id

def call_pdd_api(
    *, client_id, client_secret, access_token, type_name, extra_params, files=None, retries=2, base_url=None
):
    url = base_url or "https://gw-upload.pinduoduo.com/api/upload"
    params = {
        "type": type_name,
        "client_id": client_id,
        "access_token": access_token,
        "timestamp": str(int(time.time())),
        "data_type": "JSON",
    }
    params.update(extra_params or {})
    sign = sign_params(params, client_secret)
    params["sign"] = sign
    attempt = 0
    while True:
        attempt += 1
        timeout = UPLOAD_HTTP_TIMEOUT if files else DEFAULT_HTTP_TIMEOUT
        if files:
            body, content_type = encode_multipart(params, files)
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": content_type, "Content-Length": str(len(body))},
                method="POST",
            )
        else:
            body = urllib.parse.urlencode(params).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
        ctx = get_ssl_context()
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.URLError as exc:
            if attempt <= retries:
                delay = min(2 ** attempt, 8)
                append_log(f"网络错误，{delay}s 后重试 ({attempt}/{retries})：{exc!r}", level="error")
                time.sleep(delay)
                continue
            raise RuntimeError(f"网络错误: {exc!r}") from exc
        parsed = json.loads(raw)
        if "error_response" in parsed:
            err = parsed["error_response"]
            code = str(err.get("error_code") or "")
            msg = err.get("error_msg") or "unknown"
            request_id = err.get("request_id") or ""
            if code in RETRYABLE_ERROR_CODES and attempt <= retries:
                delay = min(2 ** attempt, 8)
                append_log(
                    f"接口错误 {code}:{msg} type={type_name} request_id={request_id}，{delay}s 后重试 ({attempt}/{retries})",
                    level="error",
                )
                time.sleep(delay)
                continue
            suffix = f" request_id={request_id}" if request_id else ""
            raise RuntimeError(f"{code}:{msg} type={type_name}{suffix}")
        return parsed.get("response", parsed)


def is_video_file(path: Path) -> bool:
    ext = path.suffix.lower()
    if ext in VIDEO_EXTS:
        return True
    mime = mimetypes.guess_type(path.name)[0] or ""
    return mime.startswith("video/")


def get_ssl_context():
    if os.getenv("PDD_INSECURE_SSL") == "1":
        return ssl._create_unverified_context()
    return None


def sign_params(params: dict, client_secret: str) -> str:
    items = sorted(params.items())
    base = client_secret + "".join(k + str(v) for k, v in items) + client_secret
    return hashlib.md5(base.encode("utf-8")).hexdigest().upper()


def encode_multipart(fields: dict, files: dict):
    boundary = "----WebKitFormBoundary" + uuid.uuid4().hex
    body = b""
    for name, value in fields.items():
        body += f"--{boundary}\r\n".encode("utf-8")
        body += f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8")
        body += str(value).encode("utf-8") + b"\r\n"
    for name, (filename, content, content_type) in files.items():
        body += f"--{boundary}\r\n".encode("utf-8")
        body += f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode("utf-8")
        body += f"Content-Type: {content_type}\r\n\r\n".encode("utf-8")
        body += content + b"\r\n"
    body += f"--{boundary}--\r\n".encode("utf-8")
    content_type = f"multipart/form-data; boundary={boundary}"
    return body, content_type


def tail_logs(max_lines: int = 200):
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return list(log_buffer)
    tail = lines[-max_lines:]
    entries = []
    for line in tail:
        try:
            entries.append(json.loads(line))
        except Exception:
            entries.append({"ts": "", "level": "info", "message": line})
    if not log_clear_ts:
        return entries
    filtered = []
    for entry in entries:
        ts = entry.get("ts")
        if not ts:
            filtered.append(entry)
            continue
        try:
            epoch = time.mktime(time.strptime(ts, "%Y-%m-%d %H:%M:%S"))
        except Exception:
            filtered.append(entry)
            continue
        if epoch >= log_clear_ts:
            filtered.append(entry)
    return filtered


def get_today_stats():
    today_ymd = time.strftime("%Y%m%d", time.localtime())
    download_state = load_json(DOWNLOAD_STATE_PATH, {"files": {}, "auto_runs": {}})
    download_files = download_state.get("files", {})
    download_success = 0
    for entry in download_files.values():
        ts = entry.get("downloaded_at", "")
        if ts and ts.replace("-", "")[:8] == today_ymd:
            download_success += 1

    upload_state_local = load_json(STATE_PATH, {"tasks": []})
    upload_tasks = upload_state_local.get("tasks", [])
    upload_success = sum(
        1 for task in upload_tasks if task.get("status") == "done" and task.get("date") == today_ymd
    )
    return {"downloadSuccess": download_success, "uploadSuccess": upload_success}


def get_ffmpeg_info():
    now = time.time()
    cached = ffmpeg_cache.get("info")
    if cached and now - ffmpeg_cache.get("ts", 0) < 30:
        return cached
    path = shutil.which("ffmpeg")
    info = {"available": False, "path": "", "version": ""}
    if path:
        info["available"] = True
        info["path"] = path
        try:
            output = subprocess.check_output(
                [path, "-version"], text=True, stderr=subprocess.STDOUT, timeout=3, errors="ignore"
            )
            info["version"] = output.splitlines()[0].strip() if output else ""
        except Exception:
            info["version"] = ""
    ffmpeg_cache["info"] = info
    ffmpeg_cache["ts"] = now
    return info


if __name__ == "__main__":
    run()
