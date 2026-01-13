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
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler
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
COVER_DIR = DATA_DIR / "covers"
VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".m4v", ".flv", ".wmv", ".webm"}
DEFAULT_GOODS_ID = "861017472489"
RETRYABLE_ERROR_CODES = {"50000", "50002", "52001", "52002", "52101", "52102", "52103", "70031"}
AUTO_RUN_WINDOW_SECONDS = 120
DEFAULT_HTTP_TIMEOUT = int(os.getenv("PDD_HTTP_TIMEOUT", "30"))
UPLOAD_HTTP_TIMEOUT = int(os.getenv("PDD_UPLOAD_TIMEOUT", "120"))

DEFAULT_CONFIG = {
    "clientId": "",
    "clientSecret": "",
    "redirectUri": "",
    "authBase": "https://mms.pinduoduo.com/open.html",
    "goodsId": DEFAULT_GOODS_ID,
    "productGoodsMap": {},
    "videoDesc": "",
    "requireAuth": True,
    "downloadEnabled": True,
    "downloadTime": "08:30",
    "downloadRemoteRoot": "",
    "downloadLocalRoot": str(ROOT / "video"),
    "baiduCliPath": "",
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
    "video_root": str(ROOT / "video"),
    "time_zone": "Asia/Shanghai",
}


log_buffer = []
upload_state = {"tasks": []}
download_state = {"files": {}, "auto_runs": {}}
stop_event = Event()
ffmpeg_cache = {"ts": 0, "info": None}
last_auth_warn_ts = 0


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
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


def load_tokens():
    return load_json(TOKEN_PATH, {})


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
                    {"error": "缺少 clientId 或 redirectUri，请先保存配置。"},
                    status=HTTPStatus.BAD_REQUEST,
                )
            state = token_hex(12)
            auth_url = self.build_auth_url(config, state)
            tokens = load_json(TOKEN_PATH, {})
            tokens["lastAuthState"] = state
            save_json(TOKEN_PATH, tokens)
            return self.send_json({"url": auth_url, "state": state})
        if parsed.path == "/api/tokens":
            return self.send_json(load_json(TOKEN_PATH, {}))
        if parsed.path == "/api/schedule":
            return self.send_json(load_json(SCHEDULE_PATH, DEFAULT_SCHEDULE))
        if parsed.path == "/api/upload/status":
            return self.send_json(upload_state)
        if parsed.path == "/api/logs":
            return self.send_json({"logs": tail_logs()})
        if parsed.path == "/api/system/ffmpeg":
            return self.send_json(get_ffmpeg_info())
        if parsed.path == "/api/baidu/status":
            return self.send_json(get_baidu_cli_status())
        if parsed.path == "/auth/callback":
            return self.handle_callback(parsed)

        return self.serve_static(parsed.path)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/config":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return self.send_json({"error": "JSON 解析失败"}, status=HTTPStatus.BAD_REQUEST)

            raw_map = body.get("productGoodsMap") or {}
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
            next_conf = {
                "clientId": body.get("clientId", ""),
                "clientSecret": body.get("clientSecret", ""),
                "redirectUri": body.get("redirectUri", ""),
                "authBase": body.get("authBase", DEFAULT_CONFIG["authBase"]),
                "goodsId": str(body.get("goodsId", DEFAULT_GOODS_ID)).strip() or DEFAULT_GOODS_ID,
                "productGoodsMap": product_map,
                "videoDesc": body.get("videoDesc", ""),
                "requireAuth": bool(body.get("requireAuth", True)),
                "downloadEnabled": bool(body.get("downloadEnabled", True)),
                "downloadTime": body.get("downloadTime", DEFAULT_CONFIG["downloadTime"]),
                "downloadRemoteRoot": normalize_remote_root(body.get("downloadRemoteRoot", "")),
                "downloadLocalRoot": body.get("downloadLocalRoot", str(ROOT / "video")),
                "baiduCliPath": body.get("baiduCliPath", ""),
            }
            save_json(CONFIG_PATH, next_conf)
            append_log("配置已保存")
            return self.send_json({"ok": True, "config": next_conf})
        if parsed.path == "/api/schedule":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return self.send_json({"error": "JSON 解析失败"}, status=HTTPStatus.BAD_REQUEST)
            schedule = load_json(SCHEDULE_PATH, DEFAULT_SCHEDULE)
            schedule["video_root"] = body.get("video_root", schedule.get("video_root", str(ROOT / "video")))
            schedule["time_zone"] = body.get("time_zone", schedule.get("time_zone", "Asia/Shanghai"))
            shops = body.get("shops") or schedule.get("shops", {})
            schedule["shops"] = shops
            save_json(SCHEDULE_PATH, schedule)
            append_log("上传计划已保存")
            return self.send_json({"ok": True, "schedule": schedule})
        if parsed.path == "/api/oauth/exchange":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return self.send_json({"error": "JSON 解析失败"}, status=HTTPStatus.BAD_REQUEST)
            code = body.get("code")
            state = body.get("state")
            if not code:
                return self.send_json({"error": "缺少 code"}, status=HTTPStatus.BAD_REQUEST)
            config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
            if not config.get("clientId") or not config.get("clientSecret") or not config.get("redirectUri"):
                return self.send_json({"error": "缺少 clientId/clientSecret/redirectUri"}, status=HTTPStatus.BAD_REQUEST)
            tokens = load_tokens()
            if tokens.get("lastAuthState") and state and tokens["lastAuthState"] != state:
                append_log("授权 state 不匹配，继续尝试换取 token", level="error")
            try:
                token_resp = self.exchange_token(
                    client_id=config["clientId"],
                    client_secret=config["clientSecret"],
                    redirect_uri=config["redirectUri"],
                    code=code,
                )
                tokens["lastAuth"] = {
                    **token_resp,
                    "receivedAt": self.date_time_string(),
                    "state": state,
                }
                tokens["lastAuth"] = add_refresh_meta(tokens["lastAuth"])
                save_json(TOKEN_PATH, tokens)
                append_log("手动 code 换取 token 成功")
                return self.send_json({"ok": True})
            except Exception as exc:  # noqa: BLE001
                append_log(f"手动 code 换取失败: {exc}", level="error")
                return self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_GATEWAY)
        if parsed.path == "/api/upload/scan":
            append_log("收到立即扫描请求")
            Thread(target=run_scan_once, kwargs={"manual": True}, daemon=True).start()
            return self.send_json({"ok": True, "message": "已触发立即扫描"})
        if parsed.path == "/api/upload/reset":
            upload_state["tasks"] = []
            persist_state()
            append_log("已清空上传记录")
            return self.send_json({"ok": True})
        if parsed.path == "/api/download/manual":
            append_log("收到手动下载请求")
            Thread(target=run_download_once, kwargs={"manual": True}, daemon=True).start()
            return self.send_json({"ok": True, "message": "已触发手动下载"})
        if parsed.path == "/api/logs/clear":
            log_buffer.clear()
            try:
                LOG_PATH.write_text("", encoding="utf-8")
            except Exception:
                pass
            return self.send_json({"ok": True})

        self.send_error(HTTPStatus.NOT_FOUND)

    def handle_callback(self, parsed):
        query = urllib.parse.parse_qs(parsed.query)
        code = query.get("code", [None])[0]
        state = query.get("state", [None])[0]
        if not code:
            return self.send_html(
                HTTPStatus.BAD_REQUEST, self.render_message("缺少授权 code，无法交换访问令牌。")
            )

        config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        if not config.get("clientId") or not config.get("clientSecret") or not config.get("redirectUri"):
            return self.send_html(
                HTTPStatus.BAD_REQUEST,
                self.render_message("未找到有效配置，请先在首页填写并保存应用信息。"),
            )

        tokens = load_json(TOKEN_PATH, {})
        if tokens.get("lastAuthState") and state and tokens["lastAuthState"] != state:
            return self.send_html(HTTPStatus.BAD_REQUEST, self.render_message("state 校验失败，请重新发起授权。"))

        try:
            token_resp = self.exchange_token(
                client_id=config["clientId"],
                client_secret=config["clientSecret"],
                redirect_uri=config["redirectUri"],
                code=code,
            )
            tokens["lastAuth"] = {
                **token_resp,
                "receivedAt": self.date_time_string(),
                "state": state,
            }
            tokens["lastAuth"] = add_refresh_meta(tokens["lastAuth"])
            save_json(TOKEN_PATH, tokens)
            return self.send_html(
                HTTPStatus.OK, self.render_message("授权成功，已经拿到访问令牌。", token_resp)
            )
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"token exchange failed: {exc}\n")
            return self.send_html(
                HTTPStatus.BAD_GATEWAY, self.render_message(f"换取访问令牌失败：{exc}")
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
            raise RuntimeError(f"网络错误: {exc.reason}") from exc
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
            <title>拼多多授权回调</title>
            <style>
              body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; padding: 32px; line-height: 1.6; }}
              .box {{ max-width: 720px; margin: 0 auto; padding: 24px; border-radius: 12px; background: #f7f7fa; }}
              pre {{ background: #111827; color: #e5e7eb; padding: 16px; border-radius: 8px; overflow: auto; }}
              a {{ color: #2563eb; text-decoration: none; }}
            </style>
          </head>
          <body>
            <div class="box">
              <h2>授权结果</h2>
              <p>{self.escape_html(message)}</p>
              {body}
              <p><a href="/">返回配置页</a></p>
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
    port = int(os.environ.get("PORT", "3000"))
    server = HTTPServer(("", port), Handler)
    start_refresh_worker()
    start_upload_scheduler()
    start_download_scheduler()
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


def start_upload_scheduler():
    thread = Thread(target=upload_scheduler_loop, daemon=True)
    thread.start()


def start_download_scheduler():
    thread = Thread(target=download_scheduler_loop, daemon=True)
    thread.start()


def refresh_loop():
    while not stop_event.wait(60):
        tokens = load_json(TOKEN_PATH, {})
        last = tokens.get("lastAuth") or {}
        refresh_token = last.get("refresh_token")
        if not refresh_token:
            continue
        next_refresh = last.get("nextRefreshAt") or 0
        now = time.time()
        if now < next_refresh:
            continue
        config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        if not config.get("clientId") or not config.get("clientSecret"):
            continue
        try:
            resp = refresh_token_call(
                client_id=config["clientId"],
                client_secret=config["clientSecret"],
                refresh_token=refresh_token,
            )
            tokens["lastAuth"] = add_refresh_meta(
                {
                    **last,
                    **resp,
                    "refreshedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                }
            )
            save_json(TOKEN_PATH, tokens)
            sys.stdout.write(f"{time.asctime()} - token auto-refreshed\n")
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"{time.asctime()} - token auto-refresh failed: {exc}\n")
            # schedule a retry soon
            tokens["lastAuth"]["nextRefreshAt"] = now + 120
            tokens["lastAuth"]["nextRefreshAtIso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 120))
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


def persist_state():
    save_json(STATE_PATH, upload_state)


def load_download_state():
    global download_state
    download_state = load_json(DOWNLOAD_STATE_PATH, {"files": {}, "auto_runs": {}})
    download_state.setdefault("files", {})
    download_state.setdefault("auto_runs", {})


def persist_download_state():
    save_json(DOWNLOAD_STATE_PATH, download_state)


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
    sys.stdout.write(f"[{entry['ts']}] {level.upper()}: {message}\n")


def upload_scheduler_loop():
    while not stop_event.wait(30):
        run_scan_once(manual=False)


def download_scheduler_loop():
    while not stop_event.wait(60):
        run_download_once(manual=False)


def resolve_baidu_cli(config: dict) -> str:
    custom = (config.get("baiduCliPath") or "").strip()
    if custom and Path(custom).exists():
        return custom
    if os.name == "nt":
        return shutil.which("BaiduPCS-Go.exe") or ""
    return shutil.which("BaiduPCS-Go") or ""


def get_baidu_cli_status() -> dict:
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    cli_path = resolve_baidu_cli(config)
    if not cli_path:
        return {"available": False, "logged_in": False, "message": "未找到 BaiduPCS-Go"}
    cmd_variants = [[cli_path, "who"], [cli_path, "user"], [cli_path, "account"]]
    output = ""
    last_err = ""
    for cmd in cmd_variants:
        result = subprocess.run(cmd, capture_output=True, text=True)
        out = (result.stdout or result.stderr or "").strip()
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


def list_baidu_dir(cli_path: str, remote_dir: str, recursive: bool = False):
    cmd = [cli_path, "ls", "--json"]
    if recursive:
        cmd.append("--recursive")
    cmd.append(remote_dir)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "").strip() or "ls 失败")
    return parse_baidu_list(result.stdout)


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


def collect_remote_videos(cli_path: str, remote_dir: str):
    entries = list_baidu_dir(cli_path, remote_dir, recursive=True)
    if not entries:
        entries = list_baidu_dir(cli_path, remote_dir, recursive=False)
        result = []
        for raw in entries:
            entry = normalize_remote_entry(raw)
            if entry["is_dir"]:
                result.extend(collect_remote_videos(cli_path, entry["path"]))
            else:
                result.append(entry)
        entries = result
    files = []
    for raw in entries:
        entry = normalize_remote_entry(raw)
        if entry["is_dir"]:
            continue
        if not entry["path"]:
            continue
        ext = Path(entry["path"]).suffix.lower()
        if ext in VIDEO_EXTS:
            files.append(entry)
    return files


def download_remote_file(cli_path: str, remote_path: str, local_root: Path):
    local_root.mkdir(parents=True, exist_ok=True)
    cmd_variants = [
        [cli_path, "download", remote_path, "--outdir", str(local_root)],
        [cli_path, "download", remote_path, "-o", str(local_root)],
        [cli_path, "download", "-o", str(local_root), remote_path],
    ]
    last_err = ""
    for cmd in cmd_variants:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return
        last_err = (result.stderr or result.stdout or "").strip()
    raise RuntimeError(last_err or "download 失败")


def run_download_once(manual: bool = False):
    if manual:
        append_log("手动触发下载")
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    if not config.get("downloadEnabled", True) and not manual:
        return
    remote_root = normalize_remote_root(config.get("downloadRemoteRoot"))
    if not remote_root:
        if manual:
            append_log("未配置远端根目录，跳过下载", level="error")
        return
    remote_root = remote_root.rstrip("/")
    remote_video_dir = f"{remote_root}/video" if remote_root else "/video"
    local_root = Path(config.get("downloadLocalRoot") or str(ROOT / "video"))
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
            return
        if current_seconds > start_seconds + AUTO_RUN_WINDOW_SECONDS:
            return
        auto_runs = download_state.setdefault("auto_runs", {})
        if auto_runs.get("download") == today_str:
            return
        auto_runs["download"] = today_str
        persist_download_state()
        append_log(f"到达下载时间 {start_time}，自动开始下载")

    cli_path = resolve_baidu_cli(config)
    if not cli_path:
        append_log("未找到 BaiduPCS-Go，可在配置中指定路径", level="error")
        return
    append_log(f"扫描远端目录: {remote_video_dir}")
    try:
        remote_files = collect_remote_videos(cli_path, remote_video_dir)
    except Exception as exc:
        append_log(f"远端扫描失败: {exc}", level="error")
        return
    if not remote_files:
        append_log("远端未发现视频文件")
        return

    files_state = download_state.setdefault("files", {})
    new_files = []
    for entry in remote_files:
        path = entry.get("path") or ""
        if not path:
            continue
        if path not in files_state:
            new_files.append(entry)
    append_log(f"发现远端视频 {len(remote_files)} 个，新增 {len(new_files)} 个")
    for entry in new_files:
        remote_path = entry["path"]
        rel_path = remote_path.replace(remote_video_dir, "").lstrip("/")
        target_dir = local_root / Path(rel_path).parent
        try:
            append_log(f"开始下载 {remote_path}")
            download_remote_file(cli_path, remote_path, target_dir)
            files_state[remote_path] = {
                "size": entry.get("size", 0),
                "mtime": entry.get("mtime", 0),
                "downloaded_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "local_dir": str(target_dir),
            }
            persist_download_state()
            append_log(f"下载完成 {remote_path}")
        except Exception as exc:
            append_log(f"下载失败 {remote_path}: {exc}", level="error")


def run_scan_once(manual: bool = False):
    if manual:
        append_log("手动触发扫描上传")
    schedule = normalize_schedule(load_json(SCHEDULE_PATH, DEFAULT_SCHEDULE))
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    require_auth = config.get("requireAuth", True)
    if require_auth:
        tokens = load_tokens()
        access_token = tokens.get("lastAuth", {}).get("access_token") or tokens.get("access_token")
        if not access_token:
            global last_auth_warn_ts
            now = time.time()
            if manual or now - last_auth_warn_ts > 120:
                append_log("未授权或 access_token 缺失，已暂停上传，请先完成授权。", level="error")
                last_auth_warn_ts = now
            return
    shops = schedule.get("shops", {})
    video_root = Path(schedule.get("video_root", ROOT / "video"))
    if manual:
        append_log(f"扫描根目录: {video_root}")
    now_struct = time.localtime()
    today_str = time.strftime("%Y%m%d", now_struct)
    current_seconds = now_struct.tm_hour * 3600 + now_struct.tm_min * 60 + now_struct.tm_sec
    auto_runs = upload_state.setdefault("auto_runs", {})

    for shop, cfg in shops.items():
        if not cfg.get("enabled", True):
            if manual:
                append_log(f"[{shop}] 已禁用，跳过")
            continue
        start_time = cfg.get("start_time", "09:00")
        try:
            h, m = [int(x) for x in start_time.split(":")]
            start_seconds = h * 3600 + m * 60
        except Exception:
            start_seconds = 9 * 3600
        if not manual and current_seconds < start_seconds:
            if manual:
                append_log(f"[{shop}] 未到开始时间 {start_time}，跳过")
            continue
        if not manual:
            if current_seconds > start_seconds + AUTO_RUN_WINDOW_SECONDS:
                continue
            if auto_runs.get(shop) == today_str:
                continue
            auto_runs[shop] = today_str
            persist_state()
            append_log(f"[{shop}] 到达开始时间 {start_time}，自动开始扫描")
        daily_limit = int(cfg.get("daily_limit", 50))
        interval_seconds = int(cfg.get("interval_seconds", 300))

        shop_dir = video_root / today_str / shop
        if not shop_dir.exists():
            if manual:
                append_log(f"[{shop}] 目录不存在：{shop_dir}")
            continue
        product_dirs = sorted([p for p in shop_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
        if not product_dirs:
            if manual:
                append_log(f"[{shop}] 未找到产品目录：{shop_dir}")
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
                append_log(f"[{shop}] 目录为空或无视频文件：{shop_dir}")
            continue
        if manual:
            append_log(f"[{shop}] 发现文件 {len(files)} 个（产品目录 {len(product_dirs)} 个）")

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
            if manual:
                append_log(f"[{shop}] 已有进行中的上传任务，等待完成后再继续")
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
                            append_log(f"[{shop}] 间隔未到（{interval_seconds}s），跳过")
                        continue
                except Exception:
                    pass

        processed_files = {
            t.get("rel_path") or t.get("filename") for t in today_tasks if t.get("status") in ("done", "processing")
        }
        processed_names = {Path(name).name for name in processed_files if name}
        candidate = None
        for rel_path, file_path, product_name in files:
            if rel_path not in processed_files and file_path.name not in processed_names:
                candidate = (rel_path, file_path, product_name)
                break
        if not candidate:
            if manual:
                append_log(f"[{shop}] 今日文件已处理完毕，跳过")
            continue
        rel_path, candidate_path, product_name = candidate
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
        upload_state["tasks"].append(task)
        persist_state()
        append_log(f"[{shop}] 开始上传 {rel_path}")
        try:
            result = upload_video_file(candidate_path, product_name=product_name)
            task["status"] = "done"
            if isinstance(result, dict):
                task["vid"] = result.get("vid")
                task["video_id"] = result.get("video_id") or result.get("vid")
                task["cover_url"] = result.get("cover_url")
                task["message"] = "上传发布完成"
                append_log(
                    f"[{shop}] 上传发布完成 {rel_path} vid={result.get('vid')} video_id={result.get('video_id')}"
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


def upload_video_file(file_path: Path, product_name: str | None = None):
    tokens = load_tokens()
    access_token = tokens.get("lastAuth", {}).get("access_token") or tokens.get("access_token")
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    client_id = config.get("clientId") or config.get("client_id")
    client_secret = config.get("clientSecret") or config.get("client_secret")
    product_map = config.get("productGoodsMap") if isinstance(config.get("productGoodsMap"), dict) else {}
    goods_id = config.get("goodsId") or DEFAULT_GOODS_ID
    if product_name:
        mapped_id = product_map.get(product_name)
        if mapped_id:
            goods_id = mapped_id
        else:
            append_log(f"未找到产品商品ID映射，产品={product_name}，使用默认 goods_id={goods_id}")
    video_desc = (config.get("videoDesc") or "").strip()
    if not (client_id and client_secret and access_token):
        append_log(
            f"缺少 client_id/client_secret/access_token，当前 client_id={bool(client_id)} client_secret={bool(client_secret)} access_token={bool(access_token)}",
            level="error",
        )
        raise RuntimeError("缺少 client_id/client_secret/access_token")

    mime_type = mimetypes.guess_type(file_path.name)[0] or "video/mp4"
    file_size = file_path.stat().st_size
    append_log(f"准备上传文件: {file_path.name}, size={file_size} bytes, mime={mime_type}")
    init_resp = call_pdd_api(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        type_name="pdd.live.video.mall.upload.part.init",
        extra_params={"content_type": mime_type},
        files=None,
    )
    upload_sign = init_resp.get("upload_sign") or init_resp.get("response", {}).get("upload_sign")
    if not upload_sign:
        raise RuntimeError(f"init 未返回 upload_sign: {init_resp}")
    append_log(f"init 返回 upload_sign={str(upload_sign)[:12]}***")

    # keep below 20MB limit in doc; use 19MB to avoid unit ambiguity
    chunk_size = 19 * 1024 * 1024
    total_parts = (file_size + chunk_size - 1) // chunk_size
    append_log(f"开始分片上传，共 {total_parts} 片")
    with file_path.open("rb") as f:
        for idx, chunk in enumerate(iter(lambda: f.read(chunk_size), b"")):
            part_num = str(idx + 1)
            append_log(f"上传分片 {part_num}/{total_parts} size={len(chunk)}")
            call_pdd_api(
                client_id=client_id,
                client_secret=client_secret,
                access_token=access_token,
                type_name="pdd.live.video.mall.upload.part",
                extra_params={"part_num": part_num, "upload_sign": upload_sign},
                files={"part_file": (file_path.name, chunk, mime_type)},
            )

    complete_resp = call_pdd_api(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        type_name="pdd.live.video.mall.upload.part.complete",
        extra_params={"upload_sign": upload_sign},
        files=None,
    )
    vid = complete_resp.get("video_id") or complete_resp.get("response", {}).get("video_id")
    append_log(f"complete 返回 vid={vid}")
    if not vid:
        raise RuntimeError(f"complete 未返回 vid: {complete_resp}")

    append_log("开始生成封面")
    cover_path = extract_cover_image(file_path)
    append_log(f"封面已生成 {cover_path.name}")
    cover_url = upload_image_file(
        cover_path, client_id=client_id, client_secret=client_secret, access_token=access_token
    )
    append_log(f"封面上传完成 url={cover_url}")
    if video_desc:
        append_log(f"开始发布 vid={vid} goods_id={goods_id} desc={video_desc[:40]}")
    else:
        append_log(f"开始发布 vid={vid} goods_id={goods_id}")
    video_id = publish_video(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        vid=vid,
        cover_url=cover_url,
        goods_id=goods_id,
        desc=video_desc,
    )
    append_log(f"发布完成 video_id={video_id}")
    try:
        if cover_path.exists():
            cover_path.unlink()
    except Exception:
        pass
    return {"vid": vid, "video_id": video_id, "cover_url": cover_url}


def extract_cover_image(video_path: Path) -> Path:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("未找到 ffmpeg，无法截取封面，请先安装 ffmpeg")
    output_path = COVER_DIR / f"{video_path.stem}_{uuid.uuid4().hex[:8]}.jpg"
    cmd = [
        ffmpeg,
        "-y",
        "-ss",
        "0",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(output_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 or not output_path.exists():
        err = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"封面截取失败: {err or 'ffmpeg 执行失败'}")
    return output_path


def upload_image_file(image_path: Path, *, client_id, client_secret, access_token):
    mime_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"
    data = image_path.read_bytes()
    append_log(f"开始上传封面 {image_path.name}, size={len(data)} bytes, mime={mime_type}")
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
        raise RuntimeError(f"封面上传未返回 url: {resp}")
    return url


def publish_video(*, client_id, client_secret, access_token, vid, cover_url, goods_id, desc=""):
    payload = {"cover": cover_url, "goods_id": int(goods_id), "vid": vid}
    if desc:
        payload["desc"] = desc
    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    append_log(f"发布参数 request={payload_str}")
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
            append_log(f"发布返回 success=true 但 error_code={error_code}: {json.dumps(resp, ensure_ascii=False)}")
        return video_id
    if error_code and error_code != "0":
        append_log(f"发布响应异常: {json.dumps(resp, ensure_ascii=False)}", level="error")
        error_msg = resp.get("error_msg") or resp.get("msg") or resp.get("error_desc") or "unknown"
        raise RuntimeError(f"{error_code}:{error_msg}")
    if success_flag is False:
        append_log(f"发布响应失败: {json.dumps(resp, ensure_ascii=False)}", level="error")
        raise RuntimeError(resp.get("error_msg") or resp.get("msg") or "发布失败")
    if not video_id:
        raise RuntimeError(f"发布返回缺少 video_id: {resp}")
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
                    f"接口错误 {code}:{msg} request_id={request_id}，{delay}s 后重试 ({attempt}/{retries})",
                    level="error",
                )
                time.sleep(delay)
                continue
            suffix = f" request_id={request_id}" if request_id else ""
            raise RuntimeError(f"{code}:{msg}{suffix}")
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
    return entries


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
            output = subprocess.check_output([path, "-version"], text=True, stderr=subprocess.STDOUT, timeout=3)
            info["version"] = output.splitlines()[0].strip() if output else ""
        except Exception:
            info["version"] = ""
    ffmpeg_cache["info"] = info
    ffmpeg_cache["ts"] = now
    return info


if __name__ == "__main__":
    run()
