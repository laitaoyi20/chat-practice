"""
第 4 组 · 局域网聊天室 V3

技术栈：FastAPI + WebSocket + SQLite + 原生 HTML/JavaScript

启动：
    uv run python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

V3 功能：
    1. 已读 / 未读统计（按 last_read_id）
    2. 消息撤回（仅本人、5 分钟内、只撤一次）
    3. 离线补消息：重连后下发未读区间的消息
    4. 发送失败重试（client_id 去重，重试不会产生重复消息）
    5. 连接状态提示与自动重连（指数退避）
    6. 未读数量显示 + 回到上次阅读位置（位置标记 6 秒后消失）
    7. 输入校验与可理解的拒绝提示
    8. 新用户可阅读最近 30 条历史；加载历史期间的新消息排队稍后渲染
    9. 全部状态落在 SQLite（chat.db）

数据表：
    users        (id, ip, name, created_at, last_seen_at)   身份 = ip + name
    messages     (id, user_id, name, ip, text, created_at, status, recalled_at, client_id)
    read_state   (user_id, last_read_id, updated_at)

路由：
    GET  /          聊天页面
    GET  /about     规则说明
    GET  /health    在线人数与消息总数
    WS   /ws        长连接
"""

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).with_name("chat.db")

MAX_TEXT = 500            # 单条消息最大字数
MIN_INTERVAL = 0.4        # 两条消息最小间隔（秒），防刷屏
RECALL_WINDOW = 300       # 撤回时限（秒）
HISTORY_LIMIT = 30        # 新用户可读的历史条数
MAX_NICK = 16             # 昵称最大长度


# ---------------------------------------------------------------------------
# 数据库层
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ip           TEXT    NOT NULL,
    name         TEXT    NOT NULL,
    created_at   REAL    NOT NULL,
    last_seen_at REAL    NOT NULL,
    UNIQUE(ip, name)
);

CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    name        TEXT    NOT NULL,
    ip          TEXT    NOT NULL,
    text        TEXT    NOT NULL,
    created_at  REAL    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'active',   -- active | recalled
    recalled_at REAL,
    client_id   TEXT
);

CREATE TABLE IF NOT EXISTS read_state (
    user_id      INTEGER PRIMARY KEY REFERENCES users(id),
    last_read_id INTEGER NOT NULL DEFAULT 0,
    updated_at   REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_msg_client
    ON messages(user_id, client_id) WHERE client_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_msg_id ON messages(id);
"""

_lock = threading.Lock()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row
_conn.execute("PRAGMA journal_mode=WAL")
_conn.executescript(SCHEMA)
_conn.commit()


def upsert_user(ip: str, name: str) -> int:
    """按 (ip, name) 找用户，没有就建。返回 user_id。"""
    now = time.time()
    with _lock:
        row = _conn.execute(
            "SELECT id FROM users WHERE ip=? AND name=?", (ip, name)
        ).fetchone()
        if row:
            _conn.execute("UPDATE users SET last_seen_at=? WHERE id=?", (now, row["id"]))
            _conn.commit()
            return row["id"]
        cur = _conn.execute(
            "INSERT INTO users(ip,name,created_at,last_seen_at) VALUES(?,?,?,?)",
            (ip, name, now, now),
        )
        _conn.commit()
        return int(cur.lastrowid)


def insert_message(user_id: int, name: str, ip: str, text: str, client_id: str | None):
    """写入一条消息。

    若同一 user 用同一 client_id 重复提交（网络重试），返回已存在的那条，
    不新增记录 —— 这是"重试不产生重复消息"的实现点。
    返回 (message_id, created_at, existed)
    """
    now = time.time()
    with _lock:
        if client_id:
            row = _conn.execute(
                "SELECT id, created_at FROM messages WHERE user_id=? AND client_id=?",
                (user_id, client_id),
            ).fetchone()
            if row:
                return int(row["id"]), float(row["created_at"]), True

        cur = _conn.execute(
            "INSERT INTO messages(user_id,name,ip,text,created_at,status,client_id)"
            " VALUES(?,?,?,?,?, 'active', ?)",
            (user_id, name, ip, text, now, client_id),
        )
        _conn.commit()
        return int(cur.lastrowid), now, False


def find_by_client_id(user_id: int, client_id: str) -> tuple[int, float] | None:
    """按 client_id 查已存在的消息，用于重试去重。"""
    if not client_id:
        return None
    with _lock:
        row = _conn.execute(
            "SELECT id, created_at FROM messages WHERE user_id=? AND client_id=?",
            (user_id, client_id),
        ).fetchone()
        if row is None:
            return None
        return int(row["id"]), float(row["created_at"])


def fetch_history(limit: int, me_user_id: int) -> list[dict]:
    """最近 limit 条消息，按时间正序返回。撤回的消息只给状态，不给原文。"""
    with _lock:
        rows = _conn.execute(
            "SELECT id, user_id, name, ip, text, created_at, status"
            " FROM messages ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()

    out = []
    for row in reversed(rows):
        recalled = row["status"] == "recalled"
        out.append({
            "id": row["id"],
            "name": row["name"],
            "ip": row["ip"],
            "text": None if recalled else row["text"],
            "created_at": row["created_at"],
            "recalled": recalled,
            "mine": row["user_id"] == me_user_id,
        })
    return out


def recall_message(message_id: int, user_id: int) -> tuple[bool, str]:
    """撤回。返回 (是否成功, 失败原因)。"""
    with _lock:
        row = _conn.execute(
            "SELECT id, user_id, created_at, status FROM messages WHERE id=?",
            (message_id,),
        ).fetchone()

        if row is None:
            return False, "消息不存在或已被清理"
        if row["user_id"] != user_id:
            return False, "只能撤回自己发送的消息"
        if row["status"] == "recalled":
            return False, "这条消息已经撤回过了"
        if time.time() - row["created_at"] > RECALL_WINDOW:
            return False, f"超过 {RECALL_WINDOW // 60} 分钟，无法撤回"

        _conn.execute(
            "UPDATE messages SET status='recalled', recalled_at=? WHERE id=?",
            (time.time(), message_id),
        )
        _conn.commit()
        return True, ""


def get_last_read(user_id: int) -> int:
    with _lock:
        row = _conn.execute(
            "SELECT last_read_id FROM read_state WHERE user_id=?", (user_id,)
        ).fetchone()
        return int(row["last_read_id"]) if row else 0


def set_last_read(user_id: int, message_id: int) -> int:
    """已读位置只增不减，返回最新的位置。"""
    now = time.time()
    with _lock:
        row = _conn.execute(
            "SELECT last_read_id FROM read_state WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            _conn.execute(
                "INSERT INTO read_state(user_id,last_read_id,updated_at) VALUES(?,?,?)",
                (user_id, message_id, now),
            )
        else:
            if message_id <= row["last_read_id"]:
                return int(row["last_read_id"])
            _conn.execute(
                "UPDATE read_state SET last_read_id=?, updated_at=? WHERE user_id=?",
                (message_id, now, user_id),
            )
        _conn.commit()
    return message_id


def count_unread(user_id: int, last_read_id: int) -> int:
    with _lock:
        row = _conn.execute(
            "SELECT COUNT(*) AS n FROM messages"
            " WHERE id > ? AND user_id != ? AND status = 'active'",
            (last_read_id, user_id),
        ).fetchone()
        return int(row["n"])


def count_messages() -> int:
    with _lock:
        row = _conn.execute("SELECT COUNT(*) AS n FROM messages").fetchone()
        return int(row["n"])


# ---------------------------------------------------------------------------
# 连接状态（内存，不入库）
# ---------------------------------------------------------------------------

@dataclass
class Client:
    ws: WebSocket
    ip: str
    seq: int
    name: str | None = None
    user_id: int | None = None
    same_ip_as: str | None = None
    last_sent_at: float = 0.0


clients: dict[WebSocket, Client] = {}
_seq = 0


def next_seq() -> int:
    global _seq
    _seq += 1
    return _seq


def normalize_ip(raw: str | None) -> str:
    ip = raw or "unknown"
    if ip.startswith("::ffff:"):
        ip = ip[7:]
    if ip == "::1":
        ip = "127.0.0.1"
    return ip


def ip_members(ip: str) -> list[Client]:
    return sorted(
        (c for c in clients.values() if c.ip == ip and c.name),
        key=lambda c: c.seq,
    )


def online_ip_count() -> int:
    return len({c.ip for c in clients.values() if c.name})


def renormalize_ip(ip: str) -> None:
    """某个 IP 的成员变动后，重算主账号与关联关系。

    规则：该 IP 入场最早的人为主账号；同名成员视为本人多开，不标注关联。
    """
    members = ip_members(ip)
    if not members:
        return
    primary = members[0]
    primary.same_ip_as = None
    for member in members[1:]:
        member.same_ip_as = None if member.name == primary.name else primary.name


# ---------------------------------------------------------------------------
# 广播
# ---------------------------------------------------------------------------

async def broadcast(payload: dict) -> None:
    dead: list[Client] = []
    for client in list(clients.values()):
        if not client.name:
            continue
        try:
            await client.ws.send_json(payload)
        except Exception:
            dead.append(client)
    for client in dead:
        clients.pop(client.ws, None)


async def broadcast_system(message: str) -> None:
    await broadcast({"type": "system", "message": message})


async def broadcast_roster() -> None:
    users = [
        {"name": c.name, "ip": c.ip, "same_ip_as": c.same_ip_as}
        for c in sorted(clients.values(), key=lambda c: c.seq)
        if c.name
    ]
    await broadcast({"type": "roster", "online": online_ip_count(), "users": users})


# ---------------------------------------------------------------------------
# 业务处理
# ---------------------------------------------------------------------------

async def send_history(me: Client) -> None:
    """下发最近 30 条历史 + 已读位置 + 未读数。"""
    me_user_id = me.user_id or 0
    last_read = get_last_read(me_user_id)
    history = fetch_history(HISTORY_LIMIT, me_user_id)
    unread = count_unread(me_user_id, last_read)

    await me.ws.send_json({
        "type": "history",
        "messages": history,
        "last_read_id": last_read,
        "unread": unread,
        "limit": HISTORY_LIMIT,
    })


async def handle_join(me: Client, data: dict) -> None:
    """入场：昵称校验 → 同 IP 警告 → 建档 → 下发历史。"""
    raw = (data.get("name") or "").strip()
    if not raw:
        await me.ws.send_json({"type": "reject", "reason": "昵称不能为空"})
        return
    if len(raw) > MAX_NICK:
        await me.ws.send_json({"type": "reject", "reason": f"昵称最多 {MAX_NICK} 个字"})
        return
    name = raw

    members = ip_members(me.ip)
    # 只有「名字不同」的同 IP 成员才需要警告；同名视为本人多开或刷新重连
    others = [m for m in members if m.name != name]

    if others and not data.get("confirm"):
        await me.ws.send_json({
            "type": "duplicate",
            "ip": me.ip,
            "existing_name": others[0].name,
            "online": online_ip_count(),
        })
        return

    me.name = name
    me.user_id = upsert_user(me.ip, name)
    me.same_ip_as = others[0].name if others else None
    renormalize_ip(me.ip)

    await me.ws.send_json({
        "type": "welcome",
        "name": me.name,
        "ip": me.ip,
        "same_ip_as": me.same_ip_as,
        "online": online_ip_count(),
    })

    suffix = f"（和 {me.same_ip_as} 为同一 IP）" if me.same_ip_as else ""
    await broadcast_system(f"{me.name} 加入了聊天室{suffix}")
    await broadcast_roster()
    await send_history(me)


async def reject_chat(me: Client, client_id: str | None, reason: str) -> None:
    await me.ws.send_json({
        "type": "reject",
        "client_id": client_id,
        "reason": reason,
    })


async def handle_chat(me: Client, data: dict) -> None:
    """发言：校验 → 入库 → ack 发送者 → 广播所有人。"""
    client_id = data.get("client_id")
    text = (data.get("text") or "").strip()

    if not me.name or not me.user_id:
        await reject_chat(me, client_id, "请先设置昵称再发言")
        return
    if not text:
        await reject_chat(me, client_id, "不能发送空消息")
        return
    if len(text) > MAX_TEXT:
        await reject_chat(me, client_id, f"消息过长：{len(text)}/{MAX_TEXT} 字")
        return

    # 去重要放在限流之前：网络重试往往间隔极短，
    # 若先限流，用户点「重试」会被误判成刷屏
    existing = find_by_client_id(me.user_id, client_id)
    if existing:
        await me.ws.send_json({
            "type": "ack",
            "client_id": client_id,
            "id": existing[0],
            "created_at": existing[1],
        })
        return

    now = time.time()
    if now - me.last_sent_at < MIN_INTERVAL:
        await reject_chat(me, client_id, "发送太快，请稍后再试")
        return
    me.last_sent_at = now

    message_id, created_at, existed = insert_message(
        me.user_id, me.name, me.ip, text, client_id
    )

    await me.ws.send_json({
        "type": "ack",
        "client_id": client_id,
        "id": message_id,
        "created_at": created_at,
    })

    if existed:
        return   # 重试导致重复提交：只回 ack，不再广播

    await broadcast({
        "type": "chat",
        "id": message_id,
        "client_id": client_id,
        "name": me.name,
        "text": text,
        "created_at": created_at,
        "ip": me.ip,
        "same_ip_as": me.same_ip_as,
    })


async def handle_recall(me: Client, data: dict) -> None:
    if not me.user_id:
        return

    try:
        message_id = int(data.get("id"))
    except (TypeError, ValueError):
        await me.ws.send_json({"type": "error", "message": "撤回失败：消息编号无效"})
        return

    ok, reason = recall_message(message_id, me.user_id)
    if not ok:
        await me.ws.send_json({"type": "error", "message": f"撤回失败：{reason}"})
        return

    await broadcast({"type": "recall", "id": message_id, "by": me.name})


async def handle_read(me: Client, data: dict) -> None:
    if not me.user_id:
        return
    try:
        message_id = int(data.get("last_message_id"))
    except (TypeError, ValueError):
        return

    last_read = set_last_read(me.user_id, message_id)
    await me.ws.send_json({
        "type": "unread",
        "count": count_unread(me.user_id, last_read),
        "last_read_id": last_read,
    })


# ---------------------------------------------------------------------------
# HTTP 路由
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "online": online_ip_count(),
        "connections": len([c for c in clients.values() if c.name]),
        "messages": count_messages(),
    }


@app.get("/")
def home() -> HTMLResponse:
    return HTMLResponse(PAGE)


@app.get("/about")
def about() -> HTMLResponse:
    html = """
    <h1>聊天室规则</h1>
    <ul>
      <li>在线人数按<b>不同 IP</b> 统计：同一 IP 开多个网页只算 1 人。</li>
      <li>进入需要昵称（1~16 字）；同 IP 用第二个昵称会先警告，确认后标记「和某某为同一 IP」。</li>
      <li>消息最长 500 字，两条之间至少间隔 0.4 秒。</li>
      <li>只能撤回自己的消息，且必须在 5 分钟内；撤回后所有人看到「已撤回」。</li>
      <li>新进入可查看最近 30 条历史；未读消息会标出上次阅读位置。</li>
      <li>断线会自动重连；发送失败可点击「重试」，不会产生重复消息。</li>
    </ul>
    <p>在线人数接口：本页地址后加 <code>/health</code></p>
    """
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    await websocket.accept()

    me = Client(
        ws=websocket,
        ip=normalize_ip(websocket.client.host if websocket.client else None),
        seq=next_seq(),
    )
    clients[websocket] = me

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "消息格式错误"})
                continue

            kind = data.get("type")
            if kind == "join":
                await handle_join(me, data)
            elif kind == "chat":
                await handle_chat(me, data)
            elif kind == "recall":
                await handle_recall(me, data)
            elif kind == "read":
                await handle_read(me, data)

    except WebSocketDisconnect:
        pass

    finally:
        clients.pop(websocket, None)
        if me.name:
            renormalize_ip(me.ip)
            await broadcast_system(f"{me.name} 离开了聊天室")
            await broadcast_roster()


# ---------------------------------------------------------------------------
# 页面
# ---------------------------------------------------------------------------

PAGE = """
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>第4组聊天室</title>
<style>
  :root { --line:#d8dee6; --muted:#7b8794; --bg:#f6f8fa; --blue:#2563eb; }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:#1f2933;
       font-family:system-ui,-apple-system,"Segoe UI","Microsoft YaHei",sans-serif}
  .hidden{display:none!important}

  #join{position:fixed;inset:0;background:rgba(246,248,250,.96);display:flex;
        align-items:center;justify-content:center;z-index:30}
  .card{background:#fff;padding:28px;border-radius:12px;width:min(92vw,380px);
        box-shadow:0 8px 30px rgba(0,0,0,.08)}
  .card h2{margin:0 0 6px;font-size:20px}
  .card .hint{color:var(--muted);font-size:13px;margin:0 0 16px;line-height:1.6}
  .card input,.card button{width:100%;padding:10px 12px;font-size:15px;border-radius:8px}
  .card input{border:1px solid var(--line);outline:none}
  .card input:focus{border-color:var(--blue)}
  .card button{margin-top:12px;border:0;background:var(--blue);color:#fff;cursor:pointer}
  .warn{margin-top:14px;padding:12px;border-radius:8px;font-size:13px;line-height:1.6;
        background:#fff7ed;border:1px solid #fed7aa;color:#9a3412}
  .warn button{background:#ea580c}

  #app{max-width:760px;margin:0 auto;padding:14px 16px 88px}
  header{display:flex;flex-wrap:wrap;gap:10px;align-items:baseline}
  header h1{font-size:19px;margin:0}
  .meta{display:flex;gap:12px;font-size:13px;color:var(--muted);margin-left:auto;flex-wrap:wrap}
  .meta b{color:#1f2933}
  #badge{background:#ef4444;color:#fff;border-radius:10px;padding:1px 7px;font-size:12px}

  #banner{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px;
          padding:10px 12px;border-radius:8px;font-size:13px;
          background:#eff6ff;border:1px solid #bfdbfe;color:#1e40af}
  #banner button{border:0;background:var(--blue);color:#fff;border-radius:6px;
                 padding:5px 10px;font-size:12px;cursor:pointer}
  #banner button.ghost{background:#fff;color:#1e40af;border:1px solid #bfdbfe}

  #list{list-style:none;padding:12px;margin:12px 0 0;background:#fff;
        border:1px solid var(--line);border-radius:10px;
        height:min(62vh,540px);overflow-y:auto}
  #list li{padding:6px 4px;font-size:14px;line-height:1.6;word-break:break-word}
  #list li.sys{color:var(--muted);font-size:13px}
  .row{display:flex;gap:8px;align-items:baseline;flex-wrap:wrap}
  .who{font-weight:600;color:#1d4ed8}
  .time{color:#9aa5b1;font-size:12px}
  .note{color:#b45309;font-size:12px}
  .body{margin-top:2px}
  .mine .who{color:#047857}
  .recalled .body{color:#9aa5b1;font-style:italic}
  .status{font-size:12px;color:#9aa5b1}
  .status.fail{color:#dc2626}
  .retry{margin-left:6px;border:0;background:#fee2e2;color:#b91c1c;
         border-radius:6px;padding:2px 8px;font-size:12px;cursor:pointer}
  .recall-btn{border:0;background:none;color:#9aa5b1;font-size:12px;
              cursor:pointer;padding:0 2px}
  .recall-btn:hover{color:#dc2626}
  .divider{color:#b45309;font-size:12px;text-align:center;transition:opacity .6s}
  .fade{opacity:0}

  .inputbar{position:fixed;left:0;right:0;bottom:0;background:#fff;
            border-top:1px solid var(--line);padding:10px 16px;
            display:flex;gap:10px;max-width:760px;margin:0 auto;flex-wrap:wrap}
  .inputbar input{flex:1;min-width:180px;padding:10px 12px;font-size:15px;
                  border:1px solid var(--line);border-radius:8px;outline:none}
  .inputbar input:focus{border-color:var(--blue)}
  .inputbar button{padding:10px 20px;font-size:15px;border:0;border-radius:8px;
                   background:var(--blue);color:#fff;cursor:pointer}
  .inputbar button:disabled{background:#cbd5e1;cursor:not-allowed}
  #err{width:100%;font-size:12px;color:#dc2626;min-height:16px}
  #conn{font-size:12px;font-weight:600}
</style>
</head>
<body>

<div id="join">
  <div class="card">
    <h2>进入聊天室</h2>
    <p class="hint">输入昵称（1~16 字）。同一网络（IP）用第二个昵称会先警告，确认后标记「和某某为同一 IP」。</p>
    <input id="name" maxlength="16" placeholder="你的昵称" autocomplete="off">
    <button onclick="doJoin(false)">进入</button>
    <div id="warn" class="warn hidden">
      <div id="warnText"></div>
      <button onclick="doJoin(true)">仍然进入</button>
    </div>
  </div>
</div>

<div id="app" class="hidden">
  <header>
    <h1>第4组聊天室</h1>
    <div class="meta">
      <span>我：<b id="me">-</b></span>
      <span>在线：<b id="online">0</b> 人</span>
      <span>未读：<b id="unread">0</b></span>
      <span id="conn">正在连接…</span>
    </div>
  </header>

  <div id="banner" class="hidden">
    <span id="bannerText"></span>
    <button onclick="jumpToLastRead()">回到上次阅读位置</button>
    <button class="ghost" onclick="markAllRead()">全部标为已读</button>
  </div>

  <ul id="list"></ul>

  <div class="inputbar">
    <div id="err"></div>
    <input id="text" placeholder="说点什么…" autocomplete="off" maxlength="600">
    <button id="sendBtn" onclick="send()">发送</button>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);

let ws = null;
let myName = null;
let myIp = null;
let joined = false;
let historyLoading = false;
let queue = [];
let lastReadId = 0;
let firstUnreadId = null;
let unread = 0;
let readArmed = false;      // 历史渲染后 2.5 秒才允许自动标已读，避免一进来就把未读清零
let backoff = 2000;
let lastSendAt = 0;
const pending = new Map();     // client_id -> {el, text, id}
const MAX_TEXT = 500;

/* ---------------- 连接 ---------------- */
function connect() {
  ws = new WebSocket(`ws://${location.host}/ws`);
  setConn("正在连接…", "#b45309");

  ws.onopen = () => {
    backoff = 2000;
    setConn("已连接", "#15803d");
    $("sendBtn").disabled = false;
    $("text").disabled = false;
    if (myName) sendJoin(myName, true);   // 断线重连后自动归位
  };

  ws.onclose = () => {
    const sec = Math.round(backoff / 1000);
    setConn(`${sec} 秒后重连…`, "#c0392b");
    $("sendBtn").disabled = true;
    $("text").disabled = true;
    setTimeout(connect, backoff);
    backoff = Math.min(backoff * 2, 15000);   // 指数退避，最多 15 秒
  };

  ws.onerror = () => {};
  ws.onmessage = (e) => handle(JSON.parse(e.data));
}

function setConn(text, color) {
  $("conn").textContent = text;
  $("conn").style.color = color;
}

/* ---------------- 入场 ---------------- */
function sendJoin(name, confirm) {
  ws.send(JSON.stringify({ type: "join", name, confirm: !!confirm }));
}

function doJoin(confirm) {
  const name = $("name").value.trim();
  if (!name) { warn("昵称不能为空"); return; }
  if (name.length > 16) { warn("昵称最多 16 个字"); return; }
  myName = name;
  if (!ws || ws.readyState !== WebSocket.OPEN) { warn("还没连上服务器，请稍候"); return; }
  sendJoin(name, confirm);
}

function warn(text) {
  $("warnText").textContent = text;
  $("warn").classList.remove("hidden");
}

/* ---------------- 消息分发 ---------------- */
function handle(msg) {
  if (msg.type === "welcome") {
    joined = true;
    myIp = msg.ip;
    $("join").classList.add("hidden");
    $("app").classList.remove("hidden");
    $("me").textContent = msg.name + (msg.same_ip_as ? `（和 ${msg.same_ip_as} 为同一IP）` : "");
    $("online").textContent = msg.online;
    historyLoading = true;
    $("text").focus();
    return;
  }

  if (msg.type === "duplicate") {
    // 用 DOM 拼装而不是 innerHTML：昵称是用户输入，直接拼 HTML 会有注入风险
    const box = $("warnText");
    box.textContent = "";
    const bIp = document.createElement("b");
    bIp.textContent = msg.ip;
    const bName = document.createElement("b");
    bName.textContent = msg.existing_name;
    box.append("此网络（IP ", bIp, "）已有昵称「", bName, "」在线。");
    box.appendChild(document.createElement("br"));
    box.append(`继续进入将以「${myName}」登录，并标记为「和 ${msg.existing_name} 为同一IP」。`);
    $("warn").classList.remove("hidden");
    return;
  }

  if (msg.type === "history") {
    renderHistory(msg.messages, msg.last_read_id, msg.unread, msg.limit);
    return;
  }

  if (msg.type === "reject" && !msg.client_id) {
    warn(msg.reason || "操作被拒绝");
    return;
  }

  // 历史还没渲染完时，实时事件先排队，稍后按顺序补上
  if (historyLoading) { queue.push(msg); return; }
  live(msg);
}

function live(msg) {
  switch (msg.type) {
    case "chat":      onChat(msg); break;
    case "ack":       onAck(msg); break;
    case "reject":    onReject(msg); break;
    case "recall":    onRecall(msg); break;
    case "system":    addSystem(msg.message); break;
    case "roster":    $("online").textContent = msg.online; break;
    case "unread":    setUnread(msg.count); lastReadId = msg.last_read_id; break;
    case "error":     showErr(msg.message); break;
  }
}

/* ---------------- 历史渲染 ---------------- */
function renderHistory(messages, lastRead, unreadCount, limit) {
  $("list").innerHTML = "";
  firstUnreadId = null;
  lastReadId = lastRead || 0;

  messages.forEach((m) => {
    const el = addMessage({
      id: m.id, name: m.name, text: m.text, created_at: m.created_at,
      recalled: m.recalled, mine: m.mine, sameIp: (m.ip === myIp && m.name !== myName)
    });
    if (!firstUnreadId && m.id > lastReadId && !m.mine) firstUnreadId = m.id;
  });

  if (firstUnreadId) insertDivider(firstUnreadId);

  setUnread(unreadCount || 0);
  if (messages.length === 0) addSystem("还没有消息，说点什么吧。");
  else if (messages.length >= limit) addSystem(`以上为最近 ${limit} 条消息。`);

  historyLoading = false;
  const rest = queue; queue = [];
  rest.forEach(live);
  scrollToBottom(true);

  // 给用户 2.5 秒看清未读和阅读位置，再允许自动标已读
  readArmed = false;
  setTimeout(() => { readArmed = true; }, 2500);
}

function insertDivider(beforeId) {
  const target = $("list").querySelector(`li[data-id="${beforeId}"]`);
  if (!target) return;
  const div = document.createElement("li");
  div.className = "divider";
  div.id = "readDivider";
  div.textContent = "—— 上次阅读位置 ——";
  target.parentNode.insertBefore(div, target);
  setTimeout(() => {                     // 6 秒后淡出并移除
    div.classList.add("fade");
    setTimeout(() => div.remove(), 700);
  }, 6000);
}

/* ---------------- 聊天消息 ---------------- */
function addMessage(o) {
  const li = document.createElement("li");
  li.dataset.id = o.id || "";
  if (o.mine) li.classList.add("mine");
  if (o.recalled) li.classList.add("recalled");

  const row = document.createElement("div");
  row.className = "row";

  const who = document.createElement("span");
  who.className = "who";
  who.textContent = o.name;
  row.appendChild(who);

  const time = document.createElement("span");
  time.className = "time";
  time.textContent = fmtTime(o.created_at);
  row.appendChild(time);

  if (o.sameIp) {
    const note = document.createElement("span");
    note.className = "note";
    note.textContent = "（和对方为同一 IP）";
    row.appendChild(note);
  }

  if (o.mine && !o.recalled && canRecall(o.created_at)) {
    const btn = document.createElement("button");
    btn.className = "recall-btn";
    btn.textContent = "撤回";
    btn.onclick = () => recall(o.id);
    row.appendChild(btn);
  }

  const body = document.createElement("div");
  body.className = "body";
  body.textContent = o.recalled ? "该消息已撤回" : o.text;

  li.appendChild(row);
  li.appendChild(body);
  $("list").appendChild(li);
  return li;
}

function canRecall(createdAt) {
  return (Date.now() / 1000) - createdAt < 300;
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

function addSystem(text) {
  const li = document.createElement("li");
  li.className = "sys";
  li.textContent = text;
  $("list").appendChild(li);
}

function onChat(msg) {
  if (msg.client_id && pending.has(msg.client_id)) {
    confirmPending(msg.client_id, msg.id);
    return;   // 自己发的消息：已经显示过，只更新状态
  }
  addMessage({
    id: msg.id, name: msg.name, text: msg.text, created_at: msg.created_at,
    mine: msg.name === myName && msg.ip === myIp,
    sameIp: (msg.ip === myIp && msg.name !== myName)
  });
  scrollToBottom();
}

/* ---------------- 发送 / 重试 ---------------- */
function send() {
  showErr("");
  const box = $("text");
  const text = box.value.trim();

  if (!text) { showErr("不能发送空消息"); return; }
  if (text.length > MAX_TEXT) { showErr(`消息过长：${text.length}/${MAX_TEXT} 字`); return; }
  if (!ws || ws.readyState !== WebSocket.OPEN) { showErr("连接已断开，正在重连，消息未发出"); return; }
  if (Date.now() - lastSendAt < 400) { showErr("发送太快，请稍后再试"); return; }
  lastSendAt = Date.now();

  const cid = (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + Math.random());
  const li = addMessage({
    id: "", name: myName, text, created_at: Date.now() / 1000,
    mine: true, sameIp: false
  });
  const status = document.createElement("span");
  status.className = "status";
  status.textContent = "发送中…";
  li.appendChild(status);

  pending.set(cid, { el: li, text, status });
  ws.send(JSON.stringify({ type: "chat", text, client_id: cid }));
  armTimeout(cid);
  box.value = "";
  box.focus();
}

function armTimeout(cid) {
  const item = pending.get(cid);
  if (!item) return;
  item.timer = setTimeout(() => {
    if (!pending.has(cid)) return;
    item.status.textContent = "发送失败 ";
    item.status.className = "status fail";
    const btn = document.createElement("button");
    btn.className = "retry";
    btn.textContent = "重试";
    btn.onclick = () => retry(cid);
    item.status.appendChild(btn);
  }, 3000);
}

function retry(cid) {
  const item = pending.get(cid);
  if (!item) return;
  if (!ws || ws.readyState !== WebSocket.OPEN) { showErr("还没连上，稍后自动重试"); return; }
  clearTimeout(item.timer);
  item.status.className = "status";
  item.status.textContent = "重试中…";
  // 同一个 client_id 重发：服务端会识别为重复提交，不会产生两条消息
  ws.send(JSON.stringify({ type: "chat", text: item.text, client_id: cid }));
  armTimeout(cid);
}

function confirmPending(cid, id) {
  const item = pending.get(cid);
  if (!item) return;
  clearTimeout(item.timer);
  item.el.dataset.id = id;
  item.status.textContent = "";
  pending.delete(cid);
  scrollToBottom();
}

function onAck(msg) {
  if (msg.client_id) confirmPending(msg.client_id, msg.id);
}

function onReject(msg) {
  if (!msg.client_id) { warn(msg.reason || "操作被拒绝"); return; }
  const item = pending.get(msg.client_id);
  if (!item) return;
  clearTimeout(item.timer);
  item.status.className = "status fail";
  item.status.textContent = msg.reason || "发送失败";
  const btn = document.createElement("button");
  btn.className = "retry";
  btn.textContent = "重试";
  btn.onclick = () => retry(msg.client_id);
  item.status.appendChild(btn);
}

/* ---------------- 撤回 ---------------- */
function recall(id) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({ type: "recall", id }));
}

function onRecall(msg) {
  const li = $("list").querySelector(`li[data-id="${msg.id}"]`);
  if (!li) return;
  li.classList.add("recalled");
  const body = li.querySelector(".body");
  if (body) body.textContent = "该消息已撤回";
  const btn = li.querySelector(".recall-btn");
  if (btn) btn.remove();
  return;
}

/* ---------------- 已读 / 未读 ---------------- */
function setUnread(n) {
  unread = n;
  $("unread").textContent = n;
  if (n > 0) {
    $("bannerText").textContent = `你有 ${n} 条未读消息`;
    $("banner").classList.remove("hidden");
  } else {
    $("banner").classList.add("hidden");
  }
}

function maxMessageId() {
  const items = [...$("list").querySelectorAll("li[data-id]")];
  if (!items.length) return 0;
  return Number(items[items.length - 1].dataset.id) || 0;
}

function maybeMarkRead() {
  if (!joined || historyLoading || !readArmed) return;
  const list = $("list");
  const atBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 40;
  if (!atBottom) return;
  const id = maxMessageId();
  if (!id || id <= lastReadId) return;
  lastReadId = id;
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: "read", last_message_id: id }));
  }
}

function markAllRead() {
  const id = maxMessageId();
  if (!id) return;
  lastReadId = id;
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: "read", last_message_id: id }));
  }
  setUnread(0);
  const div = $("readDivider");
  if (div) div.remove();
}

function jumpToLastRead() {
  let target = $("readDivider");
  if (!target && firstUnreadId) {
    target = $("list").querySelector(`li[data-id="${firstUnreadId}"]`);
  }
  if (target) target.scrollIntoView({ behavior: "smooth", block: "center" });
}

/* ---------------- 工具 ---------------- */
function showErr(text) { $("err").textContent = text; }

function scrollToBottom(force) {
  const list = $("list");
  const atBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 60;
  if (force || atBottom) list.scrollTop = list.scrollHeight;
}

$("list").addEventListener("scroll", () => {
  clearTimeout(window._readTimer);
  window._readTimer = setTimeout(maybeMarkRead, 500);
});

$("text").addEventListener("keydown", (e) => { if (e.key === "Enter") send(); });
$("name").addEventListener("keydown", (e) => { if (e.key === "Enter") doJoin(false); });

connect();
</script>
</body>
</html>
"""
