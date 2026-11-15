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
    8. 历史消息每次加载 50 条，可反复「加载更早的消息」直到库中最早一条；
       加载期间到达的新消息排队，渲染完再补
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

# 固定三个房间。房间之间数据完全隔离，互不可见
ROOMS = ("茶室", "咖啡厅", "漫画工作室")

MAX_TEXT = 500            # 单条消息最大字数
MIN_INTERVAL = 0.4        # 两条消息最小间隔（秒），防刷屏
RECALL_WINDOW = 300       # 撤回时限（秒）
HISTORY_BATCH = 50        # 每次加载的历史条数（首次进入与「加载更多」共用）
DUP_TEXT_WINDOW = 10      # 同一用户重发相同内容的拦截窗口（秒）
MAX_NICK = 16             # 昵称最大长度


# ---------------------------------------------------------------------------
# 数据库层
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    room         TEXT    NOT NULL,
    ip           TEXT    NOT NULL,
    name         TEXT    NOT NULL,
    created_at   REAL    NOT NULL,
    last_seen_at REAL    NOT NULL,
    UNIQUE(room, ip, name)
);

CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    room        TEXT    NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_msg_room ON messages(room, id);
CREATE INDEX IF NOT EXISTS idx_msg_id ON messages(id);
"""

_lock = threading.Lock()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row
_conn.execute("PRAGMA journal_mode=WAL")
_conn.executescript(SCHEMA)
_conn.commit()


def upsert_user(room: str, ip: str, name: str) -> int:
    """按 (房间, ip, 昵称) 找人，没有就建。返回 user_id。

    同一个 IP + 昵称在不同房间里是两条独立档案，互不影响。
    """
    now = time.time()
    with _lock:
        row = _conn.execute(
            "SELECT id FROM users WHERE room=? AND ip=? AND name=?",
            (room, ip, name),
        ).fetchone()
        if row:
            _conn.execute("UPDATE users SET last_seen_at=? WHERE id=?", (now, row["id"]))
            _conn.commit()
            return row["id"]
        cur = _conn.execute(
            "INSERT INTO users(room,ip,name,created_at,last_seen_at) VALUES(?,?,?,?,?)",
            (room, ip, name, now, now),
        )
        _conn.commit()
        return int(cur.lastrowid)


def insert_message(room: str, user_id: int, name: str, ip: str, text: str,
                   client_id: str | None):
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
            "INSERT INTO messages(room,user_id,name,ip,text,created_at,status,client_id)"
            " VALUES(?,?,?,?,?,?, 'active', ?)",
            (room, user_id, name, ip, text, now, client_id),
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


def fetch_history(room: str, limit: int, me_user_id: int,
                  before_id: int | None = None) -> tuple[list[dict], bool]:
    """取历史消息，正序返回，并告知是否还有更早的。

    before_id 为空：取最近 limit 条。
    before_id 有值：取 id 小于它的 limit 条（「加载更多」）。
    撤回的消息只给状态，不给原文。
    """
    with _lock:
        if before_id:
            rows = _conn.execute(
                "SELECT id, user_id, name, ip, text, created_at, status"
                " FROM messages WHERE room=? AND id < ? ORDER BY id DESC LIMIT ?",
                (room, before_id, limit),
            ).fetchall()
        else:
            rows = _conn.execute(
                "SELECT id, user_id, name, ip, text, created_at, status"
                " FROM messages WHERE room=? ORDER BY id DESC LIMIT ?",
                (room, limit),
            ).fetchall()

        has_more = False
        if rows:
            oldest = rows[-1]["id"]
            has_more = _conn.execute(
                "SELECT 1 FROM messages WHERE room=? AND id < ? LIMIT 1",
                (room, oldest),
            ).fetchone() is not None

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
    return out, has_more


def recall_message(message_id: int, user_id: int) -> tuple[bool, str]:
    """撤回。返回 (是否成功, 失败原因)。

    只按 user_id 校验：user_id 本身已经绑定了房间，
    所以不同房间的消息不可能被误撤。
    """
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


def count_unread(room: str, user_id: int, last_read_id: int) -> int:
    with _lock:
        row = _conn.execute(
            "SELECT COUNT(*) AS n FROM messages"
            " WHERE room=? AND id > ? AND user_id != ? AND status = 'active'",
            (room, last_read_id, user_id),
        ).fetchone()
        return int(row["n"])


def count_messages(room: str) -> int:
    with _lock:
        row = _conn.execute(
            "SELECT COUNT(*) AS n FROM messages WHERE room=?", (room,)
        ).fetchone()
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
    room: str | None = None


clients: dict[WebSocket, Client] = {}
_seq = 0

# 最近一次发送内容，用于「同一句话短时间内重复发送」的拦截
_recent_text: dict[int, tuple[str, float]] = {}


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


def ip_members(room: str, ip: str) -> list[Client]:
    """同一房间内、同一 IP 下已入场的成员。"""
    return sorted(
        (c for c in clients.values() if c.room == room and c.ip == ip and c.name),
        key=lambda c: c.seq,
    )


def online_ip_count(room: str) -> int:
    """某房间的在线人数：按 IP 去重。"""
    return len({c.ip for c in clients.values() if c.name and c.room == room})


def renormalize_ip(room: str, ip: str) -> None:
    """某个 IP 的成员变动后，重算主账号与关联关系。

    规则：该 IP 入场最早的人为主账号；同名成员视为本人多开，不标注关联。
    """
    members = ip_members(room, ip)
    if not members:
        return
    primary = members[0]
    primary.same_ip_as = None
    for member in members[1:]:
        member.same_ip_as = None if member.name == primary.name else primary.name


# ---------------------------------------------------------------------------
# 广播
# ---------------------------------------------------------------------------

async def broadcast(room: str, payload: dict) -> None:
    """只发给同一房间的人。房间之间完全隔离。"""
    dead: list[Client] = []
    for client in list(clients.values()):
        if not client.name or client.room != room:
            continue
        try:
            await client.ws.send_json(payload)
        except Exception:
            dead.append(client)
    for client in dead:
        clients.pop(client.ws, None)


async def broadcast_system(room: str, message: str) -> None:
    await broadcast(room, {"type": "system", "message": message})


async def broadcast_roster(room: str) -> None:
    seen: set[str] = set()
    users = []
    for c in sorted(clients.values(), key=lambda c: c.seq):
        if not c.name or c.room != room or c.name in seen:
            continue          # 只看本房间；同名只算一次
        seen.add(c.name)
        uid = c.user_id or 0
        users.append({
            "name": c.name,
            "ip": c.ip,
            "same_ip_as": c.same_ip_as,
            "last_read_id": get_last_read(uid) if uid else 0,
        })
    await broadcast(room, {"type": "roster", "online": online_ip_count(room),
                           "users": users, "room": room})


# ---------------------------------------------------------------------------
# 业务处理
# ---------------------------------------------------------------------------

async def send_history(me: Client) -> None:
    """下发首批历史（最近 50 条）+ 已读位置 + 未读数 + 是否还有更早。"""
    me_user_id = me.user_id or 0
    room = me.room or ROOMS[0]
    last_read = get_last_read(me_user_id)
    history, has_more = fetch_history(room, HISTORY_BATCH, me_user_id)
    unread = count_unread(room, me_user_id, last_read)

    await me.ws.send_json({
        "type": "history",
        "messages": history,
        "last_read_id": last_read,
        "unread": unread,
        "has_more": has_more,
        "batch": HISTORY_BATCH,
    })


async def handle_history_more(me: Client, data: dict) -> None:
    """加载更多：取比 before_id 更早的 50 条，不改动已读位置。"""
    if not me.user_id:
        return
    try:
        before_id = int(data.get("before_id"))
    except (TypeError, ValueError):
        return

    page, has_more = fetch_history(me.room or ROOMS[0], HISTORY_BATCH,
                                   me.user_id, before_id)
    await me.ws.send_json({
        "type": "history_page",
        "messages": page,
        "has_more": has_more,
        "batch": HISTORY_BATCH,
    })


async def handle_join(me: Client, data: dict) -> None:
    """入场：校验昵称与房间 → 同 IP 警告（同房间内）→ 建档 → 下发本房间历史。"""
    raw = (data.get("name") or "").strip()
    room = (data.get("room") or "").strip()

    if not raw:
        await me.ws.send_json({"type": "reject", "reason": "昵称不能为空"})
        return
    if len(raw) > MAX_NICK:
        await me.ws.send_json({"type": "reject", "reason": f"昵称最多 {MAX_NICK} 个字"})
        return
    if room not in ROOMS:
        await me.ws.send_json({"type": "reject", "reason": "请选择一个有效的聊天室"})
        return

    name = raw
    me.room = room

    # 同 IP 警告只在同一房间内判断
    members = ip_members(room, me.ip)
    others = [m for m in members if m.name != name]

    if others and not data.get("confirm"):
        await me.ws.send_json({
            "type": "duplicate",
            "ip": me.ip,
            "room": room,
            "existing_name": others[0].name,
            "online": online_ip_count(room),
        })
        return

    me.name = name
    me.user_id = upsert_user(room, me.ip, name)
    me.same_ip_as = others[0].name if others else None
    renormalize_ip(room, me.ip)

    await me.ws.send_json({
        "type": "welcome",
        "name": me.name,
        "ip": me.ip,
        "room": room,
        "same_ip_as": me.same_ip_as,
        "online": online_ip_count(room),
    })

    suffix = f"（和 {me.same_ip_as} 为同一 IP）" if me.same_ip_as else ""
    await broadcast_system(room, f"{me.name} 加入了{room}{suffix}")
    await broadcast_roster(room)
    await send_history(me)


async def reject_chat(me: Client, client_id: str | None, reason: str,
                      code: str | None = None) -> None:
    payload = {"type": "reject", "client_id": client_id, "reason": reason}
    if code:
        payload["code"] = code
    await me.ws.send_json(payload)


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

    # 同一用户短时间内重复发送相同内容：拦下来并说明原因
    prev = _recent_text.get(me.user_id)
    if prev and prev[0] == text and now - prev[1] < DUP_TEXT_WINDOW:
        await reject_chat(
            me, client_id,
            f"{DUP_TEXT_WINDOW} 秒内不要重复发送相同内容",
            code="duplicate_text",
        )
        return

    if now - me.last_sent_at < MIN_INTERVAL:
        await reject_chat(me, client_id, "发送太快，请稍后再试")
        return
    me.last_sent_at = now
    _recent_text[me.user_id] = (text, now)

    message_id, created_at, existed = insert_message(
        me.room or ROOMS[0], me.user_id, me.name, me.ip, text, client_id
    )

    await me.ws.send_json({
        "type": "ack",
        "client_id": client_id,
        "id": message_id,
        "created_at": created_at,
    })

    if existed:
        return   # 重试导致重复提交：只回 ack，不再广播

    await broadcast(me.room or ROOMS[0], {
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

    await broadcast(me.room or ROOMS[0],
                    {"type": "recall", "id": message_id, "by": me.name})


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
        "count": count_unread(me.room or ROOMS[0], me.user_id, last_read),
        "last_read_id": last_read,
    })
    # 告诉所有人：这个人读到了哪一条
    await broadcast(me.room or ROOMS[0], {
        "type": "read_update",
        "name": me.name,
        "last_read_id": last_read,
    })


# ---------------------------------------------------------------------------
# HTTP 路由
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    """按房间分别汇报在线人数与消息数。"""
    rooms = {}
    for room in ROOMS:
        rooms[room] = {
            "online": online_ip_count(room),
            "connections": len([c for c in clients.values()
                                if c.name and c.room == room]),
            "messages": count_messages(room),
        }
    return {"ok": True, "rooms": rooms}


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
      <li>同一句话在 10 秒内不要重复发送，否则会被拒绝并提示。</li>
      <li>只能撤回自己的消息，且必须在 5 分钟内；撤回后所有人看到「已撤回」。</li>
      <li>历史消息每次加载 50 条，可点「加载更早的消息」继续往前翻，直到最早一条。</li>
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
            elif kind == "history_more":
                await handle_history_more(me, data)

    except WebSocketDisconnect:
        pass

    finally:
        clients.pop(websocket, None)
        if me.name and me.room:
            renormalize_ip(me.room, me.ip)
            await broadcast_system(me.room, f"{me.name} 离开了{me.room}")
            await broadcast_roster(me.room)


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

  /* 房间选择 */
  .rooms{display:flex;flex-direction:column;gap:9px;margin-bottom:12px}
  .room{padding:13px 14px;border:1px solid var(--line);border-radius:10px;
        background:#fff;font-size:15px;cursor:pointer;text-align:left;color:#1f2933}
  .room:hover{border-color:#93c5fd;background:#f0f7ff}
  .card .ghost{background:#fff;color:#475569;border:1px solid var(--line)}
  #leaveBtn{border:1px solid var(--line);background:#fff;color:#475569;
            border-radius:6px;padding:2px 8px;font-size:12px;cursor:pointer}
  #leaveBtn:hover{border-color:#93c5fd;color:#1d4ed8}

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

  #list{list-style:none;padding:14px 12px;margin:12px 0 0;background:#f8fafc;
        border:1px solid var(--line);border-radius:12px;
        height:min(62vh,560px);overflow-y:auto}
  #list li{font-size:14px;line-height:1.6;word-break:break-word}

  /* 消息行：自己的靠右，别人的靠左 */
  #list li.msg{display:flex;padding:5px 0}
  #list li.msg.mine{justify-content:flex-end}

  /* 气泡 */
  .bubble{max-width:78%;padding:8px 12px;border-radius:14px;border:1px solid var(--line);
          background:#fff;box-shadow:0 1px 2px rgba(15,23,42,.05)}
  .mine .bubble{background:#2563eb;border-color:#1d4ed8;color:#fff;
                border-bottom-right-radius:4px}
  .other .bubble{border-bottom-left-radius:4px}
  .mine .bubble .who,.mine .bubble .time{color:#dbeafe}
  .mine .bubble .note{color:#fde68a}
  .mine .bubble .recall-btn{color:#c7dcff}
  .mine .bubble .recall-btn:hover{color:#fff}

  #list li.sys{color:var(--muted);font-size:12px;text-align:center;padding:4px 0}
  .loadmore{text-align:center;padding:6px 0 10px}
  .loadmore button{border:1px solid var(--line);background:#fff;color:#1d4ed8;
                   border-radius:8px;padding:6px 14px;font-size:12px;cursor:pointer}
  .loadmore button:hover{border-color:#93c5fd}
  .loadmore button:disabled{color:#9aa5b1;cursor:not-allowed;border-color:var(--line)}
  .row{display:flex;gap:8px;align-items:baseline;flex-wrap:wrap}
  .who{font-weight:600;font-size:12.5px;color:#1d4ed8}
  .time{color:#9aa5b1;font-size:11px}
  .note{color:#b45309;font-size:11px}
  .body{margin-top:2px;white-space:pre-wrap}
  .recalled .body{opacity:.7;font-style:italic}
  .status{display:block;font-size:11px;color:#cbd5e1;text-align:right;margin-top:2px}
  .status.fail{color:#fca5a5}
  .retry{margin-left:6px;border:0;background:#fee2e2;color:#b91c1c;
         border-radius:6px;padding:2px 8px;font-size:11px;cursor:pointer}
  .recall-btn{border:0;background:none;color:#9aa5b1;font-size:11px;
              cursor:pointer;padding:0 2px;opacity:0;transition:opacity .15s}
  li.msg:hover .recall-btn{opacity:1}
  .divider{color:#b45309;font-size:12px;text-align:center;padding:6px 0;transition:opacity .6s}
  .fade{opacity:0}

  /* 悬浮按钮：回到最新消息 */
  #jump{position:fixed;bottom:84px;right:calc(50% - 372px);z-index:20;
        border:0;border-radius:18px;padding:8px 14px;font-size:12px;font-weight:600;
        background:#1d4ed8;color:#fff;cursor:pointer;
        box-shadow:0 6px 18px rgba(29,78,216,.35)}
  #jump:hover{background:#1e40af}
  @media (max-width:790px){ #jump{right:16px} }

  .receipt{display:block;font-size:11px;color:#cbd5e1;text-align:right;margin-top:3px}

  /* 轻量提示条 */
  #toast{position:fixed;left:50%;bottom:132px;z-index:40;
         transform:translateX(-50%) translateY(8px);
         background:rgba(15,23,42,.92);color:#fff;font-size:13px;
         padding:9px 16px;border-radius:10px;max-width:80vw;text-align:center;
         opacity:0;pointer-events:none;transition:opacity .2s, transform .2s}
  #toast.show{opacity:1;transform:translateX(-50%) translateY(0)}

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
    <p class="hint" id="joinHint">第一步：给自己起个昵称（1~16 字）</p>

    <div id="stepName">
      <input id="name" maxlength="16" placeholder="你的昵称" autocomplete="off">
      <button onclick="gotoRooms()">下一步</button>
    </div>

    <div id="stepRoom" class="hidden">
      <div class="rooms">
        <button class="room" data-room="茶室" onclick="pickRoom(this)">茶室</button>
        <button class="room" data-room="咖啡厅" onclick="pickRoom(this)">咖啡厅</button>
        <button class="room" data-room="漫画工作室" onclick="pickRoom(this)">漫画工作室</button>
      </div>
      <button class="ghost" onclick="backToName()">← 返回上一步</button>
    </div>

    <div id="warn" class="warn hidden">
      <div id="warnText"></div>
      <button onclick="confirmJoin()">仍然进入</button>
    </div>
  </div>
</div>

<div id="app" class="hidden">
  <header>
    <h1>第4组聊天室</h1>
    <div class="meta">
      <span>房间：<b id="room">-</b></span>
      <span>我：<b id="me">-</b></span>
      <span>在线：<b id="online">0</b> 人</span>
      <button id="leaveBtn" onclick="switchRoom()">换房间</button>
      <span id="conn">正在连接…</span>
    </div>
  </header>

  <div id="banner" class="hidden">
    <span id="bannerText"></span>
    <button onclick="jumpToLastRead()">回到上次阅读位置</button>
    <button class="ghost" onclick="markAllRead()">全部标为已读</button>
  </div>

  <ul id="list"></ul>

  <button id="jump" class="hidden" onclick="jumpLatest()">↓ 新消息</button>

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
let myRoom = null;
let pendingRoom = null;
let myIp = null;
let joined = false;
let historyLoading = false;
let queue = [];
let lastReadId = 0;
let firstUnreadId = null;
let unread = 0;
let readArmed = false;      // 历史渲染后 2.5 秒才允许自动标已读，避免一进来就把未读清零
let oldestId = null;        // 当前已加载的最早一条消息 id
let hasMoreHistory = false; // 是否还有更早的消息
let loadingMore = false;
let backoff = 2000;
let lastSendAt = 0;
const pending = new Map();     // client_id -> {el, text, id}
const ownSent = new Set();     // 已确认的 client_id：广播回传时用它去重
const ipMap = new Map();       // 昵称 -> 关联的同 IP 昵称
const reads = new Map();       // 在线昵称 -> 已读到哪条消息 id
const myMsgs = new Map();      // 我发出的消息 id -> <li> 元素
let newCount = 0;              // 屏幕外的未读新消息数（悬浮按钮显示）
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
    if (myName && myRoom) sendJoin(myName, myRoom, true);   // 断线重连后自动归位
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

/* ---------------- 入场：昵称 → 选房间 ---------------- */
function sendJoin(name, room, confirm) {
  ws.send(JSON.stringify({ type: "join", name, room, confirm: !!confirm }));
}

function gotoRooms() {
  const name = $("name").value.trim();
  if (!name) { warn("昵称不能为空"); return; }
  if (name.length > 16) { warn("昵称最多 16 个字"); return; }
  myName = name;
  localStorage.setItem("chat.nick", name);      // 记住昵称，换房间不用重输
  $("warn").classList.add("hidden");
  $("joinHint").textContent = `第二步：选择要进入的聊天室（昵称：${name}）`;
  $("stepName").classList.add("hidden");
  $("stepRoom").classList.remove("hidden");
}

// 页面加载时恢复上次的昵称，直接停在「选房间」这一步
function restoreNickname() {
  const saved = localStorage.getItem("chat.nick");
  if (!saved) return;
  myName = saved;
  $("name").value = saved;
  $("stepName").classList.add("hidden");
  $("stepRoom").classList.remove("hidden");
  $("joinHint").textContent = `第二步：选择要进入的聊天室（昵称：${saved}）`;
}

function backToName() {
  $("warn").classList.add("hidden");
  $("joinHint").textContent = "第一步：给自己起个昵称（1~16 字，换房间不用改）";
  $("stepRoom").classList.add("hidden");
  $("stepName").classList.remove("hidden");
  $("name").focus();
}

function pickRoom(btn) {
  if (!ws || ws.readyState !== WebSocket.OPEN) { warn("还没连上服务器，请稍候"); return; }
  pendingRoom = btn.dataset.room;
  $("warn").classList.add("hidden");
  sendJoin(myName, pendingRoom, false);
}

function confirmJoin() {
  if (!pendingRoom) return;
  sendJoin(myName, pendingRoom, true);
}

// 换房间：刷新页面。昵称存在浏览器里，刷新后会直接停在「选房间」这一步
function switchRoom() {
  location.reload();
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
    myRoom = msg.room;
    $("room").textContent = msg.room;
    $("join").classList.add("hidden");
    $("app").classList.remove("hidden");
    $("me").textContent = msg.name + (msg.same_ip_as ? `（和 ${msg.same_ip_as} 为同一IP）` : "");
    $("online").textContent = msg.online;
    if (msg.same_ip_as) ipMap.set(msg.name, msg.same_ip_as);
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
    box.append(`「${msg.room}」里（IP `, bIp, "）已有昵称「", bName, "」在线。");
    box.appendChild(document.createElement("br"));
    box.append(`继续进入将以「${myName}」登录，并标记为「和 ${msg.existing_name} 为同一IP」。`);
    $("warn").classList.remove("hidden");
    return;
  }

  if (msg.type === "roster") {
    // 名单要在渲染历史之前生效，否则历史里的「同 IP」标记会丢
    onRoster(msg);
    return;
  }

  if (msg.type === "history") {
    renderHistory(msg.messages, msg.last_read_id, msg.unread, msg.has_more, msg.batch);
    return;
  }

  if (msg.type === "history_page") {
    onHistoryPage(msg.messages, msg.has_more);
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
    case "roster":    onRoster(msg); break;
    case "unread":      lastReadId = msg.last_read_id; break;
    case "read_update": reads.set(msg.name, msg.last_read_id); refreshReceipts(); break;
    case "error":     showErr(msg.message); toast(msg.message); break;
  }
}

/* ---------------- 历史渲染 ---------------- */
function renderHistory(messages, lastRead, unreadCount, hasMore, batch) {
  $("list").innerHTML = "";
  firstUnreadId = null;
  lastReadId = lastRead || 0;
  hasMoreHistory = !!hasMore;
  loadingMore = false;
  ensureLoadMore(batch);

  messages.forEach((m) => {
    const li = buildMessage({
      id: m.id, name: m.name, text: m.text, created_at: m.created_at,
      recalled: m.recalled, mine: m.mine
    });
    $("list").appendChild(li);
    if (m.mine) registerMine(m.id, li);
    if (!firstUnreadId && m.id > lastReadId && !m.mine) firstUnreadId = m.id;
  });

  // 消息按 id 正序，第一条就是当前已加载的最早一条
  oldestId = messages.length ? messages[0].id : null;
  updateLoadMore(batch);

  if (firstUnreadId) insertDivider(firstUnreadId);

  updateBanner();
  if (messages.length === 0) addSystem("还没有消息，说点什么吧。");

  historyLoading = false;
  const rest = queue; queue = [];
  rest.forEach(live);
  scrollToBottom(true);

  // 给用户 2.5 秒看清未读和阅读位置，再允许自动标已读
  readArmed = false;
  setTimeout(() => { readArmed = true; }, 2500);
}

/* ---------------- 加载更多历史 ---------------- */
function ensureLoadMore(batch) {
  if ($("loadMore")) return;
  const li = document.createElement("li");
  li.id = "loadMore";
  li.className = "loadmore";
  const btn = document.createElement("button");
  btn.id = "loadMoreBtn";
  btn.onclick = loadMore;
  btn.textContent = `加载更早的消息（${batch || 50} 条）`;
  li.appendChild(btn);
  $("list").prepend(li);
}

function updateLoadMore(batch) {
  ensureLoadMore(batch);
  const btn = $("loadMoreBtn");
  if (!btn) return;
  if (!oldestId) {
    btn.textContent = "没有更早的消息";
    btn.disabled = true;
  } else if (!hasMoreHistory) {
    btn.textContent = "已经到最早的消息了";
    btn.disabled = true;
  } else {
    btn.textContent = `加载更早的消息（${batch || 50} 条）`;
    btn.disabled = loadingMore;
  }
}

function loadMore() {
  if (loadingMore || !hasMoreHistory || !oldestId) return;
  if (!ws || ws.readyState !== WebSocket.OPEN) { showErr("连接已断开，稍后再试"); return; }
  loadingMore = true;
  const btn = $("loadMoreBtn");
  btn.textContent = "加载中…";
  btn.disabled = true;
  ws.send(JSON.stringify({ type: "history_more", before_id: oldestId }));
}

function onHistoryPage(messages, hasMore) {
  loadingMore = false;
  hasMoreHistory = !!hasMore;

  const list = $("list");
  const prevHeight = list.scrollHeight;
  const prevTop = list.scrollTop;

  const anchor = $("loadMore") ? $("loadMore").nextSibling : list.firstChild;
  const frag = document.createDocumentFragment();
  messages.forEach((m) => {
    const li = buildMessage({
      id: m.id, name: m.name, text: m.text, created_at: m.created_at,
      recalled: m.recalled, mine: m.mine
    });
    if (m.mine) registerMine(m.id, li);
    frag.appendChild(li);
  });
  list.insertBefore(frag, anchor);

  if (messages.length) oldestId = messages[0].id;
  updateLoadMore();

  // 保持视觉位置不跳动：把新增的高度补到 scrollTop 上
  list.scrollTop = prevTop + (list.scrollHeight - prevHeight);
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
const PALETTE = ["#eef2ff", "#ecfdf5", "#fff7ed", "#f5f3ff",
                 "#ecfeff", "#fdf2f8", "#fefce8", "#f0fdfa"];

function colorFor(name) {
  let h = 0;
  for (const ch of String(name || "")) h = (h * 31 + ch.codePointAt(0)) % 9973;
  return PALETTE[h % PALETTE.length];
}

function buildMessage(o) {
  const li = document.createElement("li");
  li.className = "msg " + (o.mine ? "mine" : "other");
  li.dataset.id = o.id || "";
  li.dataset.name = o.name || "";
  if (o.recalled) li.classList.add("recalled");

  const bubble = document.createElement("div");
  bubble.className = "bubble";
  if (!o.mine) bubble.style.background = colorFor(o.name);   // 同一人固定同色

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

  // 同 IP 关联提示：对所有人生效（按昵称查关联表，而不是按自己的 IP 判断）
  const linked = ipMap.get(o.name);
  if (linked) {
    const note = document.createElement("span");
    note.className = "note";
    note.textContent = `（和 ${linked} 为同一IP）`;
    row.appendChild(note);
  }

  if (o.mine && !o.recalled && canRecall(o.created_at)) {
    const btn = document.createElement("button");
    btn.className = "recall-btn";
    btn.textContent = "撤回";
    // 用 DOM 上的 id：发送中的消息 o.id 是空串，收到 ack 后才写进 dataset
    btn.onclick = () => {
      const id = li.dataset.id;
      if (!id) { toast("这条消息还在发送中，稍后再撤"); return; }
      recall(id);
    };
    row.appendChild(btn);
  }

  const body = document.createElement("div");
  body.className = "body";
  body.textContent = o.recalled ? "该消息已撤回" : o.text;

  bubble.appendChild(row);
  bubble.appendChild(body);
  if (o.mine) {
    const receipt = document.createElement("span");
    receipt.className = "receipt";
    bubble.appendChild(receipt);
  }
  li.appendChild(bubble);
  return li;
}

function addMessage(o) {
  const li = buildMessage(o);
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
  // 服务端会把消息广播回发送者本人：必须用 client_id 去重，否则自己看到两条
  if (msg.client_id && (pending.has(msg.client_id) || ownSent.has(msg.client_id))) {
    if (pending.has(msg.client_id)) confirmPending(msg.client_id, msg.id);
    return;
  }
  const mine = (msg.name === myName && msg.ip === myIp);
  const li = addMessage({
    id: msg.id, name: msg.name, text: msg.text, created_at: msg.created_at, mine
  });
  if (mine) registerMine(msg.id, li);
  bumpIfNotBottom();
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
  const bubbleEl = li.querySelector(".bubble") || li;
  bubbleEl.appendChild(status);

  pending.set(cid, { el: li, text, status });
  ws.send(JSON.stringify({ type: "chat", text, client_id: cid }));
  armTimeout(cid);
  box.value = "";
  box.focus();
  scrollToBottom(true);
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
  ownSent.add(cid);      // 记住已确认的消息，广播回传时跳过
  pending.delete(cid);
  registerMine(id, item.el);
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

  // 重复内容：这条本来就不该发出去，直接撤掉气泡并提示
  if (msg.code === "duplicate_text") {
    item.el.remove();
    pending.delete(msg.client_id);
    toast(msg.reason);
    return;
  }
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
  const num = Number(id);
  if (!num) { toast("这条消息还没有编号，无法撤回"); return; }
  if (!ws || ws.readyState !== WebSocket.OPEN) { toast("连接已断开，稍后再试"); return; }
  ws.send(JSON.stringify({ type: "recall", id: num }));
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
function onRoster(msg) {
  $("online").textContent = msg.online;
  ipMap.clear();
  reads.clear();
  (msg.users || []).forEach((u) => {
    if (u.same_ip_as) ipMap.set(u.name, u.same_ip_as);
    reads.set(u.name, u.last_read_id || 0);
  });
  refreshNotes();
  refreshReceipts();
}

// 名单变了，已经显示的气泡也要跟着更新标记
function refreshNotes() {
  $("list").querySelectorAll("li.msg").forEach((li) => {
    const name = li.dataset.name;
    const row = li.querySelector(".row");
    if (!name || !row) return;
    const linked = ipMap.get(name);
    let note = row.querySelector(".note");
    if (linked) {
      if (!note) {
        note = document.createElement("span");
        note.className = "note";
        row.appendChild(note);
      }
      note.textContent = `（和 ${linked} 为同一IP）`;
    } else if (note) {
      note.remove();
    }
  });
}

// 登记我发出的消息，用于计算「还有多少人没读」
function registerMine(id, li) {
  const num = Number(id);
  if (!num) return;
  myMsgs.set(num, li);
  refreshReceipts();
}

// 刷新每条「我发的消息」的回执：在线的人里还有几个没读到这一条
function refreshReceipts() {
  const others = [];
  reads.forEach((lastRead, name) => {
    if (name !== myName) others.push(lastRead || 0);
  });

  myMsgs.forEach((li, id) => {
    const el = li.querySelector(".receipt");
    if (!el) return;
    if (others.length === 0) {
      el.textContent = "暂无其他人在线";
      el.style.color = "";
      return;
    }
    const unread = others.filter((r) => r < id).length;
    el.textContent = unread > 0 ? `${unread} 人未读` : "全部已读";
    el.style.color = unread > 0 ? "" : "#86efac";
  });
}

// 横幅只作为「回到上次阅读位置」的入口，不再显示收件箱未读数
function updateBanner() {
  if (firstUnreadId) {
    $("bannerText").textContent = "你上次看到这里";
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
  firstUnreadId = null;
  updateBanner();
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

// 轻量提示条：撤回失败、发送被拒等短信息
function toast(text) {
  let el = $("toast");
  if (!el) {
    el = document.createElement("div");
    el.id = "toast";
    document.body.appendChild(el);
  }
  el.textContent = text;
  el.classList.add("show");
  clearTimeout(window._toastTimer);
  window._toastTimer = setTimeout(() => el.classList.remove("show"), 2800);
}

function nearBottom() {
  const list = $("list");
  return list.scrollTop + list.clientHeight >= list.scrollHeight - 60;
}

function updateJump() {
  const btn = $("jump");
  if (!btn) return;
  if (nearBottom()) {
    btn.classList.add("hidden");
    return;
  }
  btn.classList.remove("hidden");
  btn.textContent = newCount > 0 ? `↓ 新消息 ${newCount}` : "↓ 到底部";
}

function bumpIfNotBottom() {
  const list = $("list");
  if (nearBottom()) {
    list.scrollTop = list.scrollHeight;
    newCount = 0;
  } else {
    newCount += 1;
  }
  updateJump();
}

function jumpLatest() {
  const list = $("list");
  list.scrollTop = list.scrollHeight;
  newCount = 0;
  updateJump();
  maybeMarkRead();
}

function scrollToBottom(force) {
  const list = $("list");
  if (force || nearBottom()) {
    list.scrollTop = list.scrollHeight;
    newCount = 0;
  }
  updateJump();
}

$("list").addEventListener("scroll", () => {
  updateJump();
  clearTimeout(window._readTimer);
  window._readTimer = setTimeout(maybeMarkRead, 500);
});

$("text").addEventListener("keydown", (e) => { if (e.key === "Enter") send(); });
$("name").addEventListener("keydown", (e) => { if (e.key === "Enter") doJoin(false); });

restoreNickname();
connect();
</script>
</body>
</html>
"""
