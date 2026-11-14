"""
第 4 组 · 局域网聊天室 V2

技术栈：FastAPI + WebSocket + 原生 HTML/JavaScript

启动：
    uv run python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

本版规则（V2）：
    1. 在线人数按「不同 IP」统计，同一 IP 开多个网页只算 1 人。
    2. 进入聊天室前必须设置昵称（1~16 字）。
    3. 同一 IP 的第二个昵称进入时，服务端先返回 duplicate 警告；
       用户确认后放行，并标记 same_ip_as = 该 IP 的首个昵称，
       界面显示「（和 XX 为同一IP）」。

服务端职责：
    GET  /          返回聊天页面
    GET  /about     返回规则说明
    GET  /health    返回按 IP 去重的在线人数
    WS   /ws        长连接：join / chat 两类消息
"""

import json
from dataclasses import dataclass

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------

@dataclass
class Client:
    """一条 WebSocket 连接。

    注意：一个 Client ≠ 一个人。同一 IP 的多个网页会产生多个 Client，
    但「在线人数」按 IP 去重后只算一个。
    """

    ws: WebSocket
    ip: str                 # 客户端 IP，本版的唯一身份
    seq: int                # 入场序号，用来判定同一 IP 的「首个账号」
    name: str | None = None  # 昵称；None 表示已连接但还没入场
    same_ip_as: str | None = None  # 若与同 IP 的主账号关联，记主账号昵称


# 连接表：key 是 WebSocket 对象，value 是它对应的 Client
clients: dict[WebSocket, Client] = {}

_seq = 0


def next_seq() -> int:
    """生成递增入场序号。"""
    global _seq
    _seq += 1
    return _seq


def normalize_ip(raw: str | None) -> str:
    """统一 IP 写法：本机可能拿到 ::1 或 ::ffff:127.0.0.1，统一成 127.0.0.1。"""
    ip = raw or "unknown"
    if ip.startswith("::ffff:"):
        ip = ip[7:]
    if ip == "::1":
        ip = "127.0.0.1"
    return ip


def ip_members(ip: str) -> list[Client]:
    """该 IP 下已入场的成员，按入场先后排序。"""
    return sorted(
        (c for c in clients.values() if c.ip == ip and c.name),
        key=lambda c: c.seq,
    )


def online_ip_count() -> int:
    """在线人数：按 IP 去重，而不是数连接。"""
    return len({c.ip for c in clients.values() if c.name})


def renormalize_ip(ip: str) -> None:
    """某个 IP 的成员变动后，重算主账号与关联关系。

    规则：该 IP 入场最早的人为主账号（same_ip_as = None），
    其余人一律关联到主账号。
    """
    members = ip_members(ip)
    if not members:
        return
    primary = members[0]
    primary.same_ip_as = None
    for member in members[1:]:
        member.same_ip_as = primary.name


# ---------------------------------------------------------------------------
# 广播
# ---------------------------------------------------------------------------

async def broadcast(payload: dict) -> None:
    """向所有已入场的连接发送一条 JSON；发送失败的连接就地清掉。"""
    dead: list[Client] = []
    for client in list(clients.values()):
        if not client.name:
            continue          # 还没入场的人收不到聊天内容
        try:
            await client.ws.send_json(payload)
        except Exception:
            dead.append(client)
    for client in dead:
        clients.pop(client.ws, None)


async def broadcast_system(message: str) -> None:
    await broadcast({"type": "system", "message": message})


async def broadcast_roster() -> None:
    """广播在线名单，供各端刷新「在线人数」与「同一 IP」标记。"""
    users = [
        {"name": c.name, "ip": c.ip, "same_ip_as": c.same_ip_as}
        for c in sorted(clients.values(), key=lambda c: c.seq)
        if c.name
    ]
    await broadcast({"type": "roster", "online": online_ip_count(), "users": users})


# ---------------------------------------------------------------------------
# 业务处理
# ---------------------------------------------------------------------------

async def handle_join(me: Client, data: dict) -> None:
    """处理入场请求。

    流程：
        join（不带 confirm）
            ├─ 该 IP 已有其他人 → 回 duplicate 警告，不入场
            └─ 该 IP 没人       → 直接入场
        join（带 confirm=true）
            └─ 无视警告，入场并标记 same_ip_as
    """
    name = (data.get("name") or "").strip()[:16]
    if not name:
        await me.ws.send_json({"type": "error", "message": "昵称不能为空"})
        return

    # 同名重连（刷新页面、断线重连）视为同一个人：
    # 清掉该 IP 下同名的旧连接，避免自己和自己撞车
    for other in [c for c in clients.values()
                  if c.ip == me.ip and c.name == name and c.ws is not me.ws]:
        clients.pop(other.ws, None)

    members = ip_members(me.ip)

    # 该 IP 已有人在用别的昵称，且未确认 → 只警告，不放行
    if members and not data.get("confirm"):
        await me.ws.send_json({
            "type": "duplicate",
            "ip": me.ip,
            "existing_name": members[0].name,
            "online": online_ip_count(),
        })
        return

    me.name = name
    # 同 IP 的第二个及以后的昵称，关联到首个昵称
    me.same_ip_as = members[0].name if members else None

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


async def handle_chat(me: Client, data: dict) -> None:
    """处理聊天消息。"""
    if not me.name:
        await me.ws.send_json({"type": "error", "message": "请先设置昵称再发言"})
        return

    text = (data.get("text") or "").strip()
    if not text:
        return
    text = text[:500]

    await broadcast({
        "type": "chat",
        "from": me.name,
        "text": text,
        "ip": me.ip,
        "same_ip_as": me.same_ip_as,
    })


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    """健康检查。

    online：按 IP 去重的人数（本版口径）
    connections：实际连接数，用于区分「一个人开多个网页」的情况
    """
    return {
        "ok": True,
        "online": online_ip_count(),
        "connections": len([c for c in clients.values() if c.name]),
    }


@app.get("/")
def home() -> HTMLResponse:
    """聊天室页面（结构与脚本内联，单文件即可分发）。"""
    html = """
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>第4组聊天室</title>
<style>
  :root { --line: #d8dee6; --muted: #7b8794; --bg: #f6f8fa; }
  * { box-sizing: border-box; }
  body {
    font-family: system-ui, -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif;
    margin: 0; background: var(--bg); color: #1f2933;
  }
  .hidden { display: none !important; }

  /* 入场遮罩 */
  #join {
    position: fixed; inset: 0; background: rgba(246,248,250,.96);
    display: flex; align-items: center; justify-content: center; z-index: 10;
  }
  .card {
    background: #fff; padding: 28px; border-radius: 12px; width: min(92vw, 380px);
    box-shadow: 0 8px 30px rgba(0,0,0,.08);
  }
  .card h2 { margin: 0 0 6px; font-size: 20px; }
  .card .hint { color: var(--muted); font-size: 13px; margin: 0 0 16px; }
  .card input {
    width: 100%; padding: 10px 12px; font-size: 15px;
    border: 1px solid var(--line); border-radius: 8px; outline: none;
  }
  .card input:focus { border-color: #3b82f6; }
  .card button {
    margin-top: 12px; width: 100%; padding: 10px; font-size: 15px;
    border: 0; border-radius: 8px; background: #2563eb; color: #fff; cursor: pointer;
  }
  .card button:hover { background: #1d4ed8; }
  .warn {
    margin-top: 14px; padding: 12px; border-radius: 8px; font-size: 13px; line-height: 1.6;
    background: #fff7ed; border: 1px solid #fed7aa; color: #9a3412;
  }
  .warn button { background: #ea580c; margin-top: 10px; }
  .warn button:hover { background: #c2410c; }

  /* 主界面 */
  #app { max-width: 720px; margin: 0 auto; padding: 18px 16px 90px; }
  header { display: flex; flex-wrap: wrap; gap: 12px; align-items: baseline; }
  header h1 { font-size: 20px; margin: 0; }
  .meta { display: flex; gap: 14px; font-size: 13px; color: var(--muted); margin-left: auto; }
  .meta b { color: #1f2933; }
  #state { font-weight: 600; }

  #list {
    list-style: none; padding: 12px; margin: 14px 0 0;
    background: #fff; border: 1px solid var(--line); border-radius: 10px;
    height: min(62vh, 520px); overflow-y: auto;
  }
  #list li { padding: 6px 4px; line-height: 1.6; font-size: 14px; word-break: break-word; }
  #list li b { color: #1d4ed8; }
  #list li.sys { color: var(--muted); font-size: 13px; }
  #list li .note { color: #b45309; font-size: 12px; margin-left: 4px; }

  .inputbar {
    position: fixed; left: 0; right: 0; bottom: 0; background: #fff;
    border-top: 1px solid var(--line); padding: 12px 16px;
    display: flex; gap: 10px; max-width: 720px; margin: 0 auto;
  }
  .inputbar input {
    flex: 1; padding: 10px 12px; font-size: 15px;
    border: 1px solid var(--line); border-radius: 8px; outline: none;
  }
  .inputbar input:focus { border-color: #3b82f6; }
  .inputbar button {
    padding: 10px 22px; font-size: 15px; border: 0; border-radius: 8px;
    background: #2563eb; color: #fff; cursor: pointer;
  }
  .inputbar button:hover { background: #1d4ed8; }
</style>
</head>
<body>

<!-- 入场遮罩：必须先设昵称 -->
<div id="join">
  <div class="card">
    <h2>进入聊天室</h2>
    <p class="hint">输入昵称（1~16 字）。同一网络（IP）只用输入一次，用第二个昵称会先警告。</p>
    <input id="name" maxlength="16" placeholder="你的昵称" autocomplete="off">
    <button onclick="doJoin(false)">进入</button>
    <div id="warn" class="warn hidden">
      <div id="warnText"></div>
      <button onclick="doJoin(true)">仍然进入</button>
    </div>
  </div>
</div>

<!-- 主界面 -->
<div id="app" class="hidden">
  <header>
    <h1>第4组聊天室</h1>
    <div class="meta">
      <span>我：<b id="me">-</b></span>
      <span>在线：<b id="online">0</b> 人</span>
      <span id="state">未连接</span>
    </div>
  </header>
  <ul id="list"></ul>
  <div class="inputbar">
    <input id="text" placeholder="说点什么…" autocomplete="off">
    <button onclick="send()">发送</button>
  </div>
</div>

<script>
  let ws = null;
  let myName = null;

  const $ = (id) => document.getElementById(id);

  function setState(text, color) {
    $("state").textContent = text;
    $("state").style.color = color;
  }

  function connect() {
    ws = new WebSocket(`ws://${location.host}/ws`);
    ws.onopen = () => {
      setState("已连接", "#15803d");
      if (myName) sendJoin(myName, true);   // 断线重连后自动归位
    };
    ws.onclose = () => {
      setState("已断开，2 秒后重连…", "#c0392b");
      setTimeout(connect, 2000);
    };
    ws.onmessage = (event) => handle(JSON.parse(event.data));
  }

  function sendJoin(name, confirm) {
    ws.send(JSON.stringify({ type: "join", name: name, confirm: !!confirm }));
  }

  function doJoin(confirm) {
    const name = $("name").value.trim();
    if (!name) {
      $("warnText").textContent = "昵称不能为空。";
      $("warn").classList.remove("hidden");
      return;
    }
    myName = name;
    sendJoin(name, confirm);
  }

  function handle(msg) {
    if (msg.type === "welcome") {
      $("join").classList.add("hidden");
      $("app").classList.remove("hidden");
      $("me").textContent = msg.name
        + (msg.same_ip_as ? `（和 ${msg.same_ip_as} 为同一IP）` : "");
      $("online").textContent = msg.online;
      $("text").focus();
      return;
    }

    if (msg.type === "duplicate") {
      $("warnText").innerHTML =
        `此网络（IP <b>${msg.ip}</b>）已有昵称「<b>${msg.existing_name}</b>」在线。<br>`
        + `继续进入将以「${myName}」登录，并标记为「和 ${msg.existing_name} 为同一IP」。`;
      $("warn").classList.remove("hidden");
      return;
    }

    if (msg.type === "chat") {
      addLine(msg.from, msg.text, msg.same_ip_as ? `和 ${msg.same_ip_as} 为同一IP` : null, null);
      return;
    }

    if (msg.type === "system") {
      addLine(null, msg.message, null, "sys");
      return;
    }

    if (msg.type === "roster") {
      $("online").textContent = msg.online;
      return;
    }

    if (msg.type === "error") {
      $("warnText").textContent = msg.message;
      $("warn").classList.remove("hidden");
    }
  }

  function addLine(name, text, note, cls) {
    const li = document.createElement("li");
    if (cls) li.className = cls;
    if (name) {
      const who = document.createElement("b");
      who.textContent = name;
      li.appendChild(who);
      li.appendChild(document.createTextNode("：" + text));
    } else {
      li.textContent = text;
    }
    if (note) {
      const span = document.createElement("span");
      span.className = "note";
      span.textContent = "（" + note + "）";
      li.appendChild(span);
    }
    $("list").appendChild(li);
    $("list").scrollTop = $("list").scrollHeight;
  }

  function send() {
    const box = $("text");
    const text = box.value.trim();
    if (!text) return;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      addLine(null, "（连接已断开，消息未发出）", null, "sys");
      return;
    }
    ws.send(JSON.stringify({ type: "chat", text: text }));
    box.value = "";
  }

  $("text").addEventListener("keydown", (e) => {
    if (e.key === "Enter") send();
  });
  $("name").addEventListener("keydown", (e) => {
    if (e.key === "Enter") doJoin(false);
  });

  connect();
</script>
</body>
</html>
    """
    return HTMLResponse(html)


@app.get("/about")
def about() -> HTMLResponse:
    """规则说明页。"""
    html = """
    <h1>聊天室规则</h1>
    <ul>
      <li>在线人数按<b>不同 IP</b> 统计：同一 IP 开多个网页只算 1 人。</li>
      <li>进入时需要设置昵称（1~16 字）。</li>
      <li>同一 IP 用第二个昵称进入会先收到警告，确认后放行，并标记为
          「和 首个昵称 为同一IP」。</li>
      <li>刷新页面视为同一人重连，不会触发警告。</li>
    </ul>
    <p>查看在线人数：本页地址后面加 <code>/health</code></p>
    """
    return HTMLResponse(html)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    """聊天室主连接。

    协议（均为 JSON）：
        客户端 → 服务端
            {"type": "join", "name": "张三"}                 首次入场
            {"type": "join", "name": "李四", "confirm": true} 无视同 IP 警告
            {"type": "chat", "text": "你好"}                  发言

        服务端 → 客户端
            welcome / duplicate / chat / system / roster / error
    """
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

    except WebSocketDisconnect:
        pass

    finally:
        clients.pop(websocket, None)
        if me.name:
            renormalize_ip(me.ip)   # 主账号离开时，该 IP 的关联关系要重算
            await broadcast_system(f"{me.name} 离开了聊天室")
            await broadcast_roster()
