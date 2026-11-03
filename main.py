"""
第 4 组 · 局域网聊天室

技术栈：FastAPI + WebSocket + 原生 HTML/JavaScript

启动：
    uv run python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

服务端职责：
    GET  /          返回聊天页面
    GET  /about     返回说明页
    GET  /health    返回在线连接数，用于验证设备是否真的连上
    WS   /ws        维持长连接，收到消息后广播给全部在线连接
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# 应用实例。启动命令中的 "main:app" 即「模块名:变量名」——main.py 里的 app 对象
app = FastAPI()

# 在线连接表：一条 WebSocket 连接对应一个浏览器标签页/一台设备。
# 用 set 而非 list，保证同一连接不会被重复登记，且删除是 O(1)
clients: set[WebSocket] = set()


@app.get("/health")
def health() -> dict:
    """健康检查：确认服务存活，并返回当前在线连接数。"""
    return {"ok": True, "online": len(clients)}


@app.get("/")
def home() -> HTMLResponse:
    """聊天室页面。

    页面结构与前端脚本一并内联返回：单文件即可分发，队友无需构建步骤。
    """
    html = """
    <h1>第4组聊天室</h1>
    <a href="/about">聊天室说明</a>
    <p>状态：<b id="state">未连接</b></p>

    <input id="text" placeholder="说点什么">
    <button onclick="send()">发送</button>

    <ul id="list"></ul>

    <script>
      // 用当前页面的地址建立长连接。location.host 会随访问地址自动变化，
      // 因此本机 127.0.0.1 与局域网 10.x.x.x 访问都无需改动代码
      const ws = new WebSocket(`ws://${location.host}/ws`);
      const state = document.getElementById("state");

      // 连接生命周期回调：连接成功 / 断开时更新状态文字
      ws.onopen = () => { state.textContent = "已连接"; state.style.color = "green"; };
      ws.onclose = () => { state.textContent = "已断开"; state.style.color = "red"; };

      // 收到服务器广播时渲染。所有消息都以服务器下发为准，
      // 前端不自行插入本地消息，确保各设备看到的顺序一致
      ws.onmessage = (event) => { add(event.data); };

      // 发送：只推给服务器，不在本地先行渲染，避免重复显示
      function send() {
        const box = document.getElementById("text");
        if (box.value === "") return;          // 空消息不发送
        ws.send(box.value);
        box.value = "";                        // 发送后清空输入框
      }

      // 渲染一行消息：创建 li → 填入文本 → 挂到列表末尾
      function add(text) {
        const item = document.createElement("li");
        item.textContent = text;
        document.getElementById("list").appendChild(item);
      }
    </script>

    """
    return HTMLResponse(html)


@app.get("/about")
def about() -> HTMLResponse:
    """说明页：介绍各地址的用途。"""
    html = """
    <h1>聊天室说明</h1>
    <p>另开网页：聊天室网页+/health</p>
    <p>可以查看在线人数</p>
    """
    return HTMLResponse(html)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    """聊天室主连接。

    生命周期：握手 → 登记 → 循环收消息并广播 → 断开时注销。
    每条连接对应本函数的一次独立执行，彼此互不阻塞。
    """
    await websocket.accept()   # 握手：完成协议升级，未调用则连接无法建立
    clients.add(websocket)     # 登记：加入在线表，之后才能收到广播
    print(f"有人进来了，当前在线 {len(clients)} 人")

    try:
        while True:                                  # 常驻循环：长连接收一条处理一条
            message = await websocket.receive_text()  # 阻塞等待本连接发来文本
            print(f"转发: {message}")

            # 遍历副本：广播期间若有人断开会修改 clients，
            # 直接迭代原集合将抛 RuntimeError: Set changed size during iteration
            for client in list(clients):
                try:
                    await client.send_text(message)
                except Exception:
                    # 发送失败说明对端已断开，立即移除，避免失效连接堆积
                    clients.discard(client)

    except WebSocketDisconnect:
        # 客户端主动关闭（关标签页、刷新、切换网络），属预期行为，非异常
        print("有人离开了")

    finally:
        # 无论正常断开还是异常退出都必须注销，
        # 否则在线数只增不减，最终拖垮服务
        clients.discard(websocket)
        print(f"当前在线 {len(clients)} 人")
