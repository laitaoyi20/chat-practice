from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse      # ← 新增

app = FastAPI()

clients = set()

@app.get("/health")
def health():
    return {"ok": True, "online": len(clients)}

@app.get("/")
def home():
    html = """
    <h1>第4组聊天室</h1>
    <a href="/about">聊天室说明</a>
    <p>状态：<b id="state">未连接</b></p>

    <input id="text" placeholder="说点什么">
    <button onclick="send()">发送</button>

    <ul id="list"></ul>

    <script>
      const ws = new WebSocket(`ws://${location.host}/ws`);
      const state = document.getElementById("state");

      ws.onopen = () => { state.textContent = "已连接"; state.style.color = "green"; };
      ws.onclose = () => { state.textContent = "已断开"; state.style.color = "red"; };

      // ↓↓↓ 新增：服务器发来消息时，显示出来
      ws.onmessage = (event) => { add(event.data); };

      // ↓↓↓ 改：先发给服务器
      function send() {
        const box = document.getElementById("text");
        if (box.value === "") return;
        ws.send(box.value);
        box.value = "";
      }

      // ↓↓↓ 新增：把"加一行"抽成一个函数，两处共用
      function add(text) {
        const item = document.createElement("li");
        item.textContent = text;
        document.getElementById("list").appendChild(item);
      }
    </script>

    """                                        
    return HTMLResponse(html)                    


@app.get("/about")
def about():
    html = """
    <h1>聊天室说明</h1>
    <p>另开网页：聊天室网页+/health</p>
    <p>可以查看在线人数</p>
    """                                          
    return HTMLResponse(html)


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()          # 1. 握手
    clients.add(websocket)            # 2. 登记：这个人来了
    print(f"有人进来了，当前在线 {len(clients)} 人")

    try:
        while True:                                   # 3. 一直等
            message = await websocket.receive_text()
            print(f"转发: {message}")

            for client in list(clients):              # 4. 发给每一个人
                try:
                    await client.send_text(message)
                except Exception:
                    clients.discard(client)           # 发不出去 = 他断了

    except WebSocketDisconnect:                       # 5. 正常断开
        print("有人离开了")
    finally:
        clients.discard(websocket)                    # 6. 无论如何都注销
        print(f"当前在线 {len(clients)} 人")