# 第 4 组 · 局域网聊天室

用 Python + FastAPI + WebSocket 实现的简易聊天室。同一 Wi-Fi 或手机热点下的多台设备，用浏览器打开即可实时互发消息。

**功能**

- 多设备实时群聊
- 进入时自设昵称（1~16 字）
- 在线人数按**不同 IP** 统计：同一 IP 开多个网页只算 1 人
- 同一 IP 用第二个昵称进入会先警告；确认后标记「和某某为同一 IP」
- 断线自动重连
- 单文件部署，无需数据库

**规则**

- 在线人数的单位是「人」，不是「网页」。判定依据是客户端 IP。
- 同一 IP 入场最早的人为主账号，其余人自动关联到主账号。
- 刷新页面视为同一人重连，不触发警告。

---

## 如何运行

> 前提：电脑上已安装 `uv` 和 `git`。

### 1. 获取代码

```bash
git clone https://github.com/laitaoyi20/chat-practice.git
cd chat-practice
```

### 2. 安装依赖

```bash
uv sync
```

### 3. 启动服务

```bash
uv run python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

看到 `Uvicorn running on http://0.0.0.0:8000` 即启动成功。停止服务按 `Ctrl + C`。

### 4. 打开页面

浏览器地址栏输入：

```
http://127.0.0.1:8000
```

---

## 局域网内多设备使用

1. 所有设备连接同一个 Wi-Fi / 热点
2. 在**运行服务的电脑**上执行 `ipconfig`，记下 WLAN 的 `IPv4 地址`（如 `10.244.35.95`）
3. 其他设备浏览器访问 `http://10.244.35.95:8000`
4. 打开 `http://10.244.35.95:8000/health` 可查看按 IP 统计的在线人数

> IP 每次重连网络可能变化，使用前重新查询。

---

## 页面地址

| 地址 | 说明 |
| --- | --- |
| `/` | 聊天室主页面（先输昵称再进入） |
| `/about` | 聊天室规则说明 |
| `/health` | `online` 按 IP 去重的人数；`connections` 实际连接数 |
| `/ws` | WebSocket 连接端点（JSON 协议：join / chat） |

---

## 项目结构

```
chat-practice/
├── main.py          # 全部后端与页面代码
├── pyproject.toml   # 依赖清单
├── uv.lock          # 依赖版本锁
├── README.md
└── .venv/           # 虚拟环境（不提交）
```

---

## 技术栈

Python 3.12 · FastAPI · uvicorn · WebSocket · 原生 HTML/CSS/JavaScript
