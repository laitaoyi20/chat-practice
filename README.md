# 第 4 组 · 局域网聊天室

用 Python + FastAPI + WebSocket 实现的简易聊天室。同一 Wi-Fi 或手机热点下的多台设备，用浏览器打开即可实时互发消息。

**功能**

- **三个独立房间**：茶室 / 咖啡厅 / 漫画工作室，数据完全隔离
- 进入流程：起昵称 → 选房间（换房间不用重输昵称）
- 多设备实时群聊
- 在线人数按**不同 IP** 统计：同一 IP 开多个网页只算 1 人
- 同一 IP 用第二个昵称进入会先警告；确认后标记「和某某为同一 IP」
- **已读回执**：自己发的消息下方显示「N 人未读 / 全部已读」
- **上次阅读位置**：橙色分割线标出，可一键跳回（6 秒后淡出）
- **消息撤回**：仅本人、5 分钟内，撤回后所有人显示「已撤回」
- **离线补消息**：重连后自动下发未读区间
- **发送失败重试**：失败可点「重试」，同一条消息不会重复
- **断线自动重连**：指数退避，最多 15 秒
- **输入校验**：空消息、超 500 字、0.4 秒内连发、10 秒内重复同一句，都会被拒绝并提示原因
- **历史消息**：每次加载 50 条；可点「加载更早的消息」反复往前翻
- **可添加到手机桌面**（PWA），全屏打开像 App

**规则**

- 在线人数的单位是「人」，不是「网页」。判定依据是客户端 IP。
- 同一 IP 入场最早的人为主账号，其余人自动关联到主账号。
- 刷新页面视为同一人重连，不触发警告。
- 身份 = `IP + 昵称`，同一 IP 换昵称就是另一个人。

> 完整功能清单与验收步骤见 [FEATURES.md](FEATURES.md)。

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

## 添加到手机桌面（PWA）

手机浏览器打开聊天室地址后：

| 系统 | 操作 |
| --- | --- |
| **iPhone / iPad**（Safari） | 点底部「分享」→ 向下找「添加到主屏幕」→ 添加 |
| **安卓**（Chrome / Edge） | 点右上角「⋮」→「添加到主屏幕」→ 添加 |

加完后桌面会出现一个蓝色气泡图标，点开是全屏，没有地址栏。

> ⚠️ 图标里保存的是**当时的完整地址**。如果电脑的局域网 IP 变了，图标会打不开——需要删掉重新添加。
> 想让地址长期不变，可以在路由器里给这台电脑绑定固定 IP（DHCP 静态分配）。

> 由于是 HTTP（非 HTTPS），Service Worker 被浏览器禁用，因此**不支持离线使用**；离线能力需要 HTTPS 才行。

---

## 页面地址

| 地址 | 说明 |
| --- | --- |
| `/` | 聊天室主页面（昵称 → 选房间） |
| `/about` | 聊天室规则说明 |
| `/health` | 按房间统计的 `online`（IP 去重）、`connections`、`messages` |
| `/ws` | WebSocket 端点（JSON 协议：join / chat / recall / read / history_more） |
| `/manifest.webmanifest` | PWA 清单 |
| `/icon-180.png`、`/icon-192.png`、`/icon-512.png` | 桌面图标 |

### 通信协议

```
客户端 → 服务端
  {"type":"join",   "name":"张三"}
  {"type":"join",   "name":"李四", "confirm":true}
  {"type":"chat",   "text":"你好", "client_id":"uuid"}
  {"type":"recall", "id":12}
  {"type":"read",   "last_message_id":12}
  {"type":"history_more", "before_id":71}

服务端 → 客户端
  welcome / duplicate / history / history_page
  chat / ack / reject / recall / system / roster / unread / error
```

---

## 项目结构

```
chat-practice/
├── main.py          # 全部后端、页面与数据库逻辑
├── chat.db          # SQLite 数据库（首次启动自动生成，不提交）
├── pyproject.toml   # 依赖清单
├── uv.lock          # 依赖版本锁
├── README.md
└── .venv/           # 虚拟环境（不提交）
```

---

## 数据存储

使用 Python 自带的 SQLite，**不需要安装数据库服务**。首次启动时在项目目录生成 `chat.db`。

| 表 | 字段 | 作用 |
| --- | --- | --- |
| `users` | id, ip, name, created_at, last_seen_at | 身份档案，唯一键 `(ip, name)` |
| `messages` | id, user_id, name, ip, text, created_at, status, recalled_at, client_id | 消息本体；`status=recalled` 表示已撤回 |
| `read_state` | user_id, last_read_id, updated_at | 每人读到了哪条，用于算未读 |

设计要点：

- `messages.client_id` 上有唯一索引 `(user_id, client_id)`，**重试不会产生重复消息**。
- 撤回不删除记录，只把 `status` 改成 `recalled`，历史里永远显示「已撤回」。
- 已读位置只增不减，避免乱序请求把进度改回去。
- `chat.db` 已加入 `.gitignore`，每台机器各自一份，不随仓库分发。

---

## 技术栈

Python 3.12 · FastAPI · uvicorn · WebSocket · 原生 HTML/CSS/JavaScript
