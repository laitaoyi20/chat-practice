# 第 4 组 · 局域网聊天室

用 Python + FastAPI + WebSocket 实现的简易聊天室。同一 Wi-Fi 或手机热点下的多台设备，用浏览器打开即可实时互发消息。

**功能**

- 多设备实时群聊
- 在线人数查询
- 单文件部署，无需数据库

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
4. 打开 `http://10.244.35.95:8000/health` 可查看当前在线连接数

> IP 每次重连网络可能变化，使用前重新查询。

---

## 页面地址

| 地址 | 说明 |
| --- | --- |
| `/` | 聊天室主页面 |
| `/about` | 聊天室说明 |
| `/health` | 在线人数（JSON） |
| `/ws` | WebSocket 连接端点 |

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
