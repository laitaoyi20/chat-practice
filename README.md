# 局域网聊天室

一个用 FastAPI + WebSocket 实现的简易聊天室，支持多台设备在同一局域网内实时收发消息。

## 技术栈

- Python 3.12
- FastAPI
- uvicorn
- WebSocket

## 如何运行

1. 安装 [uv](https://docs.astral.sh/uv/)
2. 克隆项目并进入目录

   ```bash
   git clone https://github.com/laitaoyi20/chat-practice.git
   cd chat-practice
3. 安装依赖
   uv sync
4. 启动服务
   uv run python -m uvicorn main:app --host 0.0.0.0 --port 8000
5. 浏览器打开 
   http://127.0.0.1:8000 

   6. 其他说明
      局域网内其他设备访问
    在运行服务的电脑上执行 ipconfig，记下 WLAN 的 IPv4 地址
    其他设备连接同一个 Wi-Fi/热点后，访问 
    http://你的IP:8000