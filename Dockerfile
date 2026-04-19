# 多阶段构建：前端构建阶段
FROM node:18-alpine AS frontend-builder

WORKDIR /app/frontend

# 复制前端依赖文件
COPY frontend/package*.json ./

# 安装前端依赖
# 构建需要 vite 等在 devDependencies，不能用 --omit=dev
RUN npm ci

# 复制前端源代码
COPY frontend/ ./

# 构建前端（生产环境）
RUN npm run build

# Python 后端阶段
FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖（curl：compose healthcheck；gcc/g++：部分 Python 包编译）
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# 复制 Python 依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制后端代码
COPY app.py .
COPY strategy_engine.py .
COPY data_fetcher.py .
COPY stock_code_utils.py .
COPY stock_list_sources.py .
COPY emotion_cycle_service.py .
COPY configs ./configs/
COPY common_strategies.json .

# 从构建阶段复制前端构建产物
COPY --from=frontend-builder /app/frontend/dist ./static

# 复制模板文件（如果需要）
COPY templates/ ./templates/

# 创建必要的目录
RUN mkdir -p cache/stock_data results

# 设置环境变量
ENV FLASK_APP=app.py
ENV FLASK_ENV=production
ENV FLASK_PORT=8086
ENV PYTHONUNBUFFERED=1

# 暴露端口
EXPOSE 8086

# 启动应用
CMD ["python", "app.py"]
