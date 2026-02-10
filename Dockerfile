# 多阶段构建：前端构建阶段
FROM node:18-alpine AS frontend-builder

WORKDIR /app/frontend

# 复制前端依赖文件
COPY frontend/package*.json ./

# 安装前端依赖
RUN npm ci --only=production

# 复制前端源代码
COPY frontend/ ./

# 构建前端（生产环境）
RUN npm run build

# Python 后端阶段
FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
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
