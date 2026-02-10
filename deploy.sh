#!/bin/bash

# 项目部署脚本
# 使用方法: ./deploy.sh [docker|traditional]

set -e

DEPLOY_METHOD=${1:-docker}

echo "🚀 开始部署项目..."

if [ "$DEPLOY_METHOD" = "docker" ]; then
    echo "📦 使用 Docker 方式部署..."
    
    # 检查 Docker 是否安装
    if ! command -v docker &> /dev/null; then
        echo "❌ 错误: 未安装 Docker，请先安装 Docker"
        exit 1
    fi
    
    # 检查 Docker Compose 是否安装
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ 错误: 未安装 Docker Compose，请先安装 Docker Compose"
        exit 1
    fi
    
    # 构建并启动
    echo "🔨 构建 Docker 镜像..."
    docker-compose build
    
    echo "🚀 启动服务..."
    docker-compose up -d
    
    echo "✅ 部署完成！"
    echo "📝 查看日志: docker-compose logs -f"
    echo "🌐 访问地址: http://localhost:8086"
    
elif [ "$DEPLOY_METHOD" = "traditional" ]; then
    echo "📦 使用传统方式部署..."
    
    # 检查 Python
    if ! command -v python3 &> /dev/null; then
        echo "❌ 错误: 未安装 Python3"
        exit 1
    fi
    
    # 检查 Node.js
    if ! command -v node &> /dev/null; then
        echo "❌ 错误: 未安装 Node.js"
        exit 1
    fi
    
    # 创建虚拟环境
    if [ ! -d "venv" ]; then
        echo "📦 创建 Python 虚拟环境..."
        python3 -m venv venv
    fi
    
    # 激活虚拟环境
    source venv/bin/activate
    
    # 安装 Python 依赖
    echo "📦 安装 Python 依赖..."
    pip install -r requirements.txt
    
    # 安装 Gunicorn（生产环境）
    pip install gunicorn
    
    # 构建前端
    echo "🔨 构建前端..."
    cd frontend
    npm install
    npm run build
    cd ..
    
    # 复制前端构建产物到 static 目录
    echo "📋 复制前端文件..."
    mkdir -p static
    cp -r frontend/dist/* static/ 2>/dev/null || true
    
    echo "✅ 部署完成！"
    echo "🚀 启动服务:"
    echo "   开发环境: python app.py"
    echo "   生产环境: gunicorn -w 4 -b 0.0.0.0:8086 --timeout 600 app:app"
    
else
    echo "❌ 错误: 未知的部署方式 '$DEPLOY_METHOD'"
    echo "使用方法: ./deploy.sh [docker|traditional]"
    exit 1
fi
