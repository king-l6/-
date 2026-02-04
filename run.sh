#!/bin/bash

# A股策略回测系统启动脚本

echo "==================================="
echo "  A股策略回测系统"
echo "==================================="
echo ""

# 检查Python是否安装
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请先安装Python"
    exit 1
fi

# 检查是否已安装依赖
if [ ! -d "venv" ]; then
    echo "创建虚拟环境..."
    python3 -m venv venv
fi

# 激活虚拟环境
echo "激活虚拟环境..."
source venv/bin/activate

# 加载.env文件（如果存在）
if [ -f .env ]; then
    echo "加载 .env 配置文件..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# 安装依赖
echo "安装依赖包..."
pip3 install -r requirements.txt -q

# 启动应用
echo ""
echo "启动应用..."
echo "访问地址: http://localhost:8086"
echo "按 Ctrl+C 停止服务"
echo ""

python3 app.py
