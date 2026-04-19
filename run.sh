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

# 检查Node.js和npm是否安装（用于构建前端）
if ! command -v node &> /dev/null; then
    echo "错误: 未找到Node.js，请先安装Node.js"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "错误: 未找到npm，请先安装npm"
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

# 安装Python依赖
echo "安装Python依赖包..."
python -m pip install -r requirements.txt -q

# 构建前端
echo ""
echo "构建前端..."
cd frontend

# 检查是否已安装前端依赖
if [ ! -d "node_modules" ]; then
    echo "安装前端依赖..."
    npm install
fi

# 构建前端
echo "构建前端应用..."
npm run build

cd ..

echo ""
echo "前端已输出到 static/。若页面仍是旧版文案或图表空白，请浏览器硬刷新：Ctrl+Shift+R（Mac：Cmd+Shift+R）。"
echo ""

# 启动应用
echo ""
echo "启动应用..."
echo "访问地址: http://localhost:8086"
echo "按 Ctrl+C 停止服务"
echo ""

python app.py
