#!/bin/bash
# 安装定时任务到 macOS launchd

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_FILE="$SCRIPT_DIR/com.bilibili.quant.scheduled.plist"
PLIST_NAME="com.bilibili.quant.scheduled"

echo "==================================="
echo "  安装定时任务（macOS launchd）"
echo "==================================="
echo ""

# 检查 plist 文件是否存在
if [ ! -f "$PLIST_FILE" ]; then
    echo "错误: 找不到 plist 文件: $PLIST_FILE"
    exit 1
fi

# 更新 plist 文件中的路径
echo "更新 plist 文件中的路径..."
# 获取 Python3 路径
PYTHON3_PATH=$(which python3)
if [ -z "$PYTHON3_PATH" ]; then
    echo "错误: 找不到 python3，请先安装 Python"
    exit 1
fi

# 使用 sed 更新路径（macOS 兼容）
sed -i '' "s|/usr/local/bin/python3|$PYTHON3_PATH|g" "$PLIST_FILE"
sed -i '' "s|/Users/bilibili/Desktop/test/量化|$SCRIPT_DIR|g" "$PLIST_FILE"

# 创建日志目录
mkdir -p "$SCRIPT_DIR/logs"

# 卸载旧的任务（如果存在）
if launchctl list | grep -q "$PLIST_NAME"; then
    echo "卸载旧的任务..."
    launchctl unload ~/Library/LaunchAgents/"$PLIST_NAME".plist 2>/dev/null || true
fi

# 复制 plist 文件到 LaunchAgents 目录
echo "复制 plist 文件到 ~/Library/LaunchAgents/..."
cp "$PLIST_FILE" ~/Library/LaunchAgents/"$PLIST_NAME".plist

# 加载任务
echo "加载定时任务..."
launchctl load ~/Library/LaunchAgents/"$PLIST_NAME".plist

# 检查任务状态
if launchctl list | grep -q "$PLIST_NAME"; then
    echo ""
    echo "✓ 定时任务安装成功！"
    echo ""
    echo "任务信息:"
    echo "  - 名称: $PLIST_NAME"
    echo "  - 执行时间: 每天 18:00"
    echo "  - 日志文件: $SCRIPT_DIR/logs/scheduled_task.log"
    echo ""
    echo "管理命令:"
    echo "  查看任务: launchctl list | grep $PLIST_NAME"
    echo "  卸载任务: launchctl unload ~/Library/LaunchAgents/$PLIST_NAME.plist"
    echo "  立即执行: launchctl start $PLIST_NAME"
    echo "  停止任务: launchctl stop $PLIST_NAME"
else
    echo ""
    echo "✗ 定时任务安装失败，请检查错误信息"
    exit 1
fi
