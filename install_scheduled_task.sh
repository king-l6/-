#!/bin/bash
# 安装定时任务到 macOS launchd（每日 18:00 增量拉数 → 18:20 概念/板块按日快照 → 18:40 增量回测+企微）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PLIST_NAMES=(
  "com.bilibili.quant.incremental.pull"
  "com.bilibili.quant.incremental.concept"
  "com.bilibili.quant.scheduled"
)

echo "==================================="
echo "  安装定时任务（macOS launchd）"
echo "==================================="
echo ""

PYTHON3_PATH=""
if [ -x "$SCRIPT_DIR/venv/bin/python3" ]; then
  PYTHON3_PATH="$SCRIPT_DIR/venv/bin/python3"
elif command -v python3 &>/dev/null; then
  PYTHON3_PATH="$(which python3)"
else
  echo "错误: 找不到 venv/bin/python3 或系统 python3"
  exit 1
fi

mkdir -p "$SCRIPT_DIR/logs"

for NAME in "${PLIST_NAMES[@]}"; do
  PLIST_FILE="$SCRIPT_DIR/${NAME}.plist"
  if [ ! -f "$PLIST_FILE" ]; then
    echo "错误: 找不到 plist 文件: $PLIST_FILE"
    exit 1
  fi
  echo "更新路径: $NAME.plist"
  sed -i '' "s|/usr/local/bin/python3|$PYTHON3_PATH|g" "$PLIST_FILE"
  sed -i '' "s|/Users/bilibili/Desktop/test/量化|$SCRIPT_DIR|g" "$PLIST_FILE"
done

for NAME in "${PLIST_NAMES[@]}"; do
  if launchctl list 2>/dev/null | grep -q "$NAME"; then
    echo "卸载旧任务: $NAME"
    launchctl unload ~/Library/LaunchAgents/"$NAME".plist 2>/dev/null || true
  fi
done

echo "复制 plist 到 ~/Library/LaunchAgents/..."
for NAME in "${PLIST_NAMES[@]}"; do
  cp "$SCRIPT_DIR/${NAME}.plist" ~/Library/LaunchAgents/"$NAME".plist
done

echo "加载定时任务..."
for NAME in "${PLIST_NAMES[@]}"; do
  launchctl load ~/Library/LaunchAgents/"$NAME".plist
done

ok=true
for NAME in "${PLIST_NAMES[@]}"; do
  if ! launchctl list 2>/dev/null | grep -q "$NAME"; then
    ok=false
    break
  fi
done

if $ok; then
    echo ""
    echo "✓ 定时任务安装成功！"
    echo ""
    echo "任务与时间:"
    echo "  - com.bilibili.quant.incremental.pull   每天 18:00  增量拉日 K（--no-backtest）"
    echo "  - com.bilibili.quant.incremental.concept 每天 18:20  增量补齐 sector_linkage/daily"
    echo "  - com.bilibili.quant.scheduled          每天 18:40  增量回测 + 企业微信"
    echo ""
    echo "日志目录: $SCRIPT_DIR/logs/"
    echo ""
    echo "管理示例:"
    echo "  launchctl list | grep bilibili.quant"
    echo "  launchctl unload ~/Library/LaunchAgents/com.bilibili.quant.scheduled.plist"
    echo "  launchctl start com.bilibili.quant.scheduled"
else
    echo ""
    echo "✗ 部分任务未出现在 launchctl list，请检查 logs 下错误输出"
    exit 1
fi
