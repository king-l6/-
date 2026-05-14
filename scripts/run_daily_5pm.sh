#!/bin/bash
# 每天下午 5 点执行：拉取数据 + 六个常用策略回测
# 用法：在 crontab 中添加（请把 /path/to/量化 换成实际项目根目录）：
#   0 17 * * * /path/to/量化/scripts/run_daily_5pm.sh >> /path/to/量化/logs/daily_backtest.log 2>&1

set -e
cd "$(dirname "$0")/.."
mkdir -p logs

# 优先使用 python3
if command -v python3 &>/dev/null; then
    PY=python3
elif command -v python &>/dev/null; then
    PY=python
else
    echo "未找到 python3 或 python"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始执行：拉取数据 + 预设策略回测"
$PY scripts/update_cache_and_backtest.py --workers 100
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 执行结束"
