#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每天下午 5 点自动：拉取数据 + 跑六个常用策略回测

用法：
  # 立即执行一次（拉取数据 + common_strategies 全部策略回测）
  python scripts/daily_backtest_17h.py
  python scripts/daily_backtest_17h.py --now

  # 以定时任务方式运行：每天 17:00 执行一次（前台常驻）
  python scripts/daily_backtest_17h.py --schedule

  # 指定拉取数据并发数
  python scripts/daily_backtest_17h.py --workers 100

配合 crontab（推荐，无需常驻进程）：
  # 每天 17:00 执行
  0 17 * * * cd /path/to/量化 && python3 scripts/update_cache_and_backtest.py >> logs/daily_backtest.log 2>&1

  或使用本脚本的“只跑一次”：
  0 17 * * * cd /path/to/量化 && python3 scripts/daily_backtest_17h.py --now >> logs/daily_backtest.log 2>&1
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault('NO_PROXY', '*')
os.environ.setdefault('no_proxy', '*')


def run_job(workers=100):
    """执行一次：拉取数据 + 预设策略回测"""
    script = os.path.join(PROJECT_ROOT, 'scripts', 'update_cache_and_backtest.py')
    cmd = [sys.executable, script, '--workers', str(workers)]
    return subprocess.run(cmd, cwd=PROJECT_ROOT).returncode


def main():
    parser = argparse.ArgumentParser(description='每天下午5点自动拉取数据并跑六个常用策略')
    parser.add_argument('--now', action='store_true', help='立即执行一次（不等待定时）')
    parser.add_argument('--schedule', action='store_true', help='启动定时任务：每天17:00执行')
    parser.add_argument('--workers', type=int, default=100, help='拉取数据并发数（默认100）')
    args = parser.parse_args()

    if args.schedule:
        try:
            import schedule
        except ImportError:
            print('请安装 schedule: pip install schedule')
            sys.exit(1)

        def job():
            print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务：开始执行')
            code = run_job(workers=args.workers)
            print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务：结束，退出码 {code}\n')

        schedule.every().day.at('17:00').do(job)
        print('已设置每天 17:00 执行拉取数据 + 预设策略回测。按 Ctrl+C 退出。')
        while True:
            schedule.run_pending()
            import time
            time.sleep(60)
        return

    # 立即执行一次
    code = run_job(workers=args.workers)
    sys.exit(code)


if __name__ == '__main__':
    main()
