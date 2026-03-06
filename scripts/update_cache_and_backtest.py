#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
补齐缓存差值数据并执行常用策略回测（六策略一起执行）

适用场景：缓存中最后一天是 2月10日，最近交易日是 2月24日 时，
自动拉取 2月11日～2月24日 的差值数据写入缓存，然后执行 common_strategies.json 中的全部策略回测。

使用方法：
  # 全量回测（默认）：补齐缓存 + 六策略全量回测，结果写 策略名_结果.jsonl
  python scripts/update_cache_and_backtest.py

  # 增量回测：补齐缓存 + 先看六策略结果最后日期，只回测「最后日期+1」到今天的 T 日并追加
  python scripts/update_cache_and_backtest.py --incremental

  # 仅补齐缓存、不执行回测
  python scripts/update_cache_and_backtest.py --no-backtest

  # 指定并发数
  python scripts/update_cache_and_backtest.py --workers 100
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime

# 保证在项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault('NO_PROXY', '*')
os.environ.setdefault('no_proxy', '*')


def main():
    parser = argparse.ArgumentParser(description='补齐缓存差值数据并执行常用策略回测')
    parser.add_argument('--no-backtest', action='store_true', help='只补齐缓存，不执行回测')
    parser.add_argument('--incremental', action='store_true',
                        help='回测用增量：先看六策略结果最后日期，只回测该日期之后到今天的 T 日并追加（不跑全量）')
    parser.add_argument('--workers', type=int, default=100, help='拉取数据时的并发数（默认 100）')
    parser.add_argument('--config', default='common_strategies.json', help='策略配置文件路径')
    args = parser.parse_args()

    from data_fetcher import DataFetcher

    print()
    print('=' * 60)
    print('步骤 1：补齐缓存差值数据')
    print('=' * 60)

    fetcher = DataFetcher()
    cache_latest = fetcher.get_local_cache_latest_date()
    last_trade = fetcher._get_last_trading_day_available()
    print(f'缓存最新日期(代表股票): {cache_latest or "无"}')
    print(f'最近交易日:             {last_trade}')
    print('[INFO] 将按股票逐个检查缓存是否缺少最近交易日数据，如有缺失则补齐。')

    fetcher.remove_duplicate_cache()
    fetcher.get_stock_list()
    fetcher.update_caches_with_today_data(max_workers=args.workers)
    print('[INFO] 差值数据检查与写入已完成（仅对确实缺数据的股票进行了更新）。')
    print()

    if args.no_backtest:
        print('[INFO] 已跳过回测（--no-backtest）')
        return

    print('=' * 60)
    if args.incremental:
        print('步骤 2：按结果最后日期增量回测（六策略，只补 T 日并追加）')
    else:
        print('步骤 2：执行常用策略回测（全量）')
    print('=' * 60)
    if args.incremental:
        cmd = [sys.executable, os.path.join(PROJECT_ROOT, 'scripts', 'backtest_append_from_last.py'),
               '--workers', str(args.workers), '--config', args.config]
    else:
        cmd = [sys.executable, 'batch_backtest.py', '--config', args.config]
    print(f'执行: {" ".join(cmd)}\n')
    code = subprocess.run(cmd)
    if code.returncode != 0:
        sys.exit(code.returncode)
    print()
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 全部完成')


if __name__ == '__main__':
    main()
