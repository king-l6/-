#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
补齐缓存差值数据并执行常用策略回测

适用场景：缓存中最后一天是 2月10日，最近交易日是 2月24日 时，
自动拉取 2月11日～2月24日 的差值数据写入缓存，然后执行 common_strategies.json 中的全部策略回测。

使用方法：
  # 在项目根目录执行
  python scripts/update_cache_and_backtest.py

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
    print(f'缓存最新日期: {cache_latest or "无"}')
    print(f'最近交易日:   {last_trade}')

    if cache_latest and cache_latest >= last_trade:
        print('[INFO] 缓存已是最新，无需拉取差值数据。')
    else:
        fetcher.remove_duplicate_cache()
        fetcher.get_stock_list()
        fetcher.update_caches_with_today_data(max_workers=args.workers)
        print('[INFO] 差值数据已写入缓存。')
    print()

    if args.no_backtest:
        print('[INFO] 已跳过回测（--no-backtest）')
        return

    print('=' * 60)
    print('步骤 2：执行常用策略回测')
    print('=' * 60)
    cmd = [sys.executable, 'batch_backtest.py', '--config', args.config]
    print(f'执行: {" ".join(cmd)}\n')
    code = subprocess.run(cmd)
    if code.returncode != 0:
        sys.exit(code.returncode)
    print()
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 全部完成')


if __name__ == '__main__':
    main()
