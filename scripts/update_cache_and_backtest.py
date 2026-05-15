#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
补齐缓存差值数据并执行常用策略回测（全部预设策略一起执行）

适用场景：缓存中最后一天是 2月10日，最近交易日是 2月24日 时，
自动拉取 2月11日～2月24日 的差值数据写入缓存，然后执行 common_strategies.json 中的全部策略回测。

使用方法：
  # 全量回测（默认）：补齐缓存 + 全部预设策略全量回测，结果写 策略名_结果.jsonl
  python scripts/update_cache_and_backtest.py

  # 增量回测：步骤 1 补齐 K 线后，步骤 2 只读本地 cache 追加回测（不再调 AkShare 补日 K）
  python scripts/update_cache_and_backtest.py --incremental

  # 仅补齐缓存、不执行回测
  python scripts/update_cache_and_backtest.py --no-backtest

  # 指定并发数
  python scripts/update_cache_and_backtest.py --workers 100

  # 多任务分片：将全部股票均匀切成 N 份，本进程只处理其中一份（适合开多个进程并行补缓存）
  # 例如：开启 4 个终端，分别执行下面 4 条命令（注意 task-index 从 0 开始）：
  #  终端1: python scripts/update_cache_and_backtest.py --no-backtest --task-index 0 --task-count 4
  #  ...
  # 所有分片都完成后，再单独跑一遍不分片的回测命令。
  #
  # 一条命令自动分片（推荐）：补缓存与增量回测使用同一分片数，只改 --auto-shard-count 即可
  #  仅补缓存（4 分片）：     python scripts/update_cache_and_backtest.py --no-backtest --auto-shard-count 4
  #  补缓存 + 增量回测（均为 4 分片）： python scripts/update_cache_and_backtest.py --incremental --auto-shard-count 4
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

# 步骤 1 已做差值补缓存后，增量回测不应再拉日 K（与定时任务 scheduled_task 一致）
_INCREMENTAL_APPEND_LOCAL_KLINE = (
    '--cache-only',
    '--no-check-cache',
    '--skip-ensure-data',
)


def main():
    parser = argparse.ArgumentParser(description='补齐缓存差值数据并执行常用策略回测')
    parser.add_argument('--no-backtest', action='store_true', help='只补齐缓存，不执行回测')
    parser.add_argument('--incremental', action='store_true',
                        help='回测用增量：先看各策略结果最后日期，只回测该日期之后到今天的 T 日并追加（不跑全量）')
    parser.add_argument('--workers', type=int, default=100, help='拉取数据时的并发数（默认 100）')
    parser.add_argument('--task-index', type=int, default=None,
                        help='多任务分片的当前任务下标（从 0 开始）；与 --task-count 搭配使用')
    parser.add_argument('--task-count', type=int, default=None,
                        help='多任务分片的总任务数（>1 生效）；与 --task-index 搭配使用')
    parser.add_argument('--auto-shard-count', type=int, default=None,
                        help='自动多进程分片数量（>=2）：补缓存与增量回测均用该分片数，只改一次即可')
    parser.add_argument('--config', default='common_strategies.json', help='策略配置文件路径')
    parser.add_argument('--strategy', default=None, help='只跑指定策略名称（精确匹配）')
    args = parser.parse_args()

    from data_fetcher import DataFetcher

    # 自动分片模式：父进程只负责起子进程，不直接补缓存
    if args.auto_shard_count is not None:
        if args.auto_shard_count <= 1:
            parser.error('--auto-shard-count 必须大于 1')
        if args.task_index is not None or args.task_count is not None:
            parser.error('--auto-shard-count 不能与 --task-index / --task-count 同时使用')

        script = os.path.join(PROJECT_ROOT, 'scripts', 'update_cache_and_backtest.py')
        print()
        print('=' * 60)
        print(f'步骤 1：补齐缓存（自动 {args.auto_shard_count} 分片并行）')
        print('=' * 60)
        procs = []
        for idx in range(args.auto_shard_count):
            cmd = [
                sys.executable, script,
                '--no-backtest',
                '--task-index', str(idx),
                '--task-count', str(args.auto_shard_count),
                '--workers', str(args.workers),
                '--config', args.config,
            ]
            print(f'[INFO] 启动分片 {idx + 1}/{args.auto_shard_count}: task-index={idx}')
            procs.append(subprocess.Popen(cmd))

        fail = False
        for idx, p in enumerate(procs):
            code = p.wait()
            if code != 0:
                print(f'[ERROR] 分片 task-index={idx} 退出码: {code}')
                fail = True
        if fail:
            sys.exit(1)
        print('[INFO] 所有分片补缓存已完成。')
        print()

        if args.no_backtest:
            print('[INFO] 已跳过回测（--no-backtest）')
            return
        print('=' * 60)
        if args.incremental:
            print(f'步骤 2：按结果最后日期增量回测（{args.auto_shard_count} 分片；日 K 仅读本地 cache）')
        else:
            print('步骤 2：执行常用策略回测（全量）')
        print('=' * 60)
        if args.incremental:
            cmd = [
                sys.executable, os.path.join(PROJECT_ROOT, 'scripts', 'backtest_append_from_last.py'),
                '--workers', str(args.workers), '--config', args.config,
                '--auto-shard-count', str(args.auto_shard_count),
                *_INCREMENTAL_APPEND_LOCAL_KLINE,
            ]
        else:
            cmd = [sys.executable, 'batch_backtest.py', '--config', args.config]
        if args.strategy:
            cmd += ['--strategies', args.strategy]
        print(f'执行: {" ".join(cmd)}\n')
        code = subprocess.run(cmd)
        if code.returncode != 0:
            sys.exit(code.returncode)
        print()
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 全部完成')
        return

    print()
    print('=' * 60)
    print('步骤 1：补齐缓存差值数据')
    print('=' * 60)

    # 参数合法性检查（分片参数）
    if (args.task_index is None) ^ (args.task_count is None):
        parser.error('使用多任务分片时，必须同时提供 --task-index 与 --task-count，或都不提供')
    if args.task_count is not None and args.task_count <= 1:
        parser.error('--task-count 必须大于 1')
    if args.task_index is not None and args.task_index < 0:
        parser.error('--task-index 不能为负数')

    fetcher = DataFetcher()
    cache_latest = fetcher.get_local_cache_latest_date()
    last_trade = fetcher._get_last_trading_day_available()
    print(f'缓存最新日期(代表股票): {cache_latest or "无"}')
    print(f'最近交易日:             {last_trade}')
    print('[INFO] 将按股票逐个检查缓存是否缺少最近交易日数据，如有缺失则补齐。')

    fetcher.remove_duplicate_cache()
    fetcher.get_stock_list()
    fetcher.update_caches_with_today_data(
        max_workers=args.workers,
        task_index=args.task_index,
        task_count=args.task_count,
    )
    print('[INFO] 差值数据检查与写入已完成（仅对确实缺数据的股票进行了更新）。')
    print()

    # 分片模式下默认不在每个分片里跑回测，避免重复计算
    if args.task_index is not None and args.task_count is not None:
        if not args.no_backtest and not args.incremental:
            print('[INFO] 检测到多任务分片参数，当前分片仅负责补缓存，已自动跳过回测。')
        print('[INFO] 多任务分片模式：建议在所有分片补缓存完成后，再单独跑一次回测脚本。')
        return

    if args.no_backtest:
        print('[INFO] 已跳过回测（--no-backtest）')
        return

    print('=' * 60)
    if args.incremental:
        print('步骤 2：按结果最后日期增量回测（common_strategies.json 内全部策略；日 K 仅读本地 cache，不补数）')
    else:
        print('步骤 2：执行常用策略回测（全量）')
    print('=' * 60)
    if args.incremental:
        cmd = [
            sys.executable,
            os.path.join(PROJECT_ROOT, 'scripts', 'backtest_append_from_last.py'),
            '--workers', str(args.workers),
            '--config', args.config,
            *_INCREMENTAL_APPEND_LOCAL_KLINE,
        ]
    else:
        cmd = [sys.executable, 'batch_backtest.py', '--config', args.config]
    if args.strategy:
        cmd += ['--strategies', args.strategy]
    print(f'执行: {" ".join(cmd)}\n')
    code = subprocess.run(cmd)
    if code.returncode != 0:
        sys.exit(code.returncode)
    print()
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 全部完成')


if __name__ == '__main__':
    main()
