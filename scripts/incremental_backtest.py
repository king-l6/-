#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日增量回测：仅以「今天」为 T 日扫描一次，输出 策略名_YYYYMMDD_结果.jsonl

适用场景：
- 缓存最新交易日 = 昨天（或今天，若 17:30 后已更新）；
- 历史结果中已有之前每一天的数据（全量或之前的增量）；
- 今日只算 match_date=今天 的信号，追加到按天分文件的历史目录。

使用方法（在项目根目录）：
  python scripts/incremental_backtest.py              # 以「最近交易日」为 T 日
  python scripts/incremental_backtest.py --date 2026-02-26  # 指定 T 日

输出目录：results/
输出文件：{策略名}_{YYYYMMDD}_结果.jsonl，例如 龙头战法_20260226_结果.jsonl
"""

import os
import sys
import json
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault('NO_PROXY', '*')
os.environ.setdefault('no_proxy', '*')


def load_strategies(config_file='common_strategies.json'):
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('strategies', [])
    except FileNotFoundError:
        print(f'[ERROR] 策略配置文件不存在: {config_file}')
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f'[ERROR] 策略配置文件格式错误: {e}')
        sys.exit(1)


def _enrich_sector_linkage_jsonl(filepath: str) -> None:
    """增量日文件写盘后补充板块联动（与全量 strategy_engine 同一入口）。"""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            'esl_dynamic',
            os.path.join(PROJECT_ROOT, 'scripts', 'enrich_sector_linkage.py'),
        )
        mod = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(mod)
        mod.enrich_results_jsonl_after_backtest(filepath)
    except Exception as e:
        print(f'       [WARN] 板块联动 enrich 跳过: {e}', flush=True)


def write_daily_results(results_dir, strategy_name, trading_date_str, results):
    """写入 策略名_YYYYMMDD_结果.jsonl"""
    date_compact = trading_date_str.replace('-', '')
    filename = f"{strategy_name}_{date_compact}_结果.jsonl"
    filepath = os.path.join(results_dir, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            meta = {
                '_meta': {
                    'strategy_name': strategy_name,
                    'run_at': datetime.now().isoformat(),
                    'match_date': trading_date_str,
                    'count': len(results),
                    'incremental': True,
                }
            }
            f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
            for r in results:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')
        return filepath
    except Exception as e:
        print(f"[WARNING] 写入 {filepath} 失败: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='每日增量回测：仅以指定 T 日扫描，输出 策略名_YYYYMMDD_结果.jsonl')
    parser.add_argument('--config', default='common_strategies.json', help='策略配置文件路径')
    parser.add_argument('--date', default=None, help='T 日，格式 YYYY-MM-DD；不传则用「最近交易日」')
    parser.add_argument('--workers', type=int, default=50, help='并发线程数')
    parser.add_argument('--no-check-cache', action='store_true', help='不检查缓存是否最新，直接跑')
    args = parser.parse_args()

    from data_fetcher import DataFetcher
    from strategy_engine import StrategyEngine

    results_dir = os.path.join(PROJECT_ROOT, 'results')
    os.makedirs(results_dir, exist_ok=True)

    fetcher = DataFetcher()
    cache_latest = fetcher.get_local_cache_latest_date()
    last_trade = fetcher._get_last_trading_day_available()

    if args.date:
        trading_date = args.date.strip()[:10]
        try:
            datetime.strptime(trading_date, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] 无效日期: {args.date}')
            sys.exit(1)
    else:
        trading_date = last_trade

    print()
    print('=' * 60)
    print('每日增量回测')
    print('=' * 60)
    print(f'缓存最新日期: {cache_latest or "无"}')
    print(f'最近交易日:   {last_trade}')
    print(f'本次 T 日:    {trading_date}')
    print()

    if not args.no_check_cache and cache_latest:
        if cache_latest < trading_date:
            print(f'[WARN] 缓存最新日期 {cache_latest} 早于 T 日 {trading_date}，建议先运行 update_cache_and_backtest.py 补齐数据后再跑增量。')
            print('       若坚持继续，可加 --no-check-cache')
    print()

    strategies = load_strategies(args.config)
    engine = StrategyEngine(fetcher, max_workers=args.workers)

    total_count = 0
    for idx, strategy_config in enumerate(strategies, 1):
        name = strategy_config['name']
        strategy = strategy_config['strategy']
        print(f'[{idx}/{len(strategies)}] {name} (T={trading_date}) ...', flush=True)
        try:
            results = engine.backtest_single_day(
                strategy, strategy_name=name, trading_date=trading_date
            )
            if results:
                path = write_daily_results(results_dir, name, trading_date, results)
                if path:
                    print(f'       -> {len(results)} 条 -> {os.path.basename(path)}', flush=True)
                    _enrich_sector_linkage_jsonl(path)
                total_count += len(results)
            else:
                print(f'       -> 0 条', flush=True)
        except Exception as e:
            print(f'       -> 失败: {e}', flush=True)

    print()
    print('=' * 60)
    print(f'增量回测完成，T 日={trading_date}，共 {total_count} 条记录')
    print('=' * 60)


if __name__ == '__main__':
    main()
