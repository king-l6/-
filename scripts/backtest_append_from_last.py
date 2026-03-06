#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按「六个战法结果文件」中最后一个数据的日期，只回测该日期之后到今天的 T 日，并把结果追加到**同一个战法结果文件**中。

步骤：
1. 读取六个战法（龙头战法、断板反包、均线上穿、情绪周期、三连板、筑底突破）的结果文件
   （主文件 策略名_结果.jsonl + 按日文件 策略名_YYYYMMDD_结果.jsonl）
2. 取每个策略中最大的 match_date，再取这六个日期中的最小值作为「最后日期」
   （若某策略无数据则视为无，只从有数据的策略中取最小最后日期）
3. 若存在最后日期，则只回测 [最后日期+1 日, 今天] 之间的所有交易日 T
4. **按股票并发**：每只股票只加载一次 K 线，在该数据上对所有 T 日×所有策略做检查，再按策略聚合写回各自 策略名_结果.jsonl（比「每 T 日×每策略」全量扫一遍快得多）。

使用方法（在项目根目录）：
  python scripts/backtest_append_from_last.py              # 自动检测最后日期并回测补齐
  python scripts/backtest_append_from_last.py --no-check-cache   # 不检查缓存是否最新
  python scripts/backtest_append_from_last.py --workers 50
"""

import os
import sys
import json
import re
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault('NO_PROXY', '*')
os.environ.setdefault('no_proxy', '*')


# 六个战法名称（与 common_strategies.json 及前端一致）
STRATEGY_NAMES = ['龙头战法', '断板反包', '均线上穿', '情绪周期', '三连板', '筑底突破']


def _read_one_results_file(filepath):
    """读取单个 .jsonl 结果文件，返回 (meta_dict, results_list[仅日期字符串])。"""
    meta = None
    results = []
    if not os.path.isfile(filepath):
        return meta, results
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return meta, results
    if not lines:
        return meta, results
    try:
        first = json.loads(lines[0].strip())
        if '_meta' in first:
            meta = first['_meta']
            lines = lines[1:]
    except Exception:
        pass
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            if 'code' not in data:
                continue
            match_date = data.get('match_date')
            if match_date is None:
                continue
            s = str(match_date).strip()
            if len(s) >= 10 and s[4] == '-' and s[7] == '-':
                results.append(s[:10])
        except Exception:
            continue
    return meta, results


def get_last_date_for_strategy(results_dir, strategy_name):
    """获取该策略在主文件 + 按日文件中出现的最大 match_date，无则返回 None。"""
    main_file = os.path.join(results_dir, f"{strategy_name}_结果.jsonl")
    pattern = re.compile(re.escape(strategy_name) + r'_(\d{8})_结果\.jsonl$')
    all_dates = []

    if os.path.isfile(main_file):
        _, rows = _read_one_results_file(main_file)
        all_dates.extend(rows)

    for filename in os.listdir(results_dir):
        if not filename.endswith('.jsonl'):
            continue
        if pattern.match(filename):
            filepath = os.path.join(results_dir, filename)
            if os.path.isfile(filepath):
                _, rows = _read_one_results_file(filepath)
                all_dates.extend(rows)

    if not all_dates:
        return None
    return max(all_dates)


def get_common_last_date(results_dir):
    """
    六个战法结果中，每个策略取最大 match_date，再取这六个中的最小值。
    若某个策略无数据则跳过；若六个都无数据则返回 None。
    """
    last_dates = []
    for name in STRATEGY_NAMES:
        d = get_last_date_for_strategy(results_dir, name)
        if d is not None:
            last_dates.append(d)
    if not last_dates:
        return None
    return min(last_dates)


def next_trading_day(date_str):
    """给定 YYYY-MM-DD，返回下一日的 YYYY-MM-DD（仅加一天，不判断是否交易日）。"""
    try:
        d = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        return (d + timedelta(days=1)).strftime('%Y-%m-%d')
    except Exception:
        return None


def weekdays_between(start_yyyy_mm_dd, end_yyyy_mm_dd):
    """生成 start 到 end 之间所有工作日（周一=0 到周五=4）的日期列表，含首尾。格式 YYYY-MM-DD。"""
    try:
        start_d = datetime.strptime(start_yyyy_mm_dd[:10], '%Y-%m-%d').date()
        end_d = datetime.strptime(end_yyyy_mm_dd[:10], '%Y-%m-%d').date()
        if start_d > end_d:
            return []
        out = []
        d = start_d
        while d <= end_d:
            if d.weekday() < 5:  # 0-4 为周一到周五
                out.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        return out
    except Exception:
        return []


def load_strategies(config_file='common_strategies.json'):
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            strategies = data.get('strategies', [])
        # 只保留六个战法
        names_set = set(STRATEGY_NAMES)
        return [s for s in strategies if s.get('name') in names_set]
    except FileNotFoundError:
        print(f'[ERROR] 策略配置文件不存在: {config_file}')
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f'[ERROR] 策略配置文件格式错误: {e}')
        sys.exit(1)


def _read_full_results_file(filepath):
    """读取结果文件，返回 (meta_dict, results_list[完整记录])。与 app._read_one_results_file 类似。"""
    meta = None
    results = []
    if not os.path.isfile(filepath):
        return meta, results
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return meta, results
    if not lines:
        return meta, results
    try:
        first = json.loads(lines[0].strip())
        if '_meta' in first:
            meta = first['_meta']
            lines = lines[1:]
    except Exception:
        pass
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            if 'code' not in data:
                continue
            results.append(data)
        except Exception:
            continue
    return meta, results


def _normalize_date_str(value):
    """将日期字段统一为 YYYY-MM-DD 字符串（若可能）。"""
    if value is None:
        return ''
    s = str(value).strip()
    if not s:
        return ''
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        return s[:10]
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return s


def append_results_to_main_file(results_dir, strategy_name, new_results, strategy_engine=None):
    """
    将新增结果追加到同一个 {策略名}_结果.jsonl 中：
    - 读取原文件所有记录 + 新记录
    - 按 (code, match_date, name) 去重
    - 所有策略统一：连续三个 A 股交易日内同股只保留第一次（需传入 strategy_engine）
    - 按 match_date, code 排序
    - 重写同一个文件
    """
    filepath = os.path.join(results_dir, f"{strategy_name}_结果.jsonl")
    _, old_results = _read_full_results_file(filepath)

    all_results = []
    if old_results:
        all_results.extend(old_results)
    if new_results:
        all_results.extend(new_results)

    # 去重
    seen = set()
    unique = []
    for r in all_results:
        match_date = _normalize_date_str(r.get('match_date'))
        key = (r.get('code', ''), match_date, r.get('name', ''))
        if key in seen:
            continue
        seen.add(key)
        r = dict(r)
        r['match_date'] = match_date
        unique.append(r)

    # 排序
    unique.sort(key=lambda x: (_normalize_date_str(x.get('match_date', '9999-99-99')), x.get('code', '')))

    # 所有策略统一：连续三个 A 股交易日内同股只保留第一次
    if strategy_engine is not None:
        unique = strategy_engine._dedupe_same_stock_within_three_trading_days(unique, trading_days=3)

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            meta = {
                '_meta': {
                    'strategy_name': strategy_name,
                    'run_at': datetime.now().isoformat(),
                    'count': len(unique),
                    'incremental': True,
                }
            }
            f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
            for r in unique:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')
        return filepath
    except Exception as e:
        print(f"[WARNING] 写入 {filepath} 失败: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description='按六个战法结果文件的最后日期，只回测之后到今天的 T 日并追加到结果文件'
    )
    parser.add_argument('--config', default='common_strategies.json', help='策略配置文件路径')
    parser.add_argument('--workers', type=int, default=50, help='并发线程数')
    parser.add_argument('--no-check-cache', action='store_true', help='不检查缓存是否最新，直接跑')
    parser.add_argument('--skip-ensure-data', action='store_true',
                        help='跳过预拉取 ensure_sufficient_data，仅使用已有缓存（适合每日轻量回测）')
    args = parser.parse_args()

    from data_fetcher import DataFetcher
    from strategy_engine import StrategyEngine

    results_dir = os.path.join(PROJECT_ROOT, 'results')
    os.makedirs(results_dir, exist_ok=True)

    fetcher = DataFetcher()
    cache_latest = fetcher.get_local_cache_latest_date()
    last_trade = fetcher._get_last_trading_day_available()

    # 1) 查看六个战法结果中的最后日期
    common_last = get_common_last_date(results_dir)
    print()
    print('=' * 60)
    print('按最后结果日期增量回测（只补 T 日并追加）')
    print('=' * 60)
    print(f'缓存最新日期: {cache_latest or "无"}')
    print(f'最近交易日:   {last_trade}')
    print(f'六个战法结果中「最后日期」: {common_last or "无（任一策略均无数据）"}')

    if common_last is None:
        print()
        print('[INFO] 未检测到任何战法结果数据，将仅回测最近一个交易日（与 incremental_backtest 行为一致）。')
        start_date = last_trade
        trading_days = [last_trade]
    else:
        start_date = next_trading_day(common_last)
        if not start_date or start_date > last_trade:
            print()
            print('[INFO] 最后日期已为最近交易日或更晚，无需回测。')
            return
        trading_days = fetcher.get_trading_days_between(start_date, last_trade)
        fallback_days = weekdays_between(start_date, last_trade)
        # 若 Baostock 未返回或返回天数明显少于「工作日」数，用工作日列表兜底，保证回测所有中间交易日
        if not trading_days:
            trading_days = fallback_days
        elif fallback_days and len(trading_days) < len(fallback_days):
            trading_days = fallback_days
        if not trading_days:
            print()
            print('[INFO] 未查询到需回测的交易日。')
            return
        print(f'本次回测 T 日范围: {trading_days[0]} ~ {trading_days[-1]}，共 {len(trading_days)} 个交易日')
    print()

    if not args.no_check_cache and cache_latest:
        if trading_days and cache_latest < trading_days[-1]:
            print(f'[WARN] 缓存最新日期 {cache_latest} 早于回测截止日 {trading_days[-1]}，建议先运行 update_cache_and_backtest.py 补齐数据。')
            print('       若坚持继续，可加 --no-check-cache')
    print()

    strategies = load_strategies(args.config)
    if len(strategies) != len(STRATEGY_NAMES):
        print(f'[WARN] 配置中六个战法不完整，仅回测: {[s["name"] for s in strategies]}')
    # [(strategy_name, strategy_dict), ...] 供「按股票只加载一次」的增量接口使用
    strategies_list = [(s['name'], s['strategy']) for s in strategies]

    if not args.skip_ensure_data:
        fetcher.ensure_sufficient_data(
            max((s['strategy'].get('timeRange', 30) for s in strategies), default=30),
            max_workers=100
        )
    stocks = fetcher.get_stock_list()
    print(f'共 {len(stocks)} 只股票，{len(trading_days)} 个 T 日 × {len(strategies)} 个策略 → 按股票并发（每只股票只加载一次 K 线）')
    print()

    engine = StrategyEngine(fetcher, max_workers=args.workers)
    # 按策略名聚合：all_results[name] = [result_rows]
    all_results = {s['name']: [] for s in strategies}
    total_stocks = len(stocks)
    processed = [0]  # 闭包用

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_stock = {
            executor.submit(engine.run_incremental_for_stock, stock, trading_days, strategies_list): stock
            for stock in stocks
        }
        for future in as_completed(future_to_stock):
            processed[0] += 1
            if processed[0] % 100 == 0 or processed[0] == total_stocks:
                pct = 100 * processed[0] // total_stocks if total_stocks else 0
                print(f'进度: {processed[0]}/{total_stocks} ({pct}%)', flush=True)
            try:
                per_stock = future.result(timeout=60)
                for name, rows in per_stock.items():
                    all_results[name].extend(rows)
            except Exception as e:
                if processed[0] % 500 == 0:
                    print(f'[WARN] 某只股票处理异常: {e}', flush=True)

    total_count = 0
    for s in strategies:
        name = s['name']
        rows = all_results.get(name, [])
        if rows:
            path = append_results_to_main_file(results_dir, name, rows, strategy_engine=engine)
            if path:
                print(f'{name}: {len(rows)} 条 → {os.path.basename(path)}')
            total_count += len(rows)
        else:
            print(f'{name}: 0 条')

    print()
    print('=' * 60)
    print(f'增量回测完成，共 {len(trading_days)} 个 T 日 × {len(strategies)} 个策略，合计 {total_count} 条记录')
    print('=' * 60)


if __name__ == '__main__':
    main()
