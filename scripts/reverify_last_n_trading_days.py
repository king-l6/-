#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
复现「最近 N 个交易日」并与 results/{策略名}_结果.jsonl 对照。

用法（项目根目录）：
  python scripts/reverify_last_n_trading_days.py --strategy 主力建仓 --n 3 --cache-only
  python scripts/reverify_last_n_trading_days.py --strategy 主力建仓 --n 3 --end 2026-05-08 --workers 50

口径（重要）：
  - **jsonl 按 match_date 计数**：全量 backtest() 在 timeRange 窗口内对每个 T 各写一行命中（同股可多日多行），
    `match_date` 即该条对应的信号日 T。
  - **单日扫描条数**：`backtest_single_day(T)` 表示「在 T 当日满足条件的股票数」，应与全量 jsonl 中 `match_date=T` 行数一致（数据与窗口对齐时）。
  - 若不一致，多为缓存右端/交易日锚点与生成 jsonl 时不一致，或策略/配置已变更。

可选校验（较慢）：`--subset-check` 会验证 jsonl 中 match_date=T 的股票代码是否全部出现在当日 `backtest_single_day(T)` 的结果中。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def load_strategy_by_name(config_path: str, name: str) -> tuple[str, dict]:
    with open(config_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    for s in data.get('strategies', []):
        if s.get('name') == name:
            return name, s.get('strategy') or {}
    raise SystemExit(f'未在 {config_path} 中找到策略: {name}')


def codes_from_jsonl_for_date(results_dir: str, strategy_name: str, day: str) -> set[str]:
    path = os.path.join(results_dir, f'{strategy_name}_结果.jsonl')
    out: set[str] = set()
    if not os.path.isfile(path):
        return out
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            if isinstance(o, dict) and o.get('_meta'):
                continue
            md = str(o.get('match_date', '')).strip()[:10]
            if md == day[:10]:
                c = o.get('code')
                if c is not None:
                    out.add(str(c))
    return out


def counts_from_jsonl(results_dir: str, strategy_name: str) -> Counter[str]:
    path = os.path.join(results_dir, f'{strategy_name}_结果.jsonl')
    c: Counter[str] = Counter()
    if not os.path.isfile(path):
        return c
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            if isinstance(o, dict) and o.get('_meta'):
                continue
            md = str(o.get('match_date', '')).strip()[:10]
            if len(md) >= 10 and md[4] == '-' and md[7] == '-':
                c[md] += 1
    return c


def max_match_date_in_jsonl(c: Counter[str]) -> str | None:
    keys = [d for d in c if d]
    return max(keys) if keys else None


def last_n_trading_days(fetcher, end_s: str, n: int) -> list[str]:
    start_dt = datetime.strptime(end_s[:10], '%Y-%m-%d') - timedelta(days=60)
    start_s = start_dt.strftime('%Y-%m-%d')
    days = fetcher.get_trading_days_between(start_s, end_s[:10])
    if len(days) < n:
        raise SystemExit(
            f'交易日不足 {n} 个: end={end_s} 区间内仅 {len(days)} 个交易日（cache_only 时依赖本地 K 推导日历）'
        )
    return days[-n:]


def main():
    p = argparse.ArgumentParser(description='复现最近 N 个交易日命中数并对照 jsonl')
    p.add_argument('--strategy', default='主力建仓', help='策略名（与 common_strategies.json 中 name 一致）')
    p.add_argument('--n', type=int, default=3, help='最近 N 个交易日')
    p.add_argument('--end', default=None, help='锚定最后一个交易日 YYYY-MM-DD；默认取该策略 jsonl 中最大 match_date')
    p.add_argument('--config', default='common_strategies.json')
    p.add_argument('--cache-only', action='store_true', help='DataFetcher.cache_only=True')
    p.add_argument('--workers', type=int, default=50, help='StrategyEngine 并发数')
    p.add_argument(
        '--subset-check',
        action='store_true',
        help='校验 jsonl 中该日 match_date 的股票代码是否 ⊆ 当日 backtest_single_day 结果（需多跑一遍集合）',
    )
    args = p.parse_args()

    results_dir = os.path.join(PROJECT_ROOT, 'results')
    file_counts = counts_from_jsonl(results_dir, args.strategy)
    end = (args.end or max_match_date_in_jsonl(file_counts) or '').strip()[:10]
    if not end:
        raise SystemExit('无法确定 end：请指定 --end 或确保 results 下存在对应 jsonl 且有数据行')

    from data_fetcher import DataFetcher
    from strategy_engine import StrategyEngine

    fetcher = DataFetcher()
    if args.cache_only:
        fetcher.cache_only = True

    days = last_n_trading_days(fetcher, end, args.n)
    name, strategy = load_strategy_by_name(args.config, args.strategy)
    engine = StrategyEngine(fetcher, max_workers=args.workers)

    print('=' * 72)
    print(f'策略: {name}  |  锚定 end={end}  |  最近 {args.n} 个交易日: {days}')
    print(f'jsonl「该日条数」: match_date=T 的行数（全量窗口内各命中日各一行）')
    print(f'单日扫描条数: backtest_single_day(T)（T 当日满足条件的股票数）')
    print('=' * 72)

    rows = []
    for d in days:
        print(f'\n>>> 正在复现 T={d} ...', flush=True)
        try:
            live = engine.backtest_single_day(strategy, strategy_name=name, trading_date=d)
            n_live = len(live) if live else 0
            live_codes = {str(r.get('code', '')) for r in (live or []) if r.get('code') is not None}
        except Exception as e:
            print(f'[ERROR] T={d}: {e}', flush=True)
            n_live = -1
            live_codes = set()
        n_file = file_counts.get(d, 0)
        subset_ok = ''
        if args.subset_check and n_live >= 0:
            jc = codes_from_jsonl_for_date(results_dir, name, d)
            missing = jc - live_codes
            subset_ok = '  jsonl⊆扫描' + (' ✓' if not missing else f' ✗缺{len(missing)}只')
            if missing and len(missing) <= 5:
                subset_ok += f' 例:{",".join(list(missing)[:5])}'
        # 「对齐」仅当两数相等；否则标「口径」或异常
        if n_live < 0:
            mark = '?'
        elif n_live == n_file:
            mark = '✓ 相等'
        elif n_live > n_file:
            mark = '— jsonl偏少(查锚点/缓存)'
        elif n_live < n_file:
            mark = '✗ 异常(jsonl>扫描)'
        rows.append((d, n_file, n_live, mark, subset_ok))
        print(f'    T={d}  jsonl={n_file}  单日扫描={n_live}  {mark}{subset_ok}', flush=True)

    print('\n' + '-' * 72)
    print(f'{"交易日":<12} {"jsonl=T":>12} {"单日扫描":>10}  说明')
    print('-' * 72)
    for d, nf, nl, mark, sub in rows:
        print(f'{d:<12} {nf:>12} {nl:>10}  {mark}{sub}')
    print('-' * 72)
    print(
        '说明: 全量 jsonl 为窗口内各命中日各一行；与 backtest_single_day(T) 在数据与锚点一致时应接近或相等。'
    )
    print()


if __name__ == '__main__':
    main()
