#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
隔日冲候选池生成器（不提供买入建议，只做信号汇总 + 风控参数模板）。

功能：
- 读取 results/ 下若干策略的 *_结果.jsonl
- 提取最近一个“可用交易日”(DataFetcher._get_last_trading_day_available) 命中的股票
- 合并去重后输出候选池（按命中策略数、代码排序）

示例：
  python3 scripts/tomorrow_watchlist_intraday.py --strategies "月内三连板+首板涨停" "龙头战法"
  python3 scripts/tomorrow_watchlist_intraday.py --date 2026-03-11
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


def _normalize_date(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def _iter_jsonl(filepath):
    if not os.path.isfile(filepath):
        return
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            # 跳过 meta
            if isinstance(obj, dict) and "_meta" in obj and len(obj) == 1:
                continue
            if isinstance(obj, dict):
                yield obj


def main():
    parser = argparse.ArgumentParser(description="隔日冲候选池生成（信号汇总，不构成投资建议）")
    parser.add_argument("--date", default=None, help="指定 match_date (YYYY-MM-DD)。不填则用最近可用交易日")
    parser.add_argument("--strategies", nargs="*", default=None, help="只汇总指定策略（策略名列表），不填则汇总 results 下所有 *_结果.jsonl")
    args = parser.parse_args()

    results_dir = os.path.join(PROJECT_ROOT, "results")
    if not os.path.isdir(results_dir):
        print(f"[ERROR] results 目录不存在: {results_dir}")
        sys.exit(1)

    if args.date:
        target_date = _normalize_date(args.date)
    else:
        # 尽量使用 DataFetcher 的“可用最近交易日”（会考虑 AkShare 当日日 K 是否已更新）。
        # 若不可用则退化为本地结果文件中的最大 match_date。
        target_date = None
        try:
            from data_fetcher import DataFetcher

            target_date = DataFetcher()._get_last_trading_day_available()
        except Exception:
            target_date = None
        if not target_date:
            # 扫描所有结果文件，取最大的 match_date 作为兜底
            latest = ""
            for fn in os.listdir(results_dir):
                if not fn.endswith("_结果.jsonl"):
                    continue
                fp = os.path.join(results_dir, fn)
                for row in _iter_jsonl(fp):
                    md = _normalize_date(str(row.get("match_date", "")))
                    if md and md > latest:
                        latest = md
            if not latest:
                print("[ERROR] 无法自动推断最近交易日：既无法调用 DataFetcher，也未在结果文件中找到 match_date")
                sys.exit(1)
            target_date = latest

    # 收集要扫描的策略文件
    strategy_files = []
    if args.strategies:
        for name in args.strategies:
            fp = os.path.join(results_dir, f"{name}_结果.jsonl")
            if os.path.isfile(fp):
                strategy_files.append((name, fp))
            else:
                print(f"[WARN] 结果文件不存在，跳过: {os.path.basename(fp)}")
    else:
        for fn in os.listdir(results_dir):
            if fn.endswith("_结果.jsonl"):
                name = fn[: -len("_结果.jsonl")]
                strategy_files.append((name, os.path.join(results_dir, fn)))

    if not strategy_files:
        print("[ERROR] 未找到任何可用结果文件（*_结果.jsonl）")
        sys.exit(1)

    merged = {}  # code -> {code,name,match_date,strategies:set}
    scanned_rows = 0
    for strategy_name, fp in strategy_files:
        for row in _iter_jsonl(fp):
            md = _normalize_date(str(row.get("match_date", "")))
            if md != target_date:
                continue
            code = (row.get("code") or "").strip()
            name = (row.get("name") or "").strip()
            if not code:
                continue
            scanned_rows += 1
            if code not in merged:
                merged[code] = {
                    "code": code,
                    "name": name,
                    "match_date": md,
                    "strategies": set(),
                }
            merged[code]["strategies"].add(strategy_name)

    items = list(merged.values())
    items.sort(key=lambda x: (-len(x["strategies"]), x["code"]))

    print()
    print("=" * 70)
    print(f"隔日冲候选池（match_date={target_date}）")
    print("仅做信号汇总，不构成投资建议；请自行结合风险承受能力与交易纪律。")
    print("=" * 70)
    print(f"扫描策略文件: {len(strategy_files)} 个 | 命中行数: {scanned_rows} | 去重后股票数: {len(items)}")
    print()
    if not items:
        print("[INFO] 当日无命中记录。")
        return

    for i, it in enumerate(items, 1):
        s = "、".join(sorted(it["strategies"]))
        print(f"{i:>3}. {it['code']} {it['name']} | 命中策略({len(it['strategies'])}): {s}")

    print()
    print("风控模板（你给的参数：单笔最大亏损 5%）：")
    print("- 计划入场后，硬止损价 ≈ 入场价 × (1 - 0.05)")
    print("- 若隔日冲：优先定义“开盘后前 X 分钟不追高 / 回撤触发止损 / 分批止盈”等纪律")
    print()


if __name__ == "__main__":
    main()

