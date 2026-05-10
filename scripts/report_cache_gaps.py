#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
扫描主板日线缓存缺口（不默认拉数，只汇报；可选一键触发已有增量逻辑）。

典型问题：软件崩溃后要不要重拉一年？
- 一般不用：磁盘上 json 仍在；`DataFetcher.update_caches_with_today_data` 会按「文件内最后交易日」
  与「最近可用交易日」比较，只补尾部缺口。
- 若回测要「约 N 个交易日」的深度，用 `ensure_sufficient_data(N)`（batch_backtest 前会调）会按起点检查并合并拉取。

用法：
  python3 scripts/report_cache_gaps.py
  python3 scripts/report_cache_gaps.py --days 250
  python3 scripts/report_cache_gaps.py --days 250 --samples 20
  python3 scripts/report_cache_gaps.py --fix-tail --workers 80    # 等价于日常「补到今天」
  python3 scripts/report_cache_gaps.py --fix-depth --days 250     # 等价于回测前「深度不够则拉」
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("no_proxy", "*")


def _norm_row_date(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10].replace("-", "")
    try:
        import pandas as pd

        return pd.to_datetime(s).strftime("%Y%m%d")
    except Exception:
        return ""


def _scan_file(fp: str) -> Optional[Tuple[str, str, str]]:
    """返回 (code, min_yyyymmdd, max_yyyymmdd) 或 None（跳过/损坏）。"""
    base = os.path.basename(fp)
    if "_" not in base or not base.endswith(".json"):
        return None
    parts = base[:-5].split("_")
    if len(parts) not in (2, 3) or len(parts[0]) != 6:
        return None
    code = parts[0]
    try:
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows = data.get("data") or []
        if not rows:
            return code, "", ""
        mins: List[str] = []
        maxs: List[str] = []
        for r in rows:
            d = _norm_row_date(r.get("日期"))
            if len(d) == 8 and d.isdigit():
                mins.append(d)
                maxs.append(d)
        if not mins:
            return code, "", ""
        return code, min(mins), max(maxs)
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="扫描 stock_data 缓存缺口")
    parser.add_argument(
        "--days",
        type=int,
        default=250,
        help="期望覆盖的交易日数量（与 ensure_sufficient_data 一致，用日历天换算起点）",
    )
    parser.add_argument("--samples", type=int, default=15, help="每类最多打印几只示例代码")
    parser.add_argument(
        "--fix-tail",
        action="store_true",
        help="扫描后执行 update_caches_with_today_data（只补各股尾部至最近交易日）",
    )
    parser.add_argument(
        "--fix-depth",
        action="store_true",
        help="扫描后执行 ensure_sufficient_data(--days)（历史不够早则合并拉取）",
    )
    parser.add_argument("--workers", type=int, default=100, help="fix-* 时的并发数")
    args = parser.parse_args()

    from data_fetcher import DataFetcher

    fetcher = DataFetcher()
    stocks = fetcher.get_stock_list()
    all_codes = [s["code"] for s in (stocks or [])]
    if not all_codes:
        print("[ERROR] 股票列表为空，请先拉 stock_list")
        sys.exit(1)

    pattern = os.path.join(fetcher.stock_data_cache_dir, "*.json")
    import glob

    files = glob.glob(pattern)
    print(f"[INFO] 股票列表 {len(all_codes)} 只；缓存文件 {len(files)} 个")

    by_code: Dict[str, Tuple[str, str]] = {}
    merged: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    if files:
        with ThreadPoolExecutor(max_workers=min(64, len(files) or 1)) as ex:
            futs = {ex.submit(_scan_file, fp): fp for fp in files}
            for fut in as_completed(futs):
                r = fut.result()
                if not r:
                    continue
                code, mn, mx = r
                merged[code].append((mn, mx))
        for code, pairs in merged.items():
            mns = [p[0] for p in pairs if p[0] and len(p[0]) == 8 and p[0].isdigit()]
            mxs = [p[1] for p in pairs if p[1] and len(p[1]) == 8 and p[1].isdigit()]
            if not mns or not mxs:
                by_code[code] = ("", "")
            else:
                by_code[code] = (min(mns), max(mxs))

    calendar_days = int(args.days * 1.6) + 10
    end_dt = datetime.now()
    required_start = (end_dt - timedelta(days=calendar_days)).strftime("%Y%m%d")
    last_trade = fetcher._get_last_trading_day_available().replace("-", "")

    no_file: List[str] = []
    head_gap: List[str] = []
    tail_gap: List[str] = []
    ok: List[str] = []

    for code in all_codes:
        if code not in by_code:
            no_file.append(code)
            continue
        mn, mx = by_code[code]
        if not mn or not mx:
            no_file.append(code)
            continue
        if mn > required_start:
            head_gap.append(code)
        if mx < last_trade:
            tail_gap.append(code)
        if mn <= required_start and mx >= last_trade:
            ok.append(code)

    def _show(title: str, arr: List[str]) -> None:
        print(f"\n{title}: {len(arr)}")
        if arr and args.samples > 0:
            print("  示例:", ", ".join(arr[: args.samples]) + (" ..." if len(arr) > args.samples else ""))

    print("\n=== 口径 ===")
    print(f"  期望最早覆盖（约 {args.days} 交易日→{calendar_days} 日历天）: >= {required_start}")
    print(f"  最近可用交易日（字符串）: {last_trade}")
    _show("无有效缓存文件（或文件内无日期）", no_file)
    _show(f"历史偏短（首条日期晚于 {required_start}）", head_gap)
    _show(f"尾部未到最近交易日（末条日期 < {last_trade}）", tail_gap)
    _show("同时满足深度与尾部", ok)

    if args.fix_tail:
        print("\n[INFO] 执行 update_caches_with_today_data ...")
        fetcher.update_caches_with_today_data(max_workers=args.workers)
    if args.fix_depth:
        print("\n[INFO] 执行 ensure_sufficient_data ...")
        fetcher.ensure_sufficient_data(args.days, max_workers=args.workers)

    if not args.fix_tail and not args.fix_depth:
        print(
            "\n[TIP] 只补最新几天：跑 "
            "`python3 scripts/update_cache_and_backtest.py --no-backtest` "
            "或本脚本 `--fix-tail`。"
        )
        print(
            "[TIP] 回测前要一年深度：跑 `python3 scripts/report_cache_gaps.py --fix-depth --days 250` "
            "或直接跑会调用 ensure_sufficient_data 的 batch_backtest。"
        )


if __name__ == "__main__":
    main()
