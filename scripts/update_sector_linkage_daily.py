#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增量补齐「按交易日」板块/概念日线快照（cache/sector_linkage/daily/）。

在定时链路中建议排在日 K 增量更新之后：只对「无有效快照」的交易日发起网络构建，
供后续 enrich / 增量回测使用。

用法：
  python scripts/update_sector_linkage_daily.py
  python scripts/update_sector_linkage_daily.py --lookback-calendar-days 50
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("no_proxy", "*")
# 本脚本专门拉概念/行业按日快照，避免与「日 K 与 daily 最大日相同则跳过概念网」逻辑冲突
os.environ["SECTOR_LINKAGE_SKIP_CONCEPT_WHEN_SYNCED"] = "0"

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _concept_daily_from_env() -> str:
    v = os.environ.get("SECTOR_LINKAGE_CONCEPT_DAILY", "ths").strip().lower()
    return v if v in ("ths", "eastmoney") else "ths"


def _industry_daily_from_env() -> str:
    v = os.environ.get("SECTOR_LINKAGE_INDUSTRY_DAILY", "ths").strip().lower()
    return v if v in ("ths", "eastmoney") else "ths"


def _hist_sleep_from_env() -> float:
    raw = os.environ.get("SECTOR_LINKAGE_HIST_SLEEP", "").strip()
    if not raw:
        return 0.12
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.12


def main() -> None:
    parser = argparse.ArgumentParser(description="增量补齐 sector_linkage 按日快照（概念/行业）")
    parser.add_argument(
        "--lookback-calendar-days",
        type=int,
        default=50,
        help="从本地日 K 最新日往前最多扫描多少自然日内的交易日（默认 50）",
    )
    parser.add_argument("--top-concepts", type=int, default=40)
    parser.add_argument("--top-industries", type=int, default=40)
    parser.add_argument(
        "--max-concept-scan",
        type=int,
        default=0,
        help="概念板块扫描上限；0=全量（与 enrich 默认一致）",
    )
    parser.add_argument(
        "--max-industry-scan",
        type=int,
        default=0,
        help="行业板块扫描上限；0=全量",
    )
    parser.add_argument("--skip-industry", action="store_true", help="只补概念侧排行，不拉行业")
    args = parser.parse_args()

    from data_fetcher import DataFetcher
    from sector_linkage import (
        load_daily_board_snapshot,
        load_or_build_daily_board_snapshots,
    )

    cache_root = os.path.join(PROJECT_ROOT, "cache", "sector_linkage")
    os.makedirs(os.path.join(cache_root, "daily"), exist_ok=True)

    fetcher = DataFetcher()
    end_d = fetcher.get_local_cache_latest_date()
    if not end_d:
        print("[ERROR] 本地 cache 无 000001 代表日 K，请先跑增量拉数。", flush=True)
        sys.exit(1)

    start_dt = datetime.strptime(end_d[:10], "%Y-%m-%d") - timedelta(
        days=max(7, int(args.lookback_calendar_days))
    )
    start_d = start_dt.strftime("%Y-%m-%d")
    trading_days = fetcher.get_trading_days_between(start_d, end_d)
    if not trading_days:
        print(f"[WARN] {start_d}～{end_d} 无交易日序列，跳过。", flush=True)
        return

    cd = _concept_daily_from_env()
    id_ = _industry_daily_from_env()
    need: list[str] = []
    for d in trading_days:
        snap = load_daily_board_snapshot(cache_root, d, concept_daily=cd, industry_daily=id_)
        if snap is None:
            need.append(d)
            continue
        if args.skip_industry:
            c0, _ = snap
            if not c0:
                need.append(d)

    if not need:
        print(
            f"[INFO] sector_linkage/daily 在 {start_d}～{end_d} 内已齐全（concept_daily={cd}），无需拉取。",
            flush=True,
        )
        return

    print(
        f"[INFO] 待补齐按日快照 {len(need)} 个交易日（concept_daily={cd}, industry_daily={id_}）："
        f" {need[0]} … {need[-1]}",
        flush=True,
    )
    load_or_build_daily_board_snapshots(
        cache_root,
        need,
        top_concepts=int(args.top_concepts),
        top_industries=int(args.top_industries),
        min_concept_pct=None,
        min_industry_pct=None,
        skip_industry=bool(args.skip_industry),
        hist_sleep_sec=_hist_sleep_from_env(),
        max_concept_scan=max(0, int(args.max_concept_scan)),
        max_industry_scan=max(0, int(args.max_industry_scan)),
        force_refresh_daily=False,
        concept_daily=cd,
        industry_daily=id_,
    )
    print("[INFO] sector_linkage 按日快照增量任务完成。", flush=True)


if __name__ == "__main__":
    main()
