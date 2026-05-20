#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用本地缓存提取交易日，补全板块/概念排行数据
"""
import json
import os
import sys
import glob
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("no_proxy", "*")
os.environ["SECTOR_LINKAGE_SKIP_CONCEPT_WHEN_SYNCED"] = "0"

def get_trading_days_from_cache():
    """从本地K线缓存提取交易日"""
    cache_dir = os.path.join(PROJECT_ROOT, "cache", "stock_data")
    files = glob.glob(os.path.join(cache_dir, "000001_*.json"))
    all_dates = set()
    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "data" in data:
                for r in data["data"]:
                    d = str(r.get("日期", ""))[:10]
                    if d and len(d) == 10 and d[4] == "-" and d[7] == "-":
                        all_dates.add(d)
        except Exception:
            continue
    return sorted(all_dates)

def main():
    from sector_linkage import (
        load_daily_board_snapshot,
        load_or_build_daily_board_snapshots,
    )

    # 1. 获取所有交易日
    all_days = get_trading_days_from_cache()
    if not all_days:
        print("[ERROR] 本地缓存无交易日", flush=True)
        return
    
    print(f"[INFO] 本地缓存共 {len(all_days)} 个交易日: {all_days[0]} ~ {all_days[-1]}", flush=True)

    # 2. 检查已有板块数据
    cache_root = os.path.join(PROJECT_ROOT, "cache", "sector_linkage")
    daily_dir = os.path.join(cache_root, "daily")
    existing = set()
    if os.path.isdir(daily_dir):
        for f in os.listdir(daily_dir):
            if f.endswith(".json") and len(f) == 14:
                existing.add(f[:-5])
    
    print(f"[INFO] 已有板块数据 {len(existing)} 天", flush=True)

    # 3. 找出缺失的日期
    need = [d for d in all_days if d not in existing]
    if not need:
        print("[INFO] 板块数据已齐全，无需补全", flush=True)
        return
    
    print(f"[INFO] 需要补全 {len(need)} 天: {need[0]} ~ {need[-1]}", flush=True)

    # 4. 批量补全（每批10天，避免内存溢出）
    batch_size = 10
    for i in range(0, len(need), batch_size):
        batch = need[i:i+batch_size]
        print(f"[INFO] 补全批次 {i//batch_size + 1}: {batch[0]} ~ {batch[-1]}", flush=True)
        try:
            load_or_build_daily_board_snapshots(
                cache_root,
                batch,
                top_concepts=40,
                top_industries=40,
                min_concept_pct=None,
                min_industry_pct=None,
                skip_industry=False,
                hist_sleep_sec=0.15,
                max_concept_scan=0,
                max_industry_scan=0,
                force_refresh_daily=False,
                concept_daily="ths",
                industry_daily="ths",
            )
        except Exception as e:
            print(f"[WARN] 批次失败: {e}", flush=True)
        time.sleep(1)  # 批次间暂停
    
    print("[INFO] 板块数据补全完成", flush=True)

if __name__ == "__main__":
    main()
