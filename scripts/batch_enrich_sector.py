#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量为所有回测结果添加板块联动
"""
import json
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.chdir(PROJECT_ROOT)

from scripts.enrich_sector_linkage import enrich_results_jsonl_inplace

RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")

def main():
    files = [f for f in os.listdir(RESULTS_DIR) if f.endswith('_结果.jsonl')]
    
    if not files:
        print("[INFO] 无结果文件", flush=True)
        return
    
    print(f"[INFO] 共 {len(files)} 个结果文件需要处理", flush=True)
    
    success_count = 0
    fail_count = 0
    
    for i, filename in enumerate(sorted(files), 1):
        filepath = os.path.join(RESULTS_DIR, filename)
        print(f"\n[{i}/{len(files)}] 处理: {filename}", flush=True)
        
        try:
            total, touched = enrich_results_jsonl_inplace(
                filepath,
                top_concepts=40,
                top_industries=20,
                match_date_align=True,
                hist_sleep_sec=0.15,
                concept_daily="ths",
                industry_daily="ths",
            )
            print(f"[INFO] 完成: {touched}/{total} 条有联动", flush=True)
            success_count += 1
        except Exception as e:
            print(f"[ERROR] 失败: {e}", flush=True)
            fail_count += 1
        
        time.sleep(0.5)
    
    print(f"\n[INFO] 批量处理完成: 成功 {success_count}, 失败 {fail_count}", flush=True)

if __name__ == "__main__":
    main()
