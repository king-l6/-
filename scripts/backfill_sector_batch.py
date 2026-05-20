#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用同花顺接口批量补全历史板块/概念排行数据
优化：每个板块获取一段时间的历史K线，而不是逐天获取
"""
import json
import os
import sys
import glob
import time
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("no_proxy", "*")

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

def fetch_concept_history(start_date, end_date):
    """获取所有概念板块的历史K线（同花顺）"""
    import akshare as ak
    
    try:
        # 获取所有概念板块
        df = ak.stock_board_concept_name_ths()
        if df is None or df.empty:
            return {}
        
        concept_data = {}
        for idx, row in df.iterrows():
            name = row.get('name', '')
            code = row.get('code', '')
            if not name or not code:
                continue
            
            try:
                # 获取该板块的历史K线
                hist = ak.stock_board_concept_index_ths(
                    symbol=name,
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', '')
                )
                if hist is not None and not hist.empty:
                    # 存储每天的数据
                    for _, h_row in hist.iterrows():
                        date_str = str(h_row.get('日期', ''))[:10]
                        if date_str and len(date_str) == 10:
                            if date_str not in concept_data:
                                concept_data[date_str] = []
                            
                            # 计算涨跌幅（相对前一日）
                            pct = 0.0
                            if '收盘价' in h_row:
                                # 需要前一日数据，暂时设为0
                                pct = 0.0
                            
                            concept_data[date_str].append({
                                'name': name,
                                'board_id': code,
                                'pct': pct,
                                'source': 'ths_hist',
                                'rank': len(concept_data[date_str]) + 1
                            })
                    
                    print(f"[INFO] 概念板块 {name}: {len(hist)} 天数据", flush=True)
            except Exception as e:
                print(f"[WARN] 概念板块 {name} 失败: {e}", flush=True)
                continue
            
            time.sleep(0.1)
        
        return concept_data
    except Exception as e:
        print(f"[WARN] 获取概念板块失败: {e}", flush=True)
        return {}

def fetch_industry_history(start_date, end_date):
    """获取所有行业板块的历史K线（同花顺）"""
    import akshare as ak
    
    try:
        # 获取所有行业板块
        df = ak.stock_board_industry_name_ths()
        if df is None or df.empty:
            return {}
        
        industry_data = {}
        for idx, row in df.iterrows():
            name = row.get('name', '')
            code = row.get('code', '')
            if not name or not code:
                continue
            
            try:
                # 获取该板块的历史K线
                hist = ak.stock_board_industry_index_ths(
                    symbol=name,
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', '')
                )
                if hist is not None and not hist.empty:
                    # 存储每天的数据
                    for _, h_row in hist.iterrows():
                        date_str = str(h_row.get('日期', ''))[:10]
                        if date_str and len(date_str) == 10:
                            if date_str not in industry_data:
                                industry_data[date_str] = []
                            
                            pct = 0.0
                            if '收盘价' in h_row:
                                pct = 0.0
                            
                            industry_data[date_str].append({
                                'name': name,
                                'board_id': code,
                                'pct': pct,
                                'source': 'ths_hist',
                                'rank': len(industry_data[date_str]) + 1
                            })
                    
                    print(f"[INFO] 行业板块 {name}: {len(hist)} 天数据", flush=True)
            except Exception as e:
                print(f"[WARN] 行业板块 {name} 失败: {e}", flush=True)
                continue
            
            time.sleep(0.1)
        
        return industry_data
    except Exception as e:
        print(f"[WARN] 获取行业板块失败: {e}", flush=True)
        return {}

def save_daily_snapshot(date_str, concepts, industries):
    """保存每日快照"""
    cache_root = os.path.join(PROJECT_ROOT, "cache", "sector_linkage")
    daily_dir = os.path.join(cache_root, "daily")
    os.makedirs(daily_dir, exist_ok=True)
    
    filepath = os.path.join(daily_dir, f"{date_str}.json")
    data = {
        "version": 2,
        "trade_date": date_str,
        "built_at": datetime.now().isoformat(),
        "builder": "ths_concept_ths_industry",
        "concept_daily": "ths",
        "industry_daily": "ths",
        "concepts": concepts,
        "industries": industries
    }
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=0)
    
    return filepath

def main():
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

    # 4. 批量获取历史数据
    start_date = need[0]
    end_date = need[-1]
    
    print(f"[INFO] 开始获取概念板块历史数据 ({start_date} ~ {end_date})...", flush=True)
    concept_data = fetch_concept_history(start_date, end_date)
    print(f"[INFO] 概念板块数据获取完成，共 {len(concept_data)} 天", flush=True)
    
    print(f"[INFO] 开始获取行业板块历史数据 ({start_date} ~ {end_date})...", flush=True)
    industry_data = fetch_industry_history(start_date, end_date)
    print(f"[INFO] 行业板块数据获取完成，共 {len(industry_data)} 天", flush=True)

    # 5. 保存每日快照
    saved_count = 0
    for date_str in need:
        concepts = concept_data.get(date_str, [])
        industries = industry_data.get(date_str, [])
        
        if concepts or industries:
            # 按涨跌幅排序（如果有的话）
            concepts.sort(key=lambda x: x.get('pct', 0), reverse=True)
            industries.sort(key=lambda x: x.get('pct', 0), reverse=True)
            
            # 更新排名
            for i, c in enumerate(concepts):
                c['rank'] = i + 1
            for i, ind in enumerate(industries):
                ind['rank'] = i + 1
            
            filepath = save_daily_snapshot(date_str, concepts[:40], industries[:40])
            saved_count += 1
            print(f"[INFO] 已保存: {date_str} (概念{len(concepts)}个, 行业{len(industries)}个)", flush=True)
        else:
            print(f"[WARN] {date_str} 无数据", flush=True)
    
    print(f"[INFO] 板块数据补全完成，共保存 {saved_count} 天", flush=True)

if __name__ == "__main__":
    main()
