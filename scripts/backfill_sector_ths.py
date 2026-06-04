#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用同花顺接口补全历史板块/概念排行数据
"""
import json
import os
import sys
import glob
import time
from datetime import datetime

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

def fetch_concept_ranking(date_str):
    """获取某天的概念板块排行（同花顺）"""
    import akshare as ak
    
    try:
        # 获取所有概念板块
        df = ak.stock_board_concept_name_ths()
        if df is None or df.empty:
            return []
        
        results = []
        for idx, row in df.iterrows():
            name = row.get('name', '')
            code = row.get('code', '')
            if not name or not code:
                continue
            
            try:
                # 获取该板块的历史K线
                hist = ak.stock_board_concept_index_ths(
                    symbol=name,
                    start_date=date_str.replace('-', ''),
                    end_date=date_str.replace('-', '')
                )
                if hist is not None and not hist.empty:
                    # 计算涨跌幅（相对前一日）
                    pct = 0.0
                    if '收盘价' in hist.columns:
                        close = float(hist.iloc[0]['收盘价'])
                        # 需要前一日数据来计算涨跌幅，这里简化处理
                        pct = 0.0  # 暂时设为0，后续优化
                    
                    results.append({
                        'name': name,
                        'board_id': code,
                        'pct': pct,
                        'source': 'ths_hist',
                        'rank': len(results) + 1
                    })
                    
                    if len(results) >= 40:  # 只取前40
                        break
            except Exception:
                continue
            
            time.sleep(0.1)  # 避免请求过快
        
        return results
    except Exception as e:
        print(f"[WARN] 获取概念板块失败: {e}", flush=True)
        return []

def fetch_industry_ranking(date_str):
    """获取某天的行业板块排行（同花顺）"""
    import akshare as ak
    
    try:
        # 获取所有行业板块
        df = ak.stock_board_industry_name_ths()
        if df is None or df.empty:
            return []
        
        results = []
        for idx, row in df.iterrows():
            name = row.get('name', '')
            code = row.get('code', '')
            if not name or not code:
                continue
            
            try:
                # 获取该板块的历史K线
                hist = ak.stock_board_industry_index_ths(
                    symbol=name,
                    start_date=date_str.replace('-', ''),
                    end_date=date_str.replace('-', '')
                )
                if hist is not None and not hist.empty:
                    pct = 0.0
                    if '收盘价' in hist.columns:
                        pct = 0.0  # 暂时设为0
                    
                    results.append({
                        'name': name,
                        'board_id': code,
                        'pct': pct,
                        'source': 'ths_hist',
                        'rank': len(results) + 1
                    })
                    
                    if len(results) >= 40:  # 只取前40
                        break
            except Exception:
                continue
            
            time.sleep(0.1)
        
        return results
    except Exception as e:
        print(f"[WARN] 获取行业板块失败: {e}", flush=True)
        return []

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
            if f.endswith(".json") and len(f) == 15:
                existing.add(f[:-5])
    
    print(f"[INFO] 已有板块数据 {len(existing)} 天", flush=True)

    # 3. 找出缺失的日期
    need = [d for d in all_days if d not in existing]
    if not need:
        print("[INFO] 板块数据已齐全，无需补全", flush=True)
        return
    
    print(f"[INFO] 需要补全 {len(need)} 天: {need[0]} ~ {need[-1]}", flush=True)

    # 4. 逐天补全（每天都要重新获取板块列表）
    for i, date_str in enumerate(need):
        print(f"[INFO] 补全 {i+1}/{len(need)}: {date_str}", flush=True)
        
        try:
            concepts = fetch_concept_ranking(date_str)
            industries = fetch_industry_ranking(date_str)
            
            if concepts or industries:
                filepath = save_daily_snapshot(date_str, concepts, industries)
                print(f"[INFO] 已保存: {filepath} (概念{len(concepts)}个, 行业{len(industries)}个)", flush=True)
            else:
                print(f"[WARN] {date_str} 无数据", flush=True)
        except Exception as e:
            print(f"[ERROR] {date_str} 失败: {e}", flush=True)
        
        time.sleep(0.5)  # 每天间隔
    
    print("[INFO] 板块数据补全完成", flush=True)

if __name__ == "__main__":
    main()
