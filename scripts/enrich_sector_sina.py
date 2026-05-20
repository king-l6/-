#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用新浪API拉板块成份，为回测结果做概念联动（东财挂了的替代方案）
"""
import json
import os
import sys
import time
from typing import Dict, List, Set, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from sector_linkage import (
    extract_match_date_iso,
    format_linkage_text,
    normalize_stock_code,
    concept_hits_rank_pct_averages,
)

LINKAGE_CACHE = os.path.join(PROJECT_ROOT, "cache", "sector_linkage")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")


def get_sina_concept_map() -> Dict[str, str]:
    """获取新浪概念板块名→label映射"""
    import akshare as ak
    df = ak.stock_sector_spot(indicator="概念")
    if df is None or df.empty:
        return {}
    m = {}
    for _, r in df.iterrows():
        label = str(r.get("label", "")).strip()
        name = str(r.get("板块", "")).strip()
        if label and name:
            m[name] = label
    return m


def get_sina_industry_map() -> Dict[str, str]:
    """获取新浪行业板块名→label映射"""
    import akshare as ak
    df = ak.stock_sector_spot(indicator="行业")
    if df is None or df.empty:
        return {}
    m = {}
    for _, r in df.iterrows():
        label = str(r.get("label", "")).strip()
        name = str(r.get("板块", "")).strip()
        if label and name:
            m[name] = label
    return m


def get_sina_board_members(label: str) -> Set[str]:
    """获取新浪板块成份股票代码"""
    import akshare as ak
    try:
        df = ak.stock_sector_detail(sector=label)
        if df is None or df.empty:
            return set()
        col = "code" if "code" in df.columns else None
        if not col:
            return set()
        codes = set()
        for x in df[col].tolist():
            c = normalize_stock_code(x)
            if c:
                codes.add(c)
        return codes
    except Exception:
        return set()


def load_daily_snapshot(date_str: str) -> Tuple[List[dict], List[dict]]:
    """加载每日板块快照"""
    path = os.path.join(LINKAGE_CACHE, "daily", f"{date_str}.json")
    if not os.path.isfile(path):
        return [], []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("concepts", []), data.get("industries", [])


def enrich_file(filepath: str, sina_concepts: Dict[str, str], sina_industries: Dict[str, str]):
    """为单个结果文件添加板块联动"""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    meta_lines = []
    data_objs = []
    for raw in lines:
        line = raw.rstrip("\n\r")
        if not line.strip():
            meta_lines.append(raw)
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            meta_lines.append(raw)
            continue
        if isinstance(obj, dict) and obj.get("_meta") is not None:
            meta_lines.append(raw)
            continue
        if not isinstance(obj, dict) or "code" not in obj:
            meta_lines.append(raw)
            continue
        data_objs.append(obj)

    if not data_objs:
        return 0, 0

    # 按日期分组
    dates = sorted({d for d in (extract_match_date_iso(o) for o in data_objs) if d})
    print(f"  日期范围: {len(dates)} 个交易日 ({dates[0]}~{dates[-1]})", flush=True)

    # 缓存板块成员
    member_cache: Dict[str, Set[str]] = {}
    
    def get_members(board_name: str, board_kind: str) -> Set[str]:
        cache_key = f"{board_kind}:{board_name}"
        if cache_key in member_cache:
            return member_cache[cache_key]
        
        # 先找新浪label
        label = ""
        if board_kind == "concept":
            label = sina_concepts.get(board_name, "")
        elif board_kind == "industry":
            label = sina_industries.get(board_name, "")
        
        members = set()
        if label:
            members = get_sina_board_members(label)
            time.sleep(0.05)
        
        member_cache[cache_key] = members
        return members

    # 按日期处理
    membership_by_date: Dict[str, Tuple[Dict, Dict]] = {}
    
    for date_str in dates:
        concepts, industries = load_daily_snapshot(date_str)
        if not concepts and not industries:
            continue
        
        # 构建当天的板块成员映射
        concept_membership: Dict[str, List[Tuple[str, float, int]]] = {}
        industry_membership: Dict[str, List[Tuple[str, float, int]]] = {}
        
        # 只处理 top40 概念
        for c in concepts[:40]:
            name = c.get("name", "")
            pct = c.get("pct", 0.0)
            rank = c.get("rank", 0)
            if not name:
                continue
            members = get_members(name, "concept")
            for code in members:
                if code not in concept_membership:
                    concept_membership[code] = []
                concept_membership[code].append((name, pct, rank))
        
        # 只处理 top20 行业
        for ind in industries[:20]:
            name = ind.get("name", "")
            pct = ind.get("pct", 0.0)
            rank = ind.get("rank", 0)
            if not name:
                continue
            members = get_members(name, "industry")
            for code in members:
                if code not in industry_membership:
                    industry_membership[code] = []
                industry_membership[code].append((name, pct, rank))
        
        membership_by_date[date_str] = (concept_membership, industry_membership)
    
    print(f"  板块成员缓存: {len(member_cache)} 个板块", flush=True)

    # 写入联动字段
    asof = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
    touched = 0
    
    for obj in data_objs:
        code = normalize_stock_code(obj.get("code"))
        if not code:
            continue
        trade_d = extract_match_date_iso(obj)
        
        ch = []
        ih = []
        board_trade_date = None
        date_aligned = False
        
        if trade_d and trade_d in membership_by_date:
            mc, mi = membership_by_date[trade_d]
            ch = sorted(mc.get(code, []), key=lambda x: (-x[1], x[2]))
            ih = sorted(mi.get(code, []), key=lambda x: (-x[1], x[2]))
            date_aligned = True
            board_trade_date = trade_d
        
        industry_hit = (ih[0][0], ih[0][1], ih[0][2]) if ih else None
        text = format_linkage_text(ch, industry_hit, ranking_trade_date=board_trade_date if date_aligned else None)
        
        obj["linkage_text"] = text
        obj["linkage_concepts"] = [
            {"name": n, "pct": round(p, 2), "rank": int(rk)} for n, p, rk in ch
        ]
        ca = concept_hits_rank_pct_averages(ch)
        if ca is not None:
            obj["linkage_concept_rank_avg"] = round(float(ca[0]), 2)
            obj["linkage_concept_pct_avg"] = round(float(ca[1]), 2)
        else:
            obj["linkage_concept_rank_avg"] = None
            obj["linkage_concept_pct_avg"] = None
        obj["linkage_industry"] = industry_hit[0] if industry_hit else ""
        obj["linkage_industry_pct"] = round(float(industry_hit[1]), 2) if industry_hit else None
        obj["linkage_industry_rank"] = int(industry_hit[2]) if industry_hit else None
        obj["linkage_fetched_at"] = asof
        obj["linkage_date_aligned"] = date_aligned
        obj["linkage_board_trade_date"] = board_trade_date
        if text:
            touched += 1

    out_lines = meta_lines + [json.dumps(o, ensure_ascii=False) + "\n" for o in data_objs]
    if out_lines:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(out_lines)

    return len(data_objs), touched


def main():
    print("=" * 60, flush=True)
    print("新浪API板块联动 enrich", flush=True)
    print("=" * 60, flush=True)

    # 预加载新浪板块映射
    print("[1/4] 加载新浪概念板块映射...", flush=True)
    sina_concepts = get_sina_concept_map()
    print(f"  概念板块: {len(sina_concepts)} 个", flush=True)

    print("[2/4] 加载新浪行业板块映射...", flush=True)
    sina_industries = get_sina_industry_map()
    print(f"  行业板块: {len(sina_industries)} 个", flush=True)

    # 获取结果文件
    files = sorted([
        os.path.join(RESULTS_DIR, f)
        for f in os.listdir(RESULTS_DIR)
        if f.endswith("_结果.jsonl")
    ])
    print(f"\n[3/4] 共 {len(files)} 个结果文件需要处理", flush=True)

    total_rows = 0
    total_hit = 0
    
    for i, fp in enumerate(files, 1):
        fn = os.path.basename(fp)
        print(f"\n[{i}/{len(files)}] {fn}", flush=True)
        try:
            n, hit = enrich_file(fp, sina_concepts, sina_industries)
            print(f"  ✓ {hit}/{n} 条有联动", flush=True)
            total_rows += n
            total_hit += hit
        except Exception as e:
            print(f"  ✗ 失败: {e}", flush=True)

    print(f"\n[4/4] 全部完成: {total_hit}/{total_rows} 条有联动", flush=True)


if __name__ == "__main__":
    main()
