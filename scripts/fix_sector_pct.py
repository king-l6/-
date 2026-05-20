#!/usr/bin/env python3
"""修复板块涨跌幅：拉一次K线覆盖所有日期，避免重复请求。"""
import os, sys, json, time
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
sys.path.insert(0, PROJECT_ROOT)

import akshare as ak
import pandas as pd

CACHE_DIR = os.path.join(PROJECT_ROOT, "cache", "sector_linkage", "daily")
HIST_SLEEP = 0.12
TOP_N = 40


def get_trading_days(days_back=30):
    """从已有的板块缓存或数据获取交易日列表"""
    daily_dir = os.path.join(PROJECT_ROOT, "cache", "sector_linkage", "daily")
    if os.path.isdir(daily_dir):
        files = sorted([f[:-5] for f in os.listdir(daily_dir) if f.endswith(".json") and len(f) == 15])
        if files:
            return files[-days_back:]
    # 备用：从stock_data文件名提取日期
    stock_dir = os.path.join(PROJECT_ROOT, "cache", "stock_data")
    if os.path.isdir(stock_dir):
        dates = set()
        for f in os.listdir(stock_dir):
            if "_" in f and f.endswith(".json"):
                d = f.split("_")[1].replace(".json", "")
                if len(d) == 8:
                    dates.add(f"{d[:4]}-{d[4:6]}-{d[6:]}")
        return sorted(dates)[-days_back:]
    return []


def fetch_board_list(board_type="concept"):
    """获取板块名称和代码列表"""
    if board_type == "concept":
        df = ak.stock_board_concept_name_ths()
    else:
        df = ak.stock_board_industry_name_ths()
    if df is None or df.empty:
        return []
    return [(str(r["name"]).strip(), str(r["code"]).strip()) for _, r in df.iterrows()]


def fetch_all_board_klines(board_list, board_type, start_date, end_date):
    """拉取所有板块的K线数据，返回 {name: DataFrame}"""
    klines = {}
    total = len(board_list)
    for idx, (name, code) in enumerate(board_list):
        if (idx + 1) % 20 == 0 or idx == 0:
            print(f"  [{board_type}] 拉K线 {idx+1}/{total}", flush=True)
        try:
            if board_type == "concept":
                df = ak.stock_board_concept_index_ths(symbol=name, start_date=start_date, end_date=end_date)
            else:
                df = ak.stock_board_industry_index_ths(symbol=name, start_date=start_date, end_date=end_date)
            if df is not None and not df.empty:
                df["_d"] = pd.to_datetime(df["日期"], errors="coerce")
                df = df.dropna(subset=["_d"]).sort_values("_d").reset_index(drop=True)
                klines[name] = (code, df)
        except Exception:
            pass
        time.sleep(HIST_SLEEP)
    return klines


def calc_pct(klines_dict, trade_date_iso, board_type):
    """从缓存的K线数据计算某天所有板块的涨跌幅"""
    results = []
    target = trade_date_iso[:10]
    for name, (code, df) in klines_dict.items():
        mask = df["_d"].dt.strftime("%Y-%m-%d") == target
        hits = df.loc[mask]
        if hits.empty:
            continue
        pos = int(hits.index[0])
        if pos <= 0:
            continue
        try:
            c_prev = float(pd.to_numeric(df.iloc[pos - 1]["收盘价"], errors="coerce"))
            c = float(pd.to_numeric(df.iloc[pos]["收盘价"], errors="coerce"))
        except (ValueError, TypeError):
            continue
        if pd.isna(c_prev) or pd.isna(c) or c_prev <= 0:
            continue
        pct = (c / c_prev - 1.0) * 100.0
        results.append({"name": name, "board_id": code, "pct": round(pct, 2), "source": "ths_hist"})

    results.sort(key=lambda x: -x["pct"])
    for i, item in enumerate(results[:TOP_N]):
        item["rank"] = i + 1
    return results[:TOP_N]


def save_snapshot(trade_date_iso, concepts, industries):
    """保存快照文件"""
    payload = {
        "version": 2,
        "trade_date": trade_date_iso,
        "built_at": datetime.utcnow().isoformat(),
        "builder": "ths_concept_ths_industry",
        "concept_daily": "ths",
        "industry_daily": "ths",
        "concepts": concepts,
        "industries": industries,
    }
    path = os.path.join(CACHE_DIR, f"{trade_date_iso}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30, help="回溯多少个交易日")
    parser.add_argument("--skip-industry", action="store_true")
    args = parser.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)

    trading_days = get_trading_days(args.days)
    if not trading_days:
        print("[ERROR] 无交易日", flush=True)
        return

    # K线查询窗口覆盖所有交易日
    start_date = (datetime.strptime(trading_days[0], "%Y-%m-%d") - timedelta(days=10)).strftime("%Y%m%d")
    end_date = (datetime.strptime(trading_days[-1], "%Y-%m-%d") + timedelta(days=5)).strftime("%Y%m%d")

    print(f"[INFO] 修复 {len(trading_days)} 天: {trading_days[0]} ~ {trading_days[-1]}", flush=True)
    print(f"[INFO] K线窗口: {start_date} ~ {end_date}", flush=True)

    # 一次性拉取所有板块K线
    print("\n[INFO] 拉取概念板块K线…", flush=True)
    concept_list = fetch_board_list("concept")
    print(f"[INFO] 概念板块: {len(concept_list)} 个", flush=True)
    concept_klines = fetch_all_board_klines(concept_list, "concept", start_date, end_date)
    print(f"[INFO] 概念K线: {len(concept_klines)} 个有效", flush=True)

    industry_klines = {}
    if not args.skip_industry:
        print("\n[INFO] 拉取行业板块K线…", flush=True)
        industry_list = fetch_board_list("industry")
        print(f"[INFO] 行业板块: {len(industry_list)} 个", flush=True)
        industry_klines = fetch_all_board_klines(industry_list, "industry", start_date, end_date)
        print(f"[INFO] 行业K线: {len(industry_klines)} 个有效", flush=True)

    # 逐日计算
    for d in trading_days:
        concepts = calc_pct(concept_klines, d, "concept")
        industries = calc_pct(industry_klines, d, "industry") if not args.skip_industry else []
        save_snapshot(d, concepts, industries)
        c_top = concepts[0]["name"] if concepts else "N/A"
        c_pct = concepts[0]["pct"] if concepts else 0
        i_top = industries[0]["name"] if industries else "N/A"
        i_pct = industries[0]["pct"] if industries else 0
        print(f"  {d}: 概念{len(concepts)}个(榜首{c_top} {c_pct:+.2f}%) | 行业{len(industries)}个(榜首{i_top} {i_pct:+.2f}%)", flush=True)

    print(f"\n[完成] 修复了 {len(trading_days)} 天", flush=True)


if __name__ == "__main__":
    main()
