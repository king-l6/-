#!/usr/bin/env python3
"""预计算MACD并存储EMA种子值，支持后续增量更新"""
import json, os, glob
from datetime import datetime

CACHE_DIR = "/Users/bilibili/Desktop/test/量化/cache/stock_data"

def ema_seq(data, period):
    k = 2/(period+1)
    r = [data[0]]
    for v in data[1:]:
        r.append(v*k + r[-1]*(1-k))
    return r

def process_stock(filepath):
    try:
        with open(filepath) as f:
            data = json.load(f)
    except:
        return False
    
    rows = data.get("data", [])
    if len(rows) < 35:
        return False
    
    # 已有种子值则跳过
    if data.get("ema12_seed") is not None:
        return False
    
    closes = [r["收盘"] for r in rows]
    
    ema12 = ema_seq(closes, 12)
    ema26 = ema_seq(closes, 26)
    dif = [f-s for f,s in zip(ema12, ema26)]
    dea = ema_seq(dif, 9)
    bars = [2*(d-e) for d,e in zip(dif, dea)]
    
    # 写入每行的MACD值
    for i, row in enumerate(rows):
        row["DIF"] = round(dif[i], 4)
        row["DEA"] = round(dea[i], 4)
        row["MACD_BAR"] = round(bars[i], 4)
    
    # 存储种子值（最后一天的EMA，用于增量计算）
    data["ema12_seed"] = round(ema12[-1], 6)
    data["ema26_seed"] = round(ema26[-1], 6)
    data["dea_seed"] = round(dea[-1], 6)
    data["macd_cached_at"] = datetime.now().isoformat()
    data["data"] = rows
    
    with open(filepath, "w") as f:
        json.dump(data, f, ensure_ascii=False)
    return True

files = glob.glob(os.path.join(CACHE_DIR, "*.json"))
print(f"共 {len(files)} 只", flush=True)

success = 0
skip = 0
for idx, fp in enumerate(files):
    if (idx+1) % 500 == 0:
        print(f"进度: {idx+1}/{len(files)} (成功:{success} 跳过:{skip})", flush=True)
    if process_stock(fp):
        success += 1
    else:
        skip += 1

print(f"\n完成: 成功{success} 跳过{skip}", flush=True)
