#!/usr/bin/env python3
"""扫描最近符合回调5缩4扩信号的股票（用缓存MACD）"""
import json, glob, os

CACHE_DIR = "/Users/bilibili/Desktop/test/量化/cache/stock_data"
with open(os.path.expanduser("~/.hermes/backtest_names.json")) as f:
    raw = json.load(f)
nm = {k.replace("sz","").replace("sh",""): v for k, v in raw.items()}

files = sorted(glob.glob(f"{CACHE_DIR}/*.json"))
recent = []

for fpath in files:
    try:
        with open(fpath) as fh: d = json.load(fh)
    except: continue
    if "data" not in d or len(d["data"]) < 60: continue
    code = d.get("code", os.path.basename(fpath).split("_")[0])
    name = nm.get(code, code)
    klines = d["data"]
    
    # 跳过没有MACD_BAR的记录
    clean = [k for k in klines if "MACD_BAR" in k]
    if len(clean) < 60: continue
    
    n = len(clean)
    for i in range(max(25, n-10), n):
        k = clean[i]
        if k["MACD_BAR"] <= 0: continue
        bars = [clean[i-j]["MACD_BAR"] for j in range(11, -1, -1)]
        had_pullback = False
        for start in range(0, 7):
            if start + 4 < len(bars):
                if all(bars[start+j] > bars[start+j+1] > 0 for j in range(4)):
                    had_pullback = True; break
        if not had_pullback: continue
        ok = True
        for j in range(4):
            if clean[i-j]["MACD_BAR"] <= 0: ok = False; break
            if j > 0 and clean[i-j]["MACD_BAR"] >= clean[i-j+1]["MACD_BAR"]: ok = False; break
        if not ok: continue
        pc = clean[i-1]["close"]
        if pc > 0 and (k["close"] - pc) / pc >= 0.098: continue
        sig_date = k["date"].split(" ")[0]
        if i + 1 < n:
            buy_date = clean[i+1]["date"].split(" ")[0]
            buy_price = clean[i+1]["open"]
        else:
            buy_date = "明天"
            buy_price = k["close"]
        chg = (k["close"] - pc) / pc * 100 if pc > 0 else 0
        recent.append({"name": name, "sig_date": sig_date, "buy_date": buy_date,
            "buy_price": buy_price, "close": k["close"], "change": chg})
        break

recent.sort(key=lambda x: x["sig_date"], reverse=True)
print("最近10天符合条件的股票:")
for s in recent[:30]:
    sign = "+" if s["change"] > 0 else ""
    print(f"{s['sig_date']} {s['name']} 收盘{s['close']:.2f} {sign}{s['change']:.1f}% 次日买入{s['buy_price']:.2f} ({s['buy_date']})")
print(f"共 {len(recent)} 只")
