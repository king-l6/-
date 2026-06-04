#!/usr/bin/env python3
"""
每日MACD柱加速选股脚本（使用预计算MACD数据）
策略: MACD柱连续3天变长且>0 → 买入
参数: 持仓15天 | 止损10% | 无止盈 | 单仓全押4万
过滤: 信号强度>=800
"""
import json, os, sys, glob

CACHE_DIR = "/Users/bilibili/Desktop/test/量化/cache/stock_data"
CAPITAL = 40000

# 加载股票名称
name_map = {}
try:
    with open("/Users/bilibili/Desktop/test/量化/cache/stock_list.json") as f:
        sl = json.load(f)
        for s in sl.get("stocks", []):
            name_map[s["code"]] = s["name"]
except:
    pass

def main():
    files = glob.glob(os.path.join(CACHE_DIR, "*.json"))
    
    signals = []
    for fp in files:
        code = os.path.basename(fp).split("_")[0]
        try:
            with open(fp) as f:
                data = json.load(f)
        except:
            continue
        
        rows = data.get("data", [])
        if len(rows) < 35:
            continue
        
        # 检查最后3天MACD柱是否连续变长且>0
        r0 = rows[-4] if len(rows) >= 4 else None
        r1 = rows[-3]
        r2 = rows[-2]
        r3 = rows[-1]
        
        b0 = r0.get("MACD_BAR") if r0 else None
        b1 = r1.get("MACD_BAR")
        b2 = r2.get("MACD_BAR")
        b3 = r3.get("MACD_BAR")
        
        if b0 is None or b1 is None or b2 is None or b3 is None:
            continue
        
        if not (b3 > 0 and b3 > b2 > b1 > 0):
            continue
        
        # 信号强度
        sig = data.get("last_signal_strength", 0)
        if not sig:
            close = r3["收盘"]
            close3 = r1["收盘"]
            pct3 = (close / close3 - 1) * 100 if close3 > 0 else 0
            sig = abs(b3) * (100 + abs(pct3))
        
        # 今日涨幅
        pct_today = r3.get("涨跌幅", 0)
        
        # 过滤涨停（涨幅>=9.8%视为涨停，买不进）
        if pct_today >= 9.8:
            continue
        
        # 过滤价格太高买不起的（4万本金至少买1手）
        if r3["收盘"] > CAPITAL / 100:
            continue
        
        signals.append({
            "code": code,
            "name": name_map.get(code, code),
            "close": r3["收盘"],
            "macd_bar": b3,
            "signal_strength": round(sig, 1),
            "pct_today": pct_today,
            "pct_3d": round((r3["收盘"]/r1["收盘"]-1)*100, 1) if r1["收盘"]>0 else 0,
        })
    
    # 按信号强度排序
    signals.sort(key=lambda x: -x["signal_strength"])
    
    # 过滤
    filtered = [s for s in signals if s["signal_strength"] >= 800]
    
    
    print("=" * 60, flush=True)
    print("  MACD柱加速策略 - 今日选股", flush=True)
    print("  策略: MACD柱连续3天变长且>0", flush=True)
    print("  参数: 持仓15天 | 止损10% | 无止盈 | 单仓4万", flush=True)
    print("  过滤: 信号强度>=800", flush=True)
    print("=" * 60, flush=True)
    print(flush=True)
    print(f"总信号: {len(signals)} 只, 过滤后: {len(filtered)} 只", flush=True)
    print(flush=True)
    
    if not filtered:
        print("今日无符合条件的标的", flush=True)
        return
    
    print(f"{'排名':>4} {'代码':<10} {'名称':<10} {'现价':>8} {'MACD柱':>8} {'信号强度':>8} {'今日涨幅':>8} {'3日涨幅':>8}", flush=True)
    print("-" * 70, flush=True)
    
    for i, s in enumerate(filtered[:10]):
        print(f"{i+1:>4} {s['code']:<10} {s['name']:<10} {s['close']:>8.2f} {s['macd_bar']:>8.4f} {s['signal_strength']:>8.1f} {s['pct_today']:>+7.1f}% {s['pct_3d']:>+7.1f}%", flush=True)
    
    print("-" * 70, flush=True)
    print(flush=True)
    
    top = filtered[0]
    shares = int(CAPITAL / top["close"] / 100) * 100
    cost = shares * top["close"]
    stop_loss = round(top["close"] * 0.9, 2)
    
    print(f"建议: 买入 {top['name']}({top['code']})", flush=True)
    print(f"价格: {top['close']:.2f} 元（开盘价）", flush=True)
    print(f"仓位: 约 {shares} 股（{cost:.0f}元）", flush=True)
    print(f"止损: {stop_loss} 元（-10%）", flush=True)
    print(f"持仓: 15个交易日", flush=True)
    
    if len(filtered) > 1:
        print(f"备选: {filtered[1]['name']}({filtered[1]['code']}) @ {filtered[1]['close']:.2f}", flush=True)

if __name__ == "__main__":
    main()
