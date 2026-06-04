#!/usr/bin/env python3
"""暴力搜索所有突破放量参数组合，找100倍策略"""
import json, glob, os, itertools
from datetime import datetime

CACHE_DIR = "/Users/bilibili/Desktop/test/量化/cache/stock_data"
CAPITAL = 40000

def load_stocks():
    stocks = {}
    for f in glob.glob(f"{CACHE_DIR}/*.json"):
        with open(f) as fh:
            d = json.load(fh)
        code = d.get("code", os.path.basename(f).split("_")[0])
        klines = d.get("data", [])
        if len(klines) < 60:
            continue
        stocks[code] = klines
    return stocks

def get_trading_dates(stocks):
    all_dates = set()
    for klines in stocks.values():
        for k in klines:
            all_dates.add(k["日期"][:10])
    return sorted(all_dates)

def backtest(stocks, trading_dates, period, vol_mult, hold_days, sl_pct):
    signals = []
    for code, klines in stocks.items():
        n = len(klines)
        closes = [k["收盘"] for k in klines]
        volumes = [k["成交量"] for k in klines]
        dates = [k["日期"][:10] for k in klines]
        opens = [k["开盘"] for k in klines]
        lows = [k["最低"] for k in klines]
        
        for i in range(period, n-1):
            prev_high = max(closes[i-period:i])
            if closes[i] <= prev_high:
                continue
            if closes[i] < opens[i] * 1.03:
                continue
            if i < 5:
                continue
            vol_ma5 = sum(volumes[i-5:i]) / 5
            if vol_ma5 <= 0:
                continue
            if volumes[i] < vol_ma5 * vol_mult:
                continue
            
            signals.append({
                "code": code, "date": dates[i],
                "buy_idx": i+1, "buy_price": opens[i+1] if i+1 < n else closes[i],
                "closes": closes, "opens": opens, "lows": lows, "dates": dates, "n": n,
            })
    
    signals.sort(key=lambda x: x["date"])
    by_date = {}
    for s in signals:
        d = s["date"]
        if d not in by_date:
            by_date[d] = []
        by_date[d].append(s)
    
    results = {}
    for period_name, start, end in [("1y", "2025-04-20", "2026-04-20"), 
                                      ("6m", "2025-10-20", "2026-04-20"),
                                      ("3m", "2026-01-20", "2026-04-20"),
                                      ("2m", "2026-02-20", "2026-04-20"),
                                      ("1m", "2026-03-20", "2026-04-20")]:
        cap = CAPITAL
        count = 0
        wins = 0
        hold_until = ""
        
        for day in trading_dates:
            if day < start or day > end:
                continue
            if day <= hold_until:
                continue
            if day not in by_date:
                continue
            
            for best in sorted(by_date[day], key=lambda x: abs(x.get("bar", 0)), reverse=True):
                buy_idx = best["buy_idx"]
                if buy_idx >= best["n"]:
                    continue
                buy_price = best["buy_price"]
                if buy_price <= 0:
                    continue
                if buy_idx > 0:
                    prev_close = best["closes"][buy_idx-1]
                    if prev_close > 0 and (buy_price - prev_close) / prev_close * 100 >= 9.5:
                        continue
                
                sell_price = buy_price
                sell_day = day
                ret = 0
                for j in range(1, hold_days+1):
                    idx = buy_idx + j
                    if idx >= best["n"]:
                        break
                    if best["lows"][idx] <= buy_price * (1 - sl_pct):
                        sell_price = buy_price * (1 - sl_pct)
                        sell_day = best["dates"][idx]
                        ret = -sl_pct
                        break
                    sell_price = best["closes"][idx]
                    sell_day = best["dates"][idx]
                else:
                    ret = (sell_price - buy_price) / buy_price
                
                cap += cap * ret
                count += 1
                if ret > 0:
                    wins += 1
                hold_until = sell_day
                break
        
        results[period_name] = {
            "ret": (cap - CAPITAL) / CAPITAL * 100,
            "count": count,
            "win_rate": wins / count * 100 if count > 0 else 0,
        }
    
    return results

def main():
    print("加载数据...", flush=True)
    stocks = load_stocks()
    trading_dates = get_trading_dates(stocks)
    print(f"共 {len(stocks)} 只股票, {len(trading_dates)} 个交易日\n", flush=True)
    
    # 参数搜索范围
    periods = [1, 2, 3, 5, 7, 10, 15, 20]
    vol_mults = [1.2, 1.5, 2.0, 2.5, 3.0]
    hold_days_list = [1, 2, 3, 5, 7, 10]
    sl_pcts = [0.05, 0.08, 0.10, 0.12, 0.15]
    
    total = len(periods) * len(vol_mults) * len(hold_days_list) * len(sl_pcts)
    print(f"搜索 {total} 种参数组合...", flush=True)
    
    all_results = []
    count = 0
    
    for p, v, h, s in itertools.product(periods, vol_mults, hold_days_list, sl_pcts):
        count += 1
        if count % 100 == 0:
            print(f"进度: {count}/{total}", flush=True)
        
        results = backtest(stocks, trading_dates, p, v, h, s)
        
        r1y = results["1y"]
        r6m = results["6m"]
        r3m = results["3m"]
        
        if r1y["count"] < 10:
            continue
        
        all_results.append({
            "params": f"突破{p}天放量{v}倍 T+1持{h}天SL{int(s*100)}%",
            "1y": r1y["ret"],
            "6m": r6m["ret"],
            "3m": r3m["ret"],
            "2m": results["2m"]["ret"],
            "1m": results["1m"]["ret"],
            "trades": r1y["count"],
            "win_rate": r1y["win_rate"],
            "stability": r6m["ret"] / r1y["ret"] * 100 if r1y["ret"] > 0 else 0,
        })
    
    # 按近一年收益排序
    all_results.sort(key=lambda x: x["1y"], reverse=True)
    
    print(f"\n{'='*100}")
    print(f"Top 30 策略（按近一年收益排序）")
    print(f"{'='*100}")
    print(f"{'策略':<40} {'近一年':>8} {'近半年':>8} {'近三月':>8} {'近两月':>8} {'近一月':>8} {'交易':>5} {'胜率':>5} {'稳定性':>6}")
    print("-"*100)
    
    for r in all_results[:30]:
        icon = "✅" if r["6m"] > 0 and r["3m"] > 0 else "❌"
        print(f"{r['params']:<40} {r['1y']:>+7.0f}% {r['6m']:>+7.0f}% {r['3m']:>+7.0f}% {r['2m']:>+7.0f}% {r['1m']:>+7.0f}% {r['trades']:>5} {r['win_rate']:>4.0f}% {icon}{r['stability']:>5.0f}%")
    
    # 找所有时段正收益且收益最高的
    print(f"\n{'='*100}")
    print(f"所有时段正收益的策略（按近一年收益排序）")
    print(f"{'='*100}")
    
    stable = [r for r in all_results if r["1y"] > 0 and r["6m"] > 0 and r["3m"] > 0 and r["2m"] > 0 and r["1m"] > 0]
    stable.sort(key=lambda x: x["1y"], reverse=True)
    
    print(f"{'策略':<40} {'近一年':>8} {'近半年':>8} {'近三月':>8} {'近两月':>8} {'近一月':>8} {'交易':>5} {'胜率':>5} {'稳定性':>6}")
    print("-"*100)
    
    for r in stable[:20]:
        print(f"{r['params']:<40} {r['1y']:>+7.0f}% {r['6m']:>+7.0f}% {r['3m']:>+7.0f}% {r['2m']:>+7.0f}% {r['1m']:>+7.0f}% {r['trades']:>5} {r['win_rate']:>4.0f}% ✅{r['stability']:>5.0f}%")
    
    # 保存结果
    output = os.path.expanduser("~/.hermes/strategy_search_results.txt")
    with open(output, "w") as f:
        f.write(f"搜索时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"参数组合: {total}\n")
        f.write(f"有效结果: {len(all_results)}\n\n")
        for r in all_results[:50]:
            f.write(f"{r['params']}: 1y={r['1y']:+.0f}% 6m={r['6m']:+.0f}% 3m={r['3m']:+.0f}% trades={r['trades']} win={r['win_rate']:.0f}%\n")
    
    print(f"\n结果已保存: {output}")

if __name__ == "__main__":
    main()
