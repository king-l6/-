#!/usr/bin/env python3
"""回测所有已记录策略，用缓存MACD，输出5个时间段收益"""
import json, glob, os, numpy as np
from datetime import datetime, timedelta

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

def ema_seq(data, period):
    k = 2/(period+1)
    r = [data[0]]
    for v in data[1:]:
        r.append(v*k + r[-1]*(1-k))
    return r

def backtest_breakout(stocks, trading_dates, period, vol_mult, hold_days, sl_pct, use_limit_filter=True):
    """突破放量策略回测"""
    signals = []
    for code, klines in stocks.items():
        n = len(klines)
        closes = [k["收盘"] for k in klines]
        volumes = [k["成交量"] for k in klines]
        dates = [k["日期"][:10] for k in klines]
        opens = [k["开盘"] for k in klines]
        highs = [k["最高"] for k in klines]
        lows = [k["最低"] for k in klines]
        
        for i in range(period, n-1):
            # 突破条件
            prev_high = max(closes[i-period:i])
            if closes[i] <= prev_high:
                continue
            if closes[i] < opens[i] * 1.03:
                continue
            # 放量条件
            if i < 5:
                continue
            vol_ma5 = sum(volumes[i-5:i]) / 5
            if vol_ma5 <= 0:
                continue
            if volumes[i] < vol_ma5 * vol_mult:
                continue
            
            signals.append({
                "code": code,
                "date": dates[i],
                "buy_idx": i + 1,
                "buy_price": opens[i+1] if i+1 < n else closes[i],
                "klines": klines,
                "closes": closes,
                "opens": opens,
                "lows": lows,
                "dates": dates,
                "n": n,
            })
    
    signals.sort(key=lambda x: x["date"])
    return run_backtest_single(signals, trading_dates, hold_days, sl_pct, use_limit_filter)

def backtest_macd_pullback(stocks, trading_dates, pullback_n, expand_n, use_ma, t_plus, hold_days, sl_pct):
    """回调N缩M扩策略回测（用缓存MACD）"""
    signals = []
    for code, klines in stocks.items():
        n = len(klines)
        # 用缓存MACD
        clean = [k for k in klines if "MACD_BAR" in k]
        if len(clean) < 60:
            continue
        
        closes_clean = [k["收盘"] for k in clean]
        dates_clean = [k["日期"][:10] for k in clean]
        
        for i in range(pullback_n + expand_n + 2, len(clean)):
            k = clean[i]
            if k["MACD_BAR"] <= 0:
                continue
            
            # 检查缩pullback_n天
            bars = [clean[i-j]["MACD_BAR"] for j in range(pullback_n + expand_n + 1, -1, -1)]
            had_pullback = False
            for start in range(0, len(bars) - pullback_n):
                if all(bars[start+j] > bars[start+j+1] > 0 for j in range(pullback_n-1)):
                    had_pullback = True
                    break
            if not had_pullback:
                continue
            
            # 检查扩expand_n天
            ok = True
            for j in range(expand_n):
                if clean[i-j]["MACD_BAR"] <= 0:
                    ok = False
                    break
                if j > 0 and clean[i-j]["MACD_BAR"] >= clean[i-j+1]["MACD_BAR"]:
                    ok = False
                    break
            if not ok:
                continue
            
            # 均线多头检查
            if use_ma:
                ma5 = sum(closes_clean[i-4:i+1]) / 5
                ma10 = sum(closes_clean[i-9:i+1]) / 10
                ma20 = sum(closes_clean[i-19:i+1]) / 20
                if not (ma5 > ma10 > ma20 and closes_clean[i] > ma5):
                    continue
            
            # 涨停过滤
            if i > 0:
                prev_close = clean[i-1]["收盘"]
                if prev_close > 0 and (k["收盘"] - prev_close) / prev_close >= 0.098:
                    continue
            
            # 找原始klines中的索引
            sig_date = k["日期"][:10]
            orig_idx = None
            for idx, okl in enumerate(klines):
                if okl["日期"][:10] == sig_date:
                    orig_idx = idx
                    break
            if orig_idx is None or orig_idx + t_plus >= len(klines):
                continue
            
            signals.append({
                "code": code,
                "date": sig_date,
                "buy_idx": orig_idx + t_plus,
                "buy_price": klines[orig_idx + t_plus]["开盘"],
                "klines": klines,
                "closes": [kk["收盘"] for kk in klines],
                "opens": [kk["开盘"] for kk in klines],
                "lows": [kk["最低"] for kk in klines],
                "dates": [kk["日期"][:10] for kk in klines],
                "n": len(klines),
            })
    
    signals.sort(key=lambda x: x["date"])
    return run_backtest_single(signals, trading_dates, hold_days, sl_pct, True)

def run_backtest_single(signals, trading_dates, hold_days, sl_pct, use_limit_filter):
    by_date = {}
    for s in signals:
        d = s["date"]
        if d not in by_date:
            by_date[d] = []
        by_date[d].append(s)
    
    capital = CAPITAL
    trades = []
    hold_until = ""
    
    for day in trading_dates:
        if day < "2025-04-20":
            continue
        if day <= hold_until:
            continue
        if day not in by_date:
            continue
        
        candidates = sorted(by_date[day], key=lambda x: abs(x.get("bar", 0)), reverse=True)
        
        for best in candidates:
            buy_idx = best["buy_idx"]
            if buy_idx >= best["n"]:
                continue
            
            buy_price = best["buy_price"]
            if buy_price <= 0:
                continue
            
            # 涨停过滤
            if use_limit_filter and buy_idx > 0:
                prev_close = best["closes"][buy_idx - 1]
                if prev_close > 0:
                    open_pct = (buy_price - prev_close) / prev_close * 100
                    if open_pct >= 9.5:
                        continue
            
            # 持仓
            sell_price = buy_price
            sell_day = day
            ret = 0
            for j in range(1, hold_days + 1):
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
            
            pnl = capital * ret
            capital += pnl
            
            trades.append({
                "code": best["code"],
                "buy_date": best["dates"][buy_idx],
                "sell_date": sell_day,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "ret": ret,
                "pnl": pnl,
                "capital": capital,
            })
            
            hold_until = sell_day
            break
    
    return trades, capital

def calc_period_stats(trades, start_date, end_date):
    pt = [t for t in trades if start_date <= t["buy_date"] <= end_date]
    if not pt:
        return None
    wins = [t for t in pt if t["ret"] > 0]
    # 该时段独立计算收益（从CAPITAL开始）
    cap = CAPITAL
    for t in pt:
        cap += cap * t["ret"]
    total_ret = (cap - CAPITAL) / CAPITAL * 100
    return {
        "trades": len(pt),
        "win_rate": len(wins) / len(pt) * 100,
        "total_ret": total_ret,
        "final_capital": cap,
    }

def main():
    print("加载数据...", flush=True)
    stocks = load_stocks()
    trading_dates = get_trading_dates(stocks)
    print(f"共 {len(stocks)} 只股票, {len(trading_dates)} 个交易日", flush=True)
    
    periods = [
        ("近一年", "2025-04-20", "2026-04-20"),
        ("近半年", "2025-10-20", "2026-04-20"),
        ("近三月", "2026-01-20", "2026-04-20"),
        ("近两月", "2026-02-20", "2026-04-20"),
        ("近一月", "2026-03-20", "2026-04-20"),
    ]
    
    strategies = [
        ("突破7天放量1.5倍 T+1持2天SL10%", "breakout", {"period": 7, "vol_mult": 1.5, "hold_days": 2, "sl_pct": 0.10}),
        ("突破10天放量2倍 T+1持5天SL15%", "breakout", {"period": 10, "vol_mult": 2.0, "hold_days": 5, "sl_pct": 0.15}),
        ("回调5缩4扩+均线 T+3持5天SL8%", "macd", {"pullback_n": 5, "expand_n": 4, "use_ma": True, "t_plus": 3, "hold_days": 5, "sl_pct": 0.08}),
        ("回调5缩4扩无均线 T+1持5天SL10%", "macd", {"pullback_n": 5, "expand_n": 4, "use_ma": False, "t_plus": 1, "hold_days": 5, "sl_pct": 0.10}),
    ]
    
    results = []
    
    for name, stype, params in strategies:
        print(f"\n回测: {name}", flush=True)
        
        if stype == "breakout":
            trades, final = backtest_breakout(stocks, trading_dates, **params, use_limit_filter=True)
        else:
            trades, final = backtest_macd_pullback(stocks, trading_dates, **params)
        
        print(f"  总交易: {len(trades)}笔, 终值: ¥{final:,.0f}", flush=True)
        
        row = {"name": name, "trades": len(trades), "final": final}
        for pname, ps, pe in periods:
            stats = calc_period_stats(trades, ps, pe)
            if stats:
                row[pname] = stats
                print(f"  {pname}: {stats['trades']}笔, 胜率{stats['win_rate']:.0f}%, 收益{stats['total_ret']:+.0f}%", flush=True)
            else:
                row[pname] = None
                print(f"  {pname}: 无交易", flush=True)
        
        results.append(row)
    
    # 打印汇总表格
    print("\n" + "="*80)
    print("回测结果汇总")
    print("="*80)
    print(f"{'策略':<35} {'交易':>5} {'近一年':>8} {'近半年':>8} {'近三月':>8} {'近两月':>8} {'近一月':>8}")
    print("-"*80)
    
    for r in results:
        line = f"{r['name']:<35} {r['trades']:>5}"
        for pname, _, _ in periods:
            stats = r.get(pname)
            if stats:
                icon = "✅" if stats["total_ret"] > 0 else "❌"
                line += f" {icon}{stats['total_ret']:>+6.0f}%"
            else:
                line += "     N/A"
        print(line)
    
    # 保存交易明细
    output_path = os.path.expanduser("~/.hermes/backtest_all_strategies.txt")
    with open(output_path, "w") as f:
        f.write(f"回测时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"股票数: {len(stocks)}\n\n")
        for r in results:
            f.write(f"\n{'='*60}\n{r['name']}\n{'='*60}\n")
            for pname, _, _ in periods:
                stats = r.get(pname)
                if stats:
                    f.write(f"  {pname}: {stats['trades']}笔, 胜率{stats['win_rate']:.0f}%, 收益{stats['total_ret']:+.0f}%\n")
            f.write(f"  终值: ¥{r['final']:,.0f}\n")
    
    print(f"\n交易明细已保存: {output_path}")

if __name__ == "__main__":
    main()
