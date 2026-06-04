#!/usr/bin/env python3
"""暴力搜索多种策略类型，不局限于突破放量"""
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

def run_backtest(signals, trading_dates, hold_days, sl_pct, start="2025-04-20", end="2026-04-20"):
    """通用回测引擎"""
    by_date = {}
    for s in signals:
        d = s["date"]
        if d not in by_date:
            by_date[d] = []
        by_date[d].append(s)
    
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
        
        for best in sorted(by_date[day], key=lambda x: x.get("score", 0), reverse=True):
            bi = best["buy_idx"]
            if bi >= best["n"]:
                continue
            bp = best["buy_price"]
            if bp <= 0:
                continue
            # 涨停过滤
            if bi > 0:
                pc = best["closes"][bi-1]
                if pc > 0 and (bp-pc)/pc*100 >= 9.5:
                    continue
            
            sp = bp
            sd = day
            ret = 0
            for j in range(1, hold_days+1):
                idx = bi + j
                if idx >= best["n"]:
                    break
                if best["lows"][idx] <= bp*(1-sl_pct):
                    sp = bp*(1-sl_pct)
                    sd = best["dates"][idx]
                    ret = -sl_pct
                    break
                sp = best["closes"][idx]
                sd = best["dates"][idx]
            else:
                ret = (sp-bp)/bp
            
            cap += cap * ret
            count += 1
            if ret > 0:
                wins += 1
            hold_until = sd
            break
    
    return {
        "ret": (cap-CAPITAL)/CAPITAL*100,
        "count": count,
        "win_rate": wins/count*100 if count else 0,
    }

def prep_stock(klines):
    """预处理股票数据"""
    n = len(klines)
    closes = [k["收盘"] for k in klines]
    opens = [k["开盘"] for k in klines]
    highs = [k["最高"] for k in klines]
    lows = [k["最低"] for k in klines]
    volumes = [k["成交量"] for k in klines]
    dates = [k["日期"][:10] for k in klines]
    
    # 计算各种指标
    ma5 = [0]*n
    ma10 = [0]*n
    ma20 = [0]*n
    vol_ma5 = [0]*n
    
    for i in range(4, n):
        ma5[i] = sum(closes[i-4:i+1])/5
    for i in range(9, n):
        ma10[i] = sum(closes[i-9:i+1])/10
    for i in range(19, n):
        ma20[i] = sum(closes[i-19:i+1])/20
    for i in range(4, n):
        vol_ma5[i] = sum(volumes[i-4:i+1])/5
    
    return {
        "n": n, "closes": closes, "opens": opens, "highs": highs,
        "lows": lows, "volumes": volumes, "dates": dates,
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "vol_ma5": vol_ma5,
    }

def strategy_gap_up(d, code, klines):
    """跳空高开后回踩"""
    signals = []
    for i in range(20, d["n"]-1):
        # 跳空高开：开盘价 > 前一天最高价
        if d["opens"][i] <= d["highs"][i-1]:
            continue
        # 当天收阳
        if d["closes"][i] <= d["opens"][i]:
            continue
        # 跳空幅度 1-5%
        gap = (d["opens"][i] - d["highs"][i-1]) / d["highs"][i-1] * 100
        if gap < 1 or gap > 5:
            continue
        # 之后1-3天回踩但不补缺口
        for j in range(1, 4):
            if i+j >= d["n"]-1:
                break
            # 回踩到缺口附近
            if d["lows"][i+j] <= d["highs"][i-1]:
                break  # 补了缺口，不好
            # 回踩后企稳（收阳或十字星）
            if d["closes"][i+j] >= d["opens"][i+j]:
                signals.append({
                    "code": code, "date": d["dates"][i+j],
                    "buy_idx": i+j+1, "buy_price": d["opens"][i+j+1] if i+j+1 < d["n"] else d["closes"][i+j],
                    "score": gap,
                    **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
                })
                break
    return signals

def strategy_consecutive_yang(d, code, klines, n_days):
    """连续小阳线"""
    signals = []
    for i in range(n_days+5, d["n"]-1):
        # 连续n_days天收阳
        ok = True
        for j in range(n_days):
            if d["closes"][i-j] <= d["opens"][i-j]:
                ok = False
                break
        if not ok:
            continue
        # 每天涨幅不超过3%
        for j in range(n_days):
            pct = (d["closes"][i-j] - d["opens"][i-j]) / d["opens"][i-j] * 100
            if pct > 3:
                ok = False
                break
        if not ok:
            continue
        # 总涨幅不超过10%
        total_pct = (d["closes"][i] - d["closes"][i-n_days+1]) / d["closes"][i-n_days+1] * 100
        if total_pct > 10:
            continue
        # 成交量温和放大
        if d["vol_ma5"][i] > 0 and d["volumes"][i] > d["vol_ma5"][i] * 0.5:
            signals.append({
                "code": code, "date": d["dates"][i],
                "buy_idx": i+1, "buy_price": d["opens"][i+1] if i+1 < d["n"] else d["closes"][i],
                "score": total_pct,
                **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
            })
    return signals

def strategy_volume_spike_no_move(d, code, klines):
    """放量不涨（吸筹信号）"""
    signals = []
    for i in range(20, d["n"]-1):
        # 成交量是5日均量的2倍以上
        if d["vol_ma5"][i] <= 0:
            continue
        vol_ratio = d["volumes"][i] / d["vol_ma5"][i]
        if vol_ratio < 2:
            continue
        # 但涨幅不超过1%
        pct = (d["closes"][i] - d["closes"][i-1]) / d["closes"][i-1] * 100
        if abs(pct) > 1:
            continue
        # 之前几天是缩量的
        if d["volumes"][i-1] > d["vol_ma5"][i-1] * 1.5:
            continue
        signals.append({
            "code": code, "date": d["dates"][i],
            "buy_idx": i+1, "buy_price": d["opens"][i+1] if i+1 < d["n"] else d["closes"][i],
            "score": vol_ratio,
            **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
        })
    return signals

def strategy_ma_cross(d, code, klines, short, long):
    """均线金叉"""
    signals = []
    ma_s = d[f"ma{short}"]
    ma_l = d[f"ma{long}"]
    for i in range(long+5, d["n"]-1):
        if ma_s[i] <= 0 or ma_l[i] <= 0:
            continue
        if ma_s[i-1] <= ma_l[i-1] and ma_s[i] > ma_l[i]:
            # 价格在均线上方
            if d["closes"][i] > ma_s[i]:
                signals.append({
                    "code": code, "date": d["dates"][i],
                    "buy_idx": i+1, "buy_price": d["opens"][i+1] if i+1 < d["n"] else d["closes"][i],
                    "score": (ma_s[i]-ma_l[i])/ma_l[i]*100,
                    **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
                })
    return signals

def strategy_big_yang_pullback(d, code, klines):
    """大阳线后缩量回调"""
    signals = []
    for i in range(20, d["n"]-5):
        # 大阳线：涨幅>5%
        pct = (d["closes"][i] - d["opens"][i]) / d["opens"][i] * 100
        if pct < 5:
            continue
        # 之后2-5天缩量回调
        for j in range(1, 6):
            if i+j >= d["n"]-1:
                break
            # 回调但不跌破大阳线开盘价
            if d["lows"][i+j] < d["opens"][i]:
                break
            # 缩量
            if d["volumes"][i+j] > d["volumes"][i] * 0.7:
                continue
            # 回调后企稳
            if d["closes"][i+j] >= d["opens"][i+j]:
                signals.append({
                    "code": code, "date": d["dates"][i+j],
                    "buy_idx": i+j+1, "buy_price": d["opens"][i+j+1] if i+j+1 < d["n"] else d["closes"][i+j],
                    "score": pct,
                    **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
                })
                break
    return signals

def strategy_lower_shadow(d, code, klines, min_pct):
    """长下影线（支撑信号）"""
    signals = []
    for i in range(20, d["n"]-1):
        body = abs(d["closes"][i] - d["opens"][i])
        lower = min(d["opens"][i], d["closes"][i]) - d["lows"][i]
        if body <= 0:
            continue
        if lower / body < min_pct:
            continue
        # 下影线长度占总幅度的比例
        total = d["highs"][i] - d["lows"][i]
        if total <= 0:
            continue
        if lower / total < 0.5:
            continue
        signals.append({
            "code": code, "date": d["dates"][i],
            "buy_idx": i+1, "buy_price": d["opens"][i+1] if i+1 < d["n"] else d["closes"][i],
            "score": lower/body,
            **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
        })
    return signals

def strategy_new_low_reversal(d, code, klines, lookback):
    """N日新低后反转"""
    signals = []
    for i in range(lookback, d["n"]-1):
        # 创lookback日新低
        if d["lows"][i] > min(d["lows"][i-lookback:i]):
            continue
        # 当天收长下影阳线
        if d["closes"][i] <= d["opens"][i]:
            continue
        lower = min(d["opens"][i], d["closes"][i]) - d["lows"][i]
        body = abs(d["closes"][i] - d["opens"][i])
        if body <= 0 or lower / body < 2:
            continue
        signals.append({
            "code": code, "date": d["dates"][i],
            "buy_idx": i+1, "buy_price": d["opens"][i+1] if i+1 < d["n"] else d["closes"][i],
            "score": lower/body,
            **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
        })
    return signals

def strategy_narrow_range(d, code, klines, days, max_range):
    """窄幅整理后突破"""
    signals = []
    for i in range(days+5, d["n"]-1):
        # 之前days天振幅很小
        rng = (max(d["highs"][i-days:i]) - min(d["lows"][i-days:i])) / d["closes"][i-days] * 100
        if rng > max_range:
            continue
        # 当天放量突破
        if d["volumes"][i] < d["vol_ma5"][i] * 1.5:
            continue
        if d["closes"][i] <= d["opens"][i]:
            continue
        signals.append({
            "code": code, "date": d["dates"][i],
            "buy_idx": i+1, "buy_price": d["opens"][i+1] if i+1 < d["n"] else d["closes"][i],
            "score": max_range - rng,
            **{k: v for k, v in d.items() if k in ["closes","opens","lows","dates","n"]}
        })
    return signals

def main():
    print("加载数据...", flush=True)
    stocks = load_stocks()
    trading_dates = get_trading_dates(stocks)
    print(f"共 {len(stocks)} 只股票, {len(trading_dates)} 交易日\n", flush=True)
    
    # 定义所有策略
    strategy_defs = []
    
    # 跳空高开回踩
    strategy_defs.append(("跳空高开回踩", strategy_gap_up, {}))
    
    # 连续小阳线
    for n in [3, 4, 5, 6]:
        strategy_defs.append((f"连续{n}小阳线", strategy_consecutive_yang, {"n_days": n}))
    
    # 放量不涨
    strategy_defs.append(("放量不涨吸筹", strategy_volume_spike_no_move, {}))
    
    # 均线金叉
    for s, l in [(5,10), (5,20), (10,20)]:
        strategy_defs.append((f"MA{s}金叉MA{l}", strategy_ma_cross, {"short": s, "long": l}))
    
    # 大阳线后缩量回调
    strategy_defs.append(("大阳线缩量回调", strategy_big_yang_pullback, {}))
    
    # 长下影线
    for min_pct in [2, 3, 5]:
        strategy_defs.append((f"长下影线(>={min_pct}倍)", strategy_lower_shadow, {"min_pct": min_pct}))
    
    # N日新低反转
    for lb in [5, 10, 15, 20]:
        strategy_defs.append((f"{lb}日新低反转", strategy_new_low_reversal, {"lookback": lb}))
    
    # 窄幅整理突破
    for days, rng in [(5,5), (10,8), (15,10), (20,12)]:
        strategy_defs.append((f"{days}天振幅<{rng}%突破", strategy_narrow_range, {"days": days, "max_range": rng}))
    
    # 回测参数
    hold_params = [(1,0.08), (2,0.08), (2,0.10), (3,0.10), (5,0.08), (5,0.10), (5,0.15)]
    
    periods = [
        ("1y", "2025-04-20", "2026-04-20"),
        ("6m", "2025-10-20", "2026-04-20"),
        ("3m", "2026-01-20", "2026-04-20"),
        ("2m", "2026-02-20", "2026-04-20"),
        ("1m", "2026-03-20", "2026-04-20"),
    ]
    
    all_results = []
    total_combos = len(strategy_defs) * len(hold_params)
    print(f"搜索 {len(strategy_defs)} 种策略 × {len(hold_params)} 种参数 = {total_combos} 种组合\n", flush=True)
    
    # 预处理所有股票
    prepped = {}
    for code, klines in stocks.items():
        prepped[code] = prep_stock(klines)
    
    combo_count = 0
    for sname, sfunc, sparams in strategy_defs:
        # 收集所有信号
        all_signals = []
        for code, d in prepped.items():
            sigs = sfunc(d, code, stocks[code], **sparams)
            all_signals.extend(sigs)
        
        if not all_signals:
            continue
        
        for hold_days, sl_pct in hold_params:
            combo_count += 1
            if combo_count % 50 == 0:
                print(f"进度: {combo_count}/{total_combos}", flush=True)
            
            results = {}
            for pname, start, end in periods:
                results[pname] = run_backtest(all_signals, trading_dates, hold_days, sl_pct, start, end)
            
            r1y = results["1y"]
            if r1y["count"] < 5:
                continue
            
            r6m = results["6m"]
            r3m = results["3m"]
            r2m = results["2m"]
            r1m = results["1m"]
            
            all_results.append({
                "name": f"{sname} 持{hold_days}天SL{int(sl_pct*100)}%",
                "1y": r1y["ret"], "6m": r6m["ret"], "3m": r3m["ret"],
                "2m": r2m["ret"], "1m": r1m["ret"],
                "trades": r1y["count"], "wr": r1y["win_rate"],
                "stab": r6m["ret"]/r1y["ret"]*100 if r1y["ret"]>0 else 0,
            })
    
    # 按近一年收益排序
    all_results.sort(key=lambda x: x["1y"], reverse=True)
    
    print(f"\n{'='*110}")
    print(f"Top 30 策略（按近一年收益）")
    print(f"{'='*110}")
    print(f"{'策略':<45} {'近一年':>8} {'近半年':>8} {'近三月':>8} {'近两月':>8} {'近一月':>8} {'交易':>5} {'胜率':>5} {'稳定性':>6}")
    print("-"*110)
    
    for r in all_results[:30]:
        icon = "✅" if r["6m"]>0 and r["3m"]>0 else "❌"
        print(f"{r['name']:<45} {r['1y']:>+7.0f}% {r['6m']:>+7.0f}% {r['3m']:>+7.0f}% {r['2m']:>+7.0f}% {r['1m']:>+7.0f}% {r['trades']:>5} {r['wr']:>4.0f}% {icon}{r['stab']:>5.0f}%")
    
    # 所有时段正收益
    print(f"\n{'='*110}")
    print(f"所有时段正收益的策略（按近一年收益）")
    print(f"{'='*110}")
    
    stable = [r for r in all_results if r["1y"]>0 and r["6m"]>0 and r["3m"]>0 and r["2m"]>0 and r["1m"]>0]
    stable.sort(key=lambda x: x["1y"], reverse=True)
    
    print(f"{'策略':<45} {'近一年':>8} {'近半年':>8} {'近三月':>8} {'近两月':>8} {'近一月':>8} {'交易':>5} {'胜率':>5} {'稳定性':>6}")
    print("-"*110)
    
    for r in stable[:20]:
        print(f"{r['name']:<45} {r['1y']:>+7.0f}% {r['6m']:>+7.0f}% {r['3m']:>+7.0f}% {r['2m']:>+7.0f}% {r['1m']:>+7.0f}% {r['trades']:>5} {r['wr']:>4.0f}% ✅{r['stab']:>5.0f}%")
    
    print(f"\n共搜索 {combo_count} 种组合，有效 {len(all_results)} 种，所有时段正收益 {len(stable)} 种")

if __name__ == "__main__":
    main()
