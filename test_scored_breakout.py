import json, os, numpy as np

cache_path = os.path.expanduser("~/.hermes/backtest_cache.json")
with open(cache_path) as f:
    cache = json.load(f)

START_DATE = "2025-04-20"
END_DATE = "2026-04-20"
PERIOD = 7
VOL_MULT = 1.5
HOLD_DAYS = 2
SL_PCT = 0.10
CAPITAL = 40000

def calc_macd(incloses):
    ema12, ema26 = incloses[0], incloses[0]
    dif_arr = []
    for c in incloses[1:]:
        ema12 = c * 2/13 + ema12 * 11/13
        ema26 = c * 2/27 + ema26 * 25/27
        dif_arr.append(ema12 - ema26)
    if not dif_arr: return [], [], []
    dea = [dif_arr[0]]
    for d in dif_arr[1:]:
        dea.append(d * 2/11 + dea[-1] * 9/11)
    dif_arr = dif_arr[1:]
    return dif_arr, dea, [(d-e)*2 for d,e in zip(dif_arr, dea)]

all_signals = []

for code, bars in cache.items():
    if not (code.startswith("sz00") or code.startswith("sh60")): continue
    n = len(bars)
    if n < 120: continue

    dates = [b['day'] for b in bars]
    opens = [b['open'] for b in bars]
    closes = [b['close'] for b in bars]
    highs = [b['high'] for b in bars]
    lows = [b['low'] for b in bars]
    volumes = [b['volume'] for b in bars]

    dif, dea, macd_bar = calc_macd(closes)
    min_len = min(len(dif), len(closes)-1, len(volumes)-1)

    for i in range(PERIOD, min_len-2):
        d = dates[i]
        if d < START_DATE or d > END_DATE: continue

        # 基础突破条件（不变）
        if closes[i] <= max(closes[i-PERIOD:i]): continue
        if closes[i] < opens[i] * 1.03: continue
        if volumes[i] <= volumes[i-1] * VOL_MULT: continue

        # 收集特征用于打分
        prev5_gain = (closes[i] - closes[i-5]) / closes[i-5] if i >= 5 else 0
        vol_ratio = volumes[i] / volumes[i-1]
        day_gain = (closes[i] - opens[i]) / opens[i]
        daily_returns = [(closes[j]-closes[j-1])/closes[j-1] for j in range(i-9, i)]
        volatility = np.std(daily_returns)

        buy_idx = i + 1
        if buy_idx >= n: continue
        if closes[i] * 1.095 < opens[buy_idx]: continue

        # 打分：前5天涨幅越低越好，放量越温和越好
        score = 0
        if prev5_gain < 0.03: score += 3
        elif prev5_gain < 0.05: score += 2
        elif prev5_gain < 0.08: score += 1

        if 1.5 <= vol_ratio <= 2.5: score += 2
        elif vol_ratio <= 3.0: score += 1

        if day_gain < 0.06: score += 2
        elif day_gain < 0.08: score += 1

        if volatility < 0.025: score += 1

        all_signals.append({
            'code': code,
            'signal_date': d,
            'buy_idx': buy_idx,
            'buy_date': dates[buy_idx],
            'buy_price': opens[buy_idx],
            'score': score,
            'dates': dates,
            'closes': closes,
            'highs': highs,
            'lows': lows,
        })

# 按信号日+得分排序，同一天优先买入得分高的
all_signals.sort(key=lambda x: (x['signal_date'], -x['score']))

# 测试不同得分阈值
for min_score in [0, 3, 5, 7]:
    filtered = [s for s in all_signals if s['score'] >= min_score]
    
    capital = CAPITAL
    trades = []
    hold_until = ""
    holding_stock = ""

    for sig in filtered:
        if sig['signal_date'] <= hold_until: continue
        if sig['code'] == holding_stock: continue

        buy_price = sig['buy_price']
        buy_idx = sig['buy_idx']
        n_dates = len(sig['dates'])

        sell_price = buy_price
        sell_date = sig['dates'][min(buy_idx + HOLD_DAYS, n_dates-1)]
        ret = 0

        for j in range(1, HOLD_DAYS + 1):
            idx = buy_idx + j
            if idx >= n_dates: break
            if sig['lows'][idx] <= buy_price * (1 - SL_PCT):
                sell_price = buy_price * (1 - SL_PCT)
                sell_date = sig['dates'][idx]
                ret = -SL_PCT
                break
            sell_price = sig['closes'][idx]
            sell_date = sig['dates'][idx]
        else:
            ret = (sell_price - buy_price) / buy_price

        pnl = capital * ret
        capital += pnl

        holding_stock = sig['code']
        hold_until = sell_date

        trades.append({
            'code': sig['code'],
            'signal_date': sig['signal_date'],
            'buy_date': sig['buy_date'],
            'buy_price': buy_price,
            'sell_date': sell_date,
            'sell_price': sell_price,
            'ret': ret,
            'score': sig['score'],
            'pnl': pnl,
            'capital': capital,
        })

    # 分时段
    periods = [
        ("近一年", "2025-04-20", "2026-04-20"),
        ("近半年", "2025-10-20", "2026-04-20"),
        ("近三月", "2026-01-20", "2026-04-20"),
        ("近两月", "2026-02-20", "2026-04-20"),
        ("近一月", "2026-03-20", "2026-04-20"),
    ]

    print(f"=== 得分>={min_score} (共{len(filtered)}个信号, 执行{len(trades)}笔) ===")
    for name, s, e in periods:
        pt = [t for t in trades if s <= t['buy_date'] <= e]
        if not pt:
            print(f"  {name}: 0笔")
            continue
        wins = [t for t in pt if t['ret'] > 0]
        sub_cap = CAPITAL
        for t in pt:
            sub_cap += sub_cap * t['ret']
        total_ret = (sub_cap - CAPITAL) / CAPITAL * 100
        print(f"  {name}: {len(pt)}笔, 胜率{len(wins)/len(pt)*100:.0f}%, 累计{total_ret:+.0f}%")
    print(f"  最终资金: ¥{capital:,.0f} ({(capital-CAPITAL)/CAPITAL*100:+.0f}%)")
    print()
