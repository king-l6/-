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

all_trades = []

for code, bars in cache.items():
    if not (code.startswith("sz00") or code.startswith("sh60")): continue
    n = len(bars)
    if n < 120: continue

    dates = [b['day'] for b in bars]
    opens = [b['open'] for b in bars]
    closes = [b['close'] for b in bars]
    volumes = [b['volume'] for b in bars]

    dif, dea, macd_bar = calc_macd(closes)
    min_len = min(len(dif), len(closes)-1, len(volumes)-1)

    for i in range(PERIOD, min_len-2):
        d = dates[i]
        if d < START_DATE or d > END_DATE: continue

        # 基础突破条件
        if closes[i] <= max(closes[i-PERIOD:i]): continue
        if closes[i] < opens[i] * 1.03: continue
        if volumes[i] <= volumes[i-1] * VOL_MULT: continue

        # 特征
        prev5_gain = (closes[i] - closes[i-5]) / closes[i-5] if i >= 5 else 0
        prev10_gain = (closes[i] - closes[i-10]) / closes[i-10] if i >= 10 else 0
        prev20_gain = (closes[i] - closes[i-20]) / closes[i-20] if i >= 20 else 0
        day_gain = (closes[i] - opens[i]) / opens[i]
        vol_ratio = volumes[i] / volumes[i-1]
        up_days = sum(1 for j in range(i-4, i+1) if closes[j] > closes[j-1])
        max_daily_gain = max((closes[j] - closes[j-1])/closes[j-1] for j in range(i-5, i))
        daily_returns = [(closes[j]-closes[j-1])/closes[j-1] for j in range(i-9, i)]
        volatility = np.std(daily_returns)

        di = i-2
        if di < len(dif) and di >= 0:
            dif_val = dif[di]
            dea_val = dea[di]
            bar_val = macd_bar[di]
            bar_growing = macd_bar[di] > macd_bar[di-1] if di > 0 and di < len(macd_bar) else False
        else:
            dif_val = dea_val = bar_val = 0
            bar_growing = False

        buy_idx = i + 1
        if buy_idx >= n: continue
        if closes[i] * 1.095 < opens[buy_idx]: continue

        sell_idx = min(buy_idx + HOLD_DAYS, n-1)
        sell_price = closes[sell_idx]
        ret = (sell_price - opens[buy_idx]) / opens[buy_idx]
        if opens[buy_idx] <= 0: continue

        all_trades.append({
            'code': code, 'date': d, 'buy_date': dates[buy_idx],
            'buy_price': opens[buy_idx], 'sell_price': sell_price, 'ret': ret,
            'prev5_gain': prev5_gain, 'prev10_gain': prev10_gain, 'prev20_gain': prev20_gain,
            'day_gain': day_gain, 'vol_ratio': vol_ratio, 'up_days': up_days,
            'max_daily_gain': max_daily_gain, 'volatility': volatility,
            'dif': dif_val, 'dea': dea_val, 'macd_bar': bar_val,
            'macd_above_zero': dif_val > 0, 'macd_golden': dif_val > dea_val,
            'macd_bar_positive': bar_val > 0, 'macd_bar_growing': bar_growing,
        })

big_win = [t for t in all_trades if t['ret'] > 0.10]
big_lose = [t for t in all_trades if t['ret'] < -0.05]
small_win = [t for t in all_trades if 0 < t['ret'] <= 0.10]
small_lose = [t for t in all_trades if -0.05 <= t['ret'] <= 0]

print(f"总交易: {len(all_trades)}笔")
print(f"大赚(>10%): {len(big_win)}笔 ({len(big_win)/len(all_trades)*100:.0f}%)")
print(f"小赚(0-10%): {len(small_win)}笔 ({len(small_win)/len(all_trades)*100:.0f}%)")
print(f"小亏(-5%-0): {len(small_lose)}笔 ({len(small_lose)/len(all_trades)*100:.0f}%)")
print(f"大亏(<-5%): {len(big_lose)}笔 ({len(big_lose)/len(all_trades)*100:.0f}%)")
print()

features = ['prev5_gain', 'prev10_gain', 'prev20_gain', 'day_gain', 'vol_ratio',
            'up_days', 'max_daily_gain', 'volatility', 'dif', 'macd_bar']

print(f"{'特征':<18} {'大赚':>10} {'小赚':>10} {'小亏':>10} {'大亏':>10} {'差异':>10}")
print("-" * 73)

for feat in features:
    w = [t[feat] for t in big_win]
    sw = [t[feat] for t in small_win]
    sl = [t[feat] for t in small_lose]
    l = [t[feat] for t in big_lose]
    w_avg = np.mean(w) if w else 0
    sw_avg = np.mean(sw) if sw else 0
    sl_avg = np.mean(sl) if sl else 0
    l_avg = np.mean(l) if l else 0
    diff = w_avg - l_avg
    if feat in ['prev5_gain', 'prev10_gain', 'prev20_gain', 'day_gain', 'max_daily_gain']:
        print(f"{feat:<18} {w_avg*100:>9.1f}% {sw_avg*100:>9.1f}% {sl_avg*100:>9.1f}% {l_avg*100:>9.1f}% {diff*100:>9.1f}%")
    elif feat == 'vol_ratio':
        print(f"{feat:<18} {w_avg:>10.2f}x {sw_avg:>10.2f}x {sl_avg:>10.2f}x {l_avg:>10.2f}x {diff:>10.2f}x")
    elif feat == 'up_days':
        print(f"{feat:<18} {w_avg:>10.1f}天 {sw_avg:>10.1f}天 {sl_avg:>10.1f}天 {l_avg:>10.1f}天 {diff:>10.1f}天")
    elif feat == 'volatility':
        print(f"{feat:<18} {w_avg*100:>9.2f}% {sw_avg*100:>9.2f}% {sl_avg*100:>9.2f}% {l_avg*100:>9.2f}% {diff*100:>9.2f}%")
    else:
        print(f"{feat:<18} {w_avg:>10.3f} {sw_avg:>10.3f} {sl_avg:>10.3f} {l_avg:>10.3f} {diff:>10.3f}")

print()
bool_features = ['macd_above_zero', 'macd_golden', 'macd_bar_positive', 'macd_bar_growing']
print(f"{'布尔特征':<22} {'大赚':>10} {'大亏':>10} {'差异':>10}")
print("-" * 57)
for feat in bool_features:
    w_pct = sum(1 for t in big_win if t[feat]) / len(big_win) * 100
    l_pct = sum(1 for t in big_lose if t[feat]) / len(big_lose) * 100
    print(f"{feat:<22} {w_pct:>9.0f}% {l_pct:>9.0f}% {w_pct-l_pct:>+9.0f}%")

print()
print("=== 特征筛选测试 ===")
print()

conditions = [
    ("无条件(基准)", lambda t: True),
    ("前5天涨幅<3%", lambda t: t['prev5_gain'] < 0.03),
    ("前5天涨幅<5%", lambda t: t['prev5_gain'] < 0.05),
    ("前5天涨幅>0%", lambda t: t['prev5_gain'] > 0),
    ("前5天涨3-4天", lambda t: 3 <= t['up_days'] <= 4),
    ("前5天涨>=3天", lambda t: t['up_days'] >= 3),
    ("放量1.5-2.5倍", lambda t: 1.5 <= t['vol_ratio'] <= 2.5),
    ("放量1.5-3倍", lambda t: 1.5 <= t['vol_ratio'] <= 3.0),
    ("放量>3倍", lambda t: t['vol_ratio'] > 3.0),
    ("当天涨幅3-6%", lambda t: 0.03 <= t['day_gain'] <= 0.06),
    ("当天涨幅<6%", lambda t: t['day_gain'] < 0.06),
    ("当天涨幅>6%", lambda t: t['day_gain'] > 0.06),
    ("DIF>0", lambda t: t['dif'] > 0),
    ("DIF<0", lambda t: t['dif'] < 0),
    ("MACD金叉", lambda t: t['macd_golden']),
    ("MACD柱>0", lambda t: t['macd_bar'] > 0),
    ("MACD柱增长", lambda t: t['macd_bar_growing']),
    ("前20天涨幅<10%", lambda t: t['prev20_gain'] < 0.10),
    ("低波动率<3%", lambda t: t['volatility'] < 0.03),
    ("前5天<3%+放量1.5-3x", lambda t: t['prev5_gain'] < 0.03 and 1.5 <= t['vol_ratio'] <= 3.0),
    ("前5天<5%+MACD柱>0", lambda t: t['prev5_gain'] < 0.05 and t['macd_bar'] > 0),
    ("DIF>0+前5天<5%", lambda t: t['dif'] > 0 and t['prev5_gain'] < 0.05),
    ("MACD金叉+放量1.5-3x", lambda t: t['macd_golden'] and 1.5 <= t['vol_ratio'] <= 3.0),
    ("MACD柱增长+当天<6%", lambda t: t['macd_bar_growing'] and t['day_gain'] < 0.06),
    ("MACD柱增长+放量<3x", lambda t: t['macd_bar_growing'] and t['vol_ratio'] < 3.0),
]

print(f"{'条件':<35} {'笔数':>5} {'胜率':>6} {'均收益':>8} {'大赚%':>7} {'大亏%':>7}")
print("-" * 75)

for name, cond in conditions:
    filtered = [t for t in all_trades if cond(t)]
    if len(filtered) < 3: continue
    wins = [t for t in filtered if t['ret'] > 0]
    big_w = [t for t in filtered if t['ret'] > 0.10]
    big_l = [t for t in filtered if t['ret'] < -0.05]
    avg_ret = np.mean([t['ret'] for t in filtered])
    mark = " <<<" if avg_ret > 0.05 and len(filtered) >= 20 else ""
    print(f"{name:<35} {len(filtered):>5} {len(wins)/len(filtered)*100:>5.0f}% {avg_ret*100:>+7.1f}% {len(big_w)/len(filtered)*100:>6.0f}% {len(big_l)/len(filtered)*100:>6.0f}%{mark}")
