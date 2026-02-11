#!/usr/bin/env python3
"""调试：为什么 大位科技(600589) 2026-02-06 未命中筑底突破策略"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime, timedelta


def load_from_cache(code):
    """从缓存文件加载数据，不依赖 baostock"""
    cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache', 'stock_data')
    pattern = f"{code}_*.json"
    import glob
    files = glob.glob(os.path.join(cache_dir, pattern))
    if not files:
        return None
    with open(files[0], 'r', encoding='utf-8') as f:
        d = json.load(f)
    df = pd.DataFrame(d.get('data', []))
    if df.empty:
        return None
    df['日期'] = pd.to_datetime(df['日期'])
    for col in ['开盘', '收盘', '最高', '最低', '成交量']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.sort_values('日期').reset_index(drop=True)


def debug_day(df, i):
    """逐项检查第 i 日是否符合筑底突破，打印每步结果"""
    close = df['收盘'].astype(float).values
    low = df['最低'].astype(float).values
    high = df['最高'].astype(float).values
    dates = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')

    print(f"\n=== 检查 T日 index={i} 日期={dates[i]} ===")
    print(f"  开盘={df['开盘'].iloc[i]:.2f} 收盘={close[i]:.2f} 最高={high[i]:.2f} 最低={low[i]:.2f}")

    # 1. T日：阳线
    open_i = float(df['开盘'].iloc[i])
    ok1 = close[i] > open_i
    print(f"\n1. T日条件: 阳线(收>{open_i:.2f})={ok1}")
    if not ok1:
        print(f"   ✗ 非阳线")
        return

    # 2. 找 low3
    window = 3
    low3_idx = None
    for j in range(i, max(window, i - 25) - 1, -1):
        if j < window or j >= len(low) - window:
            continue
        left, right = max(0, j - window), min(len(low), j + window + 1)
        if low[j] <= low[left:right].min():
            low3_idx, low3_val = j, low[j]
            break
    if low3_idx is None or low3_idx < window + 5:
        print(f"\n2. 找 low3: ✗ 未找到或太近")
        return
    ok2b = low3_idx in (i, i - 1, i - 2)
    print(f"\n2. low3 index={low3_idx} 日期={dates[low3_idx]} 最低={low3_val:.2f} (T/T-1/T-2={ok2b})")
    if not ok2b:
        print(f"   ✗ 低点3 必须在 T、T-1 或 T-2")
        return

    # 3. 找 peak2
    peak2_idx = None
    for k in range(low3_idx - 1, max(window, low3_idx - 20) - 1, -1):
        if k < window or k >= len(high) - window:
            continue
        left, right = max(0, k - window), min(len(high), k + window + 1)
        if high[k] >= high[left:right].max():
            peak2_idx, peak2_val = k, high[k]
            break
    if peak2_idx is None:
        print(f"\n3. 找 peak2: ✗ 未找到")
        return
    print(f"\n3. peak2 index={peak2_idx} 日期={dates[peak2_idx]} 最高={peak2_val:.2f}")

    # 4. 找 low2
    low2_idx = None
    for m in range(peak2_idx - 1, max(window, peak2_idx - 25) - 1, -1):
        if m < window or m >= len(low) - window:
            continue
        left, right = max(0, m - window), min(len(low), m + window + 1)
        if low[m] <= low[left:right].min():
            low2_idx, low2_val = m, low[m]
            break
    if low2_idx is None or low2_val <= 0:
        print(f"\n4. 找 low2: ✗ 未找到")
        return
    print(f"\n4. low2 index={low2_idx} 日期={dates[low2_idx]} 最低={low2_val:.2f}")

    # 5. 二次筑底
    ok5a = low2_val * 0.95 <= low3_val <= low2_val * 1.05
    drop2_pct = (peak2_val - low3_val) / peak2_val * 100
    ok5b = drop2_pct > 10
    print(f"\n5. 二次筑底: 低点2×95%~105%包含低点3={ok5a} (low3={low3_val:.2f} in [{low2_val*0.95:.2f},{low2_val*1.05:.2f}])")
    print(f"   峰2→低点3 回调={drop2_pct:.1f}% >10%={ok5b}")
    if not ok5a:
        print(f"   ✗ 低点3 不在 [low2×95%, low2×105%]")
        return
    if not ok5b:
        print(f"   ✗ 回调不足10%")
        return

    # 6. 涨一小波
    rise_pct = (peak2_val - low2_val) / low2_val * 100
    ok6 = rise_pct > 10
    print(f"\n6. 涨一小波 低点2→峰2: 涨幅={rise_pct:.1f}% >10%={ok6}")
    if not ok6:
        print(f"   ✗ 涨幅不足10%")
        return

    # 7. 前面涨了一波：任意10日窗口内 最高-最低≥30%
    has_rise = False
    peak1_val = 0
    for start in range(max(0, low2_idx - 19), low2_idx - 9):
        end = start + 10
        if end > low2_idx:
            continue
        w_high = high[start:end].max()
        w_low = low[start:end].min()
        if w_low <= 0:
            continue
        range_pct = (w_high - w_low) / w_low * 100
        ok7a = range_pct >= 30
        ok7b = low2_idx - (end - 1) < 10
        drop1 = (w_high - low2_val) / w_high * 100
        ok7c = drop1 > 10
        if ok7a and ok7b and ok7c:
            has_rise = True
            peak1_val = w_high
            print(f"\n7. 前面涨了一波: 窗口[{start},{end}) 最高={w_high:.2f} 最低={w_low:.2f} 相差={range_pct:.1f}%≥30% ✓")
            print(f"   low2距窗口末={low2_idx - (end-1)}日 <10 ✓, 峰→low2回调={drop1:.1f}%>10% ✓")
            break
    if not has_rise:
        print(f"\n7. 前面涨了一波: ✗ 未找到满足条件的10日窗口")
        return

    # 8. 回调形成低点2
    drop1_pct = (peak1_val - low2_val) / peak1_val * 100
    ok8 = drop1_pct > 10
    print(f"\n8. 回调形成低点2 峰1→低点2: 回调={drop1_pct:.1f}% >10%={ok8}")
    if not ok8:
        print(f"   ✗ 回调不足10%")
        return

    # 9. 涨小波、二次筑底 <10 日
    d3 = peak2_idx - low2_idx
    d4 = low3_idx - peak2_idx
    ok9 = d3 < 10 and d4 < 10
    print(f"\n9. 每波交易日<10: 涨小波={d3}, 二次筑底={d4} → {ok9}")
    if not ok9:
        print(f"   ✗ 某波≥10日")
        return

    print(f"\n✓ 全部通过！应能命中")


def main():
    code = "600589"
    target_date = "2026-02-06"

    data = load_from_cache(code)
    if data is None or data.empty:
        print("无法获取 600589 数据（请确保 cache/stock_data/ 下有缓存）")
        return

    start = datetime(2025, 9, 1)
    end = datetime.now()
    dates = pd.to_datetime(data['日期']).dt.strftime('%Y-%m-%d')

    # 找 2026-02-06 的 index
    idx = None
    for i, d in enumerate(dates):
        if str(d)[:10] == target_date:
            idx = i
            break
    if idx is None:
        print(f"数据中无 {target_date}，最近日期: {dates.iloc[-5:].tolist()}")
        # 用最后一天测试
        idx = len(data) - 1
        print(f"改用最后一日 index={idx} 日期={dates.iloc[idx]}")

    print(f"\n大位科技(600589) 共 {len(data)} 行，目标日 {target_date} index={idx}")

    # 逐项调试目标日
    debug_day(data, idx)


if __name__ == "__main__":
    main()
