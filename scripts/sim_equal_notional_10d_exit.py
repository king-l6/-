#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
等本金模拟：每笔投入相同金额，规则为
- 次日开盘买入；
- 若 10 个交易日内最高价相对买入价涨幅 >= 5%，则在 +5% 卖出（与结果字段 day2_buy_hit_5pct_day 一致）；
- 否则在第 10 个交易日收盘价卖出（收益率用 day2_buy_10d_close_pct）。

依赖结果文件中已由回测/ enrich 脚本写好的字段：
  day2_buy_hit_5pct_day, day2_buy_10d_close_pct, day2_buy_10d_max_gain_pct

用法（项目根目录）：
  python scripts/sim_equal_notional_10d_exit.py
  python scripts/sim_equal_notional_10d_exit.py --notional 10000
  python scripts/sim_equal_notional_10d_exit.py --file 筑底突破_结果.jsonl
"""
import argparse
import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(PROJECT_ROOT, 'results')


def _read_rows(path):
    rows = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict) or 'code' not in obj:
                continue
            if '_meta' in obj and len(obj) == 1:
                continue
            name = (obj.get('name') or '').strip()
            if len(name) > 4:
                continue
            rows.append(obj)
    return rows


def _exit_return_pct(row):
    """
    单笔收益率(%)：触发 5% 则按 +5% 平仓；否则用第10日收盘相对次日开盘。
    无法计算返回 None。
    """
    hit = row.get('day2_buy_hit_5pct_day')
    if hit is not None:
        return 5.0
    close_pct = row.get('day2_buy_10d_close_pct')
    if close_pct is not None:
        return float(close_pct)
    # 无完整 10 日收盘时，若从未达标且仅有窗口内最高涨幅，无法按规则在第十日卖出
    return None


def summarize(rows, notional):
    valid = []
    skipped = 0
    for r in rows:
        pct = _exit_return_pct(r)
        if pct is None:
            skipped += 1
            continue
        pnl = notional * (pct / 100.0)
        valid.append((r, pct, pnl))

    n = len(valid)
    if n == 0:
        return {
            'n': 0,
            'skipped': skipped,
            'hit_5': 0,
            'total_pnl': 0.0,
            'total_invest': 0.0,
            'avg_ret_pct': None,
        }

    hit_5 = sum(1 for r, pct, _ in valid if r.get('day2_buy_hit_5pct_day') is not None)
    total_pnl = sum(p for _, _, p in valid)
    total_invest = notional * n
    avg_ret = sum(pct for _, pct, _ in valid) / n

    return {
        'n': n,
        'skipped': skipped,
        'hit_5': hit_5,
        'total_pnl': total_pnl,
        'total_invest': total_invest,
        'avg_ret_pct': avg_ret,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--notional', type=float, default=10000.0, help='每笔本金（元）')
    parser.add_argument('--file', type=str, default=None, help='只统计该 results 文件名')
    args = parser.parse_args()
    notional = float(args.notional)

    if args.file:
        paths = [os.path.join(RESULTS_DIR, args.file)]
        for p in paths:
            if not os.path.isfile(p):
                print(f'[ERROR] 文件不存在: {p}')
                sys.exit(1)
    else:
        paths = sorted(
            os.path.join(RESULTS_DIR, f)
            for f in os.listdir(RESULTS_DIR)
            if f.endswith('.jsonl') and os.path.isfile(os.path.join(RESULTS_DIR, f))
        )

    print(f'每笔本金: {notional:,.0f} 元')
    print(f'卖出规则: 10日内触及 +5% 则按 +5% 卖出；否则按第10交易日收盘卖出')
    print()

    grand_rows = []
    for fp in paths:
        rows = _read_rows(fp)
        s = summarize(rows, notional)
        name = os.path.basename(fp)
        grand_rows.extend(rows)
        if s['n'] == 0:
            print(f'{name}: 可计算 0 笔，跳过 {s["skipped"]} 笔（缺字段或无法推断）')
            continue
        roi = (s['total_pnl'] / s['total_invest']) * 100 if s['total_invest'] else 0
        print(
            f'{name}: 可计算 {s["n"]} 笔 | 触发+5%卖出 {s["hit_5"]} 笔 | '
            f'跳过 {s["skipped"]} 笔 | 合计盈亏 {s["total_pnl"]:+,.2f} 元 | '
            f'本金合计 {s["total_invest"]:,.0f} 元 | 组合收益率 {roi:+.2f}% | 单笔均收益 {s["avg_ret_pct"]:+.2f}%'
        )

    print()
    print('--- 全文件合并（同一笔可能在多文件重复，仅作参考）---')
    g = summarize(grand_rows, notional)
    if g['n'] == 0:
        print('无可计算记录')
        return
    roi = (g['total_pnl'] / g['total_invest']) * 100
    print(
        f'可计算 {g["n"]} 笔 | 触发+5%卖出 {g["hit_5"]} 笔 | 跳过 {g["skipped"]} 笔\n'
        f'合计盈亏: {g["total_pnl"]:+,.2f} 元\n'
        f'本金合计: {g["total_invest"]:,.0f} 元\n'
        f'组合收益率: {roi:+.2f}%\n'
        f'单笔平均收益率: {g["avg_ret_pct"]:+.2f}%'
    )
    if g['total_pnl'] > 0:
        print('\n结论: 按当前历史样本与上述简化规则，合计为盈利。')
    elif g['total_pnl'] < 0:
        print('\n结论: 按当前历史样本与上述简化规则，合计为亏损。')
    else:
        print('\n结论: 合计盈亏约为 0。')


if __name__ == '__main__':
    main()
