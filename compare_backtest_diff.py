#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
比较今天和昨天回测策略结果的 diff。
规则：对于同一 match_date，今天回测得到的该日数据 应 与 昨天回测得到的该日数据 一致。
若 (code, match_date) 的 match_price 不同，或 代码集合不同，说明数据/策略有问题。
"""

import json
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')


def load_jsonl(filepath):
    """加载 jsonl 结果文件，返回 [(code, name, match_date, match_price, current_price), ...]"""
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if '_meta' in obj:
                continue
            code = obj.get('code')
            match_date = obj.get('match_date')
            if not code or not match_date:
                continue
            rows.append({
                'code': code,
                'name': obj.get('name', ''),
                'match_date': match_date,
                'match_price': obj.get('match_price'),
                'current_price': obj.get('current_price'),
            })
    return rows


def group_by_match_date(rows):
    """按 match_date 分组: date -> {code: {match_price, name, ...}}"""
    by_date = defaultdict(dict)
    for r in rows:
        d = r['match_date']
        by_date[d][r['code']] = r
    return dict(by_date)


def find_latest_files():
    """找到今天和昨天的最新回测结果文件（按文件名日期）"""
    files = [f for f in os.listdir(RESULTS_DIR) if f.endswith('_结果.jsonl') and f.startswith('策略_')]
    by_date = {}  # YYYYMMDD -> fullpath
    for f in files:
        # 策略_20260205_181010_结果.jsonl -> 20260205
        parts = f.replace('_结果.jsonl', '').split('_')
        if len(parts) >= 2:
            date_str = parts[1]
            if len(date_str) == 8 and date_str.isdigit():
                path = os.path.join(RESULTS_DIR, f)
                if date_str not in by_date or os.path.getmtime(path) > os.path.getmtime(by_date[date_str]):
                    by_date[date_str] = path

    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    today_file = by_date.get(today)
    yesterday_file = by_date.get(yesterday)

    return today_file, yesterday_file, today, yesterday


def main():
    today_file, yesterday_file, today_str, yesterday_str = find_latest_files()

    print('=' * 70)
    print('回测结果 Diff 比对')
    print('=' * 70)
    print(f'今天 ({today_str}) 结果: {today_file or "无"}')
    print(f'昨天 ({yesterday_str}) 结果: {yesterday_file or "无"}')
    print()

    if not today_file:
        print('未找到今天的回测结果')
        sys.exit(1)

    if not yesterday_file:
        print('未找到昨天的回测结果，将使用最近一次非今日的结果作为基准')
        # 用除今天外最近的一天
        files = [f for f in os.listdir(RESULTS_DIR) if f.endswith('_结果.jsonl') and f.startswith('策略_')]
        by_date = {}
        for f in files:
            parts = f.replace('_结果.jsonl', '').split('_')
            if len(parts) >= 2:
                ds = parts[1]
                if len(ds) == 8 and ds.isdigit() and ds != today_str:
                    by_date[ds] = os.path.join(RESULTS_DIR, f)
        if by_date:
            yesterday_str = max(by_date.keys())
            yesterday_file = by_date[yesterday_str]
            print(f'使用 {yesterday_str} 的结果: {yesterday_file}')
        else:
            print('无其他日期的结果可对比')
            sys.exit(1)

    today_rows = load_jsonl(today_file)
    yesterday_rows = load_jsonl(yesterday_file)

    today_by_date = group_by_match_date(today_rows)
    yesterday_by_date = group_by_match_date(yesterday_rows)

    # 重叠日期：双方都有数据且日期 <= 昨天（昨天回测时可见）
    common_dates = sorted(set(today_by_date.keys()) & set(yesterday_by_date.keys()))
    # 只比对 昨天及之前 的日期（昨天回测时已确定的日期）
    cutoff = yesterday_str[:4] + '-' + yesterday_str[4:6] + '-' + yesterday_str[6:8]
    common_dates = [d for d in common_dates if d <= cutoff]

    if not common_dates:
        print('无重叠的 match_date 可对比（仅比对昨天及之前的日期）')
        return

    print(f'\n比对日期范围: {common_dates[0]} ~ {common_dates[-1]}（共 {len(common_dates)} 天）')
    print()

    has_issue = False
    for d in common_dates:
        t_codes = today_by_date[d]
        y_codes = yesterday_by_date[d]

        t_set = set(t_codes.keys())
        y_set = set(y_codes.keys())

        only_today = t_set - y_set
        only_yesterday = y_set - t_set
        both = t_set & y_set

        # 同 code 的 match_price 应一致
        price_diff = []
        for code in both:
            tp = t_codes[code].get('match_price')
            yp = y_codes[code].get('match_price')
            if tp is not None and yp is not None and abs(float(tp) - float(yp)) > 1e-6:
                price_diff.append((code, t_codes[code].get('name'), tp, yp))

        if only_today or only_yesterday or price_diff:
            has_issue = True
            print(f'[异常] match_date={d}')
            if only_yesterday:
                print(f'  仅昨天有、今天无: {sorted(only_yesterday)}')
            if only_today:
                print(f'  仅今天有、昨天无: {sorted(only_today)}')
            if price_diff:
                for code, name, tp, yp in price_diff[:10]:
                    print(f'  match_price 不一致: {code} {name} 今天={tp} 昨天={yp}')
                if len(price_diff) > 10:
                    print(f'  ... 共 {len(price_diff)} 只 match_price 不一致')
            print()

    if not has_issue:
        print('✓ 所有重叠日期的 (code, match_price) 与昨天回测结果一致，无异常。')
    else:
        print('存在不一致，可能原因：数据更新、缓存、或策略/代码变更。')


if __name__ == '__main__':
    main()
