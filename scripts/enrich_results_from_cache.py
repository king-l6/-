#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用本地缓存 K 线数据补全回测结果文件中的「匹配日/次日/第三日」涨跌幅与振幅，并写回原文件。

策略结果文件里已有 code、match_date 等，但可能缺少 day1_amplitude、day1_change_pct、day2_*、day3_*。
本脚本根据 code + match_date 在 cache/stock_data 下找到对应股票的缓存，从缓存中取出匹配日及后两日
的 开盘/收盘，计算上述字段后写回结果文件（覆盖原行，保留其他字段）。

使用（在项目根目录）：
  python scripts/enrich_results_from_cache.py                    # 处理 results 下所有 .jsonl
  python scripts/enrich_results_from_cache.py --file 龙头战法_结果.jsonl  # 只处理指定文件
  python scripts/enrich_results_from_cache.py --dry-run          # 只打印将要补全的条数，不写回
"""

import os
import sys
import json
import glob
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CACHE_DIR = os.path.join(PROJECT_ROOT, 'cache', 'stock_data')
RESULTS_DIR = os.path.join(PROJECT_ROOT, 'results')


def _norm_date(d):
    """从缓存里的 日期 取 YYYY-MM-DD。"""
    if d is None:
        return None
    s = str(d).strip()
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        return s[:10]
    return s[:10] if len(s) >= 10 else s


def _load_cache_rows_for_code(code):
    """
    加载某只股票在缓存中的 K 线列表，按日期排序。
    返回 [(date_str, row), ...]，row 含 开盘、收盘 等。
    若未找到或数据为空返回 []。
    """
    pattern = os.path.join(CACHE_DIR, f'{code}_*.json')
    files = glob.glob(pattern)
    if not files:
        return []
    # 多文件时取一份（通常 remove_duplicate 后每码一份）
    best = None
    best_end = ''
    for fp in files:
        name = os.path.basename(fp)
        if '_' not in name or not name.endswith('.json'):
            continue
        parts = name[:-5].split('_')
        if len(parts) != 3 or parts[0] != code or len(parts[1]) != 8 or len(parts[2]) != 8:
            continue
        if parts[2] > best_end:
            best_end = parts[2]
            best = fp
    if not best or not os.path.isfile(best):
        return []
    try:
        with open(best, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return []
    rows = data.get('data') or []
    out = []
    for r in rows:
        ds = r.get('日期')
        if not ds:
            continue
        date_str = _norm_date(ds)
        if not date_str:
            continue
        try:
            open_ = float(r.get('开盘', 0))
            close = float(r.get('收盘', 0))
        except (TypeError, ValueError):
            continue
        out.append((date_str, {'开盘': open_, '收盘': close, 'row': r}))
    out.sort(key=lambda x: x[0])
    return out


def enrich_one_record(record, cache_rows):
    """
    根据 cache_rows [(date_str, row), ...] 为 record 补全 day1/day2/day3 等字段。
    record 需有 code、match_date；会原地修改 record，并返回 True。
    若 match_date 不在 cache 中或数据不足，不修改并返回 False。
    """
    match_date = (record.get('match_date') or '').strip()[:10]
    if not match_date or len(match_date) < 10:
        return False
    dates = [x[0] for x in cache_rows]
    if match_date not in dates:
        return False
    idx = dates.index(match_date)
    n = len(cache_rows)

    day1 = cache_rows[idx][1]
    day1_open, day1_close = day1['开盘'], day1['收盘']
    prev_close = None
    if idx >= 1:
        prev_close = cache_rows[idx - 1][1]['收盘']

    # 匹配日当天
    if day1_open and day1_open > 0:
        record['day1_amplitude'] = round((day1_close - day1_open) / day1_open * 100, 2)
    if prev_close and prev_close > 0:
        record['day1_change_pct'] = round((day1_close - prev_close) / prev_close * 100, 2)
    record['match_price'] = round(day1_close, 2)

    # 次日
    day2_close = None
    if idx + 1 < n:
        day2 = cache_rows[idx + 1][1]
        day2_open, day2_close = day2['开盘'], day2['收盘']
        if day2_open and day2_open > 0:
            record['day2_amplitude'] = round((day2_close - day2_open) / day2_open * 100, 2)
        if day1_close and day1_close > 0:
            record['day2_change_pct'] = round((day2_close - day1_close) / day1_close * 100, 2)

    # 第三日
    if idx + 2 < n:
        day3 = cache_rows[idx + 2][1]
        day3_open, day3_close = day3['开盘'], day3['收盘']
        if day3_open and day3_open > 0:
            record['day3_amplitude'] = round((day3_close - day3_open) / day3_open * 100, 2)
        if idx + 1 < n:
            d2 = cache_rows[idx + 1][1]
            if d2['收盘'] and d2['收盘'] > 0:
                record['day3_change_pct'] = round((day3_close - d2['收盘']) / d2['收盘'] * 100, 2)

    # 当前价：缓存最后一条的收盘
    if cache_rows:
        record['current_price'] = round(cache_rows[-1][1]['收盘'], 2)
    return True


def process_file(filepath, dry_run=False):
    """
    处理单个 .jsonl 文件：逐行读取，对数据行用缓存补全后写回（除非 dry_run）。
    返回 (total_data_lines, enriched_count)。
    """
    if not os.path.isfile(filepath):
        return 0, 0
    lines = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"[WARN] 读取失败 {filepath}: {e}")
        return 0, 0
    if not lines:
        return 0, 0

    meta_lines = []  # _meta 及前面的空行、非数据行
    data_objs = []   # 数据行对象
    total = 0
    enriched = 0
    cache_by_code = {}  # code -> cache_rows

    for raw in lines:
        line = raw.strip()
        if not line:
            meta_lines.append(raw)
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            meta_lines.append(raw)
            continue
        if isinstance(obj, dict) and obj.get('_meta') is not None:
            meta_lines.append(line + '\n' if not line.endswith('\n') else line)
            continue
        if not isinstance(obj, dict) or 'code' not in obj:
            meta_lines.append(raw)
            continue
        total += 1
        code = (obj.get('code') or '').strip()
        if code not in cache_by_code:
            cache_by_code[code] = _load_cache_rows_for_code(code) if code else []
        if code:
            ok = enrich_one_record(obj, cache_by_code[code])
            if ok:
                enriched += 1
        data_objs.append(obj)

    # 同一交易日内按涨幅、振幅降序：match_date 升序，同日按 day1_change_pct 降序、day1_amplitude 降序、code 升序
    def _sort_key(o):
        d = o.get('match_date') or ''
        pct = o.get('day1_change_pct') if o.get('day1_change_pct') is not None else o.get('day2_change_pct')
        amp = o.get('day1_amplitude') if o.get('day1_amplitude') is not None else o.get('day2_amplitude')
        pct = pct if pct is not None else -9999
        amp = amp if amp is not None else -9999
        return (d, -float(pct), -float(amp), (o.get('code') or ''))
    data_objs.sort(key=_sort_key)

    out_lines = meta_lines + [json.dumps(obj, ensure_ascii=False) + '\n' for obj in data_objs]

    if not dry_run and out_lines:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.writelines(out_lines)
        except Exception as e:
            print(f"[WARN] 写回失败 {filepath}: {e}")
    return total, enriched


def main():
    parser = argparse.ArgumentParser(description='用缓存 K 线补全回测结果文件中的涨跌幅/振幅并写回')
    parser.add_argument('--file', type=str, default=None, help='仅处理此结果文件（如 龙头战法_结果.jsonl）')
    parser.add_argument('--dry-run', action='store_true', help='只统计可补全条数，不写回文件')
    args = parser.parse_args()

    if not os.path.isdir(CACHE_DIR):
        print(f"[ERROR] 缓存目录不存在: {CACHE_DIR}")
        sys.exit(1)
    if not os.path.isdir(RESULTS_DIR):
        print(f"[ERROR] 结果目录不存在: {RESULTS_DIR}")
        sys.exit(1)

    if args.file:
        filepath = os.path.join(RESULTS_DIR, args.file)
        if not os.path.isfile(filepath):
            print(f"[ERROR] 文件不存在: {filepath}")
            sys.exit(1)
        files = [filepath]
    else:
        files = [
            os.path.join(RESULTS_DIR, f)
            for f in os.listdir(RESULTS_DIR)
            if f.endswith('.jsonl') and os.path.isfile(os.path.join(RESULTS_DIR, f))
        ]
        files.sort()

    if not files:
        print('[INFO] 未找到任何 .jsonl 结果文件')
        return

    mode = '[DRY-RUN] ' if args.dry_run else ''
    total_all = 0
    enriched_all = 0
    for fp in files:
        total, enriched = process_file(fp, dry_run=args.dry_run)
        total_all += total
        enriched_all += enriched
        name = os.path.basename(fp)
        print(f"{mode}{name}: 数据行 {total}，补全 {enriched}")
    print(f"{mode}合计: 数据行 {total_all}，补全 {enriched_all}")
    if args.dry_run and enriched_all > 0:
        print('去掉 --dry-run 后将写回上述文件。')


if __name__ == '__main__':
    main()
