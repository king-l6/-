#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 match_date、code 对 results 下 *_结果.jsonl 就地排序（保留首行 _meta，更新 count）。"""
import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _norm_date(value):
    if value is None:
        return ''
    s = str(value).strip()
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        return s[:10]
    return s


def sort_file(filepath):
    if not os.path.isfile(filepath):
        print(f'[ERROR] 文件不存在: {filepath}')
        return 1
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    if not lines:
        print(f'[WARN] 空文件: {filepath}')
        return 0
    meta_line = lines[0].strip()
    meta = None
    try:
        first = json.loads(meta_line)
        if '_meta' in first:
            meta = first['_meta']
            body = lines[1:]
        else:
            body = lines
    except json.JSONDecodeError:
        body = lines

    rows = []
    for line in body:
        line = line.strip()
        if not line:
            continue
        try:
            o = json.loads(line)
        except json.JSONDecodeError:
            continue
        if 'code' not in o:
            continue
        rows.append(o)

    def key_fn(r):
        md = _norm_date(r.get('match_date')) or '9999-99-99'
        return (md, str(r.get('code', '')))

    rows.sort(key=key_fn)

    with open(filepath, 'w', encoding='utf-8') as f:
        if meta is not None:
            meta['count'] = len(rows)
            meta['sorted_at'] = __import__('datetime').datetime.now().isoformat()
            f.write(json.dumps({'_meta': meta}, ensure_ascii=False, default=str) + '\n')
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')

    print(f'[OK] {filepath} 已按日期+代码排序，共 {len(rows)} 条')
    return 0


def main():
    if len(sys.argv) < 2:
        print('用法: python3 scripts/sort_results_jsonl.py <results/xxx_结果.jsonl> [更多文件...]')
        print('示例: python3 scripts/sort_results_jsonl.py results/主力建仓_结果.jsonl')
        return 1
    code = 0
    for p in sys.argv[1:]:
        path = p if os.path.isabs(p) else os.path.join(PROJECT_ROOT, p)
        code = sort_file(path) or code
    return code


if __name__ == '__main__':
    sys.exit(main())
