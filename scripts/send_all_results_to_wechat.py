#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性：把当前六个战法的所有结果文件，按表格形式发到企业微信机器人。

使用方式（在项目根目录）：
  source venv/bin/activate  # 建议
  python3 scripts/send_all_results_to_wechat.py
"""

import os
import json

# 复用定时任务里的配置和发送函数
from scheduled_task import _send_wechat_message, STRATEGY_NAMES


def _load_all_records(project_root, strategy_name):
    """读取指定策略结果文件中的所有记录（跳过 _meta 行）。"""
    results_file = os.path.join(project_root, 'results', f'{strategy_name}_结果.jsonl')
    records = []
    if not os.path.isfile(results_file):
        print(f"[INFO] 结果文件不存在，跳过: {results_file}")
        return records
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"[WARNING] 读取结果文件失败 {results_file}: {e}")
        return records

    if not lines:
        return records

    # 跳过首行 _meta
    try:
        first = json.loads(lines[0].strip())
        if isinstance(first, dict) and '_meta' in first:
            lines = lines[1:]
    except Exception:
        pass

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            if 'code' not in data:
                continue
            records.append(data)
        except Exception:
            continue
    return records


def _format_markdown_all(strategy_name, records):
    """格式化为企业微信 markdown 表格（包含所有历史记录）。"""
    title = f"【量化回测】{strategy_name}（全部历史结果）"
    if not records:
        return title + "\n\n暂无历史结果。"

    # 按 match_date 升序、代码排序，方便查看
    def _key(r):
        d = str(r.get('match_date') or '')
        if len(d) >= 10 and d[4] == '-' and d[7] == '-':
            d = d[:10]
        return (d, str(r.get('code') or ''))

    records_sorted = sorted(records, key=_key)

    lines = [
        title,
        "",
        "| 序号 | 匹配日 | 代码 | 名称 | 匹配价 | 当前价 | T+1涨跌幅 |",
        "|------|--------|------|------|--------|--------|-----------|",
    ]
    for idx, r in enumerate(records_sorted, 1):
        match_date = str(r.get('match_date') or '')
        if len(match_date) >= 10 and match_date[4] == '-' and match_date[7] == '-':
            match_date = match_date[:10]
        code = str(r.get('code', '') or '')
        name = str(r.get('name', '') or '')
        match_price = r.get('match_price')
        current_price = r.get('current_price')
        day1_change_pct = r.get('day1_change_pct')
        mp = f"{match_price:.2f}" if isinstance(match_price, (int, float)) else "-"
        cp = f"{current_price:.2f}" if isinstance(current_price, (int, float)) else "-"
        pct = f"{day1_change_pct:+.2f}%" if isinstance(day1_change_pct, (int, float)) else "-"
        lines.append(f"| {idx} | {match_date} | {code} | {name} | {mp} | {cp} | {pct} |")
    return "\n".join(lines)


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(f"项目根目录: {project_root}")

    for strategy_name in STRATEGY_NAMES:
        print(f"\n===== 策略：{strategy_name} =====")
        records = _load_all_records(project_root, strategy_name)
        print(f"共 {len(records)} 条记录，准备发送到企业微信机器人...")
        content = _format_markdown_all(strategy_name, records)
        _send_wechat_message(content)


if __name__ == '__main__':
    main()

