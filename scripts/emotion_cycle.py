#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""情绪周期判断脚本（命令行展示版）。"""

import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from emotion_cycle_service import analyze_emotion_cycle


def parse_args():
    parser = argparse.ArgumentParser(description="情绪周期判断器")
    parser.add_argument(
        "--force",
        action="store_true",
        help="忽略情绪周期滚动缓存，重新全量合并股票日线缓存",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        report = analyze_emotion_cycle(force_refresh=args.force)
    except Exception as e:
        print(f"[ERROR] {e}")
        return

    market = report["market_metrics"]
    scores = report["scores"]
    cycle = report["cycle"]

    print("=" * 80)
    print("情绪周期诊断")
    print("=" * 80)
    print(f"交易日: {report['date']}")
    print(f"市场样本数: {market['total']}")
    print()
    print("[市场面]")
    print(f"- 涨停数: {market['limit_up_count']} ({market['limit_up_ratio_pct']}%)")
    print(f"- 强势数(>=5%): {market['strong_count']} ({market['strong_ratio_pct']}%)")
    print(f"- 大跌数(<=-5%): {market['big_drop_count']} ({market['big_drop_ratio_pct']}%)")
    print(f"- 平均涨跌幅: {market['avg_pct_change']}%")
    print(f"- 市场情绪分（=综合分）: {scores['market_score']}")
    print()
    print("[结论]")
    print(f"- 综合情绪分: {scores['total_score']}")
    print(f"- 当前周期: {cycle}")
    print()
    print("建议:")
    if cycle in ("冰点", "弱修复"):
        print("- 以防守和小仓试错为主，优先看 `游资策略包-弱势`。")
    elif cycle == "中性震荡":
        print("- 做标准接力，控制节奏，优先看 `游资策略包-中性`。")
    else:
        print("- 可提升进攻性，关注龙头二波，优先看 `游资策略包-强势`。")


if __name__ == "__main__":
    main()
