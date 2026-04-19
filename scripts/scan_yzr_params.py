#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
游资三件套参数扫描脚本。

目标：
1. 扫描三套游资策略（首板试错 / 分歧转一致 / 二波加速）的关键参数组合
2. 计算每组参数的核心表现（样本数、次日胜率、次日均收益、第三日均收益）
3. 输出终端排行榜，并将完整结果写入 results/param_scan_*.json

使用示例（在项目根目录）：
  python3 scripts/scan_yzr_params.py
  python3 scripts/scan_yzr_params.py --workers 30 --top 12
  python3 scripts/scan_yzr_params.py --family "游资分歧转一致"
  python3 scripts/scan_yzr_params.py --promote-top 3
"""

import os
import sys
import json
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def safe_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def evaluate_rows(rows):
    """从回测结果行计算统计指标。"""
    d2_values = []
    d3_values = []
    d2_win = 0
    d3_win = 0

    for row in rows:
        d2 = safe_float(row.get("day2_change_pct"))
        d3 = safe_float(row.get("day3_change_pct"))
        if d2 is not None:
            d2_values.append(d2)
            if d2 > 0:
                d2_win += 1
        if d3 is not None:
            d3_values.append(d3)
            if d3 > 0:
                d3_win += 1

    d2_count = len(d2_values)
    d3_count = len(d3_values)
    d2_avg = round(sum(d2_values) / d2_count, 4) if d2_count else None
    d3_avg = round(sum(d3_values) / d3_count, 4) if d3_count else None
    d2_win_rate = round(d2_win / d2_count * 100, 2) if d2_count else None
    d3_win_rate = round(d3_win / d3_count * 100, 2) if d3_count else None

    return {
        "sample_size": len(rows),
        "day2_samples": d2_count,
        "day2_win_rate_pct": d2_win_rate,
        "day2_avg_change_pct": d2_avg,
        "day3_samples": d3_count,
        "day3_win_rate_pct": d3_win_rate,
        "day3_avg_change_pct": d3_avg,
    }


def build_family_configs():
    """定义三件套参数网格。"""
    families = []

    # 一、游资首板试错
    family_name = "游资首板试错"
    amount_values = [300_000_000, 500_000_000]
    volume_values = [1.1, 1.2, 1.3]
    for amount in amount_values:
        for vol in volume_values:
            params = {
                "avg_amount_gte": amount,
                "volume_ratio": vol,
            }
            strategy = {
                "conditions": [
                    {"type": "listed_days_gte", "date1": 0, "days": 120},
                    {"type": "avg_amount_gte", "date1": 0, "days": 20, "value": amount},
                    {"type": "pct_change_lt", "date1": -1, "value": 9.8},
                    {"type": "limit_up", "date1": 0},
                    {"type": "volume_ratio", "date1": 0, "date2": -1, "ratio": vol},
                ],
                "exclude": {"kcb": True, "cyb": True, "bjs": True, "st": True, "delist": True},
                "timeRange": 120,
            }
            families.append({"family": family_name, "params": params, "strategy": strategy})

    # 二、游资分歧转一致
    family_name = "游资分歧转一致"
    amount_values = [500_000_000, 800_000_000]
    pullback_ranges = [(-6, 2), (-5, 3), (-4, 4)]
    volume_values = [1.2, 1.3, 1.5]
    for amount in amount_values:
        for pullback_min, pullback_max in pullback_ranges:
            for vol in volume_values:
                params = {
                    "avg_amount_gte": amount,
                    "pullback_min": pullback_min,
                    "pullback_max": pullback_max,
                    "volume_ratio": vol,
                }
                strategy = {
                    "conditions": [
                        {"type": "listed_days_gte", "date1": 0, "days": 120},
                        {"type": "avg_amount_gte", "date1": 0, "days": 20, "value": amount},
                        {"type": "recent_limit_up", "date1": -1, "days": 10},
                        {"type": "pct_change_between", "date1": -1, "minValue": pullback_min, "maxValue": pullback_max},
                        {"type": "limit_up", "date1": 0},
                        {"type": "volume_ratio", "date1": 0, "date2": -1, "ratio": vol},
                    ],
                    "exclude": {"kcb": True, "cyb": True, "bjs": True, "st": True, "delist": True},
                    "timeRange": 120,
                }
                families.append({"family": family_name, "params": params, "strategy": strategy})

    # 三、游资二波加速
    family_name = "游资二波加速"
    amount_values = [500_000_000, 800_000_000]
    three_board_days_values = [20, 30, 45]
    volume_values = [1.1, 1.2, 1.3]
    for amount in amount_values:
        for days in three_board_days_values:
            for vol in volume_values:
                params = {
                    "avg_amount_gte": amount,
                    "three_limit_up_days": days,
                    "volume_ratio": vol,
                }
                strategy = {
                    "conditions": [
                        {"type": "listed_days_gte", "date1": 0, "days": 120},
                        {"type": "avg_amount_gte", "date1": 0, "days": 20, "value": amount},
                        {"type": "three_limit_up", "date1": -1, "days": days},
                        {"type": "pct_change_lt", "date1": -1, "value": 9.8},
                        {"type": "limit_up", "date1": 0},
                        {"type": "volume_ratio", "date1": 0, "date2": -1, "ratio": vol},
                    ],
                    "exclude": {"kcb": True, "cyb": True, "bjs": True, "st": True, "delist": True},
                    "timeRange": 120,
                }
                families.append({"family": family_name, "params": params, "strategy": strategy})

    return families


def rank_key(item):
    """排序：先次日胜率，再次日均收益，再样本数。"""
    metrics = item.get("metrics", {})
    d2_wr = metrics.get("day2_win_rate_pct")
    d2_avg = metrics.get("day2_avg_change_pct")
    size = metrics.get("sample_size", 0)
    d2_wr = d2_wr if d2_wr is not None else -1
    d2_avg = d2_avg if d2_avg is not None else -999
    return (d2_wr, d2_avg, size)


def format_param_text(params):
    parts = []
    for k, v in params.items():
        parts.append(f"{k}={v}")
    return ", ".join(parts)


def load_common_strategies():
    path = os.path.join(PROJECT_ROOT, "common_strategies.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "strategies" not in data or not isinstance(data["strategies"], list):
        data["strategies"] = []
    return path, data


def promote_top_to_common_strategies(ranked, top_n, source_note):
    """将扫描结果前N名回写到 common_strategies.json。"""
    if top_n <= 0 or not ranked:
        return 0, None

    file_path, data = load_common_strategies()
    strategies = data["strategies"]
    now = datetime.now().strftime("%Y%m%d")
    added = 0

    for idx, item in enumerate(ranked[:top_n], start=1):
        family = item["family"]
        params = item["params"]
        strategy = item["strategy"]
        metrics = item["metrics"]
        name = f"{family}-参数优选{idx}-{now}"
        description = (
            f"参数扫描优选策略（{source_note}）。"
            f"参数: {format_param_text(params)}；"
            f"样本={metrics.get('sample_size')}，次日胜率={metrics.get('day2_win_rate_pct')}%，"
            f"次日均收益={metrics.get('day2_avg_change_pct')}%。"
        )
        strategies.append(
            {
                "name": name,
                "description": description,
                "strategy": strategy,
            }
        )
        added += 1

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return added, file_path


def main():
    parser = argparse.ArgumentParser(description="游资三件套参数扫描")
    parser.add_argument("--workers", type=int, default=50, help="回测并发线程数（默认 50）")
    parser.add_argument("--top", type=int, default=10, help="终端展示前N名（默认 10）")
    parser.add_argument(
        "--family",
        type=str,
        default=None,
        choices=["游资首板试错", "游资分歧转一致", "游资二波加速"],
        help="只扫描指定策略家族（可选）",
    )
    parser.add_argument(
        "--promote-top",
        type=int,
        default=0,
        help="将全局前N名自动追加到 common_strategies.json（默认0=不回写）",
    )
    args = parser.parse_args()

    from data_fetcher import DataFetcher
    from strategy_engine import StrategyEngine

    fetcher = DataFetcher()
    engine = StrategyEngine(fetcher, max_workers=args.workers)

    all_configs = build_family_configs()
    if args.family:
        all_configs = [cfg for cfg in all_configs if cfg["family"] == args.family]

    if not all_configs:
        print("[ERROR] 没有可扫描的参数组合")
        sys.exit(1)

    print("=" * 80)
    print("游资三件套参数扫描")
    print("=" * 80)
    print(f"组合总数: {len(all_configs)}")
    print(f"并发线程: {args.workers}")
    if args.family:
        print(f"策略家族: {args.family}")
    print()

    results = []
    for idx, cfg in enumerate(all_configs, start=1):
        family = cfg["family"]
        params = cfg["params"]
        strategy = cfg["strategy"]
        strategy_name = f"{family}_参数扫描_{idx:03d}"
        print(f"[{idx}/{len(all_configs)}] 扫描: {family} | 参数: {params}", flush=True)
        rows = engine._backtest_impl(strategy, strategy_name=strategy_name, only_t_date=None, write_results=False)
        metrics = evaluate_rows(rows)
        results.append(
            {
                "family": family,
                "strategy_name": strategy_name,
                "params": params,
                "strategy": strategy,
                "metrics": metrics,
            }
        )
        print(
            f"  -> 样本={metrics['sample_size']}, 次日胜率={metrics['day2_win_rate_pct']}%, 次日均收益={metrics['day2_avg_change_pct']}%",
            flush=True,
        )

    # 全局榜单
    ranked = sorted(results, key=rank_key, reverse=True)
    print("\n" + "=" * 80)
    print(f"全局TOP {min(args.top, len(ranked))}")
    print("=" * 80)
    for i, item in enumerate(ranked[: args.top], start=1):
        m = item["metrics"]
        print(
            f"{i:>2}. {item['family']} | 参数={item['params']} | 样本={m['sample_size']} | 次日胜率={m['day2_win_rate_pct']}% | 次日均收益={m['day2_avg_change_pct']}% | 第三日均收益={m['day3_avg_change_pct']}%"
        )

    # 分家族榜单
    families = sorted({x["family"] for x in results})
    for fam in families:
        fam_rows = [x for x in ranked if x["family"] == fam]
        print("\n" + "-" * 80)
        print(f"{fam} TOP {min(args.top, len(fam_rows))}")
        print("-" * 80)
        for i, item in enumerate(fam_rows[: args.top], start=1):
            m = item["metrics"]
            print(
                f"{i:>2}. 参数={item['params']} | 样本={m['sample_size']} | 次日胜率={m['day2_win_rate_pct']}% | 次日均收益={m['day2_avg_change_pct']}% | 第三日均收益={m['day3_avg_change_pct']}%"
            )

    output = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "workers": args.workers,
        "top": args.top,
        "family_filter": args.family,
        "total_combinations": len(results),
        "ranked_results": ranked,
    }
    out_dir = os.path.join(PROJECT_ROOT, "results")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"param_scan_yzr_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("\n结果已保存:", out_file)

    if args.promote_top > 0:
        source_note = f"scan_yzr_params.py {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        added, target_file = promote_top_to_common_strategies(ranked, args.promote_top, source_note)
        print(f"已回写参数优选策略 {added} 条 -> {target_file}")


if __name__ == "__main__":
    main()

