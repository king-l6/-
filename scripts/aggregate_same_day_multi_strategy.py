#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
读取 results/ 下各策略「*_结果.jsonl」，按 match_date 汇总「同一天、多策略同时命中」的股票。

模式（--mode）：
  overlap（默认）：某日某股在「至少 N 个策略」的结果里同时出现（默认 N=2），
    输出命中了哪几个策略；同一交易日内按「命中策略数」从多到少排序。
  intersection：某日某股在「全部」参与策略里都有命中（少见于策略很多时）。
  union：某日任意策略命中的股票，可按 --min-strategies 过滤。

输出：jsonl，首行 _meta（含字段说明），后续每行含：
  match_date, code, name；
  overlap_strategies / overlap_strategies_text / overlap_summary — 明确写出「重叠的是哪几个策略」；
  strategies / strategies_joined / strategy_count — 与 overlap_* 同义，便于旧代码兼容。

示例：
  python3 scripts/aggregate_same_day_multi_strategy.py
  python3 scripts/aggregate_same_day_multi_strategy.py --min-strategies 3
  python3 scripts/aggregate_same_day_multi_strategy.py --mode intersection
  python3 scripts/aggregate_same_day_multi_strategy.py --mode union --min-strategies 1
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Set, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _norm_date(s: Any) -> str:
    t = str(s or "").strip()
    if len(t) >= 10 and t[4] == "-" and t[7] == "-":
        return t[:10]
    return ""


def _iter_rows(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(o, dict):
                continue
            if o.get("_meta") is not None and "code" not in o:
                continue
            if "code" not in o:
                continue
            yield o


LINKAGE_FIELD_KEYS = (
    "linkage_text",
    "linkage_concepts",
    "linkage_concept_rank_avg",
    "linkage_concept_pct_avg",
    "linkage_industry",
    "linkage_industry_pct",
    "linkage_industry_rank",
    "linkage_fetched_at",
)


def _linkage_slice(row: Dict[str, Any]) -> Dict[str, Any]:
    """从策略结果行取出联动相关字段（若有）。"""
    return {k: row[k] for k in LINKAGE_FIELD_KEYS if k in row}


def _linkage_score(payload: Dict[str, Any]) -> int:
    lc = payload.get("linkage_concepts")
    if isinstance(lc, list):
        return len(lc)
    t = str(payload.get("linkage_text") or "").strip()
    return 1 if t else 0


def _overlap_row_fields(match_date: str, st_sorted: List[str]) -> Dict[str, Any]:
    """人类可读：重叠的是哪几个策略。"""
    n = len(st_sorted)
    joined = "、".join(st_sorted)
    return {
        "overlap_strategies": st_sorted,
        "overlap_strategies_text": joined,
        "overlap_summary": f"{match_date} 该股在以下 {n} 个策略的结果中同日均有命中：{joined}",
    }


def _discover_strategy_files(
    results_dir: str,
    exclude_basenames: Set[str],
) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for fn in sorted(os.listdir(results_dir)):
        if not fn.endswith("_结果.jsonl"):
            continue
        if fn in exclude_basenames:
            continue
        base = fn[: -len("_结果.jsonl")]
        if base.startswith("多策略"):
            continue
        fp = os.path.join(results_dir, fn)
        if os.path.isfile(fp):
            out.append((base, fp))
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="按日聚合多策略结果：同日多策略重叠 / 全交集 / 并集")
    p.add_argument(
        "--results-dir",
        default=os.path.join(PROJECT_ROOT, "results"),
        help="结果目录（默认项目 results/）",
    )
    p.add_argument(
        "--mode",
        choices=("overlap", "intersection", "union"),
        default="overlap",
        help="overlap=至少 N 个策略同日命中（默认 N=2），日内按策略数降序；intersection=全部策略均命中；union=并集",
    )
    p.add_argument(
        "--strategies",
        nargs="*",
        default=None,
        help="只使用这些策略名；默认目录下全部 *_结果.jsonl（排除多策略*）",
    )
    p.add_argument(
        "--min-strategies",
        type=int,
        default=None,
        help="overlap/union：至少命中多少个策略才输出（overlap 默认 2；union 默认 1；可显式覆盖）",
    )
    p.add_argument(
        "--out",
        default=None,
        help="输出 jsonl；默认 results/多策略同日_重叠.jsonl / _交集.jsonl / _并集.jsonl",
    )
    args = p.parse_args()

    results_dir = os.path.abspath(args.results_dir)
    if not os.path.isdir(results_dir):
        print(f"[ERROR] 目录不存在: {results_dir}", file=sys.stderr)
        sys.exit(1)

    if args.min_strategies is None:
        min_n = 2 if args.mode == "overlap" else 1
    else:
        min_n = max(1, int(args.min_strategies))

    out_path = args.out
    if not out_path:
        suf = {"overlap": "重叠", "intersection": "交集", "union": "并集"}[args.mode]
        out_path = os.path.join(results_dir, f"多策略同日_{suf}.jsonl")
    out_path = os.path.abspath(out_path)
    out_basename = os.path.basename(out_path)

    exclude = {out_basename} if out_basename.endswith(".jsonl") else set()
    files = _discover_strategy_files(results_dir, exclude_basenames=exclude)
    if args.strategies:
        want = set(args.strategies)
        files = [(n, p) for n, p in files if n in want]
        missing = want - {n for n, _ in files}
        if missing:
            print(f"[WARN] 未找到结果文件: {', '.join(sorted(missing))}", file=sys.stderr)

    if len(files) < 1:
        print("[ERROR] 未找到任何策略结果文件（*_结果.jsonl）", file=sys.stderr)
        sys.exit(1)

    strategy_names = [n for n, _ in files]

    by_s_d: Dict[str, Dict[str, Set[str]]] = {n: defaultdict(set) for n in strategy_names}
    name_for: Dict[Tuple[str, str], str] = {}
    strat_for: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    linkage_for: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for sname, path in files:
        for row in _iter_rows(path):
            d = _norm_date(row.get("match_date"))
            code = str(row.get("code", "")).strip()
            if not d or not code:
                continue
            by_s_d[sname][d].add(code)
            key = (d, code)
            if key not in name_for:
                nm = (row.get("name") or "").strip()
                name_for[key] = nm if nm else code
            strat_for[key].add(sname)
            lp = _linkage_slice(row)
            if not lp:
                continue
            prev = linkage_for.get(key)
            if prev is None or _linkage_score(lp) > _linkage_score(prev):
                linkage_for[key] = lp

    all_dates: Set[str] = set()
    for m in by_s_d.values():
        all_dates.update(m.keys())
    all_dates_sorted = sorted(all_dates)

    out_rows: List[Dict[str, Any]] = []

    if args.mode == "intersection":
        for d in all_dates_sorted:
            sets = [by_s_d[s].get(d, set()) for s in strategy_names]
            if any(len(x) == 0 for x in sets):
                continue
            inter = set.intersection(*sets)
            for code in sorted(inter):
                key = (d, code)
                st_list = list(strategy_names)
                base = {
                    "match_date": d,
                    "code": code,
                    "name": name_for.get(key, code),
                    "strategies": st_list,
                    "strategy_count": len(st_list),
                    "strategies_joined": "、".join(st_list),
                }
                base.update(_overlap_row_fields(d, st_list))
                base.update(linkage_for.get(key, {}))
                out_rows.append(base)
    else:
        # overlap 与 union：按 (日, 股) 聚合策略集合，再按 min_n 过滤
        for d in all_dates_sorted:
            codes_on_d: Set[str] = set()
            for s in strategy_names:
                codes_on_d |= by_s_d[s].get(d, set())
            for code in codes_on_d:
                key = (d, code)
                st = strat_for.get(key, set())
                if len(st) < min_n:
                    continue
                st_sorted = sorted(st)
                base = {
                    "match_date": d,
                    "code": code,
                    "name": name_for.get(key, code),
                    "strategies": st_sorted,
                    "strategy_count": len(st_sorted),
                    "strategies_joined": "、".join(st_sorted),
                }
                base.update(_overlap_row_fields(d, st_sorted))
                base.update(linkage_for.get(key, {}))
                out_rows.append(base)

    # 同一交易日内：命中策略数多的在前；再按代码升序
    out_rows.sort(key=lambda r: (r["match_date"], -r["strategy_count"], r["code"]))

    meta = {
        "_meta": {
            "kind": "multi_strategy_same_day",
            "mode": args.mode,
            "strategies_scanned": strategy_names,
            "note": "每行「重叠」指：同一 match_date 下该 code 同时出现在 overlap_strategies 所列各策略的 *_结果.jsonl 中。",
            "row_fields": {
                "overlap_strategies": "发生重叠的策略名列表（与 strategies 相同）",
                "overlap_strategies_text": "顿号拼接，便于粘贴",
                "overlap_summary": "一句话说明是哪些策略在同日命中该股",
                "strategy_count": "overlap_strategies 的长度",
                "linkage_*": "从各策略 *_结果.jsonl 合并的板块联动（优先概念条数多的那条）；含 linkage_concept_rank_avg / linkage_concept_pct_avg 时一并合并",
            },
            "min_strategies": min_n if args.mode != "intersection" else len(strategy_names),
            "sort_within_day": "strategy_count_desc_then_code_asc",
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "count": len(out_rows),
            "output": os.path.relpath(out_path, PROJECT_ROOT),
        }
    }

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(meta, ensure_ascii=False) + "\n")
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(
        f"[OK] 模式={args.mode} min_strategies={min_n} 扫描策略数={len(strategy_names)} "
        f"输出行数={len(out_rows)} → {os.path.relpath(out_path, PROJECT_ROOT)}"
    )


if __name__ == "__main__":
    main()
