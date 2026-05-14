#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为回测结果 jsonl 增加「板块/概念联动」字段（含概念/行业当日涨幅名次）。全量回测落盘后
`strategy_engine`、按日 `incremental_backtest`、追加合并 `backtest_append_from_last` 均通过
`enrich_results_jsonl_after_backtest` 统一调用（并行 batch 时互斥）；也可手动跑本脚本。

**按 match_date 对齐（默认）**：结果行里有 `match_date` 且未 `--no-match-date` 时写入
`cache/sector_linkage/daily/`。其中 **概念** 默认用 **同花顺概念指数**（逐概念拉区间 K 算 T 日涨跌幅，较慢；`--concept-daily ths`）；
**行业** 默认用 **同花顺行业指数**（`--industry-daily ths`）；`--industry-daily eastmoney` 可改回东财行业日 K。`--concept-daily eastmoney` 可改回东财概念日 K（每板块只拉 T 日一根，快但易断连）。
成份股：概念名、行业名与东财「板块名称」**精确一致**时用东财成份。

`--source` 只影响「未做按日对齐」时的运行时刻强势榜（auto / sina）。

关闭按日对齐：`SECTOR_LINKAGE_MATCH_DATE=0` 或 `--no-match-date`。

用法（项目根目录）：
  python scripts/enrich_sector_linkage.py
  python scripts/enrich_sector_linkage.py --file 游资分歧转一致_结果.jsonl
  python scripts/enrich_sector_linkage.py --dry-run
  python scripts/enrich_sector_linkage.py --no-proxy
  python scripts/enrich_sector_linkage.py --source sina --no-match-date   # 全新浪、运行时刻榜
  python scripts/enrich_sector_linkage.py --out results/xxx_enriched.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from akshare_setup import configure_akshare_http

from sector_linkage import (
    build_board_membership_for_codes,
    clear_env_proxy,
    concept_hits_rank_pct_averages,
    extract_match_date_iso,
    format_linkage_text,
    linkage_offline_from_env,
    load_or_build_daily_board_snapshots,
    load_top_concept_boards,
    load_top_industry_boards,
    normalize_stock_code,
)

RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
LINKAGE_CACHE = os.path.join(PROJECT_ROOT, "cache", "sector_linkage")


def enrich_results_jsonl_inplace(
    filepath: str,
    *,
    out_path: str | None = None,
    top_concepts: int = 40,
    top_industries: int = 20,
    min_concept_pct: float | None = None,
    min_industry_pct: float | None = None,
    skip_industry: bool = False,
    cons_cache_days: float = 7.0,
    force_refresh_cons: bool = False,
    dry_run: bool = False,
    board_source: str = "auto",
    clear_proxy: bool = False,
    match_date_align: bool = True,
    hist_sleep_sec: float = 0.12,
    max_concept_hist_scan: int = 0,
    max_industry_hist_scan: int = 0,
    force_refresh_daily: bool = False,
    concept_daily: str = "ths",
    industry_daily: str = "ths",
) -> tuple[int, int]:
    """
    为单份结果 jsonl 原地写入板块/概念联动（含涨幅名次）。
    供 strategy_engine 回测落盘后调用；也可被 CLI 使用。
    """
    configure_akshare_http()
    if clear_proxy:
        clear_env_proxy(disable_getproxies=True)
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    meta_lines: list[str] = []
    data_objs: list[dict] = []
    for raw in lines:
        line = raw.rstrip("\n\r")
        if not line.strip():
            meta_lines.append(raw)
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            meta_lines.append(raw)
            continue
        if isinstance(obj, dict) and obj.get("_meta") is not None:
            meta_lines.append(raw)
            continue
        if not isinstance(obj, dict) or "code" not in obj:
            meta_lines.append(raw)
            continue
        data_objs.append(obj)

    codes_set = {
        normalize_stock_code(o.get("code"))
        for o in data_objs
        if normalize_stock_code(o.get("code"))
    }

    asof = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

    if not codes_set:
        return 0, 0

    os.makedirs(LINKAGE_CACHE, exist_ok=True)
    cons_concept = os.path.join(LINKAGE_CACHE, "concept_cons")
    cons_industry = os.path.join(LINKAGE_CACHE, "industry_cons")
    os.makedirs(cons_concept, exist_ok=True)
    os.makedirs(cons_industry, exist_ok=True)

    dates_sorted = sorted(
        {d for d in (extract_match_date_iso(o) for o in data_objs) if d}
    )
    use_match_dates = bool(match_date_align and dates_sorted)

    if not match_date_align and len(dates_sorted) > 1:
        print(
            f"[WARN] 板块/概念联动未按 T 日（match_date）对齐：当前为「运行时刻强势榜」口径，"
            f"文件中共有 {len(dates_sorted)} 个不同 match_date，各日将共用同一套涨跌幅排行。"
            f"若需 T 日指数涨跌幅：勿设 SECTOR_LINKAGE_MATCH_DATE=0，且勿使用 --no-match-date。",
            flush=True,
        )

    membership_by_date: dict[str, tuple[dict, dict]] = {}
    membership_c_legacy: dict = {}
    membership_i_legacy: dict = {}
    csrc = ""
    isrc = ""

    if use_match_dates:
        print(
            f"[INFO] {os.path.basename(filepath)} 板块联动按 match_date 对齐 "
            f"（{len(dates_sorted)} 个交易日）；概念={concept_daily}，行业={industry_daily}，缺失日写入 cache/sector_linkage/daily/",
            flush=True,
        )
        boards_by_date = load_or_build_daily_board_snapshots(
            LINKAGE_CACHE,
            dates_sorted,
            top_concepts=top_concepts,
            top_industries=top_industries,
            min_concept_pct=min_concept_pct,
            min_industry_pct=min_industry_pct,
            skip_industry=skip_industry,
            hist_sleep_sec=hist_sleep_sec,
            max_concept_scan=max_concept_hist_scan,
            max_industry_scan=max_industry_hist_scan,
            force_refresh_daily=force_refresh_daily,
            concept_daily=concept_daily,
            industry_daily=industry_daily,
        )
        for d, (tc, ti, tag) in boards_by_date.items():
            mc = build_board_membership_for_codes(
                tc,
                codes_set,
                "concept",
                cons_concept,
                cons_max_age_days=cons_cache_days,
                force_refresh_cons=force_refresh_cons,
            )
            if skip_industry or not ti:
                mi: dict = {c: [] for c in codes_set}
            else:
                mi = build_board_membership_for_codes(
                    ti,
                    codes_set,
                    "industry",
                    cons_industry,
                    cons_max_age_days=cons_cache_days,
                    force_refresh_cons=force_refresh_cons,
                )
            membership_by_date[d] = (mc, mi)
            csrc = tag
            isrc = tag if not skip_industry else "skipped"
        print(
            f"[INFO] 强势板块（按日）— 概念/行业构建标签:{csrc} 行业侧:{isrc}",
            flush=True,
        )
    else:
        top_con_rows, csrc = load_top_concept_boards(
            top_concepts, min_pct=min_concept_pct, source=board_source
        )
        if skip_industry:
            top_ind_rows, isrc = [], "skipped"
        else:
            top_ind_rows, isrc = load_top_industry_boards(
                top_industries, min_pct=min_industry_pct, source=board_source
            )
        print(
            f"[INFO] {os.path.basename(filepath)} 强势板块数据源（运行时刻快照）— 概念:{csrc} 行业:{isrc}",
            flush=True,
        )
        membership_c_legacy = build_board_membership_for_codes(
            top_con_rows,
            codes_set,
            "concept",
            cons_concept,
            cons_max_age_days=cons_cache_days,
            force_refresh_cons=force_refresh_cons,
        )
        if not skip_industry and top_ind_rows:
            membership_i_legacy = build_board_membership_for_codes(
                top_ind_rows,
                codes_set,
                "industry",
                cons_industry,
                cons_max_age_days=cons_cache_days,
                force_refresh_cons=force_refresh_cons,
            )

    touched = 0
    for obj in data_objs:
        code = normalize_stock_code(obj.get("code"))
        if not code:
            continue
        trade_d = extract_match_date_iso(obj) if use_match_dates else None
        if use_match_dates and trade_d and trade_d in membership_by_date:
            mc, mi = membership_by_date[trade_d]
            ch = sorted(mc.get(code, []), key=lambda x: (-x[1], x[2]))
            ih = sorted(mi.get(code, []), key=lambda x: (-x[1], x[2]))
            date_aligned = True
            board_trade_date = trade_d
        elif use_match_dates:
            ch = []
            ih = []
            date_aligned = False
            board_trade_date = None
        else:
            ch = sorted(
                membership_c_legacy.get(code, []),
                key=lambda x: (-x[1], x[2]),
            )
            ih = sorted(
                membership_i_legacy.get(code, []),
                key=lambda x: (-x[1], x[2]),
            )
            date_aligned = False
            board_trade_date = None

        industry_hit = (ih[0][0], ih[0][1], ih[0][2]) if ih else None
        text = format_linkage_text(
            ch,
            industry_hit,
            ranking_trade_date=board_trade_date if date_aligned and board_trade_date else None,
        )
        obj["linkage_text"] = text
        obj["linkage_concepts"] = [
            {"name": n, "pct": round(p, 2), "rank": int(rk)} for n, p, rk in ch
        ]
        ca = concept_hits_rank_pct_averages(ch)
        if ca is not None:
            obj["linkage_concept_rank_avg"] = round(float(ca[0]), 2)
            obj["linkage_concept_pct_avg"] = round(float(ca[1]), 2)
        else:
            obj["linkage_concept_rank_avg"] = None
            obj["linkage_concept_pct_avg"] = None
        obj["linkage_industry"] = industry_hit[0] if industry_hit else ""
        obj["linkage_industry_pct"] = (
            round(float(industry_hit[1]), 2) if industry_hit else None
        )
        obj["linkage_industry_rank"] = (
            int(industry_hit[2]) if industry_hit else None
        )
        obj["linkage_fetched_at"] = asof
        obj["linkage_date_aligned"] = date_aligned
        obj["linkage_board_trade_date"] = board_trade_date
        if text:
            touched += 1

    dest = out_path or filepath
    out_lines = meta_lines + [
        json.dumps(o, ensure_ascii=False) + "\n" for o in data_objs
    ]
    if not dry_run and out_lines:
        with open(dest, "w", encoding="utf-8") as f:
            f.writelines(out_lines)

    return len(data_objs), touched


# 并行 batch 多策略同时落盘 enrich 时互斥，避免 AkShare/HTTP 竞态导致整文件未写入 linkage_*
_POST_BACKTEST_ENRICH_LOCK = threading.Lock()


def results_path_eligible_for_sector_linkage_enrich(filepath: str) -> bool:
    if not filepath:
        return False
    bn = os.path.basename(filepath)
    if bn.startswith("多策略"):
        return False
    return bn.endswith("_结果.jsonl")


def enrich_results_jsonl_after_backtest(filepath: str) -> None:
    """
    任意策略、全量主文件或按日 `*_结果.jsonl` 写盘后的统一入口（含追加/合并）。
    新策略只要产出同名规则文件即自动适用；设 SKIP_SECTOR_LINKAGE_ENRICH=1 可跳过。
    """
    v = os.environ.get("SKIP_SECTOR_LINKAGE_ENRICH", "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return
    fp = os.path.abspath(os.path.expanduser(filepath or ""))
    if not fp or not os.path.isfile(fp):
        return
    if not results_path_eligible_for_sector_linkage_enrich(fp):
        return
    fn = os.path.basename(fp)
    with _POST_BACKTEST_ENRICH_LOCK:
        src = os.environ.get("SECTOR_LINKAGE_SOURCE", "auto").strip().lower()
        if src not in ("auto", "sina"):
            src = "auto"
        clear_px = os.environ.get("SECTOR_LINKAGE_CLEAR_PROXY", "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )

        def _match_date_align_from_env() -> bool:
            v = os.environ.get("SECTOR_LINKAGE_MATCH_DATE", "1").strip().lower()
            return v not in ("0", "false", "no", "off")

        def _hist_sleep_from_env() -> float:
            raw = os.environ.get("SECTOR_LINKAGE_HIST_SLEEP", "").strip()
            if not raw:
                return 0.12
            try:
                return max(0.0, float(raw))
            except ValueError:
                return 0.12

        def _concept_daily_from_env() -> str:
            v = os.environ.get("SECTOR_LINKAGE_CONCEPT_DAILY", "ths").strip().lower()
            return v if v in ("ths", "eastmoney") else "ths"

        def _industry_daily_from_env() -> str:
            v = os.environ.get("SECTOR_LINKAGE_INDUSTRY_DAILY", "ths").strip().lower()
            return v if v in ("ths", "eastmoney") else "ths"

        def _cons_cache_days_from_env() -> float:
            if linkage_offline_from_env():
                return 0.0
            raw = os.environ.get("SECTOR_LINKAGE_CONS_CACHE_DAYS", "").strip()
            if not raw:
                return 7.0
            try:
                return max(0.0, float(raw))
            except ValueError:
                return 7.0

        def _run(s: str, cp: bool) -> tuple[int, int]:
            return enrich_results_jsonl_inplace(
                fp,
                board_source=s,
                clear_proxy=cp,
                match_date_align=_match_date_align_from_env(),
                hist_sleep_sec=_hist_sleep_from_env(),
                concept_daily=_concept_daily_from_env(),
                industry_daily=_industry_daily_from_env(),
                cons_cache_days=_cons_cache_days_from_env(),
            )

        try:
            n, hit = _run(src, clear_px)
        except Exception as e:
            print(f"[WARN] 板块/概念联动 enrich 失败（{src}）: {e}", flush=True)
            if linkage_offline_from_env():
                print(
                    "[INFO] 当前 SECTOR_LINKAGE_OFFLINE=1，不会自动改用新浪重试；"
                    "请补全 cache/sector_linkage/daily/ 与 concept_cons / industry_cons 后再 enrich。",
                    flush=True,
                )
                return
            print(
                "[INFO] 若失败与按 match_date 拉板块数据有关：可试 --max-concept-scan、调大 SECTOR_LINKAGE_HIST_SLEEP；"
                "概念改东财可设 SECTOR_LINKAGE_CONCEPT_DAILY=eastmoney；行业改东财可设 SECTOR_LINKAGE_INDUSTRY_DAILY=eastmoney；"
                "或临时 SECTOR_LINKAGE_MATCH_DATE=0 / --no-match-date。",
                flush=True,
            )
            if src == "auto":
                try:
                    n, hit = _run("sina", clear_px)
                    print("[INFO] 已改用新浪重试板块联动", flush=True)
                except Exception as e2:
                    print(
                        f"[WARN] 新浪重试仍失败: {e2}；请手动: "
                        f"python scripts/enrich_sector_linkage.py --source sina --no-proxy --file {fn}",
                        flush=True,
                    )
                    return
            else:
                try:
                    n, hit = _run("sina", True)
                    print("[INFO] 已带清除代理用新浪重试板块联动", flush=True)
                except Exception as e2:
                    print(
                        f"[WARN] 板块/概念联动 enrich 仍失败: {e2}；请手动: "
                        f"python scripts/enrich_sector_linkage.py --source sina --no-proxy --file {fn}",
                        flush=True,
                    )
                    return
        print(
            f"[INFO] 板块/概念联动已写入 {hit}/{n} 条（有联动文案/数据行）",
            flush=True,
        )
        if hit == 0 and n > 0:
            print(
                f"[INFO] 本文件 {n} 条均无联动命中（或接口无数据）。可检查网络后执行: "
                f"python scripts/enrich_sector_linkage.py --source sina --no-proxy --file {fn}",
                flush=True,
            )


def main() -> None:
    os.chdir(PROJECT_ROOT)
    parser = argparse.ArgumentParser(description="为结果 jsonl 写入板块/概念联动字段")
    parser.add_argument("--file", type=str, default=None, help="仅处理该结果文件名（位于 results/）")
    parser.add_argument("--out", type=str, default=None, help="输出路径（默认覆盖原文件）")
    parser.add_argument("--top-concepts", type=int, default=40, help="强势概念板块数量（按涨跌幅）")
    parser.add_argument("--top-industries", type=int, default=20, help="强势行业板块数量")
    parser.add_argument(
        "--min-concept-pct",
        type=float,
        default=None,
        help="概念板块涨跌幅下限；不设则不过滤",
    )
    parser.add_argument(
        "--min-industry-pct",
        type=float,
        default=None,
        help="行业板块涨跌幅下限；不设则不过滤",
    )
    parser.add_argument("--skip-industry", action="store_true", help="不计算行业板块联动")
    parser.add_argument(
        "--cons-cache-days",
        type=float,
        default=7.0,
        help="板块成份缓存最长有效天数；0 表示每次重拉",
    )
    parser.add_argument("--refresh-cons", action="store_true", help="忽略成份缓存重新拉取")
    parser.add_argument(
        "--source",
        type=str,
        choices=("auto", "sina"),
        default="auto",
        help="未 --no-match-date 时无效；仅「运行时刻强势榜」：auto=东财 spot 优先再新浪，sina=仅新浪",
    )
    parser.add_argument("--no-proxy", action="store_true", help="清除环境变量中的代理再请求")
    parser.add_argument("--dry-run", action="store_true", help="不写文件，仍会请求网络")
    parser.add_argument(
        "--no-match-date",
        action="store_true",
        help="不按 match_date 对齐；使用运行时刻板块 spot 排行（与旧版一致）",
    )
    parser.add_argument(
        "--concept-daily",
        type=str,
        choices=("ths", "eastmoney"),
        default="ths",
        help="按 match_date 时概念涨跌幅来源：ths=同花顺概念指数（默认，较慢）；eastmoney=东财概念日K，每板块只拉 T 日一根",
    )
    parser.add_argument(
        "--industry-daily",
        type=str,
        choices=("ths", "eastmoney"),
        default="ths",
        help="按 match_date 时行业涨跌幅来源：ths=同花顺行业指数（默认）；eastmoney=东财行业日K",
    )
    parser.add_argument(
        "--refresh-daily-boards",
        action="store_true",
        help="忽略 cache/sector_linkage/daily/*.json，按当前 --concept-daily / --industry-daily 重建各交易日快照",
    )
    parser.add_argument(
        "--hist-sleep",
        type=float,
        default=0.12,
        help="按日拉板块接口时每次请求后的休眠秒数（同花顺概念指数与东财均适用），断连时可试 0.2～0.35",
    )
    parser.add_argument(
        "--max-concept-scan",
        type=int,
        default=0,
        help="按日构建时最多扫描的概念板块数；0 表示全量（同花顺/东财均可能较慢）",
    )
    parser.add_argument(
        "--max-industry-scan",
        type=int,
        default=0,
        help="按日构建时最多扫描的行业板块数，0 表示全量",
    )
    args = parser.parse_args()

    if args.no_proxy:
        clear_env_proxy(disable_getproxies=True)
    configure_akshare_http()  # CLI 再设一次无妨

    if not os.path.isdir(RESULTS_DIR):
        print(f"[ERROR] 结果目录不存在: {RESULTS_DIR}")
        sys.exit(1)

    if args.file:
        files = [os.path.join(RESULTS_DIR, args.file)]
        if not os.path.isfile(files[0]):
            print(f"[ERROR] 文件不存在: {files[0]}")
            sys.exit(1)
    else:
        files = [
            os.path.join(RESULTS_DIR, f)
            for f in os.listdir(RESULTS_DIR)
            if f.endswith(".jsonl") and os.path.isfile(os.path.join(RESULTS_DIR, f))
        ]
        files.sort()

    if not files:
        print("[INFO] 未找到 .jsonl 结果文件")
        return

    if args.out and len(files) > 1:
        print("[ERROR] 指定 --out 时请先使用 --file 只处理单个文件")
        sys.exit(1)

    out_arg = args.out
    if out_arg and not os.path.isabs(out_arg):
        out_arg = os.path.join(PROJECT_ROOT, out_arg)

    prefix = "[DRY-RUN] " if args.dry_run else ""
    total_rows = 0
    total_hit = 0
    try:
        for fp in files:
            out_single = out_arg if len(files) == 1 and out_arg else None
            n, hit = enrich_results_jsonl_inplace(
                fp,
                out_path=out_single,
                top_concepts=args.top_concepts,
                top_industries=args.top_industries,
                min_concept_pct=args.min_concept_pct,
                min_industry_pct=args.min_industry_pct,
                skip_industry=args.skip_industry,
                cons_cache_days=args.cons_cache_days,
                force_refresh_cons=args.refresh_cons,
                dry_run=args.dry_run,
                board_source=args.source,
                clear_proxy=False,
                match_date_align=not args.no_match_date,
                hist_sleep_sec=args.hist_sleep,
                max_concept_hist_scan=args.max_concept_scan,
                max_industry_hist_scan=args.max_industry_scan,
                force_refresh_daily=args.refresh_daily_boards,
                concept_daily=args.concept_daily,
                industry_daily=args.industry_daily,
            )
            total_rows += n
            total_hit += hit
            print(f"{prefix}{os.path.basename(fp)}: 数据行 {n}，有联动文案 {hit}")
    except Exception as e:  # noqa: BLE001
        print(f"[ERROR] 拉取板块/成份失败: {e}")
        print("  按 match_date 拉板块数据失败：可试 --no-proxy、--hist-sleep 0.25、--max-concept-scan 200；")
        print("  同花顺过慢可改东财：SECTOR_LINKAGE_CONCEPT_DAILY=eastmoney 或 --concept-daily eastmoney；"
              "行业改东财：SECTOR_LINKAGE_INDUSTRY_DAILY=eastmoney 或 --industry-daily eastmoney；或 --no-match-date。")
        sys.exit(1)
    print(f"{prefix}合计: 数据行 {total_rows}，有联动文案 {total_hit}")
    if args.dry_run:
        print("（dry-run 未写回文件；去掉 --dry-run 后写入）")


if __name__ == "__main__":
    main()
