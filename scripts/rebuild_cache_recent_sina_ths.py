#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
删除项目 cache/ 下数据后，在最近 N 个交易日窗口内：
  - 个股日 K：强制 DATA_FETCH_STOCK_HIST_SOURCE=sina（仅新浪 stock_zh_a_daily）
  - 概念 + 行业日快照：同花顺（sector_linkage.load_or_build_daily_board_snapshots）

默认会清空整个 cache/（可用 --keep-emotion 保留情绪周期 JSON）。
多进程分片只拉个股；全部分片完成后请再跑一次 --board-only。

示例：
  ./venv/bin/python scripts/rebuild_cache_recent_sina_ths.py --days 30 --workers 40
  ./venv/bin/python scripts/rebuild_cache_recent_sina_ths.py --task-index 0 --task-count 4 --workers 30
  ./venv/bin/python scripts/rebuild_cache_recent_sina_ths.py --board-only --days 30 --no-wipe

板块阶段偏慢属正常（同花顺概念全量约 375 次/日 × 间隔休眠）。可加速：
  --hist-sleep 0.06  --max-concept-scan 150  --max-industry-scan 60  或  --skip-industry

按交易日并行（须已 --board-only，例如近 30 日拆成 30 个进程各建 1 天 daily）：
  --board-shard-index i --board-shard-total 30  （i 从 0 到 29）
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CACHE_ROOT = os.path.join(PROJECT_ROOT, "cache")
LINKAGE_CACHE = os.path.join(CACHE_ROOT, "sector_linkage")


def wipe_cache_dir(cache_root: str, *, keep_emotion: bool, dry_run: bool) -> None:
    if not os.path.isdir(cache_root):
        return
    for name in sorted(os.listdir(cache_root)):
        if name.startswith("."):
            continue
        if keep_emotion and name.startswith("emotion_cycle"):
            continue
        path = os.path.join(cache_root, name)
        if dry_run:
            print(f"[dry-run] 将删除: {path}", flush=True)
            continue
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
        except OSError as e:
            print(f"[WARNING] 删除失败 {path}: {e}", flush=True)


def ensure_dirs() -> None:
    os.makedirs(os.path.join(CACHE_ROOT, "stock_data"), exist_ok=True)
    os.makedirs(os.path.join(LINKAGE_CACHE, "daily"), exist_ok=True)
    os.makedirs(os.path.join(LINKAGE_CACHE, "concept_cons"), exist_ok=True)
    os.makedirs(os.path.join(LINKAGE_CACHE, "industry_cons"), exist_ok=True)


def last_n_trade_dates(fetcher, n: int):
    import pandas as pd

    last_trade = fetcher._get_last_trading_day_available()
    last_dt = datetime.strptime(last_trade[:10], "%Y-%m-%d")
    span_days = max(n * 4 + 40, 200)
    start_cal = last_dt - timedelta(days=span_days)
    start_ymd = start_cal.strftime("%Y%m%d")
    end_ymd = last_trade.replace("-", "")
    df = fetcher.get_stock_data("000001", start_ymd, end_ymd, force_refresh=True)
    if df is None or df.empty:
        raise SystemExit("无法获取 000001 日 K 推断交易日，请检查网络或 AkShare")
    work = df.copy()
    work["日期"] = pd.to_datetime(work["日期"])
    u = sorted(work["日期"].dt.strftime("%Y-%m-%d").unique().tolist())
    tail = u[-n:] if len(u) >= n else u
    if len(tail) < n:
        print(
            f"[WARN] 请求 {n} 个交易日，000001 在窗口内仅 {len(u)} 根日 K，实际使用 {len(tail)} 日",
            flush=True,
        )
    return tail, start_ymd, end_ymd


def main() -> None:
    p = argparse.ArgumentParser(
        description="清空 cache 后按新浪日 K + 同花顺概念/行业重建近 N 交易日"
    )
    p.add_argument("--days", type=int, default=30, help="交易日数量（默认 30）")
    p.add_argument("--workers", type=int, default=40, help="个股拉取并发（默认 40）")
    p.add_argument(
        "--keep-emotion",
        action="store_true",
        help="保留 cache 下 emotion_cycle 开头的 json",
    )
    p.add_argument("--no-wipe", action="store_true", help="不删除 cache")
    p.add_argument("--dry-run", action="store_true", help="只打印将删除项，不删不写")
    p.add_argument("--skip-board", action="store_true", help="不构建 sector_linkage/daily")
    p.add_argument(
        "--board-only",
        action="store_true",
        help="仅按当前 cache 中的日 K 构建板块日线快照（不删 cache、不拉个股）",
    )
    p.add_argument("--task-index", type=int, default=None)
    p.add_argument("--task-count", type=int, default=None)
    p.add_argument(
        "--board-shard-index",
        type=int,
        default=None,
        help="与 --board-shard-total 同用：只构建「近 --days 根交易日」列表中的第几块（必须配合 --board-only）",
    )
    p.add_argument(
        "--board-shard-total",
        type=int,
        default=None,
        help="交易日分片总数；如与 --days 同为 30，则每进程约 1 个交易日 daily",
    )
    p.add_argument("--hist-sleep", type=float, default=0.15, help="THS 指数请求间隔（秒）；过小易被限流，可试 0.05～0.1")
    p.add_argument(
        "--max-concept-scan",
        type=int,
        default=0,
        help="同花顺概念最多扫描多少个板块再算涨跌幅排序；0=全量(约375步/日，最慢最稳)",
    )
    p.add_argument(
        "--max-industry-scan",
        type=int,
        default=0,
        help="同花顺行业最多扫描多少个；0=全量(约90步/日)",
    )
    p.add_argument(
        "--skip-industry",
        action="store_true",
        help="不构建行业侧日线快照（省掉每日约90次行业指数请求）",
    )
    p.add_argument("--top-concepts", type=int, default=40)
    p.add_argument("--top-industries", type=int, default=20)
    args = p.parse_args()

    if args.days < 1:
        p.error("--days 至少为 1")
    if args.workers < 1:
        p.error("--workers 至少为 1")
    if args.board_only and (args.task_count or args.task_index is not None):
        p.error("--board-only 不能与 --task-index/--task-count（个股分片）同时使用")

    if args.task_index is not None or args.task_count is not None:
        if args.board_shard_index is not None or args.board_shard_total is not None:
            p.error("不要同时使用个股分片（--task-index）与交易日分片（--board-shard-index）")
        if args.task_index is None or args.task_count is None or args.task_count < 1:
            p.error("--task-index 与 --task-count 需同时给出且 task_count>=1")
        if not (0 <= args.task_index < args.task_count):
            p.error("--task-index 必须在 [0, task_count)")

    if (args.board_shard_index is not None) ^ (args.board_shard_total is not None):
        p.error("--board-shard-index 与 --board-shard-total 须同时给出或同时省略")
    if args.board_shard_index is not None:
        if not args.board_only:
            p.error("按交易日分片仅支持配合 --board-only（请先拉完个股日 K，再并行建 daily）")
        if args.board_shard_total < 1 or not (0 <= args.board_shard_index < args.board_shard_total):
            p.error("--board-shard-total 须 >=1 且 0 <= --board-shard-index < --board-shard-total")

    os.environ.setdefault("NO_PROXY", "*")
    os.environ.setdefault("no_proxy", "*")
    os.environ["DATA_FETCH_STOCK_HIST_SOURCE"] = "sina"
    os.environ.setdefault("SECTOR_LINKAGE_SKIP_CONCEPT_WHEN_SYNCED", "0")

    board_only = args.board_only
    do_wipe = (not args.no_wipe) and (not board_only) and (not args.dry_run)
    if board_only:
        args.no_wipe = True

    if args.dry_run and not args.no_wipe:
        wipe_cache_dir(CACHE_ROOT, keep_emotion=args.keep_emotion, dry_run=True)
    elif do_wipe:
        print("[INFO] 正在清空 cache/ …", flush=True)
        wipe_cache_dir(CACHE_ROOT, keep_emotion=args.keep_emotion, dry_run=False)
        ensure_dirs()
    elif not board_only:
        print("[INFO] --no-wipe：保留已有 cache 文件", flush=True)

    if args.dry_run:
        print("[INFO] dry-run 结束（未拉数据）", flush=True)
        return

    from data_fetcher import DataFetcher
    from sector_linkage import load_or_build_daily_board_snapshots

    fetcher = DataFetcher()
    dates_iso, start_ymd, end_ymd = last_n_trade_dates(fetcher, args.days)
    if not dates_iso:
        raise SystemExit("交易日列表为空，请检查 000001 日 K 或增大脚本内日历窗口")

    n_dates_full = len(dates_iso)
    if args.board_shard_index is not None:
        m = args.board_shard_total
        chunk = (n_dates_full + m - 1) // m
        lo = args.board_shard_index * chunk
        hi = min(n_dates_full, lo + chunk)
        dates_iso = dates_iso[lo:hi]
        print(
            f"[INFO] 交易日分片 {args.board_shard_index}/{m}：全列表共 {n_dates_full} 日，"
            f"本进程处理下标 [{lo}:{hi}) 共 {len(dates_iso)} 日 → {dates_iso[0] if dates_iso else '(无)'} … "
            f"{dates_iso[-1] if dates_iso else ''}",
            flush=True,
        )
        if not dates_iso:
            print("[WARN] 本分片无交易日，退出", flush=True)
            return

    print(
        f"[INFO] 最近 {len(dates_iso)} 个交易日：{dates_iso[0]} ~ {dates_iso[-1]} "
        f"（000001 拉数窗口 {start_ymd}–{end_ymd}）",
        flush=True,
    )

    shard_board = args.task_count is not None and args.task_count > 1

    if not board_only:
        stocks = fetcher.get_stock_list()
        rows = sorted(stocks, key=lambda r: r["code"])
        if args.task_count is not None:
            n = len(rows)
            size = (n + args.task_count - 1) // args.task_count
            lo = args.task_index * size
            hi = min(n, lo + size)
            rows = rows[lo:hi]
            print(
                f"[INFO] 分片 task {args.task_index}/{args.task_count}，本进程股票数 {len(rows)}",
                flush=True,
            )

        codes = [r["code"] for r in rows]

        def pull_one(code: str) -> bool:
            try:
                df = fetcher.get_stock_data(code, start_ymd, end_ymd, force_refresh=True)
                return bool(df is not None and not df.empty)
            except Exception as e:
                print(f"[WARN] {code} 拉取失败: {e}", flush=True)
                return False

        print(
            f"[INFO] 拉取 {len(codes)} 只股票日 K（DATA_FETCH_STOCK_HIST_SOURCE=sina）…",
            flush=True,
        )
        ok = 0
        fail = 0
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futures = {ex.submit(pull_one, c): c for c in codes}
            for i, fu in enumerate(as_completed(futures), 1):
                if fu.result():
                    ok += 1
                else:
                    fail += 1
                if i % 200 == 0 or i == len(futures):
                    print(f"  进度 {i}/{len(codes)}，成功 {ok}，失败 {fail}", flush=True)
        print(f"[INFO] 个股完成：成功 {ok}/{len(codes)}，失败 {fail}", flush=True)

    if args.skip_board:
        print("[INFO] --skip-board：跳过板块/概念日线快照", flush=True)
        return

    if shard_board:
        print(
            "[INFO] 分片模式已跳过板块快照。全部股票分片跑完后请执行一次（会强制刷新 daily）：\n"
            f"  ./venv/bin/python scripts/rebuild_cache_recent_sina_ths.py "
            f"--board-only --days {args.days} --no-wipe "
            f"--hist-sleep {args.hist_sleep} --max-concept-scan {args.max_concept_scan} "
            f"--max-industry-scan {args.max_industry_scan}"
            + (" --skip-industry" if args.skip_industry else ""),
            flush=True,
        )
        return

    print(
        "[INFO] 构建同花顺概念"
        + ("+ 行业" if not args.skip_industry else "（已 --skip-industry）")
        + " 按日快照（force_refresh_daily=True）…",
        flush=True,
    )
    load_or_build_daily_board_snapshots(
        LINKAGE_CACHE,
        dates_iso,
        top_concepts=args.top_concepts,
        top_industries=args.top_industries,
        min_concept_pct=None,
        min_industry_pct=None,
        skip_industry=args.skip_industry,
        hist_sleep_sec=args.hist_sleep,
        max_concept_scan=max(0, int(args.max_concept_scan)),
        max_industry_scan=max(0, int(args.max_industry_scan)),
        force_refresh_daily=True,
        concept_daily="ths",
        industry_daily="ths",
    )
    print("[INFO] 全部完成。", flush=True)


if __name__ == "__main__":
    main()
