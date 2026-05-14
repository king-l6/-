#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘中约 14:30：拉新浪全 A 快照 stock_zh_a_spot，落盘后以快照行并入本地日 K，
对「主力建仓」策略仅扫 T=指定日（默认今天）选股。

说明（与 DATA_SOURCE.md 一致）：
- 快照为请求时刻截面，不等同收盘后正式日 K；合并后 MA/主力建仓逻辑与日线引擎一致，但 T 日量价实为盘中近似。
- 股票池与项目一致：主板 + stock_code_utils.universe_exclusion_reason；北交所/科创/创业等已在 spot 行上过滤。
- **日 K 默认只读本地**：与「快照当 T 日收盘近似 + 历史缓存」一致；除 **1 次** `stock_zh_a_spot` 外，**不为日 K 打东财/新浪补数**。缺本地 K 的股票会跳过（无静默拉网）。若确需补数：`--allow-fetch-k`。
- 落盘后会自动调用 `enrich_sector_linkage.enrich_results_jsonl_after_backtest` 写入 `linkage_*`（与全量回测一致）；板块 enrich 仍会请求东财/新浪，不需要可设 `SKIP_SECTOR_LINKAGE_ENRICH=1`。

用法（项目根）：
  python scripts/intraday_snapshot_main_force.py
  python scripts/intraday_snapshot_main_force.py --date 2026-05-08
  python scripts/intraday_snapshot_main_force.py --skip-ensure   # 与默认叠加时仍跳过 ensure（默认已不拉日 K 网）
  python scripts/intraday_snapshot_main_force.py --allow-fetch-k  # 本地 K 不足时允许走接口补数（旧行为）

注意：
- 新浪 `stock_zh_a_spot` 返回的「名称」有时字间带空格（如「金 螳 螂」），全文搜索「金螳螂」可能搜不到，请用代码 **002081** 或 **sz002081** 检索。
- **并发不宜过大**：AkShare 东财 K 线路径依赖 mini_racer，多线程易触发 macOS 上 Python 意外退出（SIGTRAP）。建议 **--workers 4～8**；默认已取保守值。
"""

import argparse
import json
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("no_proxy", "*")


def _spot_code_to_six(raw: str) -> str | None:
    s = (raw or "").strip().lower()
    if s.startswith("sh") and len(s) >= 8:
        return s[2:8]
    if s.startswith("sz") and len(s) >= 8:
        return s[2:8]
    return None


def fetch_sina_spot_dataframe():
    from akshare_setup import configure_akshare_http

    configure_akshare_http()
    import akshare as ak

    return ak.stock_zh_a_spot()


def build_spot_by_code(df_spot):
    from stock_code_utils import universe_exclusion_reason

    out = {}
    if df_spot is None or getattr(df_spot, "empty", True):
        return out
    for _, row in df_spot.iterrows():
        code = _spot_code_to_six(str(row.get("代码") or ""))
        if not code or len(code) != 6 or not code.isdigit():
            continue
        name = str(row.get("名称") or "").strip()
        ex = "sh" if code.startswith("60") else "sz"
        if universe_exclusion_reason(code, name, exchange=ex) is not None:
            continue
        out[code] = row.to_dict()
    return out


def load_main_force_strategy(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("strategies", []):
        if item.get("name") == "主力建仓":
            return item["name"], item["strategy"]
    raise SystemExit("[ERROR] common_strategies.json 中未找到「主力建仓」")


def save_snapshot(df_spot, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    records = []
    if df_spot is not None and not df_spot.empty:
        records = json.loads(df_spot.to_json(orient="records", force_ascii=False))
    meta = {
        "_meta": {
            "source": "akshare.stock_zh_a_spot",
            "saved_at": datetime.now().isoformat(),
            "rows": len(records),
        }
    }
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(meta, ensure_ascii=False) + "\n")
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def write_results_jsonl(results_dir: str, strategy_name: str, trading_date: str, results: list, note: str) -> str | None:
    safe = strategy_name.replace("/", "-")
    fn = f"{safe}_intraday_{trading_date.replace('-', '')}_{datetime.now().strftime('%H%M%S')}_结果.jsonl"
    path = os.path.join(results_dir, fn)
    try:
        os.makedirs(results_dir, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            meta = {
                "_meta": {
                    "strategy_name": strategy_name,
                    "run_at": datetime.now().isoformat(),
                    "match_date": trading_date,
                    "count": len(results),
                    "intraday_spot": True,
                    "note": note,
                }
            }
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for r in results:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
        return path
    except OSError as e:
        print(f"[WARN] 写入结果失败: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="新浪全市场快照 + 主力建仓（并入当日 K）选股")
    parser.add_argument("--date", default=None, help="T 日 YYYY-MM-DD，默认今天（日历日）")
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="拉日 K 并发线程数（过大易触发 mini_racer 崩溃或东财断连，建议 4～8）",
    )
    parser.add_argument(
        "--skip-ensure",
        action="store_true",
        help="跳过 ensure_sufficient_data 批量预拉（默认已不拉日 K 网，此项多用于显式与旧文档一致）",
    )
    parser.add_argument(
        "--allow-fetch-k",
        action="store_true",
        help="允许日 K 走网络补数（东财→新浪/腾讯）；默认关闭，仅本地 K + 本次 spot",
    )
    parser.add_argument(
        "--config",
        default=os.path.join(PROJECT_ROOT, "common_strategies.json"),
        help="策略配置路径",
    )
    args = parser.parse_args()

    trading_date = (args.date or datetime.now().strftime("%Y-%m-%d")).strip()[:10]
    try:
        datetime.strptime(trading_date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"[ERROR] 无效日期: {args.date}")

    snap_dir = os.path.join(PROJECT_ROOT, "cache", "intraday_snapshots")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = os.path.join(snap_dir, f"sina_spot_{ts}.jsonl")

    print("拉取 stock_zh_a_spot …", flush=True)
    df_spot = fetch_sina_spot_dataframe()
    save_snapshot(df_spot, snap_path)
    print(f"快照已写: {snap_path}（行数含全市场）", flush=True)

    spot_by_code = build_spot_by_code(df_spot)
    print(f"主板可合并代码数: {len(spot_by_code)}", flush=True)

    name, strategy = load_main_force_strategy(args.config)

    from data_fetcher import DataFetcher
    from strategy_engine import StrategyEngine

    fetcher = DataFetcher()
    if not args.allow_fetch_k:
        fetcher.cache_only = True
        print(
            "[INFO] 日 K 仅使用本地 cache/stock_data + 本次新浪 spot 合并；不为日 K 请求接口（加 --allow-fetch-k 可补数）",
            flush=True,
        )
    else:
        print("[INFO] --allow-fetch-k：日 K 不足时将走东财/新浪等补数", flush=True)

    engine = StrategyEngine(fetcher, max_workers=args.workers)

    print(f"运行 {name}，T={trading_date}，并入新浪快照 …", flush=True)
    ensure_data = not args.skip_ensure and not getattr(fetcher, "cache_only", False)
    results = engine.backtest_single_day_with_spot(
        strategy,
        strategy_name=name,
        trading_date=trading_date,
        spot_by_code=spot_by_code,
        ensure_sufficient_data=ensure_data,
    )
    n = len(results) if results else 0
    print(f"命中: {n} 只", flush=True)

    note = "T日K由新浪spot并入本地历史；非收盘正式K"
    out_path = write_results_jsonl(
        os.path.join(PROJECT_ROOT, "results"),
        name,
        trading_date,
        results or [],
        note,
    )
    if out_path:
        print(f"结果: {out_path}", flush=True)
        try:
            import importlib.util

            esl_path = os.path.join(PROJECT_ROOT, "scripts", "enrich_sector_linkage.py")
            spec = importlib.util.spec_from_file_location(
                "enrich_sector_linkage_intraday", esl_path
            )
            mod = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(mod)
            print("[INFO] 补充板块/概念联动字段 …", flush=True)
            mod.enrich_results_jsonl_after_backtest(os.path.abspath(out_path))
        except Exception as e:
            print(f"[WARN] 板块联动 enrich 失败（结果文件已保存）: {e}", flush=True)
            print(
                f"  可稍后执行: python scripts/enrich_sector_linkage.py --no-proxy --file {os.path.basename(out_path)}",
                flush=True,
            )

    if results:
        for i, r in enumerate(results[:30], 1):
            print(f"  {i}. {r.get('code')} {r.get('name')} match_date={r.get('match_date')}")
        if len(results) > 30:
            print(f"  … 共 {len(results)} 条，仅列出前 30")


if __name__ == "__main__":
    main()
