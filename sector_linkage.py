# -*- coding: utf-8 -*-
"""
板块/概念联动：多数据源链（与 data_fetcher 日 K 思路一致）——`enrich_sector_linkage` 默认 **auto**
（新浪 spot 优先 0.2~0.4s，失败再东财 34~72s）；`--source eastmoney` 仅用东财。

- 概念（按 match_date 默认）：**同花顺** `stock_board_concept_name_ths` + `stock_board_concept_index_ths`，逐概念拉区间 K 算 T 日涨跌幅（较慢、较稳）；可选 `SECTOR_LINKAGE_CONCEPT_DAILY=eastmoney` 或 `--concept-daily eastmoney` 改东财 `stock_board_concept_hist_em`（每板块只拉 T 日一根，快但易 `RemoteDisconnected`）。
- 概念（运行时刻 spot）：东财 + 新浪 `stock_sector_spot` / `stock_sector_detail`。
- 行业：按 match_date 时默认 **同花顺** `stock_board_industry_name_ths` + `stock_board_industry_index_ths`；`SECTOR_LINKAGE_INDUSTRY_DAILY=eastmoney` 可改东财行业日 K。spot 时仍可用东财/新浪榜。

**按 match_date 对齐（默认）**：写入 `cache/sector_linkage/daily/YYYY-MM-DD.json`，含 `concept_daily` / `industry_daily`（v2；v1 仅概念字段，行业视为东财）。关闭对齐：`SECTOR_LINKAGE_MATCH_DATE=0` 或 enrich `--no-match-date`。
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

# (展示名, 拉成份用 id, 涨跌幅%, 数据源, 当日涨幅名次)
# 名次：在当前排序列表（接口返回并经涨跌幅排序；若使用 --min-concept-pct 等则为过滤后列表）
# 中的 1-based 排名；仅对随后参与拉成份的 top_n 板块写入命中结果。
BoardRow = Tuple[str, str, float, str, int]


def normalize_stock_code(code: Any) -> str:
    if code is None:
        return ""
    s = str(code).strip()
    if "." in s:
        s = s.rsplit(".", 1)[-1]
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:].zfill(6)
    if digits:
        return digits.zfill(6)[:6]
    return (s or "")[:6].zfill(6)


def clear_env_proxy(*, disable_getproxies: bool = False) -> None:
    for k in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        os.environ.pop(k, None)
    if disable_getproxies:
        import urllib.request as _urllib_request

        _urllib_request.getproxies = lambda: {}  # type: ignore[method-assign]


def _read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=0)
    os.replace(tmp, path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def linkage_offline_from_env() -> bool:
    """为 1/true/yes/on 时：板块 enrich 不发起 HTTP（仅用已有 daily / 成份 JSON）。"""
    v = os.environ.get("SECTOR_LINKAGE_OFFLINE", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _cache_age_days(path: str) -> Optional[float]:
    try:
        m = os.path.getmtime(path)
    except OSError:
        return None
    return max(0.0, (time.time() - m) / 86400.0)


def retry_call(fn: Callable[[], Any], retries: int = 6, sleep_sec: float = 1.5) -> Any:
    last = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            if i + 1 >= retries:
                raise
            time.sleep(sleep_sec * (i + 1))
    raise last  # type: ignore[misc]


def _is_transient_network_err(exc: BaseException) -> bool:
    s = f"{type(exc).__name__} {exc!s}".lower()
    if "remote" in s or "disconnect" in s or "connection" in s or "timeout" in s:
        return True
    if "protocolerror" in s or "connectionerror" in s or "chunked" in s:
        return True
    return False


def _safe_cache_token(board_id: str) -> str:
    s = re.sub(r"[^\w.\-]+", "_", str(board_id).strip())
    return (s or "x")[:160]


def _top_concepts_eastmoney(
    top_n: int, min_pct: Optional[float]
) -> List[BoardRow]:
    import akshare as ak

    df = retry_call(ak.stock_board_concept_name_em)
    if df is None or df.empty or "涨跌幅" not in df.columns:
        return []
    work = df.copy()
    work["涨跌幅"] = pd.to_numeric(work["涨跌幅"], errors="coerce")
    work = work.dropna(subset=["板块名称", "板块代码", "涨跌幅"])
    if min_pct is not None:
        work = work[work["涨跌幅"] >= min_pct]
    work = work.sort_values("涨跌幅", ascending=False).reset_index(drop=True)
    rows: List[BoardRow] = []
    for idx, r in work.head(int(top_n)).iterrows():
        name = str(r["板块名称"]).strip()
        bcode = str(r["板块代码"]).strip()
        pct = float(r["涨跌幅"])
        rank = int(idx) + 1
        if name and bcode:
            rows.append((name, bcode, pct, "eastmoney", rank))
    return rows


def _top_concepts_sina(top_n: int, min_pct: Optional[float]) -> List[BoardRow]:
    import akshare as ak

    df = retry_call(lambda: ak.stock_sector_spot(indicator="概念"))
    if df is None or df.empty or "涨跌幅" not in df.columns:
        return []
    work = df.copy()
    work["涨跌幅"] = pd.to_numeric(work["涨跌幅"], errors="coerce")
    work = work.dropna(subset=["label", "板块", "涨跌幅"])
    if min_pct is not None:
        work = work[work["涨跌幅"] >= min_pct]
    work = work.sort_values("涨跌幅", ascending=False).reset_index(drop=True)
    out: List[BoardRow] = []
    for idx, r in work.head(int(top_n)).iterrows():
        label = str(r["label"]).strip()
        name = str(r["板块"]).strip()
        pct = float(r["涨跌幅"])
        rank = int(idx) + 1
        if label and name:
            out.append((name, label, pct, "sina", rank))
    return out


def load_top_concept_boards(
    top_n: int,
    min_pct: Optional[float] = None,
    *,
    source: str = "auto",
) -> Tuple[List[BoardRow], str]:
    """
    source: 'auto' 先新浪再东财（新浪 0.4s vs 东财 72s）；'eastmoney' 仅用东财。
    返回 (板块行列表, 实际使用的数据源标签)。
    """
    if source == "eastmoney":
        try:
            em = _top_concepts_eastmoney(top_n, min_pct)
            if em:
                return em, "eastmoney"
        except Exception:
            pass
        return [], "none"
    # auto: 新浪优先（快 160x），失败再东财
    sina = _top_concepts_sina(top_n, min_pct)
    if sina:
        return sina, "sina"
    try:
        em = _top_concepts_eastmoney(top_n, min_pct)
        if em:
            return em, "eastmoney"
    except Exception:  # noqa: BLE001
        pass
    return [], "none"


def _top_industry_eastmoney(top_n: int, min_pct: Optional[float]) -> List[BoardRow]:
    import akshare as ak

    df = retry_call(ak.stock_board_industry_name_em)
    if df is None or df.empty:
        return []
    work = df.copy()
    work["涨跌幅"] = pd.to_numeric(work["涨跌幅"], errors="coerce")
    work = work.dropna(subset=["板块名称", "板块代码", "涨跌幅"])
    if min_pct is not None:
        work = work[work["涨跌幅"] >= min_pct]
    work = work.sort_values("涨跌幅", ascending=False).reset_index(drop=True)
    out: List[BoardRow] = []
    for idx, r in work.head(int(top_n)).iterrows():
        name = str(r["板块名称"]).strip()
        bcode = str(r["板块代码"]).strip()
        pct = float(r["涨跌幅"])
        rank = int(idx) + 1
        if name and bcode:
            out.append((name, bcode, pct, "eastmoney", rank))
    return out


def _top_industry_sina(top_n: int, min_pct: Optional[float]) -> List[BoardRow]:
    import akshare as ak

    df = retry_call(lambda: ak.stock_sector_spot(indicator="行业"))
    if df is None or df.empty or "涨跌幅" not in df.columns:
        return []
    work = df.copy()
    work["涨跌幅"] = pd.to_numeric(work["涨跌幅"], errors="coerce")
    work = work.dropna(subset=["label", "板块", "涨跌幅"])
    if min_pct is not None:
        work = work[work["涨跌幅"] >= min_pct]
    work = work.sort_values("涨跌幅", ascending=False).reset_index(drop=True)
    out: List[BoardRow] = []
    for idx, r in work.head(int(top_n)).iterrows():
        label = str(r["label"]).strip()
        name = str(r["板块"]).strip()
        pct = float(r["涨跌幅"])
        rank = int(idx) + 1
        if label and name:
            out.append((name, label, pct, "sina", rank))
    return out


def load_top_industry_boards(
    top_n: int,
    min_pct: Optional[float] = None,
    *,
    source: str = "auto",
) -> Tuple[List[BoardRow], str]:
    """
    source: 'auto' 先新浪再东财（新浪 0.2s vs 东财 34s）；'eastmoney' 仅用东财。
    """
    if source == "eastmoney":
        try:
            em = _top_industry_eastmoney(top_n, min_pct)
            if em:
                return em, "eastmoney"
        except Exception:
            pass
        return [], "none"
    # auto: 新浪优先（快 187x），失败再东财
    sina = _top_industry_sina(top_n, min_pct)
    if sina:
        return sina, "sina"
    try:
        em = _top_industry_eastmoney(top_n, min_pct)
        if em:
            return em, "eastmoney"
    except Exception:  # noqa: BLE001
        pass
    return [], "none"


LINKAGE_DAILY_SNAPSHOT_VERSION = 2


def linkage_daily_snapshot_path(cache_root: str, trade_date_iso: str) -> str:
    return os.path.join(cache_root, "daily", f"{trade_date_iso}.json")


def extract_match_date_iso(record: Mapping[str, Any]) -> Optional[str]:
    """从结果行解析 T 日（YYYY-MM-DD）；兼容多种字段名。"""
    for key in (
        "match_date",
        "matchDate",
        "date",
        "signal_date",
        "trade_date",
        "signalDate",
        "tradeDate",
    ):
        raw = record.get(key)
        if raw is None:
            continue
        s = str(raw).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            y, m, d = s[:10].split("-")
            if len(y) == 4 and y.isdigit() and m.isdigit() and d.isdigit():
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) >= 8:
            y, m, d = digits[:4], digits[4:6], digits[6:8]
            if y.isdigit() and m.isdigit() and d.isdigit():
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return None


def _em_compact_yyyymmdd(trade_date_iso: str) -> str:
    return trade_date_iso.replace("-", "")[:8]


_em_concept_name_to_bk_cache: Optional[Dict[str, str]] = None


def _em_concept_name_to_bk_map() -> Dict[str, str]:
    """东财概念「板块名称」-> BK 代码；供同花顺概念名对齐后拉成份。"""
    global _em_concept_name_to_bk_cache
    if _em_concept_name_to_bk_cache is not None:
        return _em_concept_name_to_bk_cache
    if linkage_offline_from_env():
        _em_concept_name_to_bk_cache = {}
        return _em_concept_name_to_bk_cache
    import akshare as ak

    df = retry_call(ak.stock_board_concept_name_em, retries=10, sleep_sec=1.5)
    if df is None or df.empty or "板块名称" not in df.columns or "板块代码" not in df.columns:
        _em_concept_name_to_bk_cache = {}
        return _em_concept_name_to_bk_cache
    m: Dict[str, str] = {}
    for _, r in df.iterrows():
        nm = str(r.get("板块名称") or "").strip()
        bk = str(r.get("板块代码") or "").strip()
        if nm and bk.startswith("BK"):
            m[nm] = bk
    _em_concept_name_to_bk_cache = m
    return _em_concept_name_to_bk_cache


def em_bk_for_matched_concept_name(name: str) -> str:
    return _em_concept_name_to_bk_map().get(name.strip(), "")


_em_industry_name_to_bk_cache: Optional[Dict[str, str]] = None


def _em_industry_name_to_bk_map() -> Dict[str, str]:
    """东财行业「板块名称」-> BK 代码；供同花顺行业名对齐后拉成份。"""
    global _em_industry_name_to_bk_cache
    if _em_industry_name_to_bk_cache is not None:
        return _em_industry_name_to_bk_cache
    if linkage_offline_from_env():
        _em_industry_name_to_bk_cache = {}
        return _em_industry_name_to_bk_cache
    import akshare as ak

    df = retry_call(ak.stock_board_industry_name_em, retries=10, sleep_sec=1.5)
    if df is None or df.empty or "板块名称" not in df.columns or "板块代码" not in df.columns:
        _em_industry_name_to_bk_cache = {}
        return _em_industry_name_to_bk_cache
    m: Dict[str, str] = {}
    for _, r in df.iterrows():
        nm = str(r.get("板块名称") or "").strip()
        bk = str(r.get("板块代码") or "").strip()
        if nm and bk.startswith("BK"):
            m[nm] = bk
    _em_industry_name_to_bk_cache = m
    return _em_industry_name_to_bk_cache


def em_bk_for_matched_industry_name(name: str) -> str:
    return _em_industry_name_to_bk_map().get(name.strip(), "")


def _stock_cache_latest_date_iso() -> Optional[str]:
    """与 DataFetcher.get_local_cache_latest_date 一致：读 000001 缓存最后交易日。"""
    try:
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache", "stock_data")
        pattern = os.path.join(root, "000001_*.json")
        import glob as _glob

        files = _glob.glob(pattern)
        if not files:
            return None
        latest_dt = None
        for fp in files:
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                rows = data.get("data") or []
                for r in rows:
                    ds = r.get("日期")
                    if ds:
                        dt = pd.to_datetime(ds)
                        if latest_dt is None or dt > latest_dt:
                            latest_dt = dt
            except Exception:
                continue
        return latest_dt.strftime("%Y-%m-%d") if latest_dt is not None else None
    except Exception:
        return None


def _linkage_daily_folder_max_date(cache_root: str) -> Optional[str]:
    """`cache/sector_linkage/daily/*.json` 文件名中的最大 YYYY-MM-DD。"""
    daily = os.path.join(cache_root, "daily")
    if not os.path.isdir(daily):
        return None
    best = ""
    for fn in os.listdir(daily):
        if not fn.endswith(".json"):
            continue
        stem = fn[:-5]
        if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
            if stem > best:
                best = stem
    return best if best else None


def _env_skip_concept_when_synced() -> bool:
    v = os.environ.get("SECTOR_LINKAGE_SKIP_CONCEPT_WHEN_SYNCED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _ths_index_window_yyyymmdd(trade_date_iso: str) -> Tuple[str, str]:
    """同花顺概念指数接口需区间；取 T 日前约 15 自然日至 T 日（仅需前收+T 收算涨跌，不必拉 40+ 天）。"""
    t = datetime.strptime(trade_date_iso[:10], "%Y-%m-%d").date()
    start = (t - timedelta(days=15)).strftime("%Y%m%d")
    end = t.strftime("%Y%m%d")
    return start, end


def _pct_from_ths_index_closes(hist_df: Any, trade_date_iso: str) -> Optional[float]:
    """同花顺概念指数 K 线无「涨跌幅」列时，用 T 日收盘相对前一交易日的涨跌幅（%）。"""
    if hist_df is None or getattr(hist_df, "empty", True):
        return None
    if "收盘价" not in hist_df.columns or "日期" not in hist_df.columns:
        return None
    df = hist_df.copy()
    df["_d"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["_d"]).sort_values("_d").reset_index(drop=True)
    if df.empty:
        return None
    target = trade_date_iso[:10]
    mask = df["_d"].dt.strftime("%Y-%m-%d") == target
    hits = df.loc[mask]
    if hits.empty:
        return None
    pos = int(hits.index[0])
    if pos <= 0:
        return None
    c_prev = float(pd.to_numeric(df.iloc[pos - 1]["收盘价"], errors="coerce"))
    c = float(pd.to_numeric(df.iloc[pos]["收盘价"], errors="coerce"))
    if pd.isna(c_prev) or pd.isna(c) or c_prev <= 0:
        return None
    return (c / c_prev - 1.0) * 100.0


def _pct_from_hist_day(hist_df: Any, trade_date_iso: str) -> Optional[float]:
    if hist_df is None or getattr(hist_df, "empty", True):
        return None
    if "涨跌幅" not in hist_df.columns:
        return None
    dt_col = "日期" if "日期" in hist_df.columns else None
    if not dt_col:
        return None
    sub = hist_df[hist_df[dt_col].astype(str).str.slice(0, 10) == trade_date_iso]
    if sub.empty:
        compact = _em_compact_yyyymmdd(trade_date_iso)
        sub = hist_df[hist_df[dt_col].astype(str).str.replace("-", "", regex=False) == compact]
    if sub.empty:
        return None
    v = pd.to_numeric(sub.iloc[0]["涨跌幅"], errors="coerce")
    if pd.isna(v):
        return None
    return float(v)


def _board_rows_to_jsonable(rows: Sequence[BoardRow]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, bid, pct, prov, rank in rows:
        out.append(
            {
                "name": name,
                "board_id": bid,
                "pct": float(pct),
                "source": prov,
                "rank": int(rank),
            }
        )
    return out


def _jsonable_to_board_rows(items: Any) -> List[BoardRow]:
    if not isinstance(items, list):
        return []
    rows: List[BoardRow] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            rows.append(
                (
                    str(it["name"]).strip(),
                    str(it["board_id"]).strip(),
                    float(it["pct"]),
                    str(it.get("source") or "eastmoney_hist"),
                    int(it["rank"]),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return rows


def _snapshot_concept_daily_mode(raw: dict) -> str:
    """从旧/新 daily 快照推断概念按日数据源。"""
    v = raw.get("concept_daily")
    if isinstance(v, str) and v.strip():
        return v.strip().lower()
    concepts = raw.get("concepts")
    if isinstance(concepts, list) and concepts:
        s0 = concepts[0]
        if isinstance(s0, dict):
            src = str(s0.get("source") or "")
            if "ths" in src.lower():
                return "ths"
    return "eastmoney"


def _snapshot_industry_daily_mode(raw: dict) -> str:
    """从 daily 快照推断行业按日数据源（v1 无字段时按行 source 推断，否则视为东财）。"""
    v = raw.get("industry_daily")
    if isinstance(v, str) and v.strip():
        return v.strip().lower()
    industries = raw.get("industries")
    if isinstance(industries, list) and industries:
        s0 = industries[0]
        if isinstance(s0, dict):
            src = str(s0.get("source") or "")
            if "ths" in src.lower():
                return "ths"
    return "eastmoney"


def load_daily_board_snapshot(
    cache_root: str,
    trade_date_iso: str,
    *,
    concept_daily: str = "ths",
    industry_daily: str = "ths",
) -> Optional[Tuple[List[BoardRow], List[BoardRow]]]:
    path = linkage_daily_snapshot_path(cache_root, trade_date_iso)
    raw = _read_json(path)
    ver = raw.get("version") if isinstance(raw, dict) else None
    if not raw or ver not in (1, 2):
        return None
    if str(raw.get("trade_date") or "").strip() != trade_date_iso:
        return None
    want = concept_daily.strip().lower()
    if want not in ("ths", "eastmoney"):
        want = "ths"
    if _snapshot_concept_daily_mode(raw) != want:
        return None
    want_i = industry_daily.strip().lower()
    if want_i not in ("ths", "eastmoney"):
        want_i = "ths"
    if _snapshot_industry_daily_mode(raw) != want_i:
        return None
    c = _jsonable_to_board_rows(raw.get("concepts"))
    i = _jsonable_to_board_rows(raw.get("industries"))
    # 空快照视为无效（避免失败后写入的空文件被永久当作「已缓存」）
    if not c and not i:
        return None
    return c, i


def save_daily_board_snapshot(
    cache_root: str,
    trade_date_iso: str,
    concept_rows: Sequence[BoardRow],
    industry_rows: Sequence[BoardRow],
    *,
    builder: str,
    concept_daily: str,
    industry_daily: str,
) -> None:
    os.makedirs(os.path.join(cache_root, "daily"), exist_ok=True)
    path = linkage_daily_snapshot_path(cache_root, trade_date_iso)
    cd = concept_daily.strip().lower()
    if cd not in ("ths", "eastmoney"):
        cd = "ths"
    idy = industry_daily.strip().lower()
    if idy not in ("ths", "eastmoney"):
        idy = "ths"
    payload = {
        "version": LINKAGE_DAILY_SNAPSHOT_VERSION,
        "trade_date": trade_date_iso,
        "built_at": _utc_now_iso(),
        "builder": builder,
        "concept_daily": cd,
        "industry_daily": idy,
        "concepts": _board_rows_to_jsonable(concept_rows),
        "industries": _board_rows_to_jsonable(industry_rows),
    }
    try:
        _write_json(path, payload)
    except OSError:
        pass


def build_concept_board_rows_for_trade_date_em(
    trade_date_iso: str,
    top_n: int,
    min_pct: Optional[float],
    *,
    hist_sleep_sec: float = 0.12,
    max_boards: int = 0,
) -> List[BoardRow]:
    """
    用东财各概念板块指数日 K，取 trade_date_iso 当日涨跌幅，排序后得到 BoardRow（与 spot 版语义一致：
    先按涨跌幅排序，可选 min_pct 过滤，再取前 top_n，名次为过滤后列表中的 1-based 序）。
    调用方应对返回值做磁盘缓存；本函数会逐板块请求接口，全市场约数百次 HTTP。
    """
    import akshare as ak

    # 东财列表接口易被限频/断连，单独加长重试
    print(f"[INFO] 东财概念日K {trade_date_iso}：拉取板块列表…", flush=True)
    name_df = retry_call(ak.stock_board_concept_name_em, retries=12, sleep_sec=2.0)
    if name_df is None or name_df.empty:
        return []
    if "板块名称" not in name_df.columns or "板块代码" not in name_df.columns:
        return []
    work = name_df[["板块名称", "板块代码"]].dropna()
    cd = _em_compact_yyyymmdd(trade_date_iso)
    scored: List[Tuple[str, str, float]] = []
    n_iter = 0
    total_cap = min(len(work), max_boards) if max_boards else len(work)
    progress_every = 60
    print(
        f"[INFO] 东财概念日K {trade_date_iso}：共 {total_cap} 步，开始逐块拉日K…",
        flush=True,
    )
    for _, r in work.iterrows():
        if max_boards and n_iter >= max_boards:
            break
        n_iter += 1
        if progress_every and (n_iter == 1 or n_iter % progress_every == 0):
            print(
                f"[INFO] 东财概念日K {trade_date_iso} 进度 {n_iter}/{total_cap}（已取到涨跌幅 {len(scored)}）…",
                flush=True,
            )
        name = str(r["板块名称"]).strip()
        bcode = str(r["板块代码"]).strip()
        if not name or not bcode.startswith("BK"):
            continue
        try:
            hist_df = retry_call(
                lambda bc=bcode: ak.stock_board_concept_hist_em(
                    symbol=bc,
                    period="daily",
                    start_date=cd,
                    end_date=cd,
                    adjust="",
                ),
                retries=8,
                sleep_sec=1.5,
            )
        except Exception:  # noqa: BLE001
            time.sleep(hist_sleep_sec)
            continue
        pct = _pct_from_hist_day(hist_df, trade_date_iso)
        if pct is not None:
            scored.append((name, bcode, pct))
        time.sleep(hist_sleep_sec)
    scored.sort(key=lambda x: -x[2])
    filtered = [(n, c, p) for n, c, p in scored if min_pct is None or p >= float(min_pct)]
    out: List[BoardRow] = []
    for i, (name, bcode, pct) in enumerate(filtered[: int(top_n)]):
        out.append((name, bcode, pct, "eastmoney_hist", i + 1))
    return out


def build_concept_board_rows_for_trade_date_ths(
    trade_date_iso: str,
    top_n: int,
    min_pct: Optional[float],
    *,
    hist_sleep_sec: float = 0.12,
    max_boards: int = 0,
) -> List[BoardRow]:
    """
    用同花顺概念板块指数 K 线，取 trade_date_iso 当日收盘相对前一交易日的涨跌幅（%），
    排序后得到 BoardRow。board_id 为同花顺概念 code；source 为 ths_hist。
    成份股在 fetch 侧按「同花顺概念名」与东财板块名称精确匹配后拉东财成份（见 fetch_board_member_codes）。
    """
    import akshare as ak

    print(f"[INFO] 同花顺概念指数 {trade_date_iso}：拉取概念列表…", flush=True)
    name_df = retry_call(ak.stock_board_concept_name_ths, retries=10, sleep_sec=1.5)
    if name_df is None or name_df.empty:
        return []
    if "name" not in name_df.columns or "code" not in name_df.columns:
        return []
    work = name_df[["name", "code"]].dropna()
    start_w, end_w = _ths_index_window_yyyymmdd(trade_date_iso)
    scored: List[Tuple[str, str, float]] = []
    n_iter = 0
    total_cap = min(len(work), max_boards) if max_boards else len(work)
    progress_every = 40
    print(
        f"[INFO] 同花顺概念指数 {trade_date_iso}：共 {total_cap} 步，区间 {start_w}~{end_w}…",
        flush=True,
    )
    for _, r in work.iterrows():
        if max_boards and n_iter >= max_boards:
            break
        n_iter += 1
        if progress_every and (n_iter == 1 or n_iter % progress_every == 0):
            print(
                f"[INFO] 同花顺概念 {trade_date_iso} 进度 {n_iter}/{total_cap}（已取到涨跌幅 {len(scored)}）…",
                flush=True,
            )
        name = str(r["name"]).strip()
        ths_code = str(r["code"]).strip()
        if not name or not ths_code:
            continue
        try:
            hist_df = retry_call(
                lambda sym=name: ak.stock_board_concept_index_ths(
                    symbol=sym,
                    start_date=start_w,
                    end_date=end_w,
                ),
                retries=5,
                sleep_sec=1.2,
            )
        except Exception:  # noqa: BLE001
            time.sleep(hist_sleep_sec)
            continue
        pct = _pct_from_ths_index_closes(hist_df, trade_date_iso)
        if pct is not None:
            scored.append((name, ths_code, pct))
        time.sleep(hist_sleep_sec)
    scored.sort(key=lambda x: -x[2])
    filtered = [(n, c, p) for n, c, p in scored if min_pct is None or p >= float(min_pct)]
    out: List[BoardRow] = []
    for i, (name, ths_code, pct) in enumerate(filtered[: int(top_n)]):
        out.append((name, ths_code, pct, "ths_hist", i + 1))
    return out


def build_industry_board_rows_for_trade_date_em(
    trade_date_iso: str,
    top_n: int,
    min_pct: Optional[float],
    *,
    hist_sleep_sec: float = 0.12,
    max_boards: int = 0,
) -> List[BoardRow]:
    import akshare as ak

    print(f"[INFO] 东财行业日K {trade_date_iso}：拉取板块列表…", flush=True)
    name_df = retry_call(ak.stock_board_industry_name_em, retries=12, sleep_sec=2.0)
    if name_df is None or name_df.empty:
        return []
    if "板块名称" not in name_df.columns or "板块代码" not in name_df.columns:
        return []
    work = name_df[["板块名称", "板块代码"]].dropna()
    cd = _em_compact_yyyymmdd(trade_date_iso)
    scored: List[Tuple[str, str, float]] = []
    n_iter = 0
    total_cap = min(len(work), max_boards) if max_boards else len(work)
    progress_every = 40
    print(
        f"[INFO] 东财行业日K {trade_date_iso}：共 {total_cap} 步，开始逐块拉日K…",
        flush=True,
    )
    for _, r in work.iterrows():
        if max_boards and n_iter >= max_boards:
            break
        n_iter += 1
        if progress_every and (n_iter == 1 or n_iter % progress_every == 0):
            print(
                f"[INFO] 东财行业日K {trade_date_iso} 进度 {n_iter}/{total_cap}（已取到涨跌幅 {len(scored)}）…",
                flush=True,
            )
        name = str(r["板块名称"]).strip()
        bcode = str(r["板块代码"]).strip()
        if not name or not bcode.startswith("BK"):
            continue
        try:
            hist_df = retry_call(
                lambda bc=bcode: ak.stock_board_industry_hist_em(
                    symbol=bc,
                    start_date=cd,
                    end_date=cd,
                    period="日k",
                    adjust="",
                ),
                retries=8,
                sleep_sec=1.5,
            )
        except Exception:  # noqa: BLE001
            time.sleep(hist_sleep_sec)
            continue
        pct = _pct_from_hist_day(hist_df, trade_date_iso)
        if pct is not None:
            scored.append((name, bcode, pct))
        time.sleep(hist_sleep_sec)
    scored.sort(key=lambda x: -x[2])
    filtered = [(n, c, p) for n, c, p in scored if min_pct is None or p >= float(min_pct)]
    out: List[BoardRow] = []
    for i, (name, bcode, pct) in enumerate(filtered[: int(top_n)]):
        out.append((name, bcode, pct, "eastmoney_hist", i + 1))
    return out


def build_industry_board_rows_for_trade_date_ths(
    trade_date_iso: str,
    top_n: int,
    min_pct: Optional[float],
    *,
    hist_sleep_sec: float = 0.12,
    max_boards: int = 0,
) -> List[BoardRow]:
    """
    用同花顺行业板块指数 K 线，取 trade_date_iso 当日收盘相对前一交易日的涨跌幅（%），
    排序后得到 BoardRow。board_id 为同花顺行业 code；source 为 ths_hist。
    成份股按行业名与东财行业「板块名称」精确匹配后拉东财行业成份。
    """
    import akshare as ak

    print(f"[INFO] 同花顺行业指数 {trade_date_iso}：拉取行业列表…", flush=True)
    name_df = retry_call(ak.stock_board_industry_name_ths, retries=10, sleep_sec=1.5)
    if name_df is None or name_df.empty:
        return []
    if "name" not in name_df.columns or "code" not in name_df.columns:
        return []
    work = name_df[["name", "code"]].dropna()
    start_w, end_w = _ths_index_window_yyyymmdd(trade_date_iso)
    scored: List[Tuple[str, str, float]] = []
    n_iter = 0
    total_cap = min(len(work), max_boards) if max_boards else len(work)
    progress_every = 40
    print(
        f"[INFO] 同花顺行业指数 {trade_date_iso}：共 {total_cap} 步，区间 {start_w}~{end_w}…",
        flush=True,
    )
    for _, r in work.iterrows():
        if max_boards and n_iter >= max_boards:
            break
        n_iter += 1
        if progress_every and (n_iter == 1 or n_iter % progress_every == 0):
            print(
                f"[INFO] 同花顺行业 {trade_date_iso} 进度 {n_iter}/{total_cap}（已取到涨跌幅 {len(scored)}）…",
                flush=True,
            )
        name = str(r["name"]).strip()
        ths_code = str(r["code"]).strip()
        if not name or not ths_code:
            continue
        try:
            hist_df = retry_call(
                lambda sym=name: ak.stock_board_industry_index_ths(
                    symbol=sym,
                    start_date=start_w,
                    end_date=end_w,
                ),
                retries=5,
                sleep_sec=1.2,
            )
        except Exception:  # noqa: BLE001
            time.sleep(hist_sleep_sec)
            continue
        pct = _pct_from_ths_index_closes(hist_df, trade_date_iso)
        if pct is not None:
            scored.append((name, ths_code, pct))
        time.sleep(hist_sleep_sec)
    scored.sort(key=lambda x: -x[2])
    filtered = [(n, c, p) for n, c, p in scored if min_pct is None or p >= float(min_pct)]
    out: List[BoardRow] = []
    for i, (name, ths_code, pct) in enumerate(filtered[: int(top_n)]):
        out.append((name, ths_code, pct, "ths_hist", i + 1))
    return out


def load_or_build_daily_board_snapshots(
    cache_root: str,
    trade_dates: Sequence[str],
    *,
    top_concepts: int,
    top_industries: int,
    min_concept_pct: Optional[float],
    min_industry_pct: Optional[float],
    skip_industry: bool,
    hist_sleep_sec: float = 0.12,
    max_concept_scan: int = 0,
    max_industry_scan: int = 0,
    force_refresh_daily: bool = False,
    concept_daily: str = "ths",
    industry_daily: str = "ths",
) -> Dict[str, Tuple[List[BoardRow], List[BoardRow], str]]:
    """
    对每个交易日：读缓存；若无则拉取（概念默认同花顺指数；行业默认同花顺行业指数）。
    概念侧故障时可在东财概念日 K 间切换重试；行业侧由 industry_daily 决定 ths / eastmoney。
    当本地日 K 缓存最新日与 sector_linkage/daily 下最大快照日相同且未强制刷新时，跳过概念侧网络拉取（缺当日文件则概念为空，仅保留行业构建）。
    """
    cd_mode = concept_daily.strip().lower()
    if cd_mode not in ("ths", "eastmoney"):
        cd_mode = "ths"
    id_mode = industry_daily.strip().lower()
    if id_mode not in ("ths", "eastmoney"):
        id_mode = "ths"
    os.makedirs(os.path.join(cache_root, "daily"), exist_ok=True)
    out: Dict[str, Tuple[List[BoardRow], List[BoardRow], str]] = {}
    seen: Set[str] = set()
    dates_list = [x for x in trade_dates if x]
    max_req = max(dates_list) if dates_list else ""
    kl = _stock_cache_latest_date_iso()
    sl = _linkage_daily_folder_max_date(cache_root)
    skip_concept_net = (
        not force_refresh_daily
        and _env_skip_concept_when_synced()
        and kl
        and sl
        and kl == sl
        and (not max_req or max_req <= kl)
    )
    if skip_concept_net:
        print(
            f"[INFO] 日K缓存最新日 {kl} 与板块联动 daily 最大日 {sl} 一致，本次跳过概念侧网络拉取"
            f"（缺缓存的交易日概念联动可能为空；设 SECTOR_LINKAGE_SKIP_CONCEPT_WHEN_SYNCED=0 可关闭）",
            flush=True,
        )

    for d in trade_dates:
        if not d or d in seen:
            continue
        seen.add(d)
        if not force_refresh_daily:
            cached = load_daily_board_snapshot(
                cache_root, d, concept_daily=cd_mode, industry_daily=id_mode
            )
            if not cached and cd_mode == "ths":
                cached = load_daily_board_snapshot(
                    cache_root, d, concept_daily="eastmoney", industry_daily=id_mode
                )
            if not cached and cd_mode == "eastmoney":
                cached = load_daily_board_snapshot(
                    cache_root, d, concept_daily="ths", industry_daily=id_mode
                )
            if cached:
                c0, i0 = cached
                if skip_industry:
                    out[d] = (c0, [], "daily_cache")
                else:
                    out[d] = (c0, i0, "daily_cache")
                continue
        if linkage_offline_from_env():
            print(
                f"[WARN] SECTOR_LINKAGE_OFFLINE=1：无 {d} 的 daily 快照文件，"
                "跳过网络构建（该股 T 日联动可能为空）。",
                flush=True,
            )
            out[d] = ([], [], "offline_missing_daily")
            continue
        fallback_order: List[str] = (
            ["eastmoney", "ths"] if cd_mode == "eastmoney" else ["ths", "eastmoney"]
        )
        print(
            f"[INFO] 板块联动按日快照缺失，构建 {d}（概念优先={fallback_order[0]}，"
            f"失败则换 {fallback_order[1]}；行业={id_mode}；概念扫描上限={max_concept_scan or '全量'}）…",
            flush=True,
        )
        c_rows: List[BoardRow] = []
        i_rows: List[BoardRow] = []
        used_concept_mode = cd_mode
        used_industry_mode = id_mode
        max_per_source = 3
        last_err: Optional[BaseException] = None
        built = False

        if skip_concept_net:
            c_rows = []
            used_concept_mode = cd_mode
            try:
                if skip_industry:
                    i_rows = []
                elif id_mode == "ths":
                    i_rows = build_industry_board_rows_for_trade_date_ths(
                        d,
                        top_industries,
                        min_industry_pct,
                        hist_sleep_sec=hist_sleep_sec,
                        max_boards=max_industry_scan,
                    )
                else:
                    i_rows = build_industry_board_rows_for_trade_date_em(
                        d,
                        top_industries,
                        min_industry_pct,
                        hist_sleep_sec=hist_sleep_sec,
                        max_boards=max_industry_scan,
                    )
                if not c_rows and not i_rows:
                    raise RuntimeError(
                        f"{d} 已跳过概念网络拉取且行业无有效排行（可能非交易日或过滤过严）"
                    )
                built = True
            except Exception as e:  # noqa: BLE001
                last_err = e
                print(f"[WARN] 跳过概念拉取后仅建行业失败 {d}: {e!s}", flush=True)

        if not built:
            for mi, concept_src in enumerate(fallback_order):
                for attempt in range(1, max_per_source + 1):
                    try:
                        if concept_src == "ths":
                            c_rows = build_concept_board_rows_for_trade_date_ths(
                                d,
                                top_concepts,
                                min_concept_pct,
                                hist_sleep_sec=hist_sleep_sec,
                                max_boards=max_concept_scan,
                            )
                        else:
                            c_rows = build_concept_board_rows_for_trade_date_em(
                                d,
                                top_concepts,
                                min_concept_pct,
                                hist_sleep_sec=hist_sleep_sec,
                                max_boards=max_concept_scan,
                            )
                        if skip_industry:
                            i_rows = []
                        elif id_mode == "ths":
                            i_rows = build_industry_board_rows_for_trade_date_ths(
                                d,
                                top_industries,
                                min_industry_pct,
                                hist_sleep_sec=hist_sleep_sec,
                                max_boards=max_industry_scan,
                            )
                        else:
                            i_rows = build_industry_board_rows_for_trade_date_em(
                                d,
                                top_industries,
                                min_industry_pct,
                                hist_sleep_sec=hist_sleep_sec,
                                max_boards=max_industry_scan,
                            )
                        if not c_rows and not i_rows:
                            raise RuntimeError(
                                f"{d} 概念源={concept_src} 与行业均无有效排行（可能非交易日或过滤过严）"
                            )
                        used_concept_mode = concept_src
                        used_industry_mode = id_mode
                        built = True
                        if concept_src != fallback_order[0]:
                            print(
                                f"[INFO] {d} 概念侧已用备用源「{concept_src}」成功（东财不可用时自动切换）",
                                flush=True,
                            )
                        break
                    except Exception as e:  # noqa: BLE001
                        last_err = e
                        transient = _is_transient_network_err(e)
                        if transient and attempt < max_per_source:
                            wait = 12.0 * attempt
                            print(
                                f"[WARN] 按日快照 {d}（概念={concept_src}）失败（{e!s}），"
                                f"{wait:.0f}s 后重试 ({attempt}/{max_per_source})…",
                                flush=True,
                            )
                            time.sleep(wait)
                            continue
                        if not transient:
                            print(
                                f"[WARN] 按日快照 {d}（概念={concept_src}）非瞬时错误：{e!s}",
                                flush=True,
                            )
                        break
                if built:
                    break
                if mi + 1 < len(fallback_order):
                    nxt = fallback_order[mi + 1]
                    print(
                        f"[INFO] {d} 概念数据源「{concept_src}」已连续失败，改用「{nxt}」…",
                        flush=True,
                    )

        if not built:
            if last_err is not None:
                raise last_err
            raise RuntimeError(f"按日构建 {d} 失败且无可用错误信息")

        if not c_rows and not i_rows:
            raise RuntimeError(
                f"按日构建 {d} 未得到任何板块涨跌幅（接口失败、非交易日或 min_pct 过滤过严）；"
                f"未写入 daily 缓存。请检查网络后重试，或暂时放宽 --min-concept-pct / --min-industry-pct。"
            )
        if used_concept_mode == "ths" and used_industry_mode == "ths":
            b_tag = "ths_concept_ths_industry"
        elif used_concept_mode == "ths":
            b_tag = "ths_concept_em_industry"
        elif used_industry_mode == "ths":
            b_tag = "em_concept_ths_industry"
        else:
            b_tag = "eastmoney_hist_em"
        save_daily_board_snapshot(
            cache_root,
            d,
            c_rows,
            i_rows,
            builder=b_tag,
            concept_daily=used_concept_mode,
            industry_daily=used_industry_mode,
        )
        tag = b_tag
        if skip_industry:
            out[d] = (c_rows, [], tag)
        else:
            out[d] = (c_rows, i_rows, tag)
    return out


def fetch_board_member_codes(
    board_id: str,
    board_name: str,
    provider: str,
    em_board_kind: str,
    cache_path: str,
    max_age_days: float,
    force_refresh: bool,
) -> Set[str]:
    """
    em_board_kind: 'concept' | 'industry'（eastmoney / eastmoney_hist 直接用东财 id；ths_hist 时按名对齐东财后拉成份）。
    """
    if linkage_offline_from_env():
        if os.path.isfile(cache_path):
            raw = _read_json(cache_path)
            if raw and isinstance(raw.get("codes"), list):
                return {normalize_stock_code(x) for x in raw["codes"] if x}
        return set()

    if (
        not force_refresh
        and os.path.isfile(cache_path)
        and (max_age_days <= 0 or (_cache_age_days(cache_path) or 999) <= max_age_days)
    ):
        raw = _read_json(cache_path)
        if raw and isinstance(raw.get("codes"), list):
            return {normalize_stock_code(x) for x in raw["codes"] if x}

    import akshare as ak

    codes: Set[str] = set()
    if provider in ("eastmoney", "eastmoney_hist"):
        if em_board_kind == "industry":
            df = retry_call(lambda: ak.stock_board_industry_cons_em(symbol=board_id))
        else:
            df = retry_call(lambda: ak.stock_board_concept_cons_em(symbol=board_id))
        if df is not None and not df.empty and "代码" in df.columns:
            for x in df["代码"].tolist():
                c = normalize_stock_code(x)
                if c:
                    codes.add(c)
    elif provider == "ths_hist":
        # 同花顺板块涨幅 + 东财成份：按板块名称与东财「板块名称」精确匹配
        if em_board_kind == "concept":
            bk = em_bk_for_matched_concept_name(board_name)
            if not bk.startswith("BK"):
                return set()
            df = retry_call(lambda: ak.stock_board_concept_cons_em(symbol=bk))
        elif em_board_kind == "industry":
            bk = em_bk_for_matched_industry_name(board_name)
            if not bk.startswith("BK"):
                return set()
            df = retry_call(lambda: ak.stock_board_industry_cons_em(symbol=bk))
        else:
            return set()
        if df is not None and not df.empty and "代码" in df.columns:
            for x in df["代码"].tolist():
                c = normalize_stock_code(x)
                if c:
                    codes.add(c)
    elif provider == "sina":
        df = retry_call(lambda: ak.stock_sector_detail(sector=board_id))
        if df is not None and not df.empty:
            col = "code" if "code" in df.columns else None
            if col:
                for x in df[col].tolist():
                    c = normalize_stock_code(x)
                    if c:
                        codes.add(c)
    payload = {
        "board_id": board_id,
        "board_name": board_name,
        "provider": provider,
        "em_board_kind": em_board_kind,
        "codes": sorted(codes),
        "fetched_at": _utc_now_iso(),
    }
    try:
        _write_json(cache_path, payload)
    except OSError:
        pass
    return codes


def build_board_membership_for_codes(
    top_boards: Sequence[BoardRow],
    target_codes: Set[str],
    em_board_kind: str,
    cons_cache_dir: str,
    cons_max_age_days: float,
    force_refresh_cons: bool,
) -> Dict[str, List[Tuple[str, float, int]]]:
    """code -> [(板块名, 涨跌幅%, 当日涨幅名次), …]"""
    out: Dict[str, List[Tuple[str, float, int]]] = {c: [] for c in target_codes}
    for name, bid, pct, prov, rank in top_boards:
        tok = _safe_cache_token(f"{prov}_{bid}")
        path = os.path.join(cons_cache_dir, f"cons_{tok}.json")
        members = fetch_board_member_codes(
            bid,
            name,
            prov,
            em_board_kind,
            path,
            cons_max_age_days,
            force_refresh_cons,
        )
        hit = members & target_codes
        for c in hit:
            out[c].append((name, pct, rank))
    return out


def _fmt_board_day_pct(pct: float) -> str:
    """板块当日涨跌幅，带符号与百分号。"""
    try:
        x = float(pct)
    except (TypeError, ValueError):
        x = 0.0
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%"


def concept_hits_rank_pct_averages(
    concept_hits: Sequence[Tuple[str, float, int]],
) -> Optional[Tuple[float, float]]:
    """
    对「该股命中的全部强势概念」求：涨幅名次算术平均、涨跌幅算术平均。
    用于联动列摘要；单概念时即为该概念的名次与涨幅。
    """
    if not concept_hits:
        return None
    ranks: List[float] = []
    pcts: List[float] = []
    for _n, p, rk in concept_hits:
        try:
            ranks.append(float(rk))
            pcts.append(float(p))
        except (TypeError, ValueError):
            continue
    if not pcts:
        return None
    rank_avg = sum(ranks) / len(ranks) if ranks else 0.0
    pct_avg = sum(pcts) / len(pcts)
    return (rank_avg, pct_avg)


def format_linkage_text(
    concept_hits: Sequence[Tuple[str, float, int]],
    industry_hit: Optional[Tuple[str, float, int]],
    max_concepts: int = 8,
    *,
    ranking_trade_date: Optional[str] = None,
) -> str:
    parts: List[str] = []
    d0 = (ranking_trade_date or "").strip()[:10]
    prefix = f"[T日{d0}] " if d0 else ""
    if concept_hits:
        shown = concept_hits[:max_concepts]
        chunks = [
            f"{n}({_fmt_board_day_pct(p)}·涨幅第{rk}名)" for n, p, rk in shown
        ]
        s = "、".join(chunks)
        more = len(concept_hits) - len(shown)
        if more > 0:
            s += f" 等{more}个"
        concept_line = f"联动概念:{s}"
        av = concept_hits_rank_pct_averages(concept_hits)
        if av is not None:
            rk_a, pct_a = av
            concept_line += f"｜概念均值:名次均{rk_a:.1f}·涨幅均{_fmt_board_day_pct(pct_a)}"
        parts.append(concept_line)
    if industry_hit:
        iname, ipct, irk = industry_hit[0], industry_hit[1], industry_hit[2]
        parts.append(
            f"联动行业:{iname}({_fmt_board_day_pct(ipct)}·涨幅第{irk}名)"
        )
    body = "；".join(parts)
    if body:
        return prefix + body
    if d0:
        return (
            prefix
            + "强势概念/行业榜（当前 topN 过滤范围内）无成份命中；"
            + "名次与涨跌幅均为该交易日板块指数快照口径（概念/行业默认同花顺指数，可配置为东财）。"
        )
    return ""
