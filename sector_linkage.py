# -*- coding: utf-8 -*-
"""
板块/概念联动：多数据源链（与 data_fetcher 日 K 思路一致）——优先东财 EM，失败则用新浪板块接口。

- 概念：东财 `stock_board_concept_name_em` + `stock_board_concept_cons_em`；
  备用 `stock_sector_spot("概念")` + `stock_sector_detail`。
- 行业：东财 `stock_board_industry_name_em` + `stock_board_industry_cons_em`；
  备用 `stock_sector_spot("行业")` + `stock_sector_detail`（与概念相同，用强势行业板块成份与个股求交，
  不再调用东财个股信息接口）。

MVP：均为脚本运行时刻快照，与 match_date 无历史对齐。
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

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
    source: 'auto' 先东财再新浪；'sina' 仅用新浪（东财不可用时避免长时间重试）。
    返回 (板块行列表, 实际使用的数据源标签)。
    """
    if source == "sina":
        sina = _top_concepts_sina(top_n, min_pct)
        return sina, "sina" if sina else "none"
    try:
        em = _top_concepts_eastmoney(top_n, min_pct)
        if em:
            return em, "eastmoney"
    except Exception:  # noqa: BLE001
        pass
    sina = _top_concepts_sina(top_n, min_pct)
    return sina, "sina" if sina else "none"


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
    if source == "sina":
        sina = _top_industry_sina(top_n, min_pct)
        return sina, "sina" if sina else "none"
    try:
        em = _top_industry_eastmoney(top_n, min_pct)
        if em:
            return em, "eastmoney"
    except Exception:  # noqa: BLE001
        pass
    sina = _top_industry_sina(top_n, min_pct)
    return sina, "sina" if sina else "none"


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
    em_board_kind: 'concept' | 'industry'（仅 provider=eastmoney 时使用）。
    """
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
    if provider == "eastmoney":
        if em_board_kind == "industry":
            df = retry_call(lambda: ak.stock_board_industry_cons_em(symbol=board_id))
        else:
            df = retry_call(lambda: ak.stock_board_concept_cons_em(symbol=board_id))
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
) -> str:
    parts: List[str] = []
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
    return "；".join(parts)
