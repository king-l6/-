"""
沪深主板普通股代码表（仅列表，不含 K 线）。

优先 AkShare 交易所公开清单（沪市 主板A股 + 深市 A股列表且板块=主板）；失败或结果过少时依次尝试
东财实时全表 stock_zh_a_spot_em（必要时再拉 stock_sh_a_spot_em / stock_sz_a_spot_em）、新浪 stock_zh_a_spot。
与项目内「沪市 60xxxx、深市 000~003」及 universe_exclusion_reason 再筛一层一致。

日 K 由 data_fetcher 使用 AkShare 拉取（含多源回退）。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from akshare_setup import configure_akshare_http
from stock_code_utils import universe_exclusion_reason


def _norm_six_digit_code(v: Any) -> str:
    s = str(v).strip()
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    return s


def _exchange_from_em_plain_code(code: str) -> Optional[str]:
    """东财 spot 仅六位码时：60 为沪主板；000~003 段视为深主板（沪 000 段指数不在此表或靠名称规则剔除）。"""
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith("60"):
        return "sh"
    if code.startswith(("000", "001", "002", "003")):
        return "sz"
    return None


def _list_from_exchanges(ak: Any) -> Dict[str, Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    try:
        sh = ak.stock_info_sh_name_code(symbol="主板A股")
        for _, row in sh.iterrows():
            code = _norm_six_digit_code(row.get("证券代码"))
            if len(code) != 6 or not code.isdigit():
                continue
            name = str(row.get("证券简称") or "").strip()
            merged[code] = {"code": code, "name": name, "exchange": "sh"}
    except Exception:
        pass

    try:
        sz = ak.stock_info_sz_name_code(symbol="A股列表")
        if "板块" in sz.columns:
            sz = sz[sz["板块"].astype(str) == "主板"]
        for _, row in sz.iterrows():
            code = _norm_six_digit_code(row.get("A股代码"))
            if len(code) != 6 or not code.isdigit():
                continue
            name = str(row.get("A股简称") or "").strip()
            merged[code] = {"code": code, "name": name, "exchange": "sz"}
    except Exception:
        pass
    return merged


def _consume_em_spot_dataframe(df: Any, merged: Dict[str, Dict[str, str]]) -> int:
    """解析东财 spot DataFrame，写入 merged；返回新增可解析行数。"""
    if df is None or getattr(df, "empty", True):
        return 0
    cols = list(getattr(df, "columns", []))
    code_col = "代码" if "代码" in cols else ("code" if "code" in cols else None)
    name_col = "名称" if "名称" in cols else ("name" if "name" in cols else None)
    if not code_col or not name_col:
        return 0
    added = 0
    for _, row in df.iterrows():
        code = _norm_six_digit_code(row.get(code_col))
        if len(code) != 6 or not code.isdigit():
            continue
        ex = _exchange_from_em_plain_code(code)
        if not ex:
            continue
        name = str(row.get(name_col) or "").strip()
        merged[code] = {"code": code, "name": name, "exchange": ex}
        added += 1
    return added


def _list_from_em_spot(ak: Any) -> Dict[str, Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    main = getattr(ak, "stock_zh_a_spot_em", None)
    if callable(main):
        try:
            _consume_em_spot_dataframe(main(), merged)
        except Exception:
            pass
    # 全市场接口失败或异常偏少时，再分市场拉取
    if len(merged) < 800:
        for alt_name in ("stock_sh_a_spot_em", "stock_sz_a_spot_em"):
            alt = getattr(ak, alt_name, None)
            if not callable(alt):
                continue
            try:
                _consume_em_spot_dataframe(alt(), merged)
            except Exception:
                pass
    return merged


def _list_from_sina_spot(ak: Any) -> Dict[str, Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    fn = getattr(ak, "stock_zh_a_spot", None)
    if not callable(fn):
        return merged
    try:
        df = fn()
    except Exception:
        return merged
    if df is None or getattr(df, "empty", True):
        return merged
    cols = list(getattr(df, "columns", []))
    code_col = "代码" if "代码" in cols else ("code" if "code" in cols else None)
    name_col = "名称" if "名称" in cols else ("name" if "name" in cols else None)
    if not code_col or not name_col:
        return merged
    for _, row in df.iterrows():
        raw = str(row.get(code_col) or "").strip().lower()
        if raw.startswith("sh") and len(raw) >= 8:
            code = _norm_six_digit_code(raw[2:8])
            ex = "sh"
        elif raw.startswith("sz") and len(raw) >= 8:
            code = _norm_six_digit_code(raw[2:8])
            ex = "sz"
        else:
            continue
        if len(code) != 6 or not code.isdigit():
            continue
        name = str(row.get(name_col) or "").strip()
        merged[code] = {"code": code, "name": name, "exchange": ex}
    return merged


def _apply_universe(merged: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in merged.values():
        if universe_exclusion_reason(row["code"], row["name"], exchange=row["exchange"]) is None:
            out.append(row)
    out.sort(key=lambda x: x["code"])
    return out


def fetch_main_board_stocks_akshare() -> Optional[List[Dict[str, str]]]:
    """
    返回 [{'code','name','exchange'}, ...]；失败或结果为空返回 None。

    数据源顺序：上交所/深交所列表页 → 东财 spot（含分市场补拉）→ 新浪全 A spot。
    """
    try:
        configure_akshare_http()
        import akshare as ak
    except ImportError:
        return None

    strategies: Tuple[Tuple[str, Callable[[Any], Dict[str, Dict[str, str]]]], ...] = (
        ("exchange_sse_szse", _list_from_exchanges),
        ("eastmoney_spot_em", _list_from_em_spot),
        ("sina_spot", _list_from_sina_spot),
    )

    for label, pull in strategies:
        try:
            merged = pull(ak)
        except Exception:
            merged = {}
        if not merged:
            continue
        out = _apply_universe(merged)
        if out:
            if label != "exchange_sse_szse":
                print(f"[INFO] 股票列表使用备用数据源: {label}")
            return out
    return None
