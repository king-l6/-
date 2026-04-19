"""
沪深主板普通股代码表（仅列表，不含 K 线）。

默认使用 AkShare 从交易所公开页汇总的主板清单（沪市 symbol=主板A股；深市 A股列表且 板块=主板），
与项目内「沪市 60xxxx、深市 000~003」及 universe_exclusion_reason 再筛一层一致。

行情仍由 Baostock 拉取；环境变量 STOCK_LIST_SOURCE=baostock 可强制仅用 Baostock 全表+规则过滤。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from stock_code_utils import universe_exclusion_reason


def _norm_six_digit_code(v: Any) -> str:
    s = str(v).strip()
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    return s


def fetch_main_board_stocks_akshare() -> Optional[List[Dict[str, str]]]:
    """
    返回 [{'code','name','exchange'}, ...]；失败或结果为空返回 None。
    """
    try:
        import akshare as ak
    except ImportError:
        return None

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

    if not merged:
        return None

    out: List[Dict[str, str]] = []
    for row in merged.values():
        if universe_exclusion_reason(row["code"], row["name"], exchange=row["exchange"]) is None:
            out.append(row)
    out.sort(key=lambda x: x["code"])
    return out if out else None
