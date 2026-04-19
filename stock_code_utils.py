"""
A股代码/名称粗判：主板普通股票 vs 指数、基金、债券、REIT 等。

主板个股（本项目约定，需结合交易所）：
- 沪市：仅 60xxxx 主板 A 股（上交所 000xxx 为指数/行情线，不是个股）
- 深市：000/001/002/003 开头的 A 股主板（含原中小板）

说明：
- Baostock 返回带 sh./sz. 前缀，写入 stock_list 的 `exchange` 字段为 `sh`/`sz`；
- 无 `exchange` 的旧数据：仍用名称关键字兜底；缓存白名单以 stock_list 过滤结果为准。
"""

import json
import os
from typing import Dict, Optional, Set


def is_main_board_equity_code(code: str, exchange: Optional[str] = None) -> bool:
    """
    是否为主板普通 A 股六位代码。
    exchange: 'sh' | 'sz' | None。None 时按旧规则兼容（无法区分上交所 000 指数与深交所 000 个股）。
    """
    c = (code or "").strip()
    if len(c) != 6 or not c.isdigit():
        return False
    ex = (exchange or "").strip().lower()
    if ex == "sh":
        return c.startswith("60")
    if ex == "sz":
        return c.startswith(("000", "001", "002", "003"))
    if c.startswith("60"):
        return True
    return c.startswith(("000", "001", "002", "003"))


def universe_exclusion_reason(
    code: str,
    name: str = "",
    *,
    exchange: Optional[str] = None,
    bs_code: Optional[str] = None,
) -> Optional[str]:
    """
    是否应排除在「主板普通 A 股」股票池之外。
    返回 None 表示保留；否则为稳定英文 key，便于日志检索。

    必须结合 exchange（或 Baostock 的 bs_code）才能区分上交所 000 段指数与深市 000 个股；
    仅六位码且无交易所时，规则为兼容旧数据的折中，可能误判。
    """
    c = (code or "").strip()
    nm = name or ""
    if len(c) != 6 or not c.isdigit():
        return "invalid_code"
    bc = (bs_code or "").strip()
    if bc and is_likely_index_baostock(bc, nm):
        return "baostock_index_or_derivative_series"
    ex = (exchange or "").strip().lower() or None
    if not is_main_board_equity_code(c, ex):
        if ex == "sh":
            return "sse_not_mainboard_only_60xxxx"
        if ex == "sz":
            return "szse_not_board_prefix_000_to_003"
        return "not_mainboard_by_code_exchange_missing_or_other"
    if is_likely_non_equity_by_name(nm):
        return "name_keyword_non_equity"
    if "ST" in nm or "*ST" in nm or "st" in nm or "*st" in nm:
        return "st_in_short_name"
    if "退" in nm:
        return "delisting_marker_in_name"
    if is_likely_index_code_name(c, nm):
        return "index_code_or_name_heuristic"
    return None


# 名称中含下列字样则视为「非普通个股」（含指数、基金、债、REIT、上证行情线简称等）
_EXCLUDE_NON_STOCK_BY_NAME = (
    # 指数类
    "指数",
    "上证50",
    "沪深300",
    "中证500",
    "中证1000",
    "深证成指",
    "创业板指",
    "科创板50",
    "科创50",
    "国债",
    "企债",
    "转债指数",
    "国证",
    "全指",
    "中证",
    "A50",
    # 上证 180/380/50 系列行情线（与个股代码同为 000xxx）
    "50基本",
    "180基本",
    "380公用",
    "380波动",
    "380材料",
    "公用等权",
    "等权",
    # 基金 / 债 / REIT 等
    "ETF",
    "etf",
    "LOF",
    "lof",
    "QFII",
    "基金",
    "联接",
    "货币",
    "债基",
    "QDII",
    "FOF",
    "纯债",
    "可转债",
    "转债",
    "公司债",
    "企业债",
    "基础设施",
    "REIT",
    "reit",
    "优先股",
    "指数型",
    "复制指数",
    "分级",
    "子份额",
    "集合计划",
    "资产管理计划",
)


def is_likely_non_equity_by_name(name: str) -> bool:
    """仅凭证券简称判断是否非普通 A 股个股（指数、基金、债券、REIT 等）。"""
    nm = name or ""
    for kw in _EXCLUDE_NON_STOCK_BY_NAME:
        if kw in nm:
            return True
    return False


def is_likely_index_baostock(bs_code: str, _name: str = "") -> bool:
    """根据 Baostock 完整代码（sh./sz.）判断是否指数序列（名称类非个股由 universe_exclusion_reason 处理）。"""
    bc = (bs_code or "").strip().lower()
    if bc.startswith("sh.000"):
        return True
    if bc.startswith("sz.399") or bc.startswith("sz.395"):
        return True
    if bc.startswith("sh.88"):
        return True
    return False


def is_likely_index_code_name(code: str, name: str = "") -> bool:
    """六位代码 + 名称：明显指数段、或非普通个股名称（用于缓存文件、旧列表过滤）。"""
    code = (code or "").strip()
    nm = name or ""
    if len(code) != 6 or not code.isdigit():
        return True
    if code.startswith("399") or code.startswith("395"):
        return True
    if code.startswith(("880", "881", "882", "883", "884", "885", "889")):
        return True
    if is_likely_non_equity_by_name(nm):
        return True
    return False


def load_main_board_codes_whitelist(stock_list_path: str) -> Optional[Set[str]]:
    """
    从 stock_list.json 得到当前允许参与行情的主板个股代码集合。
    若文件不存在或解析失败返回 None（调用方退回仅用代码/名称规则）。
    """
    if not stock_list_path or not os.path.isfile(stock_list_path):
        return None
    try:
        with open(stock_list_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        stocks = data.get("stocks") or []
        out: Set[str] = set()
        for s in stocks:
            code = str(s.get("code") or "").strip()
            name = str(s.get("name") or "")
            if len(code) != 6:
                continue
            ex = s.get("exchange") or s.get("ex")
            ex = str(ex).strip().lower() if ex else None
            if not ex:
                ex = "sh" if code.startswith("60") else "sz"
            if universe_exclusion_reason(code, name, exchange=ex) is not None:
                continue
            out.add(code)
        return out if out else None
    except Exception:
        return None


def load_stock_exchange_map(stock_list_path: str) -> Dict[str, str]:
    """code -> 'sh'|'sz'，用于无白名单时的兜底。"""
    if not stock_list_path or not os.path.isfile(stock_list_path):
        return {}
    try:
        with open(stock_list_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        stocks = data.get("stocks") or []
        m: Dict[str, str] = {}
        for s in stocks:
            code = str(s.get("code") or "").strip()
            if len(code) != 6:
                continue
            ex = s.get("exchange") or s.get("ex")
            if ex:
                m[code] = str(ex).strip().lower()
        return m
    except Exception:
        return {}
