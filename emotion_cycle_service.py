"""
情绪周期分析服务（供 API 与脚本复用）。
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

from stock_code_utils import (
    is_likely_index_code_name,
    is_main_board_equity_code,
    load_main_board_codes_whitelist,
    load_stock_exchange_map,
)
from data_fetcher import parse_stock_data_cache_basename, read_stock_cache_end_ymd_quick


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(PROJECT_ROOT, "cache", "stock_data")
STOCK_LIST_CACHE_FILE = os.path.join(PROJECT_ROOT, "cache", "stock_list.json")
ROLLING_STATE_FILE = os.path.join(PROJECT_ROOT, "cache", "emotion_cycle_rolling.json")
ROLLING_STATE_VERSION = 5
# 整份情绪周期 API 结果落盘；rolling 聚合签名未变且 days 一致时直接读盘，避免每次进页全量重算
EMOTION_CYCLE_REPORT_CACHE = os.path.join(PROJECT_ROOT, "cache", "emotion_cycle_report.json")
REPORT_CACHE_ENVELOPE_VERSION = 1
# 多留一些日历天，避免 days 调大时反复全量扫描
ROLLING_CALENDAR_CAP = 520


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _norm_date(d):
    s = str(d or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s[:10] if len(s) >= 10 else s


def _latest_cache_file_by_code() -> Dict[str, str]:
    out = {}
    if not os.path.isdir(CACHE_DIR):
        return out
    name_map = _load_stock_name_map()
    wl = load_main_board_codes_whitelist(STOCK_LIST_CACHE_FILE)
    exmap = load_stock_exchange_map(STOCK_LIST_CACHE_FILE)
    for name in os.listdir(CACHE_DIR):
        if not name.endswith(".json"):
            continue
        parsed = parse_stock_data_cache_basename(name[:-5])
        if not parsed:
            continue
        code, _, end_from_name = parsed
        if len(code) != 6:
            continue
        fp_try = os.path.join(CACHE_DIR, name)
        end_s = end_from_name or read_stock_cache_end_ymd_quick(fp_try)
        if not end_s or len(end_s) != 8:
            continue
        if wl is not None:
            if code not in wl:
                continue
        else:
            ex = exmap.get(code)
            if not ex:
                ex = "sh" if code.startswith("60") else "sz"
            if not is_main_board_equity_code(code, ex):
                continue
        if is_likely_index_code_name(code, name_map.get(code, "")):
            continue
        fp = fp_try
        old = out.get(code)
        if old is None:
            out[code] = fp
            continue
        old_name = os.path.basename(old)[:-5]
        old_p = parse_stock_data_cache_basename(old_name)
        old_end = (
            old_p[2]
            if old_p and old_p[2]
            else read_stock_cache_end_ymd_quick(old)
        ) or ""
        if end_s > old_end:
            out[code] = fp
    return out


def _load_rows_from_cache(filepath: str) -> List[dict]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("data") or []
    except Exception:
        return []


def _load_stock_name_map() -> Dict[str, str]:
    """从缓存股票列表读取 code->name 映射。"""
    if not os.path.isfile(STOCK_LIST_CACHE_FILE):
        return {}
    try:
        with open(STOCK_LIST_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        stocks = data.get("stocks") or []
        out = {}
        for s in stocks:
            code = str(s.get("code") or "").strip()
            name = str(s.get("name") or "").strip()
            if len(code) == 6 and name:
                out[code] = name
        return out
    except Exception:
        return {}


def _build_latest_snapshot() -> Tuple[str, Dict[str, dict]]:
    files_by_code = _latest_cache_file_by_code()
    by_date: Dict[str, Dict[str, dict]] = {}
    for code, fp in files_by_code.items():
        rows = _load_rows_from_cache(fp)
        if not rows:
            continue
        last = rows[-1]
        ds = _norm_date(last.get("日期"))
        if not ds:
            continue
        pct = _safe_float(last.get("涨跌幅"), 0.0)
        close = _safe_float(last.get("收盘"), 0.0)
        vol = _safe_float(last.get("成交量"), 0.0)
        by_date.setdefault(ds, {})[code] = {"pct_change": pct, "close": close, "volume": vol}
    if not by_date:
        return "", {}
    latest_date = sorted(by_date.keys())[-1]
    return latest_date, by_date[latest_date]


def _rolling_state_sig() -> str:
    """用于判断 emotion_cycle_rolling 是否相对上次生成报告时发生变化。"""
    if not os.path.isfile(ROLLING_STATE_FILE):
        return ""
    try:
        with open(ROLLING_STATE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        return "|".join(
            [
                str(d.get("version", "")),
                str(d.get("updated_at", "")),
                str(d.get("dates_in_store", "")),
            ]
        )
    except Exception:
        return ""


def _stock_list_sig() -> str:
    p = STOCK_LIST_CACHE_FILE
    if not os.path.isfile(p):
        return ""
    try:
        st = os.stat(p)
        return f"{int(st.st_mtime)}:{st.st_size}"
    except OSError:
        return ""


def _try_load_emotion_report_cache(
    days: int, stock_code: str, force_refresh: bool
) -> Optional[Dict[str, Any]]:
    """命中则返回已缓存的完整 report dict；否则 None。"""
    if force_refresh or (stock_code or "").strip():
        return None
    if not os.path.isfile(EMOTION_CYCLE_REPORT_CACHE):
        return None
    try:
        with open(EMOTION_CYCLE_REPORT_CACHE, "r", encoding="utf-8") as f:
            wrapper = json.load(f)
    except Exception:
        return None
    env = wrapper.get("envelope") or {}
    if int(env.get("envelope_version", 0)) != REPORT_CACHE_ENVELOPE_VERSION:
        return None
    if int(env.get("days", -1)) != int(days):
        return None
    if env.get("rolling_sig") != _rolling_state_sig():
        return None
    if env.get("stock_list_sig") != _stock_list_sig():
        return None
    data = wrapper.get("data")
    if not isinstance(data, dict) or not data.get("timeline"):
        return None
    return data


def _save_emotion_report_cache(report: Dict[str, Any], days: int, stock_code: str) -> None:
    if (stock_code or "").strip():
        return
    env = {
        "envelope_version": REPORT_CACHE_ENVELOPE_VERSION,
        "days": int(days),
        "rolling_sig": _rolling_state_sig(),
        "stock_list_sig": _stock_list_sig(),
    }
    try:
        _atomic_write_json(EMOTION_CYCLE_REPORT_CACHE, {"envelope": env, "data": report})
    except Exception:
        pass


def _atomic_write_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def _file_merge_sig(fp: str) -> str:
    try:
        st = os.stat(fp)
        return f"{os.path.basename(fp)}:{int(st.st_mtime)}:{st.st_size}"
    except OSError:
        return ""


def _load_rolling_state() -> Optional[dict]:
    if not os.path.isfile(ROLLING_STATE_FILE):
        return None
    try:
        with open(ROLLING_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if int(data.get("version", 0)) != ROLLING_STATE_VERSION:
            return None
        bd = data.get("by_date")
        sigs = data.get("file_signatures")
        if not isinstance(bd, dict) or not isinstance(sigs, dict):
            return None
        return data
    except Exception:
        return None


def _merge_rows_into_by_date(by_date: Dict[str, Dict[str, dict]], code: str, rows: List[dict]) -> None:
    for r in rows:
        ds = _norm_date(r.get("日期"))
        if not ds:
            continue
        pct = _safe_float(r.get("涨跌幅"), 0.0)
        close = _safe_float(r.get("收盘"), 0.0)
        vol = _safe_float(r.get("成交量"), 0.0)
        by_date.setdefault(ds, {})[code] = {"pct_change": pct, "close": close, "volume": vol}


def _prune_codes_from_by_date(by_date: Dict[str, Dict[str, dict]], codes: List[str]) -> None:
    if not codes:
        return
    rm = set(codes)
    for d in list(by_date.keys()):
        snap = by_date[d]
        for c in rm:
            snap.pop(c, None)
        if not snap:
            del by_date[d]


def _trim_by_date_calendar(by_date: Dict[str, Dict[str, dict]], keep_last: int) -> None:
    if not by_date:
        return
    all_dates = sorted(by_date.keys())
    keep = set(all_dates[-max(1, int(keep_last)):])
    for d in list(by_date.keys()):
        if d not in keep:
            del by_date[d]


def _slice_snapshots_for_window(by_date: Dict[str, Dict[str, dict]], days: int) -> Dict[str, Dict[str, dict]]:
    if not by_date:
        return {}
    all_dates = sorted(by_date.keys())
    keep_dates = set(all_dates[-max(1, int(days)):])
    return {d: by_date[d] for d in all_dates if d in keep_dates}


def _build_daily_snapshots_from_by_date(by_date: Dict[str, Dict[str, dict]], days: int) -> Dict[str, Dict[str, dict]]:
    """与 _build_daily_snapshots 返回结构一致，输入为已合并的按日快照。"""
    return _slice_snapshots_for_window(by_date, days)


def _build_daily_snapshots(days: int = 120) -> Dict[str, Dict[str, dict]]:
    """
    构建按天快照：{date: {code: {pct_change, close}}}
    仅保留最近 days 个交易日。
    """
    files_by_code = _latest_cache_file_by_code()
    by_date: Dict[str, Dict[str, dict]] = {}
    for code, fp in files_by_code.items():
        rows = _load_rows_from_cache(fp)
        if not rows:
            continue
        for r in rows:
            ds = _norm_date(r.get("日期"))
            if not ds:
                continue
            pct = _safe_float(r.get("涨跌幅"), 0.0)
            close = _safe_float(r.get("收盘"), 0.0)
            vol = _safe_float(r.get("成交量"), 0.0)
            by_date.setdefault(ds, {})[code] = {"pct_change": pct, "close": close, "volume": vol}
    if not by_date:
        return {}
    all_dates = sorted(by_date.keys())
    keep_dates = set(all_dates[-max(1, int(days)):])
    return {d: by_date[d] for d in all_dates if d in keep_dates}


def _get_daily_snapshots_rolling(days: int, force_refresh: bool = False) -> Tuple[Dict[str, Dict[str, dict]], Dict[str, Any], Dict[str, Dict[str, dict]]]:
    """
    按日市场快照：优先读磁盘滚动聚合，仅对变更/新增的股票缓存文件做 JSON 合并。
    返回 (窗口内 daily_snapshots, 元信息, 日历裁剪后的全量 by_date 供连板统计预热)。
    """
    meta: Dict[str, Any] = {
        "rolling_file": ROLLING_STATE_FILE,
        "files_merged": 0,
        "files_skipped": 0,
        "force_refresh": bool(force_refresh),
    }
    files_by_code = _latest_cache_file_by_code()
    if not files_by_code:
        return {}, meta, {}

    cap = max(ROLLING_CALENDAR_CAP, int(days))
    state: dict = {}
    by_date: Dict[str, Dict[str, dict]] = {}
    sigs: Dict[str, str] = {}

    if not force_refresh:
        loaded = _load_rolling_state()
        if loaded:
            raw_bd = loaded.get("by_date")
            by_date = raw_bd if isinstance(raw_bd, dict) else {}
            sigs = {str(k): str(v) for k, v in (loaded.get("file_signatures") or {}).items()}

    prev_codes = set(sigs.keys())
    curr_codes = set(files_by_code.keys())
    removed = list(prev_codes - curr_codes)
    if removed:
        _prune_codes_from_by_date(by_date, removed)
    for c in removed:
        sigs.pop(c, None)

    for code, fp in files_by_code.items():
        want_sig = _file_merge_sig(fp)
        if not force_refresh and sigs.get(code) == want_sig and want_sig:
            meta["files_skipped"] += 1
            continue
        rows = _load_rows_from_cache(fp)
        if not rows:
            sigs.pop(code, None)
            continue
        _merge_rows_into_by_date(by_date, code, rows)
        sigs[code] = want_sig
        meta["files_merged"] += 1

    _trim_by_date_calendar(by_date, cap)
    state = {
        "version": ROLLING_STATE_VERSION,
        "by_date": by_date,
        "file_signatures": sigs,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "calendar_cap": cap,
    }
    try:
        _atomic_write_json(ROLLING_STATE_FILE, state)
    except Exception:
        pass

    meta["dates_in_store"] = len(by_date)
    daily_window = _build_daily_snapshots_from_by_date(by_date, days)
    return daily_window, meta, by_date


def _calc_market_metrics(snapshot: Dict[str, dict]) -> Dict[str, Any]:
    total = len(snapshot)
    if total == 0:
        return {
            "total": 0,
            "limit_up_count": 0,
            "strong_count": 0,
            "big_drop_count": 0,
            "avg_pct_change": 0.0,
            "limit_up_ratio_pct": 0.0,
            "strong_ratio_pct": 0.0,
            "big_drop_ratio_pct": 0.0,
        }

    pcts = [v["pct_change"] for v in snapshot.values()]
    limit_up_count = sum(1 for x in pcts if x >= 9.8)
    strong_count = sum(1 for x in pcts if x >= 5.0)
    big_drop_count = sum(1 for x in pcts if x <= -5.0)
    avg_pct = sum(pcts) / total

    return {
        "total": total,
        "limit_up_count": limit_up_count,
        "strong_count": strong_count,
        "big_drop_count": big_drop_count,
        "avg_pct_change": round(avg_pct, 3),
        "limit_up_ratio_pct": round(limit_up_count / total * 100, 2),
        "strong_ratio_pct": round(strong_count / total * 100, 2),
        "big_drop_ratio_pct": round(big_drop_count / total * 100, 2),
    }


def _calc_market_emotion_score(market: dict) -> Tuple[float, float]:
    """全市场温度分（0~100）；综合分与之相同（已移除代表票锚点加权）。"""
    market_score = (
        market["limit_up_ratio_pct"] * 5.0
        + market["strong_ratio_pct"] * 2.5
        - market["big_drop_ratio_pct"] * 2.5
        + (market["avg_pct_change"] + 2.0) * 8.0
    )
    market_score = max(0.0, min(100.0, market_score))
    s = round(market_score, 2)
    return s, s


def _approx_limit_pct_for_code(code: str) -> float:
    """主板约 10% 涨停；创业板/科创板约 20%。用于连板与涨停池判定。"""
    c = (code or "").strip()
    if len(c) < 3:
        return 9.8
    p = c[:3]
    if p in ("300", "301", "688", "689"):
        return 19.85
    return 9.8


def _judge_cycle(total_score: float) -> str:
    if total_score < 25:
        return "冰点"
    if total_score < 40:
        return "弱修复"
    if total_score < 60:
        return "中性震荡"
    if total_score < 78:
        return "强势主升"
    return "高潮/过热"


def _analyze_market_leader_rotation(
    by_date_full: Dict[str, Dict[str, dict]],
    name_map: Dict[str, str],
    visible_days: int,
    top_k: int = 5,
    limit_pct: float = 9.8,
) -> Dict[str, Any]:
    """
    市场「龙头」主导节奏（全市场、按日）：
    - 在完整交易日序列上维护「连板数」：当日满足该股近似涨停阈值则连板+1，否则清零；当日无该票截面则视为断板；
    - 近似涨停阈值：主板类约 9.8%；300/301/688/689 约 19.85%（20cm 板）；
    - 在当日涨停池中按「连板数」优先，其次「收盘×成交量」代理额、再比涨幅；
    - 取前 top_k，首只为当日主线龙头；仅输出最近 visible_days 个交易日到 daily；
    - 「连续交易日主线龙头为同一只」合并为一段周期，并附段内最高连板数。
    """
    all_dates = sorted(by_date_full.keys())
    if not all_dates:
        return {
            "daily": [],
            "segments": [],
            "limit_pct": limit_pct,
            "top_k": top_k,
            "note": "",
        }

    streak: Dict[str, int] = {}
    daily_out: List[dict] = []
    top1_series: List[Tuple[str, Optional[str]]] = []

    vis_n = max(1, int(visible_days))
    visible_set = set(all_dates[-vis_n:]) if len(all_dates) >= vis_n else set(all_dates)
    # 连板状态只需「可见窗口 + 略早预热」；避免对 rolling 里全部历史日逐日扫全市场（否则单日 O(样本数) × 数百日极慢）
    _warm = 45
    if len(all_dates) > vis_n + _warm:
        dates_iter = all_dates[-(vis_n + _warm) :]
    else:
        dates_iter = all_dates

    for d in dates_iter:
        snap = by_date_full[d]
        today_codes = set(snap.keys())

        for code in list(streak.keys()):
            if code not in today_codes:
                streak[code] = 0

        limit_up_codes: List[str] = []
        for code, v in snap.items():
            pct = _safe_float(v.get("pct_change"), 0.0)
            thr = _approx_limit_pct_for_code(code)
            if pct + 1e-9 >= thr:
                streak[code] = streak.get(code, 0) + 1
                limit_up_codes.append(code)
            else:
                streak[code] = 0

        for c in list(streak.keys()):
            if streak.get(c, 0) <= 0:
                del streak[c]

        cands: List[Tuple[str, int, float, float, float, float]] = []
        for code in limit_up_codes:
            v = snap[code]
            pct = _safe_float(v.get("pct_change"), 0.0)
            vol = _safe_float(v.get("volume"), 0.0)
            close = _safe_float(v.get("close"), 0.0)
            boards = streak.get(code, 0)
            amt = close * vol
            cands.append((code, boards, amt, vol, pct, close))
        cands.sort(key=lambda x: (-x[1], -x[2], -x[3], -x[4], -x[5]))

        if d not in visible_set:
            continue

        leaders = []
        for code, boards, _, vol, pct, _ in cands[: max(1, int(top_k))]:
            leaders.append(
                {
                    "code": code,
                    "name": name_map.get(code, ""),
                    "consecutive_boards": int(boards),
                    "pct_change": round(pct, 2),
                    "volume": round(vol, 2),
                }
            )
        top1 = leaders[0] if leaders else None
        daily_out.append(
            {
                "date": d,
                "limit_up_count": len(cands),
                "top1": top1,
                "leaders": leaders,
            }
        )
        top1_series.append((d, top1["code"] if top1 else None))

    segments: List[dict] = []
    i = 0
    while i < len(top1_series):
        _, c0 = top1_series[i]
        if c0 is None:
            i += 1
            continue
        j = i
        while j + 1 < len(top1_series) and top1_series[j + 1][1] == c0:
            j += 1
        d_start = top1_series[i][0]
        d_end = top1_series[j][0]
        segments.append(
            {
                "start_date": d_start,
                "end_date": d_end,
                "code": c0,
                "name": name_map.get(c0, ""),
                "days": j - i + 1,
            }
        )
        i = j + 1

    for seg in segments:
        mx = 0
        sd, ed = seg["start_date"], seg["end_date"]
        scode = seg["code"]
        for row in daily_out:
            if row["date"] < sd or row["date"] > ed:
                continue
            t1 = row.get("top1")
            if t1 and t1.get("code") == scode:
                mx = max(mx, int(t1.get("consecutive_boards") or 0))
        seg["max_consecutive_boards"] = mx

    return {
        "daily": daily_out,
        "segments": segments,
        "limit_pct": limit_pct,
        "top_k": top_k,
        "note": (
            "主线龙头=当日各股近似涨停池（主板约9.8%、300/301/688/689约19.85%）中连板数最高者；"
            "同连板比「收盘×成交量」代理成交额、再比涨幅与量能。连板在滚动缓存全部交易日上累计，图表仅展示最近窗口。"
        ),
    }


def _analyze_stock_cycle(stock_code: str, days: int = 180) -> Dict[str, Any]:
    """
    个股周期识别（简化版）：
    - 当5日累计涨幅 >= 15% 且窗口内至少1次涨停，视为主升窗口；
    - 连续窗口会合并为周期区间。
    """
    stock_code = (stock_code or "").strip()
    name_map = _load_stock_name_map()
    stock_name = name_map.get(stock_code, "")
    if len(stock_code) != 6:
        return {"code": stock_code, "name": stock_name, "periods": [], "phase_segments": [], "series": []}

    files_by_code = _latest_cache_file_by_code()
    fp = files_by_code.get(stock_code)
    if not fp:
        return {"code": stock_code, "name": stock_name, "periods": [], "phase_segments": [], "series": []}

    rows = _load_rows_from_cache(fp)
    if not rows:
        return {"code": stock_code, "name": stock_name, "periods": [], "phase_segments": [], "series": []}

    series = []
    for r in rows:
        ds = _norm_date(r.get("日期"))
        if not ds:
            continue
        open_p = _safe_float(r.get("开盘"), 0.0)
        high_p = _safe_float(r.get("最高"), 0.0)
        low_p = _safe_float(r.get("最低"), 0.0)
        close_p = _safe_float(r.get("收盘"), 0.0)
        series.append({
            "date": ds,
            "pct_change": _safe_float(r.get("涨跌幅"), 0.0),
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": _safe_float(r.get("成交量"), 0.0),
        })
    series = sorted(series, key=lambda x: x["date"])[-max(20, int(days)):]
    if len(series) < 10:
        return {"code": stock_code, "name": stock_name, "periods": [], "phase_segments": [], "series": series}

    flags = []
    for i in range(len(series)):
        if i < 4:
            flags.append(False)
            continue
        window = series[i - 4:i + 1]
        base = window[0]["close"]
        last = window[-1]["close"]
        if base <= 0:
            flags.append(False)
            continue
        ret5 = (last - base) / base * 100
        has_limit = any(x["pct_change"] >= 9.8 for x in window)
        flags.append(ret5 >= 15 and has_limit)

    periods = []
    start_idx = None
    for i, f in enumerate(flags):
        if f and start_idx is None:
            start_idx = i
        if (not f) and start_idx is not None:
            end_idx = i - 1
            if end_idx - start_idx + 1 >= 3:
                periods.append((start_idx, end_idx))
            start_idx = None
    if start_idx is not None:
        end_idx = len(flags) - 1
        if end_idx - start_idx + 1 >= 3:
            periods.append((start_idx, end_idx))

    out_periods = []
    for s, e in periods:
        p_window = series[s:e + 1]
        if not p_window:
            continue
        start_date = p_window[0]["date"]
        end_date = p_window[-1]["date"]
        base = p_window[0]["close"]
        last = p_window[-1]["close"]
        gain = round((last - base) / base * 100, 2) if base > 0 else 0.0
        out_periods.append({
            "start_date": start_date,
            "end_date": end_date,
            "days": len(p_window),
            "gain_pct": gain,
            "label": f"{stock_code}主升周期",
        })

    # 构造阶段分段（用于前端背景框）
    phase_segments = []
    used = [False] * len(series)
    for s, e in periods:
        for i in range(s, e + 1):
            used[i] = True

    i = 0
    while i < len(series):
        start = i
        if used[i]:
            while i + 1 < len(series) and used[i + 1]:
                i += 1
            phase_segments.append({
                "start_date": series[start]["date"],
                "end_date": series[i]["date"],
                "label": "主升",
                "kind": "rally",
            })
        else:
            while i + 1 < len(series) and not used[i + 1]:
                i += 1
            seg = series[start:i + 1]
            base = seg[0]["close"] if seg and seg[0]["close"] > 0 else 0
            last = seg[-1]["close"] if seg else 0
            ret = ((last - base) / base * 100) if base > 0 else 0.0
            if ret >= 5:
                label, kind = "首升", "warmup"
            elif ret <= -5:
                label, kind = "退潮", "cooldown"
            else:
                label, kind = "震荡", "range"
            phase_segments.append({
                "start_date": series[start]["date"],
                "end_date": series[i]["date"],
                "label": label,
                "kind": kind,
            })
        i += 1

    return {"code": stock_code, "name": stock_name, "periods": out_periods, "phase_segments": phase_segments, "series": series}


def analyze_emotion_cycle(
    days: int = 120,
    stock_code: str = "",
    force_refresh: bool = False,
) -> Dict[str, Any]:
    try:
        from data_fetcher import purge_stock_data_dir_main_board_only_once

        purge_stock_data_dir_main_board_only_once()
    except Exception:
        pass

    latest_date, snapshot = _build_latest_snapshot()
    if not latest_date or not snapshot:
        raise RuntimeError("未读取到有效缓存数据，请先更新缓存。")

    cached = _try_load_emotion_report_cache(days, stock_code, force_refresh)
    if cached is not None:
        out = dict(cached)
        out["date"] = latest_date
        return out

    name_map = _load_stock_name_map()
    market = _calc_market_metrics(snapshot)
    market_score, total_score = _calc_market_emotion_score(market)
    cycle = _judge_cycle(total_score)

    daily_snapshots, rolling_meta, by_date_for_leaders = _get_daily_snapshots_rolling(
        days=days, force_refresh=force_refresh
    )
    timeline = []
    for d in sorted(daily_snapshots.keys()):
        d_snapshot = daily_snapshots[d]
        d_market = _calc_market_metrics(d_snapshot)
        d_market_score, d_total = _calc_market_emotion_score(d_market)
        timeline.append({
            "date": d,
            "market_score": d_market_score,
            "total_score": d_total,
            "cycle": _judge_cycle(d_total),
            "limit_up_count": d_market["limit_up_count"],
            "strong_count": d_market["strong_count"],
            "big_drop_count": d_market["big_drop_count"],
            "avg_pct_change": d_market["avg_pct_change"],
        })

    stock_cycle = _analyze_stock_cycle(stock_code=stock_code, days=max(days, 120)) if stock_code else {
        "code": "",
        "name": "",
        "periods": [],
        "phase_segments": [],
        "series": [],
    }
    market_leader_rotation = _analyze_market_leader_rotation(
        by_date_for_leaders, name_map, visible_days=days
    )

    report = {
        "date": latest_date,
        "market_metrics": market,
        "scores": {
            "market_score": market_score,
            "total_score": total_score,
        },
        "cycle": cycle,
        "timeline": timeline,
        "stock_cycle": stock_cycle,
        "market_leader_rotation": market_leader_rotation,
        "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeline_rolling_meta": rolling_meta,
    }
    _save_emotion_report_cache(report, days, stock_code)
    return report


def get_emotion_cycle_health() -> Dict[str, Any]:
    """返回情绪周期数据可用性自检信息（全市场缓存截面）。"""
    files_by_code = _latest_cache_file_by_code()
    latest_date, latest_snapshot = _build_latest_snapshot()

    name_map = _load_stock_name_map()
    sample_codes = sorted(list(latest_snapshot.keys()))[:12] if latest_snapshot else []
    sample_stocks = [{"code": c, "name": name_map.get(c, "")} for c in sample_codes]

    return {
        "cache_dir": CACHE_DIR,
        "cache_file_count": len(files_by_code),
        "latest_date": latest_date,
        "latest_snapshot_size": len(latest_snapshot),
        "sample_codes": sample_codes,
        "sample_stocks": sample_stocks,
    }

