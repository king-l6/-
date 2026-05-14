"""
A股数据获取器 - 股票列表与日 K 均使用 AkShare。
"""
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
import time
import os
import json
import glob
import re
import threading
from typing import Optional

_MAIN_BOARD_CACHE_PURGED_ONCE = False

# 东财 stock_zh_a_hist 批量拉取时：累计「单只股票本次调用未拿到可用日K」达阈值后，本会话不再请求东财（改新浪/腾讯）。
EASTMONEY_HIST_CB_THRESHOLD = 50
_eastmoney_hist_cb_lock = threading.Lock()
_eastmoney_hist_cb_failures = 0
_eastmoney_hist_cb_open = False
_eastmoney_hist_cb_logged = False


def _eastmoney_hist_cb_is_open() -> bool:
    with _eastmoney_hist_cb_lock:
        return _eastmoney_hist_cb_open


def _eastmoney_hist_cb_record_stock_failure() -> None:
    """东财路径对当前 code 用尽重试仍未产出可用 DataFrame 时记一次（多线程安全）。"""
    global _eastmoney_hist_cb_failures, _eastmoney_hist_cb_open, _eastmoney_hist_cb_logged
    with _eastmoney_hist_cb_lock:
        if _eastmoney_hist_cb_open:
            return
        _eastmoney_hist_cb_failures += 1
        if _eastmoney_hist_cb_failures >= EASTMONEY_HIST_CB_THRESHOLD:
            _eastmoney_hist_cb_open = True
            if not _eastmoney_hist_cb_logged:
                _eastmoney_hist_cb_logged = True
                print(
                    f'[INFO] 东财日K(stock_zh_a_hist)累计失败已达 {EASTMONEY_HIST_CB_THRESHOLD} 只，'
                    '本会话后续将跳过东财，直接使用新浪/腾讯'
                )

# 新缓存文件名：{code}_{start}.json，结束日在文件内 end_date；增量补数不改路径，避免 Git 大量删建。
_RE_CACHE_END_DATE = re.compile(r'"end_date"\s*:\s*"(\d{8})"')


def parse_stock_data_cache_basename(stem):
    """basename 去掉 .json。返回 (code, start_ymd, end_ymd|None)；None 表示新格式，结束日读文件。"""
    if '_' not in stem:
        return None
    parts = stem.split('_')
    if len(parts) == 3:
        code, s, e = parts
        if len(code) == 6 and len(s) == 8 and len(e) == 8 and s.isdigit() and e.isdigit():
            return code, s, e
    if len(parts) == 2:
        code, s = parts
        if len(code) == 6 and len(s) == 8 and s.isdigit():
            return code, s, None
    return None


def read_stock_cache_end_ymd_quick(fp):
    """从 json 文本前部读取 end_date（落盘时 meta 在 data 之前）；失败返回 None。"""
    try:
        with open(fp, 'r', encoding='utf-8') as f:
            chunk = f.read(262144)
        m = _RE_CACHE_END_DATE.search(chunk)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


def cache_span_from_filename_or_file(fp, stem=None):
    """返回 (code, start_ymd, end_ymd) 供索引与区间判断；end 优先来自文件名（旧格式），否则读文件头。"""
    if stem is None:
        stem = os.path.basename(fp)[:-5]
    parsed = parse_stock_data_cache_basename(stem)
    if not parsed:
        return None
    code, start_str, end_str = parsed
    if end_str is None:
        end_str = read_stock_cache_end_ymd_quick(fp)
    if not end_str or len(end_str) != 8 or not end_str.isdigit():
        return None
    return code, start_str, end_str

from stock_code_utils import (
    is_likely_index_code_name,
    is_main_board_equity_code,
    load_main_board_codes_whitelist,
    universe_exclusion_reason,
)
from akshare_setup import configure_akshare_http
from stock_list_sources import fetch_main_board_stocks_akshare


def _ak_exchange_prefixed_symbol(code: str) -> str:
    """6 位 A 股代码 -> 新浪/腾讯接口所需的 sh600000 / sz000001 形式。"""
    if not code or len(code) != 6 or not code.isdigit():
        return ''
    if code.startswith('6'):
        return f'sh{code}'
    return f'sz{code}'


def _finalize_hist_df_from_chinese_columns(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """将已统一为中文列名的日 K DataFrame 做类型与衍生字段处理（与各数据源对齐后调用）。"""
    if df is None or df.empty:
        return None
    need = {'日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额'}
    if not need.issubset(df.columns):
        return None
    df = df.copy()
    df['日期'] = pd.to_datetime(df['日期'])
    for col in ['开盘', '收盘', '最高', '最低', '成交量', '成交额']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    if '涨跌幅' in df.columns:
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
    else:
        df['涨跌幅'] = 0.0
    if '换手率' in df.columns:
        df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce').fillna(0)
    else:
        df['换手率'] = 0.0
    if '涨跌额' not in df.columns:
        df['涨跌额'] = df['收盘'].diff().fillna(0)
    else:
        df['涨跌额'] = pd.to_numeric(df['涨跌额'], errors='coerce').fillna(0)
    if '振幅' not in df.columns:
        df['振幅'] = ((df['最高'] - df['最低']) / df['最低'].replace(0, float('nan')) * 100).fillna(0)
    else:
        df['振幅'] = pd.to_numeric(df['振幅'], errors='coerce').fillna(0)
    if df['涨跌幅'].abs().sum() == 0:
        prev = df['收盘'].shift(1)
        df['涨跌幅'] = ((df['收盘'] - prev) / prev.replace(0, float('nan')) * 100).fillna(0)
    df['成交量'] = df['成交量'].astype(float)
    df = df[['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]
    df = df.drop_duplicates(subset=['日期'], keep='first')
    return df.sort_values('日期').reset_index(drop=True)


def merge_sina_spot_into_hist_df(
    df: pd.DataFrame,
    trading_date_str: str,
    spot_row: dict,
) -> Optional[pd.DataFrame]:
    """
    将新浪 stock_zh_a_spot 单行并入日 K：trading_date 当日若已有则替换，否则追加，再走 _finalize 与缓存列一致。
    spot_row 键名与 AkShare 一致：最新价、今开、最高、最低、成交量、成交额、涨跌幅、昨收。
    用于盘中快照近似「当日 K」后跑与日线相同的策略指标（不等同于收盘后正式日 K）。
    """
    if df is None or df.empty or not spot_row:
        return None
    t = pd.Timestamp(str(trading_date_str)[:10])
    work = df.copy()
    work['日期'] = pd.to_datetime(work['日期'])
    t_norm = t.normalize()
    mask = work['日期'].dt.normalize() != t_norm
    base = work.loc[mask]
    prev = base.tail(1)
    prev_close = float(prev['收盘'].iloc[-1]) if not prev.empty else float(spot_row.get('昨收') or 0)
    close_v = float(spot_row.get('最新价') or 0)
    open_v = float(spot_row.get('今开') or 0)
    high_v = float(spot_row.get('最高') or 0)
    low_v = float(spot_row.get('最低') or 0)
    vol = float(spot_row.get('成交量') or 0)
    amt = float(spot_row.get('成交额') or 0)
    raw_pct = spot_row.get('涨跌幅')
    if raw_pct is None or (isinstance(raw_pct, float) and pd.isna(raw_pct)):
        pc = float(spot_row.get('昨收') or prev_close)
        pct = ((close_v - pc) / pc * 100.0) if pc > 0 else 0.0
    else:
        pct = float(raw_pct)
    append_df = pd.DataFrame(
        [
            {
                '日期': t_norm,
                '开盘': open_v,
                '收盘': close_v,
                '最高': high_v,
                '最低': low_v,
                '成交量': vol,
                '成交额': amt,
                '涨跌幅': pct,
            }
        ]
    )
    combined = pd.concat([base, append_df], ignore_index=True)
    return _finalize_hist_df_from_chinese_columns(combined)


class DataFetcher:
    """A股数据获取器 - AkShare"""

    def __init__(self):
        self.stock_list_cache = None
        self.stock_list_cache_time = None
        self.cache_duration = 3600

        self.cache_dir = os.path.join(os.path.dirname(__file__), 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.stock_list_cache_file = os.path.join(self.cache_dir, 'stock_list.json')
        self.stock_data_cache_dir = os.path.join(self.cache_dir, 'stock_data')
        os.makedirs(self.stock_data_cache_dir, exist_ok=True)

        # 缓存文件索引机制（性能优化）
        self._cache_index = None  # {code: [(start, end, path), ...]}
        self._cache_index_time = None  # 索引构建时间
        self._cache_index_ttl = 300  # 索引有效期（秒）

        # 交易日缓存机制（性能优化）
        self._trading_days_cache = {}  # {(start, end): [dates]}
        self._trading_days_full_cache = None  # 近N年完整交易日历
        self._trading_days_cache_time = None

        # _get_last_trading_day_available：000001 探测较贵，缓存「未裁切」结果；裁切仍按当前时刻每次重算
        self._lt_probe_memo: Optional[tuple[str, float]] = None
        self._lt_probe_memo_ttl_sec = 45.0
        self._lt_clip_log_key: Optional[tuple[str, str]] = None

        # 为 True 时 get_stock_data 仅用本地 json，不访问 AkShare；ensure_sufficient_data 直接跳过
        self.cache_only = False

    def _filter_stock_list_rows(self, stocks):
        """只保留主板个股，并剔除指数类（兼容旧 stock_list.json）。"""
        if not stocks:
            return stocks
        out = []
        for s in stocks:
            code = str(s.get('code') or '')
            name = str(s.get('name') or '')
            ex = s.get('exchange') or s.get('ex')
            ex = str(ex).strip().lower() if ex else None
            if not ex:
                ex = 'sh' if code.startswith('60') else 'sz'
            if universe_exclusion_reason(code, name, exchange=ex) is not None:
                continue
            out.append(s)
        return out

    def get_stock_list(self):
        """获取所有主板A股股票列表"""
        purge_stock_data_dir_main_board_only_once()

        # 检查内存缓存
        if (self.stock_list_cache is not None and
                self.stock_list_cache_time is not None and
                (datetime.now() - self.stock_list_cache_time).seconds < self.cache_duration):
            filtered = self._filter_stock_list_rows(self.stock_list_cache)
            print(f"[DEBUG] 使用内存缓存，共 {len(filtered)} 只股票")
            return filtered

        # 检查文件缓存（包括过期缓存，作为备用）
        fallback_stocks = None
        fallback_cache_time = None
        try:
            if os.path.exists(self.stock_list_cache_file):
                print(f"[DEBUG] 尝试从文件缓存加载股票列表: {self.stock_list_cache_file}")
                with open(self.stock_list_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    cache_time = datetime.fromisoformat(cache_data['cache_time'])
                    cache_age = (datetime.now() - cache_time).total_seconds()
                    print(f"[DEBUG] 缓存文件时间: {cache_time}, 缓存年龄: {cache_age/3600:.2f} 小时")
                    stocks = self._filter_stock_list_rows(cache_data.get('stocks', []))
                    if stocks:
                        fallback_stocks = stocks
                        fallback_cache_time = cache_time
                    if cache_age < 86400:  # 24小时内有效
                        if stocks:
                            print(f"[INFO] 从文件缓存加载股票列表（缓存时间: {cache_time}），共 {len(stocks)} 只股票")
                            self.stock_list_cache = stocks
                            self.stock_list_cache_time = cache_time
                            return stocks
                    else:
                        print(f"[DEBUG] 缓存文件已过期（超过24小时），尝试重新获取，如有失败将使用过期缓存")
        except Exception as e:
            print(f"[WARNING] 读取缓存文件失败: {e}")

        if getattr(self, 'cache_only', False):
            if fallback_stocks:
                self.stock_list_cache = fallback_stocks
                self.stock_list_cache_time = fallback_cache_time or datetime.now()
                print(
                    f'[INFO] cache_only：股票列表仅用本地 stock_list.json（不访问 AkShare），共 {len(fallback_stocks)} 只',
                    flush=True,
                )
                return fallback_stocks
            print('[ERROR] cache_only 模式需要有效的 cache/stock_list.json', flush=True)
            return []

        stock_list = fetch_main_board_stocks_akshare() or []
        if stock_list:
            print(
                f'[INFO] 股票列表来自 AkShare（沪深主板），共 {len(stock_list)} 只',
                flush=True,
            )
            self.stock_list_cache = stock_list
            self.stock_list_cache_time = datetime.now()
            with open(self.stock_list_cache_file, 'w', encoding='utf-8') as f:
                json.dump(
                    {'cache_time': datetime.now().isoformat(), 'stocks': stock_list},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            print(f"[INFO] 获取 {len(stock_list)} 只主板股票")
            return stock_list

        print('[WARNING] AkShare 沪深主板列表不可用或为空，尝试使用过期缓存', flush=True)
        if fallback_stocks:
            print(f"[INFO] 使用过期缓存（{len(fallback_stocks)} 只股票）")
            self.stock_list_cache = fallback_stocks
            self.stock_list_cache_time = fallback_cache_time
            return fallback_stocks

        print('[ERROR] 无法获取股票列表，且无可用缓存')
        return []

    def _get_cache_path(self, code, start_date, end_date=None):
        """落盘路径仅用起始日；end_date 仅兼容旧调用，不参与命名。"""
        sd = str(start_date).replace('-', '')[:8]
        return os.path.join(self.stock_data_cache_dir, f"{code}_{sd}.json")

    def _load_from_cache_file(self, cache_path):
        """从缓存文件加载 DataFrame，失败返回 None"""
        try:
            if not cache_path or not os.path.exists(cache_path) or os.path.getsize(cache_path) <= 100:
                return None
            try:
                import orjson
                with open(cache_path, 'rb') as f:
                    cache_data = orjson.loads(f.read())
            except ImportError:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
            if not cache_data.get('data'):
                return None
            cache_time = datetime.fromisoformat(cache_data['cache_time'])
            if (datetime.now() - cache_time).total_seconds() >= 604800:
                return None
            df = pd.DataFrame(cache_data['data'])
            df['日期'] = pd.to_datetime(df['日期'])
            return df
        except Exception:
            return None

    def _get_last_trading_day(self):
        """获取最近的 A 股交易日（周一至周五，不考虑节假日）"""
        d = datetime.now().date()
        # weekday: 0=周一, 6=周日
        if d.weekday() == 5:  # 周六
            return (d - timedelta(days=1)).strftime('%Y-%m-%d')
        if d.weekday() == 6:  # 周日
            return (d - timedelta(days=2)).strftime('%Y-%m-%d')
        return d.strftime('%Y-%m-%d')

    def _local_fallback_last_trade_date_from_stock_cache(self):
        """AkShare 不可用时：用本地 stock_data 各缓存文件中的 end_date（经索引）的最大值；无则试 000001 文件内最后一条日期。"""
        try:
            idx = self._build_cache_index()
            mx = None
            for spans in idx.values():
                for _start_str, end_str, _fp in spans:
                    if end_str and len(end_str) == 8 and end_str.isdigit():
                        if mx is None or end_str > mx:
                            mx = end_str
            if mx:
                return f'{mx[:4]}-{mx[4:6]}-{mx[6:8]}'
        except Exception:
            pass
        return self.get_local_cache_latest_date()

    @staticmethod
    def _calendar_prev_weekday(d):
        """上一工作日（跳过周六日）。"""
        x = d - timedelta(days=1)
        while x.weekday() >= 5:
            x -= timedelta(days=1)
        return x

    def _clip_last_trade_before_market_close(self, last_trade: str) -> str:
        """
        A 股日 K 收盘前数据源可能已出现「当天」不完整 K 线；在本地时间未到收盘参考点之前，
        不把「日历当天」当作可依赖的最后一根完整日 K。

        - DATA_FETCH_EOD_HHMM：默认 15:10（本地时区，与 datetime.now() 一致）
        - DATA_FETCH_DISABLE_INTRADAY_CAP=1：关闭本条，恢复仅依赖 000001 探测
        """
        v = os.environ.get('DATA_FETCH_DISABLE_INTRADAY_CAP', '').strip().lower()
        if v in ('1', 'true', 'yes', 'on'):
            return last_trade
        try:
            d_ref = datetime.strptime(str(last_trade).strip()[:10], '%Y-%m-%d').date()
        except ValueError:
            return last_trade
        now = datetime.now()
        today = now.date()
        if d_ref != today:
            return last_trade
        if today.weekday() >= 5:
            return last_trade
        raw = os.environ.get('DATA_FETCH_EOD_HHMM', '15:10').strip() or '15:10'
        parts = raw.replace('：', ':').split(':')
        try:
            eh = int(parts[0])
            em = int(parts[1]) if len(parts) > 1 else 0
        except (ValueError, IndexError):
            eh, em = 15, 10
        eh = max(0, min(eh, 23))
        em = max(0, min(em, 59))
        if now.time() >= dt_time(eh, em):
            return last_trade
        prev = self._calendar_prev_weekday(today)
        return prev.strftime('%Y-%m-%d')

    def _get_last_trading_day_available(self):
        """获取 AkShare 已有日 K 的最新交易日（当日数据通常收盘后才更新；用 000001 探测）。"""
        now_m = time.monotonic()
        memo = self._lt_probe_memo
        out: Optional[str] = None
        if memo is not None:
            raw, t0 = memo
            if now_m - t0 < self._lt_probe_memo_ttl_sec:
                out = raw

        if out is None:
            last_trade = self._get_last_trading_day()
            last_trade_str = last_trade.replace('-', '')

            def _fallback():
                loc = self._local_fallback_last_trade_date_from_stock_cache()
                if loc:
                    print(
                        f'[INFO] AkShare 探测 000001 失败或当日无 K，改用本地缓存推断「最近交易日参照」: {loc}',
                        flush=True,
                    )
                    return loc
                print(
                    f'[WARN] AkShare 与本地缓存均无法确定最近交易日，退回日历近似值: {last_trade}',
                    flush=True,
                )
                return last_trade

            try:
                df = self._fetch_from_akshare('000001', last_trade_str, last_trade_str)
                if df is not None and not df.empty:
                    out = last_trade
                else:
                    d = datetime.strptime(last_trade, '%Y-%m-%d').date()
                    for _ in range(10):
                        d = d - timedelta(days=1)
                        if d.weekday() < 5:
                            check = d.strftime('%Y%m%d')
                            df = self._fetch_from_akshare('000001', check, check)
                            if df is not None and not df.empty:
                                out = d.strftime('%Y-%m-%d')
                                break
                    if out is None:
                        out = _fallback()
            except Exception as e:
                print(f'[WARNING] 获取最近可用交易日异常: {e}', flush=True)
                out = _fallback()
            self._lt_probe_memo = (out, time.monotonic())
            self._lt_clip_log_key = None

        clipped = self._clip_last_trade_before_market_close(out)
        if clipped != out:
            key = (out, clipped)
            if self._lt_clip_log_key != key:
                self._lt_clip_log_key = key
                print(
                    f'[INFO] 收盘前口径：最近可用交易日由 {out} 调整为 {clipped}（避免中午不完整日 K）；'
                    f'可调 DATA_FETCH_EOD_HHMM 或设 DATA_FETCH_DISABLE_INTRADAY_CAP=1。',
                    flush=True,
                )
        return clipped

    # ==================== 缓存文件索引机制（性能优化）====================

    def _build_cache_index(self):
        """构建或更新缓存索引，返回 {code: [(start, end, path), ...]}"""
        now = time.time()
        # 如果索引有效，直接返回
        if self._cache_index is not None and self._cache_index_time is not None:
            if now - self._cache_index_time< self._cache_index_ttl:
                return self._cache_index

        # 扫描缓存目录构建索引
        pattern = os.path.join(self.stock_data_cache_dir, '*.json')
        files = glob.glob(pattern)

        index = {}
        for fp in files:
            name = os.path.basename(fp)
            if '_' not in name or not name.endswith('.json'):
                continue
            span = cache_span_from_filename_or_file(fp, name[:-5])
            if not span:
                continue
            code, start_str, end_str = span
            index.setdefault(code, []).append((start_str, end_str, fp))

        self._cache_index = index
        self._cache_index_time = now
        return index

    def get_cache_files_for_code(self, code):
        """获取指定股票的缓存文件列表 [(start, end, path), ...]"""
        index = self._build_cache_index()
        return index.get(code, [])

    def _list_stock_cache_files(self, code):
        """列出某只股票的 K 线缓存 json 路径（优先走内存索引，避免每次 glob 全目录）。"""
        try:
            rows = self.get_cache_files_for_code(code)
            paths = [t[2] for t in rows if t and len(t) > 2]
            if paths:
                return paths
        except Exception:
            pass
        pattern = os.path.join(self.stock_data_cache_dir, f'{code}_*.json')
        return glob.glob(pattern)

    def get_all_cache_codes(self):
        """获取所有已缓存的股票代码集合"""
        index = self._build_cache_index()
        return set(index.keys())

    def invalidate_cache_index(self):
        """使缓存索引失效（在新增/删除缓存文件后调用）"""
        self._cache_index = None
        self._cache_index_time = None

    # ==================== 交易日缓存机制（性能优化）====================

    def _fetch_trading_days_from_api(self, start_s, end_s):
        """从 AkShare（000001 日 K）提取交易日列表。"""
        try:
            start_fmt = start_s.replace('-', '')[:8]
            end_fmt = end_s.replace('-', '')[:8]
            df = self._fetch_from_akshare('000001', start_fmt, end_fmt)
            if df is None or df.empty or '日期' not in df.columns:
                return []
            out = []
            for v in df['日期']:
                if hasattr(v, 'strftime'):
                    out.append(v.strftime('%Y-%m-%d'))
                else:
                    s = str(v)[:10]
                    if len(s) == 10 and s[4] == '-' and s[7] == '-':
                        out.append(s)
            return sorted(out)
        except Exception:
            return []

    def preload_trading_days_calendar(self, years=3):
        """预加载近N年的交易日历到内存"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 366)
        start_s = start_date.strftime('%Y-%m-%d')
        end_s = end_date.strftime('%Y-%m-%d')

        # 检查是否已有缓存
        if self._trading_days_full_cache is not None:
            cache_age = (datetime.now() - self._trading_days_cache_time).total_seconds() if self._trading_days_cache_time else float('inf')
            if cache_age< 86400:  # 1天内有效
                return self._trading_days_full_cache

        print(f'[INFO] 预加载交易日历: {start_s} 至 {end_s}')
        result = self._fetch_trading_days_from_api(start_s, end_s)
        if result:
            self._trading_days_full_cache = result
            self._trading_days_cache_time = datetime.now()
            print(f'[INFO] 交易日历已缓存，共 {len(result)} 个交易日')
        return result

    def get_trading_days_between(self, start_date, end_date):
        """获取 start_date 到 end_date 之间的交易日列表（含首尾），返回 ['YYYY-MM-DD', ...]。

        优化：优先使用内存缓存，避免重复网络请求。
        """
        try:
            start_s = start_date[:10] if isinstance(start_date, str) else start_date.strftime('%Y-%m-%d')
            end_s = end_date[:10] if isinstance(end_date, str) else end_date.strftime('%Y-%m-%d')

            # 1. 检查内存缓存
            cache_key = (start_s, end_s)
            if cache_key in self._trading_days_cache:
                return self._trading_days_cache[cache_key]

            # 2. 尝试从完整日历中截取
            if self._trading_days_full_cache is not None:
                full = self._trading_days_full_cache
                if full and full[0] <= start_s <= full[-1] and full[0] <= end_s <= full[-1]:
                    result = [d for d in full if start_s <= d <= end_s]
                    self._trading_days_cache[cache_key] = result
                    return result

            # 3. cache_only：仅从本地 K 缓存推导交易日，不打 000001
            if getattr(self, 'cache_only', False):
                result = self.get_trading_days_from_cache(start_s, end_s) or []
                if result:
                    self._trading_days_cache[cache_key] = result
                return result

            # 4. 调用 API 获取
            result = self._fetch_trading_days_from_api(start_s, end_s)
            if result:
                self._trading_days_cache[cache_key] = result
            return result
        except Exception:
            return []

    def get_trading_days_from_cache(self, start_date, end_date):
        """从本地缓存的股票 K 线中提取交易日序列（兜底用）。返回 [start_date, end_date] 内含首尾的交易日列表。
        「连续三个交易日」即该列表中连续的三天。"""
        try:
            start_s = start_date[:10] if isinstance(start_date, str) else start_date
            end_s = end_date[:10] if isinstance(end_date, str) else end_date
            pattern = os.path.join(self.stock_data_cache_dir, '*.json')
            files = glob.glob(pattern)
            all_dates = set()
            for fp in files:
                try:
                    base = os.path.basename(fp)
                    if '_' in base and base.endswith('.json'):
                        code0 = base.split('_')[0]
                        wl = load_main_board_codes_whitelist(self.stock_list_cache_file)
                        if wl is not None:
                            if code0 not in wl:
                                continue
                        elif not is_main_board_equity_code(code0, None):
                            continue
                    with open(fp, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    rows = data.get('data') or []
                    for r in rows:
                        ds = r.get('日期')
                        if not ds:
                            continue
                        if isinstance(ds, str) and len(ds) >= 10 and ds[4] == '-' and ds[7] == '-':
                            d = ds[:10]
                        else:
                            d = pd.to_datetime(ds).strftime('%Y-%m-%d')
                        if start_s <= d <= end_s:
                            all_dates.add(d)
                except Exception:
                    continue
            return sorted(all_dates) if all_dates else []
        except Exception:
            return []

    def get_local_cache_latest_date(self):
        """获取本地缓存中最新一条数据的日期，无缓存返回 None"""
        try:
            # 以 000001 为代表检查
            pattern = os.path.join(self.stock_data_cache_dir, '000001_*.json')
            files = glob.glob(pattern)
            if not files:
                return None
            latest_dt = None
            for fp in files:
                try:
                    with open(fp, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    rows = data.get('data') or []
                    for r in rows:
                        ds = r.get('日期')
                        if ds:
                            dt = pd.to_datetime(ds)
                            if latest_dt is None or dt > latest_dt:
                                latest_dt = dt
                except Exception:
                    continue
            return latest_dt.strftime('%Y-%m-%d') if latest_dt is not None else None
        except Exception:
            return None

    def need_fetch_recent_data(self):
        """本地缓存最新日期是否小于最近交易日，若是则需要拉取近一个月数据"""
        if getattr(self, 'cache_only', False):
            return False
        last_trade = self._get_last_trading_day_available()
        cache_latest = self.get_local_cache_latest_date()
        if cache_latest is None:
            return True
        return cache_latest < last_trade

    def remove_non_mainboard_cache_files(self):
        """删除非主板个股及指数类 K 线缓存（仅依据文件名六位代码 + 可选 json 内 name）。"""
        try:
            pattern = os.path.join(self.stock_data_cache_dir, '*.json')
            deleted = 0
            for fp in glob.glob(pattern):
                base = os.path.basename(fp)
                if '_' not in base or not base.endswith('.json'):
                    continue
                parsed = parse_stock_data_cache_basename(base[:-5])
                if not parsed:
                    continue
                code = parsed[0]
                if len(code) != 6:
                    continue
                nm = ''
                try:
                    with open(fp, 'r', encoding='utf-8') as f:
                        meta = json.load(f)
                    nm = str(meta.get('name') or '')
                except Exception:
                    pass
                wl = load_main_board_codes_whitelist(self.stock_list_cache_file)
                if wl is not None:
                    if code in wl:
                        continue
                elif is_main_board_equity_code(code, None) and not is_likely_index_code_name(code, nm):
                    continue
                try:
                    os.remove(fp)
                    deleted += 1
                except Exception:
                    pass
            if deleted > 0:
                print(f'[INFO] 删除非主板/指数缓存 {deleted} 个文件', flush=True)
        except Exception as e:
            print(f'[WARNING] 清理非主板缓存失败: {e}', flush=True)

    def remove_duplicate_cache(self):
        """先删非主板/指数缓存，再删重复缓存：每只股票只保留一份（保留 start_date 最早的那份，覆盖范围最大）"""
        self.remove_non_mainboard_cache_files()
        try:
            pattern = os.path.join(self.stock_data_cache_dir, '*.json')
            files = glob.glob(pattern)
            # 按 code 分组: code -> [(start, end, path), ...]
            by_code = {}
            for fp in files:
                name = os.path.basename(fp)
                if '_' not in name or not name.endswith('.json'):
                    continue
                span = cache_span_from_filename_or_file(fp, name[:-5])
                if not span:
                    continue
                code, start_str, end_str = span
                by_code.setdefault(code, []).append((start_str, end_str, fp))

            deleted = 0
            for code, items in by_code.items():
                if len(items) <= 1:
                    continue
                # 保留 start_date 最早、end_date 最晚（若 start 相同）的那份
                items.sort(key=lambda x: (x[0], -int(x[1])))  # start 升序，end 降序
                keep_path = items[0][2]
                for _, _, path in items[1:]:
                    try:
                        os.remove(path)
                        deleted += 1
                    except Exception:
                        pass
            if deleted > 0:
                print(f"[INFO] 删除重复缓存 {deleted} 个文件")
        except Exception as e:
            print(f"[WARNING] 清理重复缓存失败: {e}")

    def _fetch_from_akshare(self, code, start_date, end_date):
        """从 AkShare 拉取日 K 并返回 DataFrame，不写缓存。

        默认数据源顺序：东方财富 stock_zh_a_hist → 新浪 stock_zh_a_daily → 腾讯 stock_zh_a_hist_tx（若存在）。
        单源内仍对瞬时网络错误重试；该源彻底失败再换下一源。
        环境变量 DATA_FETCH_STOCK_HIST_SOURCE：auto（默认）、sina|新浪、eastmoney|东财、tencent|tx 时仅使用该源。
        """
        configure_akshare_http()
        import akshare as ak

        start_fmt = start_date[:8]
        end_fmt = end_date[:8]
        d0 = pd.to_datetime(start_fmt, format='%Y%m%d')
        d1 = pd.to_datetime(end_fmt, format='%Y%m%d')

        def _is_transient_network_err(e):
            s = str(e).lower()
            return any(
                x in s
                for x in (
                    'remotedisconnected',
                    'connection aborted',
                    'connection reset',
                    'read timed out',
                    'timed out',
                    'empty reply',
                    'bad gateway',
                    '502',
                    '503',
                )
            )

        def _clip_date(df_in: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
            if df_in is None or df_in.empty or '日期' not in df_in.columns:
                return df_in
            dt = pd.to_datetime(df_in['日期'])
            m = (dt >= d0) & (dt <= d1)
            return df_in.loc[m].copy()

        def _pull_eastmoney() -> Optional[pd.DataFrame]:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period='daily',
                start_date=start_fmt,
                end_date=end_fmt,
                adjust='',
            )
            if df is None or df.empty:
                return None
            column_map = {
                '日期': '日期',
                '开盘': '开盘',
                '收盘': '收盘',
                '最高': '最高',
                '最低': '最低',
                '成交量': '成交量',
                '成交额': '成交额',
                '振幅': '振幅',
                '涨跌幅': '涨跌幅',
                '涨跌额': '涨跌额',
                '换手率': '换手率',
            }
            df = df.rename(columns=column_map)
            return _clip_date(df)

        def _pull_sina() -> Optional[pd.DataFrame]:
            fn = getattr(ak, 'stock_zh_a_daily', None)
            if not callable(fn):
                return None
            sym = _ak_exchange_prefixed_symbol(code)
            if not sym:
                return None
            raw = fn(symbol=sym, start_date=start_fmt, end_date=end_fmt, adjust='')
            if raw is None or raw.empty:
                return None
            col_en = {
                'date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量',
                'amount': '成交额',
                'turnover': '换手率',
            }
            df = raw.rename(columns={k: v for k, v in col_en.items() if k in raw.columns})
            if not {'日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额'}.issubset(df.columns):
                return None
            # 新浪 turnover 为成交量/流通股本，转成与东财一致的百分数口径
            if '换手率' in df.columns:
                df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce').fillna(0) * 100.0
            return _clip_date(df)

        def _pull_tencent() -> Optional[pd.DataFrame]:
            fn = getattr(ak, 'stock_zh_a_hist_tx', None)
            if not callable(fn):
                return None
            sym = _ak_exchange_prefixed_symbol(code)
            if not sym:
                return None
            raw = fn(symbol=sym, start_date=start_fmt, end_date=end_fmt, adjust='')
            if raw is None or raw.empty:
                return None
            # 腾讯接口列名为英文；文档标注 amount 单位为「手」，按 1 手=100 股换算成交量
            if 'date' not in raw.columns or 'open' not in raw.columns:
                return None
            if 'volume' in raw.columns:
                vol_shares = pd.to_numeric(raw['volume'], errors='coerce').fillna(0)
            elif 'amount' in raw.columns:
                # 文档标注 amount 单位为「手」
                vol_shares = pd.to_numeric(raw['amount'], errors='coerce').fillna(0) * 100.0
            else:
                return None
            df = pd.DataFrame(
                {
                    '日期': raw['date'],
                    '开盘': raw['open'],
                    '最高': raw['high'],
                    '最低': raw['low'],
                    '收盘': raw['close'],
                    '成交量': vol_shares,
                    '成交额': 0.0,
                    '涨跌幅': 0.0,
                    '换手率': 0.0,
                }
            )
            return _clip_date(df)

        hint = os.environ.get('DATA_FETCH_STOCK_HIST_SOURCE', 'auto').strip().lower()
        if hint in ('sina', '新浪'):
            sources = (('sina', _pull_sina),)
        elif hint in ('eastmoney', '东财'):
            sources = (('eastmoney', _pull_eastmoney),)
        elif hint in ('tencent', 'tx'):
            sources = (('tencent', _pull_tencent),)
        else:
            sources = (
                ('eastmoney', _pull_eastmoney),
                ('sina', _pull_sina),
                ('tencent', _pull_tencent),
            )
        if _eastmoney_hist_cb_is_open():
            sources = tuple(s for s in sources if s[0] != 'eastmoney')
        if not sources:
            print(
                '[ERROR] 日 K 数据源列表为空（例如东财熔断且当前仅配置东财）。'
                '请设置 DATA_FETCH_STOCK_HIST_SOURCE=sina 或稍后重试。',
                flush=True,
            )
            return None

        last_error: Optional[Exception] = None
        for i, (src_name, pull) in enumerate(sources):
            for attempt in range(3):
                try:
                    raw_df = pull()
                    if raw_df is not None and not raw_df.empty:
                        out = _finalize_hist_df_from_chinese_columns(raw_df)
                        if out is not None and not out.empty:
                            if i > 0:
                                print(f'[INFO] {code} 日K 使用备用数据源: {src_name}')
                            return out
                    break
                except Exception as e:
                    last_error = e
                    if _is_transient_network_err(e) and attempt < 2:
                        time.sleep(1.2 * (2 ** attempt))
                        continue
                    print(f'[DEBUG] AkShare({src_name}) 获取 {code} 失败: {e}')
                    break
            if src_name == 'eastmoney':
                _eastmoney_hist_cb_record_stock_failure()

        if last_error is not None:
            print(f'[DEBUG] AkShare 获取 {code} 全部数据源失败，末次错误: {last_error}')
        return None

    def _fetch_from_api(self, code, start_date, end_date):
        """从 AkShare 拉取数据并返回 DataFrame，不写缓存。"""
        return self._fetch_from_akshare(code, start_date, end_date)

    def update_caches_with_today_data(self, max_workers=100, task_index=None, task_count=None):
        """拉取今天（最近交易日）的数据，合并到对应的 json 缓存文件中
        
        Args:
            max_workers: 线程池最大并发数
            task_index: 多任务分片的当前任务下标（从 0 开始），默认为 None 表示不分片
            task_count: 多任务分片的总任务数（>1 时生效），默认为 None 表示不分片
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # 先清理重复缓存，避免更新时出现重复数据
        self.remove_duplicate_cache()

        # 使用 AkShare 已有数据的最新交易日（当日数据通常收盘后才更新）
        last_trade = self._get_last_trading_day_available()
        last_trade_str = last_trade.replace('-', '')

        # 根据缓存中实际最后交易日判断：若非上个交易日，则需拉取 [缓存最后日+1, 上个交易日]
        # 注：必须读取实际数据判断，不能依赖文件名 end_str（文件名可能与实际数据不一致）
        pattern = os.path.join(self.stock_data_cache_dir, '*.json')
        files = glob.glob(pattern)

        def _check_need_update(fp):
            try:
                name = os.path.basename(fp)
                if '_' not in name or not name.endswith('.json'):
                    return None
                parsed = parse_stock_data_cache_basename(name[:-5])
                if not parsed:
                    return None
                code, start_str, _ = parsed
                if len(code) != 6 or len(start_str) != 8:
                    return None
                with open(fp, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                rows = cache_data.get('data') or []
                if not rows:
                    return None
                actual_max = max(r['日期'][:10] for r in rows)
                # 缓存末尾晚于「最近完整收盘日」时多为盘中不完整日 K，需修剪或重拉
                if actual_max > last_trade:
                    return (code, start_str, '', fp)
                if actual_max >= last_trade:
                    return None
                return (code, start_str, '', fp)
            except Exception:
                return None

        need_update_list = []
        with ThreadPoolExecutor(max_workers=min(100, len(files) or 1)) as ex:
            for r in ex.map(_check_need_update, files):
                if r is not None:
                    need_update_list.append(r)

        by_code = {}
        for code, start_str, end_str, fp in need_update_list:
            if code not in by_code:
                by_code[code] = (start_str, end_str, fp)
            else:
                existing_start, _, _ = by_code[code]
                if start_str < existing_start:
                    by_code[code] = (start_str, end_str, fp)

        print(f'[INFO] 扫描 {len(files)} 个缓存，其中 {len(by_code)} 个需补齐至 {last_trade}')

        # 获取股票列表，并根据 task_index / task_count 进行分片（若启用）
        all_stocks = self.get_stock_list()
        selected_codes = None
        if task_index is not None and task_count is not None and task_count > 1:
            try:
                stocks_sorted = sorted(all_stocks, key=lambda s: s['code'])
                n = len(stocks_sorted)
                if n == 0:
                    selected_codes = set()
                else:
                    size = (n + task_count - 1) // task_count  # 向上取整分片大小
                    start = task_index * size
                    end = min(n, (task_index + 1) * size)
                    if start >= n:
                        selected_codes = set()
                    else:
                        selected_codes = {s['code'] for s in stocks_sorted[start:end]}
                print(f'[INFO] 多任务分片模式：task_index={task_index}, task_count={task_count}, 本任务负责 {len(selected_codes)} 只股票')
            except Exception as e:
                print(f'[WARNING] 计算多任务分片范围失败，将退回为全量模式: {e}')
                selected_codes = None
        else:
            print('[INFO] 未启用多任务分片（处理全部股票）')

        all_codes = {s['code'] for s in all_stocks}
        if selected_codes is not None:
            all_codes = all_codes & selected_codes
        cached_codes = set(by_code.keys())
        missing_codes = all_codes - cached_codes
        
        if missing_codes:
            print(f'[INFO] 发现 {len(missing_codes)} 只股票没有缓存文件，需要创建新缓存')
            # 为没有缓存的股票创建缓存（拉取近一个月数据）
            today = datetime.now()
            start_date = (today - timedelta(days=50)).strftime('%Y%m%d')
            # 右端与「最近可用完整交易日」对齐，避免中午用日历今天拉到不完整日 K
            end_date = last_trade_str
            
            def create_cache(code):
                try:
                    df = self.get_stock_data(code, start_date, end_date, force_refresh=True)
                    return code, df is not None and not df.empty
                except Exception:
                    return code, False
            
            print(f'[INFO] 为 {len(missing_codes)} 只股票创建缓存...')
            created = 0
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {ex.submit(create_cache, code): code for code in missing_codes}
                for i, future in enumerate(as_completed(futures)):
                    code, success = future.result()
                    if success:
                        created += 1
                    if (i + 1) % 50 == 0:
                        print(f'  进度: {i+1}/{len(missing_codes)} | 已创建: {created}', flush=True)
            print(f'[INFO] 已为 {created}/{len(missing_codes)} 只股票创建缓存')
            
            # 重新扫描缓存文件，按实际最后交易日判断是否需更新
            files = glob.glob(pattern)
            need_update_list2 = []
            with ThreadPoolExecutor(max_workers=min(100, len(files) or 1)) as ex:
                for r in ex.map(_check_need_update, files):
                    if r is not None:
                        need_update_list2.append(r)
            by_code = {}
            for code, start_str, end_str, fp in need_update_list2:
                if code not in by_code:
                    by_code[code] = (start_str, end_str, fp)
                else:
                    existing_start, _, _ = by_code[code]
                    if start_str < existing_start:
                        by_code[code] = (start_str, end_str, fp)

        # 若启用了分片，只保留当前任务负责的股票代码
        if selected_codes is not None:
            before = len(by_code)
            by_code = {code: v for code, v in by_code.items() if code in selected_codes}
            print(f'[INFO] 分片过滤：原待更新 {before} 只股票，本任务负责 {len(by_code)} 只')

        if not by_code:
            print('[INFO] 所有缓存已含最近交易日数据，或当前分片无待更新股票')
            return

        def update_one(code_start_end_path):
            code, start_str, end_str, fp = code_start_end_path
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                raw_rows = cache_data.get('data') or []
                if not raw_rows:
                    return code, False
                # 去掉晚于 last_trade 的行（中午拉到的「当天」不完整 K）
                rows = [r for r in raw_rows if str(r.get('日期', ''))[:10] <= last_trade]
                trimmed = len(rows) != len(raw_rows)
                if not rows:
                    return code, False
                max_dt = max(pd.to_datetime(r['日期']) for r in rows)
                fetch_start = (max_dt + timedelta(days=1)).strftime('%Y%m%d')
                if fetch_start > last_trade_str:
                    if not trimmed:
                        return code, False
                    df_merged = pd.DataFrame(rows)
                    df_merged['日期'] = pd.to_datetime(df_merged['日期'])
                    df_merged = df_merged.sort_values('日期').reset_index(drop=True)
                    new_end = max(r['日期'][:10] for r in rows).replace('-', '')
                    new_path = self._get_cache_path(code, start_str, new_end)
                    out = {
                        'cache_time': datetime.now().isoformat(),
                        'code': code, 'start_date': start_str, 'end_date': new_end,
                        'data': df_merged.to_dict('records')
                    }
                    with open(new_path, 'w', encoding='utf-8') as f:
                        json.dump(out, f, ensure_ascii=False, default=str)
                    if new_path != fp:
                        os.remove(fp)
                    return code, True

                df_new = self._fetch_from_api(code, fetch_start, last_trade_str)
                if df_new is None or df_new.empty:
                    return code, False

                df_old = pd.DataFrame(rows)
                df_old['日期'] = pd.to_datetime(df_old['日期'])
                df_merged = pd.concat([df_old, df_new], ignore_index=True)
                df_merged = df_merged.drop_duplicates(subset=['日期'], keep='last')
                df_merged = df_merged.sort_values('日期').reset_index(drop=True)

                new_end = last_trade_str
                new_path = self._get_cache_path(code, start_str, new_end)
                out = {
                    'cache_time': datetime.now().isoformat(),
                    'code': code, 'start_date': start_str, 'end_date': new_end,
                    'data': df_merged.to_dict('records')
                }
                with open(new_path, 'w', encoding='utf-8') as f:
                    json.dump(out, f, ensure_ascii=False, default=str)
                if new_path != fp:
                    os.remove(fp)
                return code, True
            except Exception:
                return code, False

        tasks = [(code, s, e, p) for code, (s, e, p) in by_code.items()]
        total = len(tasks)
        print(f'[INFO] 待更新 {total} 个缓存（缓存最后日 < 上个交易日 {last_trade}）')
        success = 0
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(update_one, t): t[0] for t in tasks}
            for i, future in enumerate(as_completed(futures)):
                _, ok = future.result()
                if ok:
                    success += 1
                step = max(1, total // 20)  # 至少每 5% 或更小集合每条
                if ((i + 1) % step == 0) or (i == total - 1):
                    print(f'进度: {i+1}/{total} | 已更新: {success}', flush=True)
        print(f'[INFO] 今日数据已落盘: 更新 {success}/{total} 个缓存')
        if success < total and total > 0:
            print(f'[TIP] 部分股票未更新可能因 AkShare 无数据（如 ST/退市股），可忽略')

    def ensure_sufficient_data(self, time_range, max_workers=100):
        """确保有足够的数据用于回测
        
        Args:
            time_range: 回测的交易日数（如30、60、90）
            max_workers: 并发线程数
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if getattr(self, 'cache_only', False):
            print('[INFO] cache_only：跳过 ensure_sufficient_data（不通过网络预拉取 K 线）')
            return
        
        # 计算需要的日历日数：约 1 交易日 ≈ 1.4 日历日，多取一些确保覆盖
        calendar_days = int(time_range * 1.6) + 10
        # 右端与 get_stock_data 一致：用「最近可用交易日」（含收盘前裁切）
        end_ref = self._get_last_trading_day_available()
        end_date = datetime.strptime(end_ref[:10], '%Y-%m-%d')
        required_start_date = (end_date - timedelta(days=calendar_days)).strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        
        print(f'[INFO] 检查数据完整性：回测需要 {time_range} 个交易日（约 {calendar_days} 个日历日）')
        print(f'[INFO] 需要的数据范围：{required_start_date} 至 {end_date_str}')
        
        # 获取所有股票
        all_stocks = self.get_stock_list()
        all_codes = [s['code'] for s in all_stocks]
        
        # 检查每只股票的缓存是否足够
        need_fetch_codes = []
        pattern = os.path.join(self.stock_data_cache_dir, '*.json')
        files = glob.glob(pattern)
        
        # 按股票代码分组，记录每只股票缓存的起止日期
        by_code = {}
        for fp in files:
            name = os.path.basename(fp)
            if '_' not in name or not name.endswith('.json'):
                continue
            span = cache_span_from_filename_or_file(fp, name[:-5])
            if not span:
                continue
            code, start_str, end_str = span
            # 记录最早 start 与最晚 end（同一股票可能存在多份缓存）
            if code not in by_code:
                by_code[code] = {'start': start_str, 'end': end_str}
            else:
                if start_str < by_code[code]['start']:
                    by_code[code]['start'] = start_str
                if end_str > by_code[code]['end']:
                    by_code[code]['end'] = end_str

        # 与 get_stock_data 右端一致：用「最近可用交易日」（含收盘前裁切），避免中午误判缺尾部日 K
        last_trade_ymd = self._get_last_trading_day_available().replace('-', '')[:8]

        # 检查哪些股票需要拉取更多数据
        for code in all_codes:
            if code not in by_code:
                # 没有缓存，需要拉取
                need_fetch_codes.append(code)
            else:
                cache_start = by_code[code]['start']
                cache_end = by_code[code]['end']

                # 若缓存已覆盖到最后交易日（本地有最后一根 K），不要求再拉尾部
                if last_trade_ymd and len(last_trade_ymd) == 8 and cache_end >= last_trade_ymd:
                    pass  # 尾部已够，仍要检查历史深度
                elif last_trade_ymd and len(last_trade_ymd) == 8:
                    need_fetch_codes.append(code)
                    continue

                # 历史深度：缓存起点晚于回测所需最早日则向前补数
                if cache_start > required_start_date:
                    need_fetch_codes.append(code)
        
        if not need_fetch_codes:
            print(f'[INFO] 所有股票的数据都已足够，无需额外拉取')
            return
        
        print(f'[INFO] 发现 {len(need_fetch_codes)} 只股票需要拉取更多数据')
        
        # 批量拉取数据
        # 使用 get_stock_data 方法，它会自动处理缓存合并
        def fetch_data(code):
            try:
                # 直接调用 get_stock_data，它会检查缓存，如果不够会自动从API拉取并合并
                df = self.get_stock_data(code, required_start_date, end_date_str, force_refresh=False)
                if df is not None and not df.empty:
                    # 检查最早日期是否满足要求（允许一些误差，因为可能没有更早的数据）
                    min_date = df['日期'].min()
                    min_date_str = pd.to_datetime(min_date).strftime('%Y%m%d')
                    # 如果最早日期在要求日期之后5天内，认为数据足够（可能股票上市较晚）
                    required_dt = pd.to_datetime(required_start_date)
                    min_dt = pd.to_datetime(min_date_str)
                    days_diff = (min_dt - required_dt).days
                    # 如果最早日期早于或等于要求日期，或者只晚几天（可能是新上市股票），认为数据足够
                    return code, days_diff <= 5
                return code, False
            except Exception as e:
                return code, False
        
        print(f'[INFO] 开始批量拉取数据...')
        success = 0
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(fetch_data, code): code for code in need_fetch_codes}
            for i, future in enumerate(as_completed(futures)):
                code, ok = future.result()
                if ok:
                    success += 1
                if (i + 1) % 50 == 0 or (i + 1) == len(need_fetch_codes):
                    print(f'  进度: {i+1}/{len(need_fetch_codes)} | 已拉取: {success}', flush=True)
        
        print(f'[INFO] 数据预拉取完成: {success}/{len(need_fetch_codes)} 只股票数据已就绪')
        
        # 清理重复缓存
        self.remove_duplicate_cache()

    def _read_stock_cache_json_any_age(self, cache_path):
        """读取 K 线缓存为 DataFrame；不因 cache_time 超过 7 天而拒绝（供 cache_only 使用）。"""
        try:
            if not cache_path or not os.path.exists(cache_path) or os.path.getsize(cache_path) <= 100:
                return None
            try:
                import orjson
                with open(cache_path, 'rb') as f:
                    cache_data = orjson.loads(f.read())
            except ImportError:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
            rows = cache_data.get('data')
            if not rows:
                return None
            df = pd.DataFrame(rows)
            if df.empty or '日期' not in df.columns:
                return None
            df['日期'] = pd.to_datetime(df['日期'])
            return df
        except Exception:
            return None

    def _normalize_hist_df(self, df):
        """与增量合并后一致的历史 K 线列，供策略侧使用。"""
        if df is None or df.empty:
            return None
        df = df.copy()
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        for col in ['开盘', '收盘', '最高', '最低', '成交量', '成交额', '涨跌幅', '换手率']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0.0
        if '涨跌额' not in df.columns:
            df['涨跌额'] = df['收盘'].diff().fillna(0)
        else:
            df['涨跌额'] = pd.to_numeric(df['涨跌额'], errors='coerce').fillna(0)
        if '振幅' not in df.columns:
            df['振幅'] = ((df['最高'] - df['最低']) / df['最低'].replace(0, float('nan')) * 100).fillna(0)
        else:
            df['振幅'] = pd.to_numeric(df['振幅'], errors='coerce').fillna(0)
        df['成交量'] = df['成交量'].astype(float)
        return df[['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]

    def _get_stock_data_cache_only(self, code, start_date, end_date):
        """仅用本地 json：先尝试文件名覆盖整段；否则用该代码下「结束日最晚」的那份缓存与请求区间的交集。"""
        try:
            s, e = pd.to_datetime(start_date), pd.to_datetime(end_date)
        except Exception:
            return None

        cache_path = self._get_cache_path(code, start_date, end_date)
        df = self._read_stock_cache_json_any_age(cache_path)
        if df is not None and not df.empty:
            df = df[(df['日期'] >= s) & (df['日期'] <= e)]
            if not df.empty:
                return self._normalize_hist_df(df)

        for fp in self._list_stock_cache_files(code):
            name = os.path.basename(fp)
            if '_' not in name or not name.endswith('.json'):
                continue
            span = cache_span_from_filename_or_file(fp, name[:-5])
            if not span:
                continue
            _, c_start, c_end = span
            if c_start <= start_date and c_end >= end_date:
                df = self._read_stock_cache_json_any_age(fp)
                if df is not None and not df.empty:
                    df = df[(df['日期'] >= s) & (df['日期'] <= e)]
                    if not df.empty:
                        return self._normalize_hist_df(df)

        best_fp = None
        best_end = ''
        for fp in self._list_stock_cache_files(code):
            name = os.path.basename(fp)
            if '_' not in name or not name.endswith('.json'):
                continue
            span = cache_span_from_filename_or_file(fp, name[:-5])
            if not span or span[0] != code:
                continue
            _, _, c_end = span
            if c_end > best_end:
                best_end = c_end
                best_fp = fp
        if best_fp:
            df = self._read_stock_cache_json_any_age(best_fp)
            if df is not None and not df.empty:
                df = df[(df['日期'] >= s) & (df['日期'] <= e)]
                if not df.empty:
                    return self._normalize_hist_df(df)
        return None

    def get_stock_data(self, code, start_date=None, end_date=None, force_refresh=False):
        """获取单只股票的历史K线数据
        
        Args:
            force_refresh: 为 True 时跳过缓存，强制从网络拉取新数据
        """
        if getattr(self, 'cache_only', False):
            ref_trade = self._get_last_trading_day().replace('-', '')[:8]
        else:
            ref_trade = self._get_last_trading_day_available().replace('-', '')[:8]

        if end_date is None:
            end_date = ref_trade
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')

        start_date = str(start_date).replace('-', '')[:8]
        end_date = str(end_date).replace('-', '')[:8]
        # 日 K 右端与「最近可用交易日」对齐（含 DATA_FETCH_EOD_HHMM 收盘前裁切），避免盘中把不完整当日写入缓存。
        if (
            len(ref_trade) == 8
            and ref_trade.isdigit()
            and len(end_date) == 8
            and end_date.isdigit()
            and end_date > ref_trade
        ):
            end_date = ref_trade

        if getattr(self, 'cache_only', False):
            return self._get_stock_data_cache_only(code, start_date, end_date)

        if not force_refresh:
            try:
                # 1. 精确匹配
                cache_path = self._get_cache_path(code, start_date, end_date)
                df = self._load_from_cache_file(cache_path)
                if df is not None:
                    return df
                # 2. 重叠匹配：任意缓存覆盖请求区间即可用（避免日期略不同时走 API）
                for fp in self._list_stock_cache_files(code):
                    name = os.path.basename(fp)
                    if '_' not in name or not name.endswith('.json'):
                        continue
                    span = cache_span_from_filename_or_file(fp, name[:-5])
                    if not span:
                        continue
                    _, c_start, c_end = span
                    if c_start <= start_date and c_end >= end_date:
                        df = self._load_from_cache_file(fp)
                        if df is not None and not df.empty:
                            s, e = pd.to_datetime(start_date), pd.to_datetime(end_date)
                            df = df[(df['日期'] >= s) & (df['日期'] <= e)]
                            if not df.empty:
                                return df.reset_index(drop=True)
            except Exception:
                pass

        # 增量补数：若有现有缓存，只拉取缺失的日期区间并合并，避免全量拉取
        try:
            existing_list = self._list_stock_cache_files(code)
            if existing_list and not force_refresh:
                # 取该股票唯一缓存（remove_duplicate 后应只有一份；多份时取覆盖 end 最大的）
                best_fp = None
                best_end = ''
                for fp in existing_list:
                    name = os.path.basename(fp)
                    if '_' not in name or not name.endswith('.json'):
                        continue
                    span = cache_span_from_filename_or_file(fp, name[:-5])
                    if not span or span[0] != code:
                        continue
                    _, _, c_end = span
                    if c_end > best_end:
                        best_end = c_end
                        best_fp = fp
                if best_fp and os.path.isfile(best_fp):
                    with open(best_fp, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    rows = cache_data.get('data') or []
                    if rows:
                        df_existing = pd.DataFrame(rows)
                        df_existing['日期'] = pd.to_datetime(df_existing['日期'])
                        cache_min = df_existing['日期'].min()
                        cache_max = df_existing['日期'].max()
                        cache_min_str = cache_min.strftime('%Y%m%d')
                        cache_max_str = cache_max.strftime('%Y%m%d')
                        need_back = start_date < cache_min_str
                        last_trade_ymd = self._get_last_trading_day_available().replace('-', '')[:8]
                        # 仅当请求区间超出缓存右端、且右端仍早于「应存在的最后交易日」时才向前补（YYYYMMDD 比较）
                        need_front = (end_date > cache_max_str) and (
                            len(last_trade_ymd) == 8 and cache_max_str < last_trade_ymd
                        )
                        if not need_back and not need_front:
                            s, e = pd.to_datetime(start_date), pd.to_datetime(end_date)
                            df_hit = df_existing[(df_existing['日期'] >= s) & (df_existing['日期'] <= e)]
                            if not df_hit.empty:
                                return df_hit.reset_index(drop=True)
                        if need_back or need_front:
                            to_merge = [df_existing]
                            new_start = cache_data.get('start_date', cache_min_str)
                            new_end = cache_data.get('end_date', cache_max_str)
                            if need_back:
                                fetch_end = (cache_min - timedelta(days=1)).strftime('%Y%m%d')
                                if start_date <= fetch_end:
                                    df_back = self._fetch_from_api(code, start_date, fetch_end)
                                    if df_back is not None and not df_back.empty:
                                        to_merge.append(df_back)
                                        new_start = start_date
                            if need_front:
                                fetch_start = (cache_max + timedelta(days=1)).strftime('%Y%m%d')
                                if fetch_start <= end_date:
                                    df_front = self._fetch_from_api(code, fetch_start, end_date)
                                    if df_front is not None and not df_front.empty:
                                        to_merge.append(df_front)
                                        new_end = end_date
                            if len(to_merge) > 1:
                                df_merged = pd.concat(to_merge, ignore_index=True)
                                df_merged = df_merged.drop_duplicates(subset=['日期'], keep='last')
                                df_merged = df_merged.sort_values('日期').reset_index(drop=True)
                                df_merged['日期'] = pd.to_datetime(df_merged['日期'])
                                for col in ['开盘','收盘','最高','最低','成交量','成交额','涨跌幅','换手率']:
                                    df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').fillna(0)
                                df_merged['成交量'] = df_merged['成交量'].astype(float)
                                df_merged['涨跌额'] = df_merged['收盘'].diff()
                                df_merged['涨跌额'] = df_merged['涨跌额'].fillna(0)
                                df_merged['振幅'] = ((df_merged['最高'] - df_merged['最低']) / df_merged['最低'].replace(0, float('nan')) * 100).fillna(0)
                                df_merged = df_merged[['日期','开盘','收盘','最高','最低','成交量','成交额','振幅','涨跌幅','涨跌额','换手率']]
                                out_path = self._get_cache_path(code, new_start, new_end)
                                out = {
                                    'cache_time': datetime.now().isoformat(),
                                    'code': code, 'start_date': new_start, 'end_date': new_end,
                                    'data': df_merged.to_dict('records')
                                }
                                with open(out_path, 'w', encoding='utf-8') as f:
                                    json.dump(out, f, ensure_ascii=False, default=str)
                                self.invalidate_cache_index()
                                if out_path != best_fp:
                                    try:
                                        os.remove(best_fp)
                                    except Exception:
                                        pass
                                s, e = pd.to_datetime(start_date), pd.to_datetime(end_date)
                                df_ret = df_merged[(df_merged['日期'] >= s) & (df_merged['日期'] <= e)]
                                if not df_ret.empty:
                                    return df_ret.reset_index(drop=True)
        except Exception:
            pass

        try:
            cache_path = self._get_cache_path(code, start_date, end_date)
            df = self._fetch_from_akshare(code, start_date, end_date)
            if df is None or df.empty:
                return None
            df = df.sort_values('日期')

            # 保存前先删除该股票代码的其他缓存文件，避免重复数据
            for existing_file in self._list_stock_cache_files(code):
                if existing_file != cache_path:
                    try:
                        os.remove(existing_file)
                    except Exception:
                        pass

            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'cache_time': datetime.now().isoformat(),
                    'code': code, 'start_date': start_date, 'end_date': end_date,
                    'data': df.to_dict('records')
                }, f, ensure_ascii=False, default=str)
            self.invalidate_cache_index()
            return df
        except Exception as e:
            print(f"[ERROR] 获取 {code} 数据失败: {e}")
        return None

    def get_recent_days_data(self, code, days=10, max_retries=3):
        """获取近N天的股票数据"""
        for attempt in range(max_retries):
            try:
                today = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=days * 2)).strftime('%Y%m%d')
                df = self.get_stock_data(code, start_date=start_date, end_date=today)
                if df is not None and not df.empty:
                    df = df.sort_values('日期').tail(days)
                    if not df.empty:
                        return df
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"[ERROR] 获取 {code} 近{days}天数据失败: {e}")
            time.sleep(1)
        return None

    def get_today_data(self, code, max_retries=3):
        """获取最新交易日数据"""
        for attempt in range(max_retries):
            try:
                today = datetime.now().strftime('%Y%m%d')
                df = self.get_stock_data(code, start_date=today, end_date=today)
                if df is None or df.empty:
                    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
                    df = self.get_stock_data(code, start_date=start_date, end_date=today)
                if df is not None and not df.empty:
                    row = df.iloc[-1]
                    return {
                        'date': pd.to_datetime(row['日期']).strftime('%Y-%m-%d'),
                        'open': float(row['开盘']),
                        'close': float(row['收盘']),
                        'high': float(row['最高']),
                        'low': float(row['最低']),
                        'volume': float(row['成交量']),
                        'amount': float(row.get('成交额', 0)),
                        'pct_change': float(row.get('涨跌幅', 0)),
                        'turnover': float(row.get('换手率', 0))
                    }
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"[ERROR] 获取 {code} 今日数据失败: {e}")
            time.sleep(1)
        return None

    def get_stock_data_by_date(self, code, date):
        """获取指定日期的股票数据"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            start_date = (date_obj - timedelta(days=5)).strftime('%Y%m%d')
            end_date = (date_obj + timedelta(days=5)).strftime('%Y%m%d')
            df = self.get_stock_data(code, start_date, end_date)
            if df is None or df.empty:
                return None
            target_date = pd.to_datetime(date)
            matching = df[df['日期'] == target_date]
            if matching.empty:
                return None
            row = matching.iloc[0]
            return {
                'date': date,
                'open': float(row['开盘']),
                'close': float(row['收盘']),
                'high': float(row['最高']),
                'low': float(row['最低']),
                'volume': float(row['成交量']),
                'amount': float(row['成交额']),
                'pct_change': float(row['涨跌幅']),
                'turnover': float(row['换手率'])
            }
        except Exception as e:
            print(f"[ERROR] 获取 {code} {date} 数据失败: {e}")
        return None

    def is_limit_up(self, code, date):
        """判断指定日期是否涨停"""
        try:
            data = self.get_stock_data_by_date(code, date)
            return data is not None and data['pct_change'] >= 9.8
        except Exception:
            return False


def purge_stock_data_dir_main_board_only_once():
    """进程内仅执行一次：删除 cache/stock_data 中非主板个股与指数类 json，并做重复文件清理。"""
    global _MAIN_BOARD_CACHE_PURGED_ONCE
    if _MAIN_BOARD_CACHE_PURGED_ONCE:
        return
    _MAIN_BOARD_CACHE_PURGED_ONCE = True
    try:
        DataFetcher().remove_duplicate_cache()
    except Exception as e:
        print(f'[WARNING] 首次清理非主板缓存失败: {e}', flush=True)
