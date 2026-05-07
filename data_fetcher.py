"""
A股数据获取器 - 股票列表与日 K 均使用 AkShare。
"""
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import json
import glob

_MAIN_BOARD_CACHE_PURGED_ONCE = False

from stock_code_utils import (
    is_likely_index_code_name,
    is_main_board_equity_code,
    load_main_board_codes_whitelist,
    universe_exclusion_reason,
)
from stock_list_sources import fetch_main_board_stocks_akshare


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

    def _get_cache_path(self, code, start_date, end_date):
        return os.path.join(self.stock_data_cache_dir, f"{code}_{start_date}_{end_date}.json")

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

    def _get_last_trading_day_available(self):
        """获取 AkShare 已有日 K 的最新交易日（当日数据通常收盘后才更新；用 000001 探测）。"""
        last_trade = self._get_last_trading_day()
        last_trade_str = last_trade.replace('-', '')
        try:
            df = self._fetch_from_akshare('000001', last_trade_str, last_trade_str)
            if df is not None and not df.empty:
                return last_trade
            d = datetime.strptime(last_trade, '%Y-%m-%d').date()
            for _ in range(10):
                d = d - timedelta(days=1)
                if d.weekday() < 5:
                    check = d.strftime('%Y%m%d')
                    df = self._fetch_from_akshare('000001', check, check)
                    if df is not None and not df.empty:
                        return d.strftime('%Y-%m-%d')
            return last_trade
        except Exception as e:
            print(f"[WARNING] 获取最近可用交易日失败，回退本地最近交易日 {last_trade}: {e}", flush=True)
        return last_trade

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
            parts = name[:-5].split('_')
            if len(parts) != 3:
                continue
            code, start_str, end_str = parts
            if len(code) != 6 or len(start_str) != 8 or len(end_str) != 8:
                continue
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

            # 3. 调用 API 获取
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
        last_trade = self._get_last_trading_day()
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
                parts = base[:-5].split('_')
                if len(parts) != 3:
                    continue
                code, _, _ = parts
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
                parts = name[:-5].split('_')  # 去掉 .json
                if len(parts) != 3:
                    continue
                code, start_str, end_str = parts
                if len(code) != 6 or len(start_str) != 8 or len(end_str) != 8:
                    continue
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
        """从 AkShare 拉取数据并返回 DataFrame，不写缓存"""
        try:
            import akshare as ak
            # 日期格式转换：YYYYMMDD -> YYYYMMDD（akshare 使用 YYYYMMDD）
            start_fmt = start_date[:8]
            end_fmt = end_date[:8]

            df = ak.stock_zh_a_hist(
                symbol=code,
                period='daily',
                start_date=start_fmt,
                end_date=end_fmt,
                adjust=''  # 不复权
            )

            if df is None or df.empty:
                return None

            # 列映射：akshare 列名 -> 内部列名
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

            # 重命名列
            df = df.rename(columns=column_map)

            # 确保日期格式正确
            df['日期'] = pd.to_datetime(df['日期'])

            # 确保数值类型正确
            for col in ['开盘', '收盘', '最高', '最低', '成交量', '成交额', '涨跌幅', '换手率']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            # 计算缺失的列
            if '涨跌额' not in df.columns:
                df['涨跌额'] = df['收盘'].diff().fillna(0)
            if '振幅' not in df.columns:
                df['振幅'] = ((df['最高'] - df['最低']) / df['最低'].replace(0, float('nan')) * 100).fillna(0)

            df['成交量'] = df['成交量'].astype(float)

            # 选择需要的列
            df = df[['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]
            df = df.drop_duplicates(subset=['日期'], keep='first')

            return df.sort_values('日期').reset_index(drop=True)
        except Exception as e:
            print(f"[DEBUG] AkShare 获取 {code} 数据失败: {e}")
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
                parts = name[:-5].split('_')
                if len(parts) != 3:
                    return None
                code, start_str, end_str = parts
                if len(code) != 6 or len(start_str) != 8 or len(end_str) != 8:
                    return None
                with open(fp, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                rows = cache_data.get('data') or []
                if not rows:
                    return None
                actual_max = max(r['日期'][:10] for r in rows)
                if actual_max >= last_trade:
                    return None
                return (code, start_str, end_str, fp)
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
            end_date = today.strftime('%Y%m%d')
            
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
                rows = cache_data.get('data') or []
                if not rows:
                    return code, False
                max_dt = max(pd.to_datetime(r['日期']) for r in rows)
                fetch_start = (max_dt + timedelta(days=1)).strftime('%Y%m%d')
                if fetch_start > last_trade_str:
                    return code, False

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
        
        # 计算需要的日历日数：约 1 交易日 ≈ 1.4 日历日，多取一些确保覆盖
        calendar_days = int(time_range * 1.6) + 10
        end_date = datetime.now()
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
            parts = name[:-5].split('_')
            if len(parts) != 3:
                continue
            code, start_str, end_str = parts
            if len(code) != 6 or len(start_str) != 8 or len(end_str) != 8:
                continue
            # 记录最早 start 与最晚 end（同一股票可能存在多份缓存）
            if code not in by_code:
                by_code[code] = {'start': start_str, 'end': end_str}
            else:
                if start_str < by_code[code]['start']:
                    by_code[code]['start'] = start_str
                if end_str > by_code[code]['end']:
                    by_code[code]['end'] = end_str

        last_available_trade_day = self._get_last_trading_day_available()
        
        # 检查哪些股票需要拉取更多数据
        for code in all_codes:
            if code not in by_code:
                # 没有缓存，需要拉取
                need_fetch_codes.append(code)
            else:
                cache_start = by_code[code]['start']
                cache_end = by_code[code]['end']

                # 若缓存已经覆盖到最近可用交易日，优先使用缓存，不再预拉取
                if cache_end >= last_available_trade_day:
                    continue

                # 如果缓存的开始日期晚于需要的开始日期，需要拉取更多数据
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

    def get_stock_data(self, code, start_date=None, end_date=None, force_refresh=False):
        """获取单只股票的历史K线数据
        
        Args:
            force_refresh: 为 True 时跳过缓存，强制从网络拉取新数据
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')

        start_date = str(start_date).replace('-', '')
        end_date = str(end_date).replace('-', '')

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
                    parts = name[:-5].split('_')
                    if len(parts) != 3 or len(parts[1]) != 8 or len(parts[2]) != 8:
                        continue
                    c_start, c_end = parts[1], parts[2]
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
                    parts = name[:-5].split('_')
                    if len(parts) != 3 or len(parts[1]) != 8 or len(parts[2]) != 8:
                        continue
                    if parts[0] != code:
                        continue
                    if parts[2] > best_end:
                        best_end = parts[2]
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
                        last_trade_day = self._get_last_trading_day()
                        need_front = (end_date > cache_max_str) and (cache_max_str < last_trade_day)
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
