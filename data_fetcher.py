"""
A股数据获取器 - 使用 Baostock（免费、稳定）
注意：Baostock 非线程安全，并发请求会混淆数据，需加锁
"""
import pandas as pd
from threading import Lock
from datetime import datetime, timedelta
import time
import os
import json
import glob

import baostock as bs


class DataFetcher:
    """A股数据获取器 - 使用 Baostock"""

    def __init__(self):
        self.stock_list_cache = None
        self.stock_list_cache_time = None
        self.cache_duration = 3600

        self.cache_dir = os.path.join(os.path.dirname(__file__), 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.stock_list_cache_file = os.path.join(self.cache_dir, 'stock_list.json')
        self.stock_data_cache_dir = os.path.join(self.cache_dir, 'stock_data')
        os.makedirs(self.stock_data_cache_dir, exist_ok=True)
        self._bs_logged_in = False
        self._bs_lock = Lock()  # Baostock 非线程安全

    def _ensure_login(self):
        if not self._bs_logged_in:
            lg = bs.login()
            self._bs_logged_in = (lg.error_code == '0')

    def _to_bs_code(self, code):
        """6位代码转 Baostock 格式：sh.600000 或 sz.000001"""
        return f"sh.{code}" if code.startswith('6') else f"sz.{code}"

    def _should_exclude(self, code, name):
        """只保留主板股票：00开头（深市主板）、60开头（沪市主板）"""
        if not (code.startswith('00') or code.startswith('60')):
            return True
        if 'ST' in name or '*ST' in name or 'st' in name or '*st' in name:
            return True
        if '退' in name:
            return True
        return False

    def get_stock_list(self):
        """获取所有主板A股股票列表"""
        # 检查内存缓存
        if (self.stock_list_cache is not None and
                self.stock_list_cache_time is not None and
                (datetime.now() - self.stock_list_cache_time).seconds < self.cache_duration):
            print(f"[DEBUG] 使用内存缓存，共 {len(self.stock_list_cache)} 只股票")
            return self.stock_list_cache

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
                    stocks = cache_data.get('stocks', [])
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

        # 从 baostock 获取
        print(f"[DEBUG] 开始从 baostock 获取股票列表...")
        try:
            with self._bs_lock:
                self._ensure_login()
                if not self._bs_logged_in:
                    print(f"[ERROR] Baostock 登录失败")
                    # 如果登录失败，使用过期缓存
                    if fallback_stocks:
                        print(f"[INFO] 使用过期缓存（{len(fallback_stocks)} 只股票）")
                        self.stock_list_cache = fallback_stocks
                        self.stock_list_cache_time = fallback_cache_time
                        return fallback_stocks
                    return []
                query_date = datetime.now().strftime('%Y-%m-%d')
                print(f"[DEBUG] 查询日期: {query_date}")
                rs = bs.query_all_stock(day=query_date)
                print(f"[DEBUG] Baostock 查询错误码: {rs.error_code}, 错误信息: {rs.error_msg}")
            stock_list = []
            if rs.error_code == '0':
                count = 0
                while rs.next():
                    row = rs.get_row_data()
                    # code: sh.600000, code_name: 浦发银行
                    bs_code, trade_status, name = row[0], row[1], row[2]
                    code = bs_code.split('.')[-1] if '.' in bs_code else bs_code
                    if len(code) != 6 or self._should_exclude(code, name):
                        continue
                    stock_list.append({'code': code, 'name': name})
                    count += 1
                print(f"[DEBUG] 从 baostock 获取到 {count} 只股票（过滤前）")

            if stock_list:
                self.stock_list_cache = stock_list
                self.stock_list_cache_time = datetime.now()
                with open(self.stock_list_cache_file, 'w', encoding='utf-8') as f:
                    json.dump({'cache_time': datetime.now().isoformat(), 'stocks': stock_list},
                              f, ensure_ascii=False, indent=2)
                print(f"[INFO] 获取 {len(stock_list)} 只主板股票")
                return stock_list
            else:
                print(f"[WARNING] 从 baostock 获取的股票列表为空，尝试使用过期缓存")
                # 如果 baostock 获取失败，使用过期缓存
                if fallback_stocks:
                    print(f"[INFO] 使用过期缓存（{len(fallback_stocks)} 只股票）")
                    self.stock_list_cache = fallback_stocks
                    self.stock_list_cache_time = fallback_cache_time
                    return fallback_stocks
        except Exception as e:
            import traceback
            print(f"[ERROR] 获取股票列表失败: {e}")
            print(f"[ERROR] 错误堆栈: {traceback.format_exc()}")
            # 如果获取失败，使用过期缓存
            if fallback_stocks:
                print(f"[INFO] 使用过期缓存（{len(fallback_stocks)} 只股票）")
                self.stock_list_cache = fallback_stocks
                self.stock_list_cache_time = fallback_cache_time
                return fallback_stocks
        
        print(f"[ERROR] 无法获取股票列表，且无可用缓存")
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
        """获取 Baostock 已有数据的最新交易日（当日数据通常收盘后才更新）"""
        last_trade = self._get_last_trading_day()
        last_trade_str = last_trade.replace('-', '')
        try:
            with self._bs_lock:
                self._ensure_login()
                rs = bs.query_history_k_data_plus(
                    'sz.000001', 'date', start_date=last_trade, end_date=last_trade,
                    frequency='d', adjustflag='3'
                )
                has_data = rs.error_code == '0' and rs.next()
            if not has_data:
                # 当日数据未更新，回退到前一交易日
                d = datetime.strptime(last_trade, '%Y-%m-%d').date()
                for _ in range(5):
                    d = d - timedelta(days=1)
                    if d.weekday() < 5:  # 周一到周五
                        check_date = d.strftime('%Y-%m-%d')
                        with self._bs_lock:
                            self._ensure_login()
                            rs = bs.query_history_k_data_plus(
                                'sz.000001', 'date', start_date=check_date, end_date=check_date,
                                frequency='d', adjustflag='3'
                            )
                            if rs.error_code == '0' and rs.next():
                                return check_date
                return last_trade  #  fallback
        except Exception:
            pass
        return last_trade

    def get_trading_days_between(self, start_date, end_date):
        """获取 start_date 到 end_date 之间的交易日列表（含首尾），返回 ['YYYY-MM-DD', ...]。"""
        try:
            start_s = start_date[:10] if isinstance(start_date, str) else start_date.strftime('%Y-%m-%d')
            end_s = end_date[:10] if isinstance(end_date, str) else end_date.strftime('%Y-%m-%d')
            with self._bs_lock:
                self._ensure_login()
                rs = bs.query_history_k_data_plus(
                    'sz.000001', 'date',
                    start_date=start_s, end_date=end_s,
                    frequency='d', adjustflag='3'
                )
            if rs.error_code != '0':
                return []
            out = []
            while rs.next():
                row = rs.get_row_data()
                if row and row[0]:
                    d = row[0].strip()
                    # 统一为 YYYY-MM-DD（Baostock 一般为该格式，少数情况为 YYYYMMDD）
                    if len(d) == 8 and d.isdigit():
                        d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                    if len(d) >= 10 and d[4] == '-' and d[7] == '-':
                        out.append(d[:10])
            return out
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

    def remove_duplicate_cache(self):
        """删除重复缓存：每只股票只保留一份（保留 start_date 最早的那份，覆盖范围最大）"""
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

    def _fetch_from_api(self, code, start_date, end_date):
        """从 API 拉取数据并返回 DataFrame，不写缓存"""
        start_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        try:
            with self._bs_lock:
                self._ensure_login()
                bs_code = self._to_bs_code(code)
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,open,high,low,close,volume,amount,pctChg,turn",
                    start_date=start_fmt, end_date=end_fmt, frequency="d", adjustflag="3"
                )
                data_list = []
                while rs.error_code == '0' and rs.next():
                    data_list.append(rs.get_row_data())
            if not data_list:
                return None
            df = pd.DataFrame(data_list, columns=['日期','开盘','最高','最低','收盘','成交量','成交额','涨跌幅','换手率'])
            df = df.drop_duplicates(subset=['日期'], keep='first')
            df['日期'] = pd.to_datetime(df['日期'])
            for col in ['开盘','收盘','最高','最低','成交量','成交额','涨跌幅','换手率']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df['成交量'] = df['成交量'].astype(float)
            df['涨跌额'] = df['收盘'].diff()
            df['涨跌额'] = df['涨跌额'].fillna(0)
            df['振幅'] = ((df['最高'] - df['最低']) / df['最低'].replace(0, float('nan')) * 100).fillna(0)
            df = df[['日期','开盘','收盘','最高','最低','成交量','成交额','振幅','涨跌幅','涨跌额','换手率']]
            return df.sort_values('日期').reset_index(drop=True)
        except Exception:
            return None

    def update_caches_with_today_data(self, max_workers=100):
        """拉取今天（最近交易日）的数据，合并到对应的 json 缓存文件中"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # 先清理重复缓存，避免更新时出现重复数据
        self.remove_duplicate_cache()

        # 使用 Baostock 已有数据的最新交易日（当日数据通常收盘后才更新）
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

        # 检查是否有股票没有缓存文件，需要创建新缓存
        all_stocks = self.get_stock_list()
        all_codes = {s['code'] for s in all_stocks}
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

        if not by_code:
            print('[INFO] 所有缓存已含最近交易日数据，无需更新')
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
            print(f'[TIP] 部分股票未更新可能因 Baostock 无数据（如 ST/退市股），可忽略')

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
        
        # 按股票代码分组，找出每只股票最早的start_date
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
            # 保留 start_date 最早的那个缓存文件
            if code not in by_code:
                by_code[code] = start_str
            else:
                if start_str < by_code[code]:
                    by_code[code] = start_str
        
        # 检查哪些股票需要拉取更多数据
        for code in all_codes:
            if code not in by_code:
                # 没有缓存，需要拉取
                need_fetch_codes.append(code)
            else:
                cache_start = by_code[code]
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
        start_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

        if not force_refresh:
            try:
                # 1. 精确匹配
                cache_path = self._get_cache_path(code, start_date, end_date)
                df = self._load_from_cache_file(cache_path)
                if df is not None:
                    return df
                # 2. 重叠匹配：任意缓存覆盖请求区间即可用（避免日期略不同时走 API）
                pattern = os.path.join(self.stock_data_cache_dir, f'{code}_*.json')
                for fp in glob.glob(pattern):
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
            pattern = os.path.join(self.stock_data_cache_dir, f'{code}_*.json')
            existing_list = glob.glob(pattern)
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
                        need_front = end_date > cache_max_str
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
            with self._bs_lock:
                self._ensure_login()
                bs_code = self._to_bs_code(code)
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,open,high,low,close,volume,amount,pctChg,turn",
                    start_date=start_fmt, end_date=end_fmt, frequency="d", adjustflag="3"
                )
                data_list = []
                while rs.error_code == '0' and rs.next():
                    data_list.append(rs.get_row_data())
            if not data_list:
                return None

            df = pd.DataFrame(data_list, columns=['日期','开盘','最高','最低','收盘','成交量','成交额','涨跌幅','换手率'])
            df = df.drop_duplicates(subset=['日期'], keep='first')  # 去重，防止异常返回
            df['日期'] = pd.to_datetime(df['日期'])
            for col in ['开盘','收盘','最高','最低','成交量','成交额','涨跌幅','换手率']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df['成交量'] = df['成交量'].astype(float)
            df['涨跌额'] = df['收盘'].diff()
            df['涨跌额'] = df['涨跌额'].fillna(0)
            df['振幅'] = ((df['最高'] - df['最低']) / df['最低'].replace(0, float('nan')) * 100).fillna(0)
            df = df[['日期','开盘','收盘','最高','最低','成交量','成交额','振幅','涨跌幅','涨跌额','换手率']]
            df = df.sort_values('日期')

            # 保存前先删除该股票代码的其他缓存文件，避免重复数据
            pattern = os.path.join(self.stock_data_cache_dir, f'{code}_*.json')
            existing_files = glob.glob(pattern)
            for existing_file in existing_files:
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
