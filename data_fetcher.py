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
        if (self.stock_list_cache is not None and
                self.stock_list_cache_time is not None and
                (datetime.now() - self.stock_list_cache_time).seconds < self.cache_duration):
            return self.stock_list_cache

        try:
            if os.path.exists(self.stock_list_cache_file):
                with open(self.stock_list_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    cache_time = datetime.fromisoformat(cache_data['cache_time'])
                    if (datetime.now() - cache_time).total_seconds() < 86400:
                        return cache_data['stocks']
        except Exception:
            pass

        try:
            with self._bs_lock:
                self._ensure_login()
                rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
            stock_list = []
            while rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
                # code: sh.600000, code_name: 浦发银行
                bs_code, trade_status, name = row[0], row[1], row[2]
                code = bs_code.split('.')[-1] if '.' in bs_code else bs_code
                if len(code) != 6 or self._should_exclude(code, name):
                    continue
                stock_list.append({'code': code, 'name': name})

            if stock_list:
                self.stock_list_cache = stock_list
                self.stock_list_cache_time = datetime.now()
                with open(self.stock_list_cache_file, 'w', encoding='utf-8') as f:
                    json.dump({'cache_time': datetime.now().isoformat(), 'stocks': stock_list},
                              f, ensure_ascii=False, indent=2)
                print(f"[INFO] 获取 {len(stock_list)} 只主板股票")
                return stock_list
        except Exception as e:
            print(f"[ERROR] 获取股票列表失败: {e}")
        return []

    def _get_cache_path(self, code, start_date, end_date):
        return os.path.join(self.stock_data_cache_dir, f"{code}_{start_date}_{end_date}.json")

    def _get_last_trading_day(self):
        """获取最近的 A 股交易日（周一至周五，不考虑节假日）"""
        d = datetime.now().date()
        # weekday: 0=周一, 6=周日
        if d.weekday() == 5:  # 周六
            return (d - timedelta(days=1)).strftime('%Y-%m-%d')
        if d.weekday() == 6:  # 周日
            return (d - timedelta(days=2)).strftime('%Y-%m-%d')
        return d.strftime('%Y-%m-%d')

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

    def update_caches_with_today_data(self, max_workers=10):
        """拉取今天（最近交易日）的数据，合并到对应的 json 缓存文件中"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        last_trade = self._get_last_trading_day()
        last_trade_str = last_trade.replace('-', '')

        pattern = os.path.join(self.stock_data_cache_dir, '*.json')
        files = glob.glob(pattern)
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
            if end_str >= last_trade_str:
                continue
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
        print(f'[INFO] 待更新 {total} 个缓存（缺少最近交易日数据）')
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

        cache_path = self._get_cache_path(code, start_date, end_date)
        try:
            if not force_refresh and os.path.exists(cache_path) and os.path.getsize(cache_path) > 100:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                if cache_data.get('data'):
                    cache_time = datetime.fromisoformat(cache_data['cache_time'])
                    if (datetime.now() - cache_time).total_seconds() < 604800:
                        df = pd.DataFrame(cache_data['data'])
                        df['日期'] = pd.to_datetime(df['日期'])
                        return df
        except Exception:
            pass

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
