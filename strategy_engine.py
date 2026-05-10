from datetime import datetime, timedelta
from data_fetcher import DataFetcher
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import json
import os
from collections import defaultdict

class StrategyEngine:
    """策略回测引擎"""
    
    def __init__(self, data_fetcher: DataFetcher, max_workers=50):
        self.data_fetcher = data_fetcher
        self.max_workers = max_workers  # 并发线程数
        self.results_lock = Lock()  # 线程锁
        # 结果持久化目录
        self.results_dir = os.path.join(os.path.dirname(__file__), 'results')
        os.makedirs(self.results_dir, exist_ok=True)
        # jsonl 批量写出缓冲（path -> 状态）
        self._jsonl_buf = defaultdict(lambda: {'strategy_name': None, 'lines': [], 'meta_written': False})
        self._jsonl_flush_size = int(os.environ.get('BACKTEST_JSONL_BUFFER', '40') or '40')
        self._match_print_count = 0

    def _prepare_df_for_strategy(self, df, conditions):
        """单次：_ds、date_map、交易日序列、指标预计算（df 须已按日期升序）。"""
        df['_ds'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        date_map = df.set_index('_ds').to_dict('index')
        dates_str = df['_ds'].tolist()
        date_pos = {s: i for i, s in enumerate(dates_str)}
        dates_sorted = [datetime.strptime(s, '%Y-%m-%d') for s in dates_str]
        indicator_ctx = self._build_indicator_ctx(conditions, df)
        return date_map, dates_str, date_pos, dates_sorted, indicator_ctx

    def _build_indicator_ctx(self, conditions, df):
        """按条件集合预计算 MA/RSI 等整列，避免在 T 日循环内重复 rolling。"""
        ctx = {'ma_cross': {}, 'ma_period': {}, 'rsi_period': {}, 'main_force_build': None}
        pairs = set()
        periods_ma = set()
        periods_rsi = set()
        for c in conditions:
            t = c.get('type')
            if t == 'ma_cross_up':
                pairs.add((int(c.get('shortPeriod', 5)), int(c.get('longPeriod', 10))))
            elif t == 'close_below_ma_deviation':
                periods_ma.add(int(c.get('period', 20)))
            elif t == 'rsi_lt':
                periods_rsi.add(int(c.get('period', 6)))
            elif t == 'main_force_build_position':
                periods_ma.update({5, 10, 20})
            elif t == 'consecutive_up_days_gte' and bool(c.get('requireMa5GtMa10')):
                periods_ma.update({5, 10})
        if not pairs and not periods_ma and not periods_rsi:
            return ctx
        close = pd.to_numeric(df['收盘'], errors='coerce')
        for short, long in pairs:
            ma_s = close.rolling(window=short, min_periods=short).mean().to_numpy(dtype=float, copy=False)
            ma_l = close.rolling(window=long, min_periods=long).mean().to_numpy(dtype=float, copy=False)
            ctx['ma_cross'][(short, long)] = (ma_s, ma_l)
        for p in periods_ma:
            ctx['ma_period'][p] = close.rolling(window=p, min_periods=p).mean().to_numpy(dtype=float, copy=False)
        for p in periods_rsi:
            delta = close.diff()
            gain = delta.clip(lower=0).rolling(window=p, min_periods=p).mean()
            loss = (-delta.clip(upper=0)).rolling(window=p, min_periods=p).mean()
            rs = gain / loss.replace(0, pd.NA)
            rsi = 100 - (100 / (1 + rs))
            ctx['rsi_period'][p] = rsi.fillna(100).to_numpy(dtype=float, copy=False)
        if 5 in ctx['ma_period'] and 10 in ctx['ma_period'] and 20 in ctx['ma_period']:
            ctx['main_force_build'] = {
                'ma5': ctx['ma_period'][5],
                'ma10': ctx['ma_period'][10],
                'ma20': ctx['ma_period'][20],
            }
        return ctx

    def _flush_jsonl_buffer(self, filepath):
        """将 filepath 对应缓冲刷盘；须在持有 results_lock 时调用。"""
        buf = self._jsonl_buf.get(filepath)
        if not buf or not buf['lines']:
            return
        strategy_name = buf['strategy_name'] or ''
        lines = buf['lines']
        buf['lines'] = []
        mode = 'a' if buf['meta_written'] else 'w'
        try:
            with open(filepath, mode, encoding='utf-8') as f:
                if not buf['meta_written']:
                    meta = {'_meta': {'strategy_name': strategy_name, 'run_at': datetime.now().isoformat()}}
                    f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
                    buf['meta_written'] = True
                for line in lines:
                    f.write(line + '\n')
        except Exception as e:
            print(f"[WARNING] 批量写出结果失败: {e}")

    def _queue_jsonl_row(self, filepath, strategy_name, result, count):
        """缓冲写入一条 jsonl；须在持有 results_lock 时调用。"""
        buf = self._jsonl_buf[filepath]
        if buf['strategy_name'] is None:
            buf['strategy_name'] = strategy_name
        buf['lines'].append(json.dumps(result, ensure_ascii=False, default=str))
        if len(buf['lines']) >= self._jsonl_flush_size:
            self._flush_jsonl_buffer(filepath)

    def backtest(self, strategy, strategy_name=None):
        """执行策略回测（优化版：分阶段筛选 + 实时持久化）"""
        return self._backtest_impl(strategy, strategy_name=strategy_name, only_t_date=None, write_results=True)

    def backtest_single_day(self, strategy, strategy_name=None, trading_date=None):
        """仅对指定交易日 T 做一次扫描，返回当日匹配结果（不写主结果文件）。
        trading_date: str 'YYYY-MM-DD' 或 datetime。若为 None 则用最近交易日。
        """
        if trading_date is None:
            trading_date = self.data_fetcher._get_last_trading_day_available()
        if hasattr(trading_date, 'strftime'):
            trading_date = trading_date.strftime('%Y-%m-%d')
        return self._backtest_impl(
            strategy, strategy_name=strategy_name, only_t_date=trading_date, write_results=False
        )

    def run_incremental_for_stock(self, stock, trading_days, strategies_list):
        """对单只股票只加载一次 K 线，在多个交易日×多个策略上检查，返回 {strategy_name: [result_rows]}。
        用于增量回测脚本，避免「每 T 日×每策略」都全量扫一遍股票。
        strategies_list: [(strategy_name, strategy_dict), ...]
        """
        if not trading_days or not strategies_list:
            return {}
        code = stock['code']
        name = stock['name']
        max_time_range = max((s.get('timeRange', 30) for _, s in strategies_list))
        end_d = max(datetime.strptime(d[:10], '%Y-%m-%d') for d in trading_days)
        start_d = min(datetime.strptime(d[:10], '%Y-%m-%d') for d in trading_days) - timedelta(days=int(max_time_range * 1.6) + 10)
        df = self.data_fetcher.get_stock_data(code, start_d.strftime('%Y%m%d'), end_d.strftime('%Y%m%d'))
        if df is None or df.empty:
            return {}

        df = df.sort_values('日期').reset_index(drop=True)

        out = {}
        for strategy_name, strategy in strategies_list:
            conditions = strategy.get('conditions', [])
            date_map, dates_str, date_pos, dates_sorted, indicator_ctx = self._prepare_df_for_strategy(df, conditions)
            time_range = strategy.get('timeRange', 30)
            for t_date in trading_days:
                if any(c.get('type') == 'bottoming_breakout' for c in conditions):
                    check = self._check_bottoming_breakout(
                        code, start_d, end_d, time_range, only_t_date=t_date, df=df, conditions=conditions
                    )
                else:
                    check = self._check_strategy_fast(
                        code, conditions, start_d, end_d, time_range, only_t_date=t_date, df=df,
                        date_map=date_map, dates_sorted=dates_sorted, dates_str=dates_str, date_pos=date_pos,
                        indicator_ctx=indicator_ctx,
                    )
                if check:
                    items = check if isinstance(check, list) else [check]
                    for item in items:
                        detail = self._get_stock_detail_from_check(code, name, conditions, item)
                        if detail:
                            if strategy_name not in out:
                                out[strategy_name] = []
                            out[strategy_name].append({'code': code, 'name': name, **detail})
        return out

    def _backtest_impl(self, strategy, strategy_name=None, only_t_date=None, write_results=True):
        """内部：执行回测，可选仅扫描 only_t_date 且不写文件。"""
        # 解析策略条件
        conditions = strategy.get('conditions', [])
        exclude_rules = strategy.get('exclude', {})
        time_range = strategy.get('timeRange', 30)
        
        # 生成策略名称
        if strategy_name is None:
            strategy_name = f"策略_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 打印策略信息
        print(f'策略名称: {strategy_name}')
        print(f'策略条件数量: {len(conditions)}')
        print(f'回测时间范围: {time_range} 个交易日' + (f'，仅扫描 T={only_t_date}' if only_t_date else ''))
        
        # 结果文件路径（仅全量回测时写入）
        results_filepath = os.path.join(self.results_dir, f"{strategy_name}_结果.jsonl") if write_results else None
        if results_filepath:
            with self.results_lock:
                self._jsonl_buf.pop(results_filepath, None)
        self._match_print_count = 0

        # 在回测开始前，确保有足够的数据
        print(f'开始数据预检查，确保有足够的数据用于回测 {time_range} 个交易日...')
        self.data_fetcher.ensure_sufficient_data(time_range, max_workers=100)
        
        # 获取所有股票
        stocks = self.data_fetcher.get_stock_list()
        print(f'获取到 {len(stocks)} 只股票')
        
        # 计算回测时间范围：timeRange 为交易日数，不含周末
        # 若策略依赖较长历史（如上市天数/长周期指标），需要拉取更长数据窗口
        max_backward_offset = 0
        for c in conditions:
            date1 = c.get('date1', 0)
            date2 = c.get('date2', 0)
            if isinstance(date1, (int, float)) and date1 < 0:
                max_backward_offset = max(max_backward_offset, abs(int(date1)))
            if isinstance(date2, (int, float)) and date2 < 0:
                max_backward_offset = max(max_backward_offset, abs(int(date2)))
            cond_type = c.get('type')
            if cond_type in ('recent_n_day_pct_change_lt', 'avg_amount_gte'):
                max_backward_offset = max(max_backward_offset, int(c.get('days', 5)) - 1)
            elif cond_type == 'close_below_ma_deviation':
                max_backward_offset = max(max_backward_offset, int(c.get('period', 20)) - 1)
            elif cond_type == 'rsi_lt':
                max_backward_offset = max(max_backward_offset, int(c.get('period', 6)) + 1)
            elif cond_type == 'stop_fall_signal':
                max_backward_offset = max(max_backward_offset, int(c.get('volumeDays', 5)) - 1)
            elif cond_type == 'listed_days_gte':
                max_backward_offset = max(max_backward_offset, int(c.get('days', 120)) - 1)
            elif cond_type == 'main_force_build_position':
                max_backward_offset = max(max_backward_offset, int(c.get('windowDays', 10)) + 20)

        # 与本地 K 缓存右端对齐：自然日「今天」若在周末，用最近周五，避免 c_end < end_date 误判未覆盖而全网拉取
        if only_t_date:
            try:
                end_date = datetime.strptime(only_t_date[:10], '%Y-%m-%d')
            except Exception:
                end_date = datetime.strptime(self.data_fetcher._get_last_trading_day(), '%Y-%m-%d')
        else:
            end_date = datetime.strptime(self.data_fetcher._get_last_trading_day(), '%Y-%m-%d')
        required_trading_days = time_range + max_backward_offset + 1
        calendar_days = int(required_trading_days * 1.6) + 10  # 覆盖 timeRange + 历史依赖窗口
        start_date = end_date - timedelta(days=calendar_days)
        
        results = []
        total_stocks = len(stocks)
        processed_count = [0]  # 使用列表以便在闭包中修改
        
        print(f"开始回测，共 {total_stocks} 只股票，回测最近 {time_range} 个交易日，使用 {self.max_workers} 个并发线程")
        
        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务（time_range=交易日数）
            future_to_stock = {
                executor.submit(self._process_stock, stock, conditions, start_date, end_date, time_range, only_t_date): stock
                for stock in stocks
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_stock):
                stock = future_to_stock[future]
                processed_count[0] += 1
                
                # 每10只股票显示一次进度（更频繁的进度更新）
                if processed_count[0] % 10 == 0:
                    percentage = 100 * processed_count[0] // total_stocks if total_stocks > 0 else 0
                    if only_t_date:
                        print(
                            f"进度: {processed_count[0]}/{total_stocks} ({percentage}%) "
                            f"- 策略: {strategy_name} - T日: {only_t_date} "
                            f"- 已找到 {len(results)} 只符合条件的股票",
                            flush=True,
                        )
                    else:
                        print(
                            f"进度: {processed_count[0]}/{total_stocks} ({percentage}%) "
                            f"- 策略: {strategy_name} "
                            f"- 已找到 {len(results)} 只符合条件的股票",
                            flush=True,
                        )
                
                try:
                    result = future.result(timeout=30)
                    if result:
                        with self.results_lock:
                            rows = result if isinstance(result, list) else [result]
                            for r in rows:
                                name = (r.get('name') or '').strip()
                                if len(name) > 4:
                                    continue
                                r['match_date'] = self._normalize_match_date(r.get('match_date'))
                                results.append(r)
                                if results_filepath:
                                    self._queue_jsonl_row(results_filepath, strategy_name, r, len(results))
                                verbose = os.environ.get('BACKTEST_VERBOSE_MATCH', '').strip().lower() in (
                                    '1', 'true', 'yes', 'on',
                                )
                                self._match_print_count += 1
                                if verbose:
                                    print(
                                        f"✓ 找到: {r['code']} {r['name']} (匹配日期: {r.get('match_date', 'N/A')})",
                                        flush=True,
                                    )
                                elif self._match_print_count % 200 == 0:
                                    print(
                                        f"[INFO] 已匹配 {self._match_print_count} 条（最新 {r['code']} {r['name']} {r.get('match_date', '')}）",
                                        flush=True,
                                    )
                except Exception as e:
                    # 输出错误信息以便调试
                    if processed_count[0] % 100 == 0:  # 每100只股票输出一次错误统计
                        print(f"[WARNING] 处理股票时出错: {type(e).__name__}: {str(e)}", flush=True)
                    continue

        if results_filepath:
            with self.results_lock:
                self._flush_jsonl_buffer(results_filepath)

        print(f"回测完成！共检查 {total_stocks} 只股票，找到 {len(results)} 条符合条件的记录")
        if results:
            # 默认规则：连续三个 A 股交易日内同一只股票只保留第一次出现的日期
            # 连阳上影策略例外：保留 3 个交易日内重复出现，用于观察连阳阶段内的连续信号
            if strategy_name != '连阳上影':
                results = self._dedupe_same_stock_within_three_trading_days(results, trading_days=3)
            results.sort(key=lambda r: (r.get('match_date', '9999-99-99'), r.get('code', '')))
            if write_results and results_filepath:
                self._write_sorted_results(results_filepath, strategy_name, results)
                print(f"结果已保存（按符合日期排序）: {results_filepath}")
        return results
    
    def _dedupe_same_stock_within_three_trading_days(self, results, trading_days=3):
        """所有策略通用：连续 N 个 A 股交易日内同一只股票只能出现一次，只保留第一次出现的日期。
        使用真实 A 股交易日历（AkShare，失败时用缓存 K 线中的日期序列）。"""
        if not results:
            return results

        def count_trading_days_fallback(d1, d2):
            """兜底：仅按周一到周五（无节假日数据时使用）"""
            if d1 > d2:
                d1, d2 = d2, d1
            n = 0
            cur = d1
            while cur < d2:
                if cur.weekday() < 5:
                    n += 1
                cur = cur + timedelta(days=1)
            return n

        # 一次拉取整个结果集日期范围内的 A 股交易日历，避免对每对日期都重复请求
        dates_in_results = []
        for r in results:
            md = (r.get('match_date') or '')[:10]
            if len(md) == 10 and md[4] == '-' and md[7] == '-':
                dates_in_results.append(md)
        if dates_in_results:
            min_d, max_d = min(dates_in_results), max(dates_in_results)
            full_days_list = self.data_fetcher.get_trading_days_between(min_d, max_d)
            if not full_days_list:
                full_days_list = self.data_fetcher.get_trading_days_from_cache(min_d, max_d)
            # 有序列表，用于计算两日之间交易日个数（含首尾）
            trading_day_list_sorted = sorted(full_days_list) if full_days_list else None
        else:
            trading_day_list_sorted = None

        def count_trading_days_inclusive(d1_str, d2_str):
            if not trading_day_list_sorted:
                d1 = datetime.strptime(d1_str[:10], '%Y-%m-%d')
                d2 = datetime.strptime(d2_str[:10], '%Y-%m-%d')
                return count_trading_days_fallback(d1, d2) + 1
            # 在已拉取的交易日列表中数 [d1_str, d2_str] 含首尾的个数
            if d1_str > d2_str:
                d1_str, d2_str = d2_str, d1_str
            n = 0
            for d in trading_day_list_sorted:
                if d < d1_str:
                    continue
                if d > d2_str:
                    break
                n += 1
            return n if n >= 1 else 1

        by_code = {}
        for r in results:
            by_code.setdefault(r.get('code', ''), []).append(r)
        out = []
        for rows in by_code.values():
            rows = sorted(rows, key=lambda x: x.get('match_date', '9999-99-99'))
            kept = []
            for r in rows:
                try:
                    d = datetime.strptime(r.get('match_date', '')[:10], '%Y-%m-%d')
                    d_str = d.strftime('%Y-%m-%d')
                except Exception:
                    kept.append(r)
                    continue
                if not kept:
                    kept.append(r)
                    continue
                last_str = kept[-1]['match_date'][:10]
                n_between = count_trading_days_inclusive(last_str, d_str)
                # 连续 3 个交易日内不能出现两次 => 保留后一条仅当 [last, d] 内交易日数 > 3
                if n_between > trading_days:
                    kept.append(r)
            out.extend(kept)
        return out

    def _append_result(self, filepath, strategy_name, result, count):
        """每找到一条符合条件的结果就写入（缓冲批量刷盘）。"""
        with self.results_lock:
            self._queue_jsonl_row(filepath, strategy_name, result, count)
    
    def _write_sorted_results(self, filepath, strategy_name, results):
        """按符合日期排序后重写结果文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                meta = {'_meta': {'strategy_name': strategy_name, 'run_at': datetime.now().isoformat(), 'count': len(results)}}
                f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
                for r in results:
                    r = dict(r)
                    r['match_date'] = self._normalize_match_date(r.get('match_date'))
                    f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')
        except Exception as e:
            print(f"[WARNING] 保存排序结果失败: {e}")
    
    def _process_stock(self, stock, conditions, start_date, end_date, time_range=30, only_t_date=None):
        """处理单只股票（用于并发）。only_t_date 有值时仅检查该 T 日。"""
        code = stock['code']
        name = stock['name']
        
        try:
            check_result = self._check_strategy(code, conditions, start_date, end_date, time_range, only_t_date=only_t_date)
            if check_result:
                # 筑底突破返回多个 match_date，其余策略返回单个
                items = check_result if isinstance(check_result, list) else [check_result]
                out = []
                for item in items:
                    detail = self._get_stock_detail_from_check(code, name, conditions, item)
                    if detail:
                        out.append({'code': code, 'name': name, **detail})
                return out if out else None
        except Exception as e:
            # 只记录严重错误，避免日志过多
            if 'timeout' in str(e).lower() or 'connection' in str(e).lower():
                pass  # 网络错误静默处理
            # 其他错误也静默处理，避免影响性能
        
        return None
    
    def _check_strategy(self, code, conditions, start_date, end_date, time_range=30, only_t_date=None, df=None):
        """检查股票是否符合策略条件。only_t_date 有值时仅检查该日作为 T。df 可选，传入时不再拉取数据。"""
        try:
            if any(c.get('type') == 'bottoming_breakout' for c in conditions):
                return self._check_bottoming_breakout(
                    code, start_date, end_date, time_range, only_t_date=only_t_date, df=df, conditions=conditions
                )

            if df is None:
                df = self.data_fetcher.get_stock_data(
                    code,
                    start_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d')
                )
            if df is None or df.empty:
                return False
            
            # 确保有足够的列
            required_columns = ['日期', '涨跌幅', '成交量']
            if not all(col in df.columns for col in required_columns):
                return False
            
            # 按日期排序（从早到晚）
            df = df.sort_values('日期').reset_index(drop=True)
            
            # 优化：检查是否有依赖涨停的条件，如果有，则检查是否有涨停日
            has_limit_up_condition = any(
                c.get('type') in ('limit_up', 'three_limit_up', 'recent_limit_up')
                for c in conditions
            )
            if has_limit_up_condition:
                # 如果有涨停条件但没有任何涨停日，直接跳过
                if (df['涨跌幅'] >= 9.8).sum() == 0:
                    return False
            
            # 计算需要的最少交易日数（T-5 需预留 5 个交易日）
            max_backward_offset = 0
            for c in conditions:
                date1 = c.get('date1', 0)
                date2 = c.get('date2', 0)
                if date1 < 0:
                    max_backward_offset = max(max_backward_offset, abs(date1))
                if date2 < 0:
                    max_backward_offset = max(max_backward_offset, abs(date2))
                cond_type = c.get('type')
                if cond_type in ('recent_n_day_pct_change_lt', 'avg_amount_gte'):
                    max_backward_offset = max(max_backward_offset, int(c.get('days', 5)) - 1)
                elif cond_type == 'close_below_ma_deviation':
                    max_backward_offset = max(max_backward_offset, int(c.get('period', 20)) - 1)
                elif cond_type == 'rsi_lt':
                    max_backward_offset = max(max_backward_offset, int(c.get('period', 6)) + 1)
                elif cond_type == 'stop_fall_signal':
                    max_backward_offset = max(max_backward_offset, int(c.get('volumeDays', 5)) - 1)
                elif cond_type == 'listed_days_gte':
                    max_backward_offset = max(max_backward_offset, int(c.get('days', 120)) - 1)
                elif cond_type == 'main_force_build_position':
                    max_backward_offset = max(max_backward_offset, int(c.get('windowDays', 10)) + 20)
            min_required_idx = max_backward_offset

            date_map, dates_str, date_pos, dates_sorted, indicator_ctx = self._prepare_df_for_strategy(df, conditions)
            ctx_tail = {
                'date_map': date_map, 'dates_str': dates_str, 'date_pos': date_pos, 'dates_sorted': dates_sorted,
                'indicator_ctx': indicator_ctx,
            }

            # 无 only_t_date：只在「最近 time_range 根 K 线」内枚举 T（全量回测找最近命中）。
            # 有 only_t_date：增量脚本已指定 T 日列表，不得再截成尾部 time_range 根，否则 timeRange 大（如主力建仓 120）
            # 时早于「df 尾窗」的 T 永远不会被扫到。
            if only_t_date:
                only_d = only_t_date[:10] if isinstance(only_t_date, str) else only_t_date.strftime('%Y-%m-%d')
                if only_d not in date_pos:
                    return False
                idx = date_pos[only_d]
                if idx < 1:
                    return False
                base_date = df.iloc[idx]['日期']
                if self._check_conditions_from_date_fast(
                    conditions, base_date, df, date_map, dates_sorted,
                    dates_str=dates_str, date_pos=date_pos, indicator_ctx=indicator_ctx,
                ):
                    return {'df': df, 'base_date': base_date, **ctx_tail}
                return False

            min_i = max(min_required_idx, len(df) - time_range)
            for i in range(len(df) - 1, min_i - 1, -1):
                base_date = df.iloc[i]['日期']
                if self._check_conditions_from_date_fast(
                    conditions, base_date, df, date_map, dates_sorted,
                    dates_str=dates_str, date_pos=date_pos, indicator_ctx=indicator_ctx,
                ):
                    return {'df': df, 'base_date': base_date, **ctx_tail}
            return False
        except Exception as e:
            # 静默处理错误
            return False

    def _check_strategy_fast(
        self, code, conditions, start_date, end_date, time_range=30, only_t_date=None, df=None,
        date_map=None, dates_sorted=None, dates_str=None, date_pos=None, indicator_ctx=None,
    ):
        """快速检查股票是否符合策略条件（使用预创建的 date_map / dates_str / indicator_ctx）。"""
        try:
            if df is None or df.empty:
                return False

            required_columns = ['日期', '涨跌幅', '成交量']
            if not all(col in df.columns for col in required_columns):
                return False

            has_limit_up_condition = any(
                c.get('type') in ('limit_up', 'three_limit_up', 'recent_limit_up')
                for c in conditions
            )
            if has_limit_up_condition:
                if (df['涨跌幅'] >= 9.8).sum() == 0:
                    return False

            max_backward_offset = 0
            for c in conditions:
                date1 = c.get('date1', 0)
                date2 = c.get('date2', 0)
                if date1 < 0:
                    max_backward_offset = max(max_backward_offset, abs(date1))
                if date2 < 0:
                    max_backward_offset = max(max_backward_offset, abs(date2))
                cond_type = c.get('type')
                if cond_type in ('recent_n_day_pct_change_lt', 'avg_amount_gte'):
                    max_backward_offset = max(max_backward_offset, int(c.get('days', 5)) - 1)
                elif cond_type == 'close_below_ma_deviation':
                    max_backward_offset = max(max_backward_offset, int(c.get('period', 20)) - 1)
                elif cond_type == 'rsi_lt':
                    max_backward_offset = max(max_backward_offset, int(c.get('period', 6)) + 1)
                elif cond_type == 'stop_fall_signal':
                    max_backward_offset = max(max_backward_offset, int(c.get('volumeDays', 5)) - 1)
                elif cond_type == 'listed_days_gte':
                    max_backward_offset = max(max_backward_offset, int(c.get('days', 120)) - 1)
                elif cond_type == 'main_force_build_position':
                    max_backward_offset = max(max_backward_offset, int(c.get('windowDays', 10)) + 20)
            min_required_idx = max_backward_offset

            if date_map is None or dates_str is None or date_pos is None or dates_sorted is None or indicator_ctx is None:
                date_map, dates_str, date_pos, dates_sorted, indicator_ctx = self._prepare_df_for_strategy(df, conditions)
            ctx_tail = {
                'date_map': date_map, 'dates_str': dates_str, 'date_pos': date_pos, 'dates_sorted': dates_sorted,
                'indicator_ctx': indicator_ctx,
            }

            if only_t_date:
                only_d = only_t_date[:10] if isinstance(only_t_date, str) else only_t_date.strftime('%Y-%m-%d')
                if only_d not in date_pos:
                    return False
                idx = date_pos[only_d]
                # 增量模式：不得用 min_required_idx（如 30）卡死 T 的下标，否则短历史新股或 df 较短时全部被拒。
                # 指标是否有效由条件内部与 NaN 判断处理；至少需有 T-1 即 idx>=1。
                if idx < 1:
                    return False
                base_date = df.iloc[idx]['日期']
                if self._check_conditions_from_date_fast(
                    conditions, base_date, df, date_map, dates_sorted,
                    dates_str=dates_str, date_pos=date_pos, indicator_ctx=indicator_ctx,
                ):
                    return {'df': df, 'base_date': base_date, **ctx_tail}
                return False

            min_i = max(min_required_idx, len(df) - time_range)
            for i in range(len(df) - 1, min_i - 1, -1):
                base_date = df.iloc[i]['日期']
                if self._check_conditions_from_date_fast(
                    conditions, base_date, df, date_map, dates_sorted,
                    dates_str=dates_str, date_pos=date_pos, indicator_ctx=indicator_ctx,
                ):
                    return {'df': df, 'base_date': base_date, **ctx_tail}
            return False
        except Exception as e:
            return False

    def _check_conditions_from_date_fast(
        self, conditions, base_date, df, date_map, dates_sorted,
        dates_str=None, date_pos=None, indicator_ctx=None,
    ):
        """快速检查条件（共享交易日序列与指标上下文）。"""
        try:
            if dates_str is None or date_pos is None:
                dates_str = df['_ds'].tolist() if '_ds' in df.columns else pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d').tolist()
                date_pos = {s: i for i, s in enumerate(dates_str)}
            if indicator_ctx is None:
                indicator_ctx = self._build_indicator_ctx(conditions, df)
            for condition in conditions:
                result = self._evaluate_condition(
                    condition, base_date, date_map, df,
                    dates_sorted=dates_sorted, dates_str=dates_str, date_pos=date_pos, indicator_ctx=indicator_ctx,
                )
                if not result:
                    return False
            return True
        except Exception as e:
            return False

    def _check_bottoming_breakout(self, code, start_date, end_date, time_range=30, only_t_date=None, df=None, conditions=None):
        """二次筑底突破策略（双底/W底）。only_t_date 有值时仅检查该日是否为买点。df 可选，传入时不再拉取数据。"""
        try:
            if df is None:
                df = self.data_fetcher.get_stock_data(
                    code,
                    start_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d')
                )
            if df is None or df.empty or len(df) < 40:
                return False
            req = ['日期', '开盘', '收盘', '最高', '最低', '成交量']
            if not all(c in df.columns for c in req):
                return False
            df = df.sort_values('日期').reset_index(drop=True)
            ctx_tail = {}
            if conditions:
                dm, dstr, dpos, dsort, indicator_ctx = self._prepare_df_for_strategy(df, conditions)
                ctx_tail = {'date_map': dm, 'dates_str': dstr, 'date_pos': dpos, 'dates_sorted': dsort, 'indicator_ctx': indicator_ctx}
            matches = []
            min_i = max(30, len(df) - time_range)
            if only_t_date:
                only_d = only_t_date[:10] if isinstance(only_t_date, str) else only_t_date.strftime('%Y-%m-%d')
                for i in range(len(df) - 1, min_i - 1, -1):
                    row_date = df.iloc[i]['日期']
                    row_str = row_date.strftime('%Y-%m-%d') if hasattr(row_date, 'strftime') else str(row_date)[:10]
                    if row_str == only_d and self._is_double_bottom_breakout_day(df, i):
                        matches.append({'df': df, 'base_date': df.iloc[i]['日期'], **ctx_tail})
                        break
            else:
                for i in range(len(df) - 1, min_i - 1, -1):
                    if self._is_double_bottom_breakout_day(df, i):
                        matches.append({'df': df, 'base_date': df.iloc[i]['日期'], **ctx_tail})
            return matches if matches else False
        except Exception:
            return False

    def _is_double_bottom_breakout_day(self, df, i):
        """判断第 i 日是否为二次筑底后的放量上涨买点日

        形态：涨一波(低点1→峰1)→回调低点2→涨一小波(低点2→峰2)→二次筑底(低点3)→T日放量上涨
        变量：low1=一轮上涨最低价, peak1, low2=第一次回调低点, peak2, low3=二次筑底
        """
        close = df['收盘'].astype(float).values
        low = df['最低'].astype(float).values
        high = df['最高'].astype(float).values
        volume = df['成交量'].astype(float).values

        if i < 4:
            return False

        # 1. T日必须：阳线
        open_i = float(df['开盘'].iloc[i])
        if close[i] <= open_i:
            return False

        # 2. 找 low3：T 日或 T 日前最近的局部最低，作为二次筑底
        window = 3
        low3_idx = None
        for j in range(i, max(window, i - 25) - 1, -1):
            if j < window or j >= len(low) - window:
                continue
            left = max(0, j - window)
            right = min(len(low), j + window + 1)
            if low[j] <= low[left:right].min():
                low3_idx = j
                low3_val = low[j]
                break
        if low3_idx is None or low3_idx < window + 5:
            return False
        if low3_idx != i and low3_idx != i - 1 and low3_idx != i - 2:
            return False

        # 3. 找 peak2：low3 之前的局部最高（涨一小波的顶点）
        peak2_idx = None
        for k in range(low3_idx - 1, max(window, low3_idx - 20) - 1, -1):
            if k < window or k >= len(high) - window:
                continue
            left = max(0, k - window)
            right = min(len(high), k + window + 1)
            if high[k] >= high[left:right].max():
                peak2_idx = k
                peak2_val = high[k]
                break
        if peak2_idx is None:
            return False

        # 4. 找 low2：peak2 之前的局部最低（第一次回调低点）
        low2_idx = None
        for m in range(peak2_idx - 1, max(window, peak2_idx - 25) - 1, -1):
            if m < window or m >= len(low) - window:
                continue
            left = max(0, m - window)
            right = min(len(low), m + window + 1)
            if low[m] <= low[left:right].min():
                low2_idx = m
                low2_val = low[m]
                break
        if low2_idx is None or low2_val <= 0:
            return False

        # 5. 二次筑底：低点2×95% ≤ 低点3 ≤ 低点2×105%，峰2→低点3 回调>10%
        if low3_val < low2_val * 0.95 or low3_val > low2_val * 1.05:
            return False
        drop2_pct = (peak2_val - low3_val) / peak2_val * 100
        if drop2_pct <= 10:
            return False

        # 6. 涨一小波：低点2→峰2 涨幅 >10%
        rise_pct = (peak2_val - low2_val) / low2_val * 100
        if rise_pct <= 10:
            return False

        # 7. 前面涨了一波：任意10个交易日内 最高-最低 相差≥30%，且窗口在low2前、low2距窗口末<10日、峰→low2回调>10%
        has_rise = False
        for start in range(max(0, low2_idx - 19), low2_idx - 9):
            end = start + 10
            if end > low2_idx:
                continue
            w_high = high[start:end].max()
            w_low = low[start:end].min()
            if w_low <= 0 or w_high <= 0:
                continue
            range_ok = (w_high - w_low) / w_low >= 0.30
            days_ok = low2_idx - (end - 1) < 10
            drop_ok = (w_high - low2_val) / w_high > 0.10
            if range_ok and days_ok and drop_ok:
                has_rise = True
                peak1_val = w_high
                break
        if not has_rise:
            return False

        # 8. 回调形成低点2：该10日窗口的最高→low2 回调>10%
        drop1_pct = (peak1_val - low2_val) / peak1_val * 100
        if drop1_pct <= 10:
            return False

        # 9. 其余每波交易日 <10（回调低2已由窗口约束，涨小波、二次筑底需<10日）
        if peak2_idx - low2_idx >= 10 or low3_idx - peak2_idx >= 10:
            return False

        return True

    def _check_conditions_from_date(self, code, conditions, base_date, df):
        """从指定日期开始检查条件（单次调用也复用共享 eval 上下文）。"""
        try:
            df = df.sort_values('日期').reset_index(drop=True)
            date_map, dates_str, date_pos, dates_sorted, indicator_ctx = self._prepare_df_for_strategy(df, conditions)
            return self._check_conditions_from_date_fast(
                conditions, base_date, df, date_map, dates_sorted,
                dates_str=dates_str, date_pos=date_pos, indicator_ctx=indicator_ctx,
            )
        except Exception as e:
            return False

    def _evaluate_condition(
        self, condition, base_date, date_map, df,
        dates_sorted=None, dates_str=None, date_pos=None, indicator_ctx=None,
    ):
        """评估单个条件（确保只使用交易日）"""
        try:
            if indicator_ctx is None:
                indicator_ctx = {'ma_cross': {}, 'ma_period': {}, 'rsi_period': {}}
            if dates_str is None or date_pos is None:
                if '_ds' in df.columns:
                    dates_str = df['_ds'].tolist()
                else:
                    dates_str = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d').tolist()
                date_pos = {s: i for i, s in enumerate(dates_str)}
            if dates_sorted is None:
                dates_sorted = [datetime.strptime(s, '%Y-%m-%d') for s in dates_str]

            cond_type = condition.get('type')

            if cond_type == 'limit_up':
                # 涨停条件：date1涨停
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                return row['涨跌幅'] >= 9.8
            
            elif cond_type == 'pct_change_gt':
                # 涨幅大于零：date1涨幅>0
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                return row['涨跌幅'] > condition.get('value', 0)
            
            elif cond_type == 'pct_change_lt':
                # 涨幅小于零：date1涨幅<0
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                return row['涨跌幅'] < condition.get('value', 0)
            
            elif cond_type == 'pct_change_between':
                # 涨幅大于且小于：date1涨幅在 [minValue, maxValue] 范围内
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                min_value = condition.get('minValue', 0)
                max_value = condition.get('maxValue', 10)
                pct_change = row['涨跌幅']
                return min_value <= pct_change <= max_value
            
            elif cond_type == 'volume_ratio':
                # 成交量比例：date1成交量 / date2成交量 > ratio
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                date2 = self._get_date_offset(base_date, condition.get('date2', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None or date2 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                date2_str = date2.strftime('%Y-%m-%d')
                
                if date1_str not in date_map or date2_str not in date_map:
                    return False
                
                vol1 = date_map[date1_str]['成交量']
                vol2 = date_map[date2_str]['成交量']
                
                if vol2 == 0:
                    return False
                
                ratio = vol1 / vol2
                return ratio > condition.get('ratio', 1)
            
            elif cond_type == 'three_limit_up':
                # 三连板：在指定时间范围内（从base_date往前推）出现连续三天涨停
                # date1: 起始日期偏移（负数表示往前推，0表示base_date）
                # days: 检查的天数范围（默认30个交易日）
                check_days = condition.get('days', 30)
                start_offset = condition.get('date1', 0)
                
                # 获取起始日期
                start_date = self._get_date_offset(base_date, start_offset, df, dates_str=dates_str, date_pos=date_pos)
                if start_date is None:
                    return False

                start_str = start_date.strftime('%Y-%m-%d')
                try:
                    start_idx = date_pos[start_str]
                except KeyError:
                    return False

                # 从起始日期往前查找，最多检查 check_days 个交易日
                end_idx = max(0, start_idx - check_days + 1)

                # 检查是否有连续三天涨停
                for i in range(start_idx, end_idx - 1, -1):
                    if i < 2:  # 至少需要3天
                        break
                    # 检查连续三天是否都涨停
                    date1_str = dates_str[i]
                    date2_str = dates_str[i - 1]
                    date3_str = dates_str[i - 2]
                    
                    if (date1_str in date_map and date2_str in date_map and date3_str in date_map):
                        row1 = date_map[date1_str]
                        row2 = date_map[date2_str]
                        row3 = date_map[date3_str]
                        
                        # 连续三天都涨停
                        if (row1['涨跌幅'] >= 9.8 and 
                            row2['涨跌幅'] >= 9.8 and 
                            row3['涨跌幅'] >= 9.8):
                            return True
                
                return False

            elif cond_type == 'touch_limit_not_close':
                """
                摸板未封条件：
                - 目标日（date1 偏移后的交易日）最高价相对前一交易日收盘涨幅 >= 9.8%
                - 但当日收盘涨跌幅 < 9.8%（即当日未收盘涨停）

                注意：这里的 9.8% 与涨停判断保持一致。
                """
                # 1. 找到目标交易日（通常是 T 日：date1=0）
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False

                target_str = date1.strftime('%Y-%m-%d')
                if target_str not in date_map:
                    return False

                # 2. 准备所需字段
                row_t = date_map[target_str]
                try:
                    pct_change_t = float(row_t.get('涨跌幅') or 0)
                    high_t = float(row_t.get('最高') or 0)
                except Exception:
                    return False

                # 如果缺少关键列，直接认为不满足
                if high_t <= 0:
                    return False

                # 3. 找到 T 日在交易日序列中的位置，获取前一交易日收盘价
                try:
                    idx = date_pos[target_str]
                except KeyError:
                    return False

                if idx == 0:
                    # 没有前一交易日，无法判断是否摸板
                    return False

                prev_str = dates_str[idx - 1]
                if prev_str not in date_map:
                    return False

                try:
                    prev_close = float(date_map[prev_str].get('收盘') or 0)
                except Exception:
                    return False

                if prev_close <= 0:
                    return False

                # 4. 计算最高价相对前收盘的涨幅
                high_pct = (high_t - prev_close) / prev_close * 100

                # 5. 满足：最高价达到或超过“涨停”阈值，但收盘未涨停
                return high_pct >= 9.8 and pct_change_t < 9.8

            elif cond_type == 'high_is_limit_up':
                """
                最高价触及涨停价条件：
                - 目标日（date1 偏移后的交易日）最高价相对前一交易日收盘涨幅 >= 9.8%
                - 用于识别当日最高价触及涨停价的情况（无论收盘是否涨停）

                注意：这里的 9.8% 与涨停判断保持一致。
                """
                # 1. 找到目标交易日
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False

                target_str = date1.strftime('%Y-%m-%d')
                if target_str not in date_map:
                    return False

                # 2. 准备所需字段
                row_t = date_map[target_str]
                try:
                    high_t = float(row_t.get('最高') or 0)
                except Exception:
                    return False

                # 如果缺少关键列，直接认为不满足
                if high_t <= 0:
                    return False

                # 3. 找到 T 日在交易日序列中的位置，获取前一交易日收盘价
                try:
                    idx = date_pos[target_str]
                except KeyError:
                    return False

                if idx == 0:
                    # 没有前一交易日，无法判断
                    return False

                prev_str = dates_str[idx - 1]
                if prev_str not in date_map:
                    return False

                try:
                    prev_close = float(date_map[prev_str].get('收盘') or 0)
                except Exception:
                    return False

                if prev_close <= 0:
                    return False

                # 4. 计算最高价相对前收盘的涨幅
                high_pct = (high_t - prev_close) / prev_close * 100

                # 5. 满足：最高价达到或超过涨停阈值
                return high_pct >= 9.8

            elif cond_type == 'recent_limit_up':
                # 近期有涨停：从指定起始日期往前检查若干个交易日内，是否至少有一天涨停
                # date1: 起始日期偏移（默认-1，即从T-1日开始往前数）
                # days: 检查的天数范围（默认10个交易日）
                check_days_raw = condition.get('days', 10)
                try:
                    check_days = int(check_days_raw)
                except Exception:
                    check_days = 10
                if check_days <= 0:
                    return False

                start_offset = condition.get('date1', -1)
                
                # 获取起始日期
                start_date = self._get_date_offset(base_date, start_offset, df, dates_str=dates_str, date_pos=date_pos)
                if start_date is None:
                    return False

                try:
                    start_idx = date_pos[start_date.strftime('%Y-%m-%d')]
                except KeyError:
                    return False

                # 从起始日期往前查找，最多检查 check_days 个交易日
                end_idx = max(0, start_idx - check_days + 1)

                for i in range(start_idx, end_idx - 1, -1):
                    d_str = dates_str[i]
                    if d_str in date_map:
                        row = date_map[d_str]
                        if row['涨跌幅'] >= 9.8:
                            return True
                
                return False
            
            elif cond_type == 'ma_cross_up':
                # 均线上穿：date1日期的短均线上穿长均线
                # date1: 检查日期偏移（0表示base_date）
                # short_period: 短期均线周期（默认5）
                # long_period: 长期均线周期（默认10）
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False

                short_period = int(condition.get('shortPeriod', 5))
                long_period = int(condition.get('longPeriod', 10))

                if len(df) < long_period:
                    return False

                date1_str = date1.strftime('%Y-%m-%d')
                key = (short_period, long_period)
                pair = indicator_ctx['ma_cross'].get(key)
                if pair is None:
                    return False
                ma_s, ma_l = pair
                try:
                    date1_idx = date_pos[date1_str]
                except KeyError:
                    return False

                if date1_idx < long_period:
                    return False

                current_ma_short = ma_s[date1_idx]
                current_ma_long = ma_l[date1_idx]
                prev_ma_short = ma_s[date1_idx - 1]
                prev_ma_long = ma_l[date1_idx - 1]

                if (pd.isna(current_ma_short) or pd.isna(current_ma_long) or
                        pd.isna(prev_ma_short) or pd.isna(prev_ma_long)):
                    return False

                return current_ma_short > current_ma_long and prev_ma_short <= prev_ma_long

            elif cond_type == 'recent_n_day_pct_change_lt':
                # 近 N 个交易日累计涨跌幅 <= value（例如近5日 <= -12%）
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                days = int(condition.get('days', 5))
                if days <= 1:
                    return False
                try:
                    end_idx = date_pos[date1.strftime('%Y-%m-%d')]
                except KeyError:
                    return False
                start_idx = end_idx - days + 1
                if start_idx < 0:
                    return False
                start_str = dates_str[start_idx]
                end_str = dates_str[end_idx]
                if start_str not in date_map or end_str not in date_map:
                    return False
                start_close = float(date_map[start_str].get('收盘') or 0)
                end_close = float(date_map[end_str].get('收盘') or 0)
                if start_close <= 0:
                    return False
                pct = (end_close - start_close) / start_close * 100
                return pct <= float(condition.get('value', -12))

            elif cond_type == 'close_below_ma_deviation':
                # 收盘价低于 N 日均线一定乖离（默认低于20日线6%）
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                period = int(condition.get('period', 20))
                deviation = float(condition.get('deviation', 0.06))
                if period <= 1:
                    return False
                date1_str = date1.strftime('%Y-%m-%d')
                ma_arr = indicator_ctx['ma_period'].get(period)
                if ma_arr is None:
                    return False
                try:
                    i = date_pos[date1_str]
                except KeyError:
                    return False
                close_v = float(df.iloc[i]['收盘'])
                ma_v = float(ma_arr[i])
                if ma_v <= 0 or pd.isna(ma_v):
                    return False
                return close_v <= ma_v * (1 - deviation)

            elif cond_type == 'rsi_lt':
                # RSI(period) < value（默认 RSI(6) < 25）
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                period = int(condition.get('period', 6))
                threshold = float(condition.get('value', 25))
                if period <= 1:
                    return False
                rsi_arr = indicator_ctx['rsi_period'].get(period)
                if rsi_arr is None:
                    return False
                try:
                    i = date_pos[date1.strftime('%Y-%m-%d')]
                except KeyError:
                    return False
                rsi_v = float(rsi_arr[i])
                return rsi_v < threshold

            elif cond_type == 'stop_fall_signal':
                # 止跌迹象：长下影 或 放量（二选一）
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                open_v = float(row.get('开盘') or 0)
                close_v = float(row.get('收盘') or 0)
                high_v = float(row.get('最高') or 0)
                low_v = float(row.get('最低') or 0)
                volume_v = float(row.get('成交量') or 0)
                lower_shadow_ratio = float(condition.get('lowerShadowRatio', 0.4))
                volume_days = int(condition.get('volumeDays', 5))
                volume_ratio = float(condition.get('volumeRatio', 1.3))

                long_lower_shadow = False
                full_range = high_v - low_v
                if full_range > 0:
                    lower_shadow = min(open_v, close_v) - low_v
                    long_lower_shadow = (lower_shadow / full_range) >= lower_shadow_ratio

                vol_spike = False
                try:
                    idx = date_pos[date1_str]
                except KeyError:
                    idx = -1
                start_idx = idx - volume_days + 1
                if idx >= 0 and start_idx >= 0:
                    vols = []
                    for j in range(start_idx, idx + 1):
                        ds = dates_str[j]
                        if ds in date_map:
                            vols.append(float(date_map[ds].get('成交量') or 0))
                    if len(vols) == volume_days:
                        ma_vol = sum(vols) / volume_days
                        if ma_vol > 0:
                            vol_spike = volume_v > ma_vol * volume_ratio

                return long_lower_shadow or vol_spike

            elif cond_type == 'listed_days_gte':
                # 上市满 N 天（以可获取交易日数量近似）
                min_days = int(condition.get('days', 120))
                return len(df) >= min_days

            elif cond_type == 'avg_amount_gte':
                # 近 N 日日均成交额 >= value
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                days = int(condition.get('days', 20))
                threshold = float(condition.get('value', 200000000))
                if days <= 1:
                    return False
                try:
                    end_idx = date_pos[date1.strftime('%Y-%m-%d')]
                except KeyError:
                    return False
                start_idx = end_idx - days + 1
                if start_idx < 0:
                    return False
                amounts = []
                for j in range(start_idx, end_idx + 1):
                    ds = dates_str[j]
                    if ds in date_map:
                        amounts.append(float(date_map[ds].get('成交额') or 0))
                if len(amounts) != days:
                    return False
                return (sum(amounts) / days) >= threshold
            elif cond_type == 'main_force_build_position':
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                date1_str = date1.strftime('%Y-%m-%d')
                try:
                    base_idx = date_pos[date1_str]
                except KeyError:
                    return False
                detail = self._compute_main_force_build_detail(
                    base_idx=base_idx,
                    dates_str=dates_str,
                    date_map=date_map,
                    indicator_ctx=indicator_ctx,
                )
                return bool(detail.get('main_force_build_tag'))
            elif cond_type == 'consecutive_up_days_gte':
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                try:
                    end_idx = date_pos[date1.strftime('%Y-%m-%d')]
                except KeyError:
                    return False
                min_days = int(condition.get('consecutiveDays', condition.get('days', 3)))
                if min_days <= 0:
                    return False
                up_days = self._compute_consecutive_up_days(end_idx=end_idx, dates_str=dates_str, date_map=date_map)
                if up_days < min_days:
                    return False
                if bool(condition.get('requireMa5GtMa10')):
                    ma5 = indicator_ctx.get('ma_period', {}).get(5)
                    ma10 = indicator_ctx.get('ma_period', {}).get(10)
                    if ma5 is None or ma10 is None:
                        return False
                    start_idx = end_idx - min_days + 1
                    if start_idx < 0:
                        return False
                    for i in range(start_idx, end_idx + 1):
                        if pd.isna(ma5[i]) or pd.isna(ma10[i]) or not (ma5[i] > ma10[i]):
                            return False
                return True
            elif cond_type == 'upper_shadow_pct_gt':
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df, dates_str=dates_str, date_pos=date_pos)
                if date1 is None:
                    return False
                try:
                    end_idx = date_pos[date1.strftime('%Y-%m-%d')]
                except KeyError:
                    return False
                threshold = float(condition.get('value', 2.0))
                window_days = int(condition.get('days', condition.get('consecutiveDays', 3)))
                if window_days <= 0:
                    return False
                consecutive_up_days = self._compute_consecutive_up_days(end_idx=end_idx, dates_str=dates_str, date_map=date_map)
                scan_days = min(window_days, consecutive_up_days)
                if scan_days <= 0:
                    return False
                max_upper_shadow_pct = self._compute_max_upper_shadow_pct(
                    end_idx=end_idx,
                    dates_str=dates_str,
                    date_map=date_map,
                    window_days=scan_days,
                )
                if max_upper_shadow_pct is None:
                    return False
                return max_upper_shadow_pct > threshold
            
            return False
        except Exception as e:
            # 静默处理错误
            return False
    
    def _get_date_offset(self, base_date, offset_days, df=None, dates_str=None, date_pos=None):
        """获取相对于基准日期的日期（交易日，跳过非交易日）。

        若传入 dates_str / date_pos（与 df 行顺序一致），则 O(1) 定位；否则回退为按 df 重建序列。
        """
        if isinstance(base_date, pd.Timestamp):
            base_date = base_date.to_pydatetime()
        elif not isinstance(base_date, datetime):
            base_date = pd.to_datetime(base_date).to_pydatetime()

        if isinstance(offset_days, str):
            try:
                return datetime.strptime(offset_days, '%Y-%m-%d')
            except Exception:
                pass

        if isinstance(offset_days, (int, float)):
            offset = int(offset_days)
            if offset == 0:
                return base_date

            if dates_str is not None and date_pos is not None:
                base_str = base_date.strftime('%Y-%m-%d')
                base_idx = date_pos.get(base_str)
                if base_idx is None:
                    return None
                target_idx = base_idx + offset
                if 0 <= target_idx < len(dates_str):
                    return datetime.strptime(dates_str[target_idx], '%Y-%m-%d')
                return None

            if df is None or df.empty:
                return None
            dates = sorted([pd.to_datetime(d).to_pydatetime() for d in df['日期'].unique()])
            try:
                base_idx = dates.index(base_date)
            except ValueError:
                return None
            target_idx = base_idx + offset
            if 0 <= target_idx < len(dates):
                return dates[target_idx]
            return None

        return base_date
    
    def _normalize_match_date(self, value):
        """将匹配日期统一为 YYYY-MM-DD 字符串"""
        if value is None:
            return ''
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')
        s = str(value).strip()
        if not s:
            return ''
        # 已有 YYYY-MM-DD 或带时间戳的，只取前 10 位
        if len(s) >= 10 and s[4] == '-' and s[7] == '-':
            return s[:10]
        try:
            dt = pd.to_datetime(value)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return s[:10] if len(s) >= 10 else s
    
    def _is_month_three_limit_first_board(self, conditions):
        """判断条件组合是否为「月内三连板+首板涨停」策略。

        该策略的特征条件：
        - T 日涨停: {'type': 'limit_up', 'date1': 0}
        - T-1 日非涨停: {'type': 'pct_change_lt', 'date1': -1, 'value': 9.8}
        - 近 30 个交易日内三连板: {'type': 'three_limit_up', 'date1': -1, 'days': 30}
        - 近 10 个交易日内有涨停: {'type': 'recent_limit_up', 'date1': -1, 'days': 10}
        """
        if not conditions:
            return False
        try:
            has_limit_up_t = any(
                c.get("type") == "limit_up" and c.get("date1", 0) == 0
                for c in conditions
            )
            has_t1_not_limit = any(
                c.get("type") == "pct_change_lt"
                and c.get("date1", 0) == -1
                and float(c.get("value", 9.8)) == 9.8
                for c in conditions
            )
            has_three_limit = any(
                c.get("type") == "three_limit_up"
                and c.get("date1", 0) == -1
                and int(c.get("days", 30)) == 30
                for c in conditions
            )
            has_recent_limit = any(
                c.get("type") == "recent_limit_up"
                and c.get("date1", 0) == -1
                and int(c.get("days", 10)) == 10
                for c in conditions
            )
            return has_limit_up_t and has_t1_not_limit and has_three_limit and has_recent_limit
        except Exception:
            return False

    def _get_stock_detail_from_check(self, code, name, conditions, check_result):
        """从check结果获取股票详细信息（避免重复获取数据）"""
        try:
            if not check_result or not isinstance(check_result, dict):
                return None
            
            df = check_result['df']
            base_date = check_result['base_date']
            
            # 确保base_date是datetime类型
            if isinstance(base_date, pd.Timestamp):
                base_date = base_date.to_pydatetime()
            elif not isinstance(base_date, datetime):
                base_date = pd.to_datetime(base_date).to_pydatetime()

            date_map = check_result.get('date_map')
            dates = check_result.get('dates_sorted')
            date_pos = check_result.get('date_pos')
            if date_map is None or dates is None:
                df_tmp = df.copy()
                df_tmp['_ds'] = pd.to_datetime(df_tmp['日期']).dt.strftime('%Y-%m-%d')
                date_map = df_tmp.set_index('_ds').to_dict('index')
                dates = sorted([pd.to_datetime(d).to_pydatetime() for d in df['日期'].unique()])
                date_pos = None

            base_date_str = base_date.strftime('%Y-%m-%d')
            
            # 提取关键信息
            detail = {
                'match_date': base_date_str,
                'current_price': float(df.iloc[-1]['收盘']),
                'match_price': float(date_map[base_date_str]['收盘']) if base_date_str in date_map else 0
            }
            
            # 计算匹配日当天（T日）的振幅和涨跌幅
            try:
                if date_pos is not None and base_date_str in date_pos:
                    base_idx = date_pos[base_date_str]
                else:
                    base_idx = dates.index(base_date)
                base_close = float(date_map[base_date_str]['收盘']) if base_date_str in date_map else None
                base_open = float(date_map[base_date_str]['开盘']) if base_date_str in date_map and '开盘' in date_map[base_date_str] else None
                
                # 匹配日振幅（(收盘-开盘)/开盘*100）
                if base_open and base_close is not None and base_open > 0:
                    day1_amplitude = ((base_close - base_open) / base_open * 100)
                    detail['day1_amplitude'] = round(day1_amplitude, 2)
                
                # 匹配日涨跌幅（(当日收盘-前收)/前收*100）
                if base_idx >= 1 and base_close is not None:
                    prev_date = dates[base_idx - 1]
                    prev_str = prev_date.strftime('%Y-%m-%d')
                    if prev_str in date_map:
                        prev_close = float(date_map[prev_str]['收盘'])
                        if prev_close > 0:
                            day1_change_pct = ((base_close - prev_close) / prev_close * 100)
                            detail['day1_change_pct'] = round(day1_change_pct, 2)
            except (ValueError, KeyError, IndexError):
                pass

            # 额外标记：月内三连板+首板策略中，T 日最高价触及涨停价但收盘未涨停
            try:
                if self._is_month_three_limit_first_board(conditions):
                    if date_pos is not None and base_date_str in date_pos:
                        base_idx = date_pos[base_date_str]
                    else:
                        base_idx = dates.index(base_date)
                    if base_idx >= 1 and base_date_str in date_map:
                        prev_date = dates[base_idx - 1]
                        prev_str = prev_date.strftime('%Y-%m-%d')
                        if prev_str in date_map:
                            prev_close = float(date_map[prev_str]['收盘'])
                            row_t = date_map[base_date_str]
                            high_t = float(row_t.get('最高') or 0)
                            pct_change_t = float(row_t.get('涨跌幅') or 0)
                            # 计算最高价相对前收盘的涨幅
                            touch_limit_not_close = False
                            if prev_close > 0 and high_t > 0:
                                high_pct = (high_t - prev_close) / prev_close * 100
                                # 最高价达到或超过 9.8%，但收盘未涨停（<9.8%）
                                if high_pct >= 9.8 and pct_change_t < 9.8:
                                    touch_limit_not_close = True
                            detail['touch_limit_not_close'] = touch_limit_not_close
            except Exception:
                # 标记失败不影响主流程
                pass
            
            # 计算第二天和第三天的振幅和涨跌幅
            try:
                if date_pos is not None and base_date_str in date_pos:
                    base_idx = date_pos[base_date_str]
                else:
                    base_idx = dates.index(base_date)
                base_close = float(date_map[base_date_str]['收盘']) if base_date_str in date_map else None
                
                # 第二天（base_date + 1个交易日）
                if base_idx + 1 < len(dates):
                    day2_date = dates[base_idx + 1]
                    day2_str = day2_date.strftime('%Y-%m-%d')
                    if day2_str in date_map:
                        day2_open = float(date_map[day2_str]['开盘'])
                        day2_close = float(date_map[day2_str]['收盘'])
                        
                        # 次日振幅（(收盘-开盘)/开盘*100，百分比形式）
                        if day2_open > 0:
                            day2_amplitude = ((day2_close - day2_open) / day2_open * 100)
                            detail['day2_amplitude'] = round(day2_amplitude, 2)
                        
                        # 次日涨跌幅（(收盘-前收盘)/前收盘*100）
                        if base_close and base_close > 0:
                            day2_change_pct = ((day2_close - base_close) / base_close * 100)
                            detail['day2_change_pct'] = round(day2_change_pct, 2)
                
                # 第三天（base_date + 2个交易日）
                if base_idx + 2 < len(dates):
                    day3_date = dates[base_idx + 2]
                    day3_str = day3_date.strftime('%Y-%m-%d')
                    if day3_str in date_map:
                        day3_open = float(date_map[day3_str]['开盘'])
                        day3_close = float(date_map[day3_str]['收盘'])
                        
                        # 第三日振幅（(收盘-开盘)/开盘*100，百分比形式）
                        if day3_open > 0:
                            day3_amplitude = ((day3_close - day3_open) / day3_open * 100)
                            detail['day3_amplitude'] = round(day3_amplitude, 2)
                        
                        # 第三日涨跌幅（(收盘-前收盘)/前收盘*100）
                        # 前收盘是第二日的收盘价
                        if base_idx + 1 < len(dates):
                            day2_date = dates[base_idx + 1]
                            day2_str = day2_date.strftime('%Y-%m-%d')
                            if day2_str in date_map:
                                day2_close = float(date_map[day2_str]['收盘'])
                                if day2_close > 0:
                                    day3_change_pct = ((day3_close - day2_close) / day2_close * 100)
                                    detail['day3_change_pct'] = round(day3_change_pct, 2)
            except (ValueError, KeyError, IndexError):
                # 如果无法获取第二天或第三天的数据，跳过
                pass

            # 次日开盘买入后，统计 10 个交易日内最高涨幅及首次达到 5% 的天数（买入当日记为第 1 天）
            try:
                if date_pos is not None and base_date_str in date_pos:
                    base_idx = date_pos[base_date_str]
                else:
                    base_idx = dates.index(base_date)
                if base_idx + 1 < len(dates):
                    buy_date = dates[base_idx + 1]
                    buy_date_str = buy_date.strftime('%Y-%m-%d')
                    buy_row = date_map.get(buy_date_str)
                    if buy_row:
                        buy_price = float(buy_row.get('开盘') or 0)
                        if buy_price > 0:
                            end_idx = min(base_idx + 10, len(dates) - 1)
                            max_gain_pct = None
                            hit_day = None
                            day10_close_pct = None
                            for idx in range(base_idx + 1, end_idx + 1):
                                cur_date_str = dates[idx].strftime('%Y-%m-%d')
                                row = date_map.get(cur_date_str)
                                if not row:
                                    continue
                                day_high = float(row.get('最高') or 0)
                                if day_high <= 0:
                                    continue
                                gain_pct = (day_high - buy_price) / buy_price * 100
                                if max_gain_pct is None or gain_pct > max_gain_pct:
                                    max_gain_pct = gain_pct
                                if hit_day is None and gain_pct >= 5:
                                    hit_day = idx - base_idx
                                if idx == end_idx:
                                    day_close = float(row.get('收盘') or 0)
                                    if day_close > 0:
                                        day10_close_pct = (day_close - buy_price) / buy_price * 100
                            detail['day2_buy_10d_max_gain_pct'] = round(max_gain_pct, 2) if max_gain_pct is not None else None
                            detail['day2_buy_10d_close_pct'] = round(day10_close_pct, 2) if day10_close_pct is not None else None
                            detail['day2_buy_hit_5pct_day'] = hit_day
            except (ValueError, KeyError, IndexError):
                pass

            # 主力建仓：输出 main_force_build_tag / T 日涨停打标等（与是否命中一致，供前端展示与筛选）
            try:
                if date_pos is not None and base_date_str in date_pos:
                    base_idx = date_pos[base_date_str]
                else:
                    base_idx = dates.index(base_date)
                ic = check_result.get('indicator_ctx')
                if ic is None:
                    ic = self._build_indicator_ctx(conditions, df)
                main_force = self._compute_main_force_build_detail(
                    base_idx=base_idx,
                    dates_str=check_result.get('dates_str') or [d.strftime('%Y-%m-%d') for d in dates],
                    date_map=date_map,
                    indicator_ctx=ic,
                )
                detail.update(main_force)
            except Exception:
                pass

            # 连阳上影相关字段：连续阳线天数、连阳区间最大上影线幅度
            try:
                if date_pos is not None and base_date_str in date_pos:
                    base_idx = date_pos[base_date_str]
                else:
                    base_idx = dates.index(base_date)
                consecutive_up_days = self._compute_consecutive_up_days(
                    base_idx,
                    check_result.get('dates_str') or [d.strftime('%Y-%m-%d') for d in dates],
                    date_map,
                )
                detail['consecutive_up_days'] = consecutive_up_days
                max_upper_shadow_pct = self._compute_max_upper_shadow_pct(
                    base_idx,
                    check_result.get('dates_str') or [d.strftime('%Y-%m-%d') for d in dates],
                    date_map,
                    window_days=consecutive_up_days,
                )
                detail['upper_shadow_pct'] = round(max_upper_shadow_pct, 2) if max_upper_shadow_pct is not None else None
                detail['consecutive_up_has_limit_touch'] = self._compute_has_limit_touch_in_window(
                    base_idx,
                    check_result.get('dates_str') or [d.strftime('%Y-%m-%d') for d in dates],
                    date_map,
                    window_days=consecutive_up_days,
                )
            except Exception:
                pass
            
            return detail
        except Exception:
            return None

    def _compute_consecutive_up_days(self, end_idx, dates_str, date_map):
        """计算截至 end_idx（含）向前连续阳线（涨跌幅>0）天数。"""
        if end_idx is None or end_idx < 0 or end_idx >= len(dates_str):
            return 0
        cnt = 0
        for i in range(end_idx, -1, -1):
            row = date_map.get(dates_str[i]) or {}
            pct = float(row.get('涨跌幅') or 0)
            if pct > 0:
                cnt += 1
            else:
                break
        return cnt

    def _compute_upper_shadow_pct(self, end_idx, dates_str, date_map):
        """计算上影线幅度：最高涨幅-收盘涨幅（相对前收盘，单位%）。"""
        if end_idx is None or end_idx <= 0 or end_idx >= len(dates_str):
            return None
        today = date_map.get(dates_str[end_idx]) or {}
        prev = date_map.get(dates_str[end_idx - 1]) or {}
        prev_close = float(prev.get('收盘') or 0)
        high_v = float(today.get('最高') or 0)
        close_pct = float(today.get('涨跌幅') or 0)
        if prev_close <= 0 or high_v <= 0:
            return None
        high_pct = (high_v - prev_close) / prev_close * 100
        return high_pct - close_pct

    def _compute_max_upper_shadow_pct(self, end_idx, dates_str, date_map, window_days):
        """计算窗口内最大上影线幅度（含 end_idx，向前 window_days 个交易日）。"""
        if end_idx is None or end_idx < 0 or end_idx >= len(dates_str) or window_days <= 0:
            return None
        start_idx = max(0, end_idx - window_days + 1)
        max_val = None
        for i in range(start_idx, end_idx + 1):
            v = self._compute_upper_shadow_pct(i, dates_str, date_map)
            if v is None:
                continue
            if max_val is None or v > max_val:
                max_val = v
        return max_val

    def _compute_has_limit_touch_in_window(self, end_idx, dates_str, date_map, window_days):
        """窗口内是否有任一日最高价触及涨停价（>=9.8%）。"""
        if end_idx is None or end_idx <= 0 or end_idx >= len(dates_str) or window_days <= 0:
            return False
        start_idx = max(1, end_idx - window_days + 1)
        for i in range(start_idx, end_idx + 1):
            today = date_map.get(dates_str[i]) or {}
            prev = date_map.get(dates_str[i - 1]) or {}
            prev_close = float(prev.get('收盘') or 0)
            high_v = float(today.get('最高') or 0)
            if prev_close <= 0 or high_v <= 0:
                continue
            high_pct = (high_v - prev_close) / prev_close * 100
            if high_pct >= 9.8:
                return True
        return False

    def _compute_main_force_build_detail(self, base_idx, dates_str, date_map, indicator_ctx):
        """主力建仓规则计算（T-10~T：收涨日=收盘>前一交易日收盘，即涨跌幅>0；叠加均线形态）。"""
        out = {
            'main_force_build_tag': False,
            'main_force_t_limit_up_tag': False,
            'main_force_bullish_days': 0,
            'main_force_slope_up_days': 0,
        }
        if base_idx is None or base_idx < 1 or base_idx >= len(dates_str):
            return out
        ma_ctx = (indicator_ctx or {}).get('main_force_build') or {}
        ma5 = ma_ctx.get('ma5')
        ma10 = ma_ctx.get('ma10')
        ma20 = ma_ctx.get('ma20')
        if ma5 is None or ma10 is None or ma20 is None:
            return out

        t_str = dates_str[base_idx]
        t_row = date_map.get(t_str) or {}
        t_pct = float(t_row.get('涨跌幅') or 0)
        t_is_limit_up = t_pct >= 9.8
        prev_str = dates_str[base_idx - 1]
        prev_pct = float((date_map.get(prev_str) or {}).get('涨跌幅') or 0)
        t1_not_limit_up = prev_pct < 9.8

        bullish_cnt = 0
        slope_up_cnt = 0
        start_idx = max(0, base_idx - 10)
        for i in range(start_idx, base_idx + 1):
            row = date_map.get(dates_str[i])
            if not row:
                continue
            if i < 1:
                continue
            prev_row = date_map.get(dates_str[i - 1])
            if not prev_row:
                continue
            prev_close = float(prev_row.get('收盘') or 0)
            close_v = float(row.get('收盘') or 0)
            if prev_close <= 0 or close_v <= 0 or close_v <= prev_close:
                continue
            m5 = ma5[i]
            m10 = ma10[i]
            m20 = ma20[i]
            if pd.isna(m5) or pd.isna(m10) or pd.isna(m20):
                continue
            if not (close_v > m5 and m5 > m10 > m20):
                continue
            bullish_cnt += 1
            if not pd.isna(ma5[i - 1]) and not pd.isna(ma10[i - 1]) and m5 > ma5[i - 1] and m10 > ma10[i - 1]:
                slope_up_cnt += 1

        out['main_force_bullish_days'] = bullish_cnt
        out['main_force_slope_up_days'] = slope_up_cnt
        out['main_force_t_limit_up_tag'] = bool(t_is_limit_up)
        out['main_force_build_tag'] = bool(
            t1_not_limit_up and
            bullish_cnt >= 5 and
            slope_up_cnt * 2 >= bullish_cnt
        )
        return out
    
