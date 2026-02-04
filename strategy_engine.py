from datetime import datetime, timedelta
from data_fetcher import DataFetcher
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import json
import os

class StrategyEngine:
    """策略回测引擎"""
    
    def __init__(self, data_fetcher: DataFetcher, max_workers=10):
        self.data_fetcher = data_fetcher
        self.max_workers = max_workers  # 并发线程数
        self.results_lock = Lock()  # 线程锁
        # 结果持久化目录
        self.results_dir = os.path.join(os.path.dirname(__file__), 'results')
        os.makedirs(self.results_dir, exist_ok=True)
    
    def backtest(self, strategy, strategy_name=None):
        """执行策略回测（优化版：分阶段筛选 + 实时持久化）"""
        # 解析策略条件
        conditions = strategy.get('conditions', [])
        exclude_rules = strategy.get('exclude', {})
        time_range = strategy.get('timeRange', 30)
        
        # 生成策略名称
        if strategy_name is None:
            strategy_name = f"策略_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 结果文件路径（每条结果实时追加）
        results_filepath = os.path.join(self.results_dir, f"{strategy_name}_结果.jsonl")
        
        # 获取所有股票
        stocks = self.data_fetcher.get_stock_list()
        
        # 计算回测时间范围：timeRange 为交易日数，不含周末
        # 约 1 交易日 ≈ 1.4 日历日，多取一些确保覆盖
        end_date = datetime.now()
        calendar_days = int(time_range * 1.6) + 10  # 确保覆盖 timeRange 个交易日
        start_date = end_date - timedelta(days=calendar_days)
        
        results = []
        total_stocks = len(stocks)
        processed_count = [0]  # 使用列表以便在闭包中修改
        
        print(f"开始回测，共 {total_stocks} 只股票，回测最近 {time_range} 个交易日，使用 {self.max_workers} 个并发线程")
        
        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务（time_range=交易日数）
            future_to_stock = {
                executor.submit(self._process_stock, stock, conditions, start_date, end_date, time_range): stock
                for stock in stocks
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_stock):
                stock = future_to_stock[future]
                processed_count[0] += 1
                
                # 每10只股票显示一次进度（更频繁的进度更新）
                if processed_count[0] % 10 == 0:
                    percentage = 100 * processed_count[0] // total_stocks if total_stocks > 0 else 0
                    print(f"进度: {processed_count[0]}/{total_stocks} ({percentage}%) - 已找到 {len(results)} 只符合条件的股票", flush=True)
                
                try:
                    result = future.result(timeout=30)  # 添加30秒超时
                    if result:
                        with self.results_lock:
                            results.append(result)
                            self._append_result(results_filepath, strategy_name, result, len(results))
                            print(f"✓ 找到符合条件的股票: {result['code']} {result['name']}", flush=True)
                except Exception as e:
                    # 输出错误信息以便调试
                    if processed_count[0] % 100 == 0:  # 每100只股票输出一次错误统计
                        print(f"[WARNING] 处理股票时出错: {type(e).__name__}", flush=True)
                    continue
        
        print(f"回测完成！共检查 {total_stocks} 只股票，找到 {len(results)} 只符合条件的股票")
        if results:
            # 按符合日期从小到大排序（日期早的在前），同日期按代码排
            results.sort(key=lambda r: (r.get('match_date', '9999-99-99'), r.get('code', '')))
            self._write_sorted_results(results_filepath, strategy_name, results)
            print(f"结果已保存（按符合日期排序）: {results_filepath}")
        return results
    
    def _append_result(self, filepath, strategy_name, result, count):
        """每找到一条符合条件的结果就追加到文件"""
        try:
            if count == 1:
                # 第一条：写入元信息
                with open(filepath, 'w', encoding='utf-8') as f:
                    meta = {'_meta': {'strategy_name': strategy_name, 'run_at': datetime.now().isoformat()}}
                    f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(result, ensure_ascii=False, default=str) + '\n')
        except Exception as e:
            print(f"[WARNING] 追加结果失败: {e}")
    
    def _write_sorted_results(self, filepath, strategy_name, results):
        """按符合日期排序后重写结果文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                meta = {'_meta': {'strategy_name': strategy_name, 'run_at': datetime.now().isoformat(), 'count': len(results)}}
                f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
                for r in results:
                    f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')
        except Exception as e:
            print(f"[WARNING] 保存排序结果失败: {e}")
    
    def _process_stock(self, stock, conditions, start_date, end_date, time_range=30):
        """处理单只股票（用于并发）"""
        code = stock['code']
        name = stock['name']
        
        try:
            # 检查是否符合策略（time_range=回测的交易日数，不含周末）
            check_result = self._check_strategy(code, conditions, start_date, end_date, time_range)
            if check_result:
                # 获取详细信息（check_result包含df和base_date，避免重复获取）
                detail = self._get_stock_detail_from_check(code, name, conditions, check_result)
                if detail:
                    return {
                        'code': code,
                        'name': name,
                        **detail
                    }
        except Exception as e:
            # 只记录严重错误，避免日志过多
            if 'timeout' in str(e).lower() or 'connection' in str(e).lower():
                pass  # 网络错误静默处理
            # 其他错误也静默处理，避免影响性能
        
        return None
    
    def _check_strategy(self, code, conditions, start_date, end_date, time_range=30):
        """检查股票是否符合策略条件
        
        优化：先检查是否有涨停日，无则直接跳过；只遍历最近 time_range 个交易日作为 T
        """
        try:
            # 获取股票数据
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
            
            # 优化：若无任何涨停日，直接跳过（T-5 需涨停，无涨停则不可能符合）
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
            min_required_days = max_backward_offset + 1
            
            # 只检查最近 time_range 个交易日作为 T（不含周末，df 每行即一交易日）
            min_i = max(min_required_days, len(df) - time_range)
            for i in range(len(df) - 1, min_i - 1, -1):
                base_date = df.iloc[i]['日期']  # 回测日期（比如1月12日）
                
                # 检查从base_date开始是否符合所有条件
                if self._check_conditions_from_date(code, conditions, base_date, df):
                    # 返回df和base_date，避免重复获取数据
                    return {'df': df, 'base_date': base_date}
            
            return False
        except Exception as e:
            # 静默处理错误
            return False
    
    def _check_conditions_from_date(self, code, conditions, base_date, df):
        """从指定日期开始检查条件"""
        try:
            # 创建日期映射（使用日期字符串作为键）
            date_map = {}
            for _, row in df.iterrows():
                date_str = pd.to_datetime(row['日期']).strftime('%Y-%m-%d')
                date_map[date_str] = row
            
            # 解析每个条件
            for condition in conditions:
                if not self._evaluate_condition(condition, base_date, date_map, df):
                    return False
            
            return True
        except Exception as e:
            return False
    
    def _evaluate_condition(self, condition, base_date, date_map, df):
        """评估单个条件（确保只使用交易日）"""
        try:
            cond_type = condition.get('type')
            
            if cond_type == 'limit_up':
                # 涨停条件：date1涨停
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df)
                if date1 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                return row['涨跌幅'] >= 9.8
            
            elif cond_type == 'pct_change_gt':
                # 涨幅大于零：date1涨幅>0
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df)
                if date1 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                return row['涨跌幅'] > condition.get('value', 0)
            
            elif cond_type == 'pct_change_lt':
                # 涨幅小于零：date1涨幅<0
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df)
                if date1 is None:
                    return False  # 无法找到对应的交易日
                date1_str = date1.strftime('%Y-%m-%d')
                if date1_str not in date_map:
                    return False
                row = date_map[date1_str]
                return row['涨跌幅'] < condition.get('value', 0)
            
            elif cond_type == 'volume_ratio':
                # 成交量比例：date1成交量 / date2成交量 > ratio
                date1 = self._get_date_offset(base_date, condition.get('date1', 0), df)
                date2 = self._get_date_offset(base_date, condition.get('date2', 0), df)
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
            
            return False
        except Exception as e:
            # 静默处理错误
            return False
    
    def _get_date_offset(self, base_date, offset_days, df=None):
        """获取相对于基准日期的日期（交易日，跳过非交易日）
        
        Args:
            base_date: 基准日期（回测日期，比如1月12日）
            offset_days: 偏移交易日数（负数表示往前推，正数表示往后推）
                        例如：-5表示往前推5个交易日（跳过周末和节假日），0表示基准日期本身
            df: 股票数据DataFrame（只包含交易日数据）
        
        Returns:
            目标日期（datetime对象），如果找不到则返回None
        """
        # 确保base_date是datetime类型
        if isinstance(base_date, pd.Timestamp):
            base_date = base_date.to_pydatetime()
        elif not isinstance(base_date, datetime):
            base_date = pd.to_datetime(base_date).to_pydatetime()
        
        # offset_days可以是相对天数，也可以是绝对日期字符串
        if isinstance(offset_days, str):
            # 尝试解析为日期
            try:
                return datetime.strptime(offset_days, '%Y-%m-%d')
            except:
                pass
        
        # 如果是数字，作为相对交易日数
        if isinstance(offset_days, (int, float)):
            offset = int(offset_days)
            
            # 如果偏移为0，直接返回基准日期
            if offset == 0:
                return base_date
            
            # 必须使用DataFrame来确保只使用交易日
            if df is None or df.empty:
                # 如果没有DataFrame，无法准确计算交易日偏移
                return None
            
            # 获取所有交易日并排序（DataFrame中的数据已经是交易日，不包含周末和节假日）
            dates = sorted([pd.to_datetime(d).to_pydatetime() for d in df['日期'].unique()])
            
            try:
                # 找到base_date在dates中的索引
                base_idx = dates.index(base_date)
                target_idx = base_idx + offset  # offset为负数时往前推，正数时往后推
                
                # 检查索引是否有效
                if 0 <= target_idx < len(dates):
                    return dates[target_idx]
                else:
                    # 索引超出范围，返回None
                    return None
            except ValueError:
                # base_date不在dates中（可能是非交易日或停牌）
                return None
            except Exception:
                return None
        
        return base_date
    
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
            
            # 创建日期映射
            date_map = {}
            for _, row in df.iterrows():
                date_str = pd.to_datetime(row['日期']).strftime('%Y-%m-%d')
                date_map[date_str] = row
            
            base_date_str = base_date.strftime('%Y-%m-%d')
            # 提取关键信息
            detail = {
                'match_date': base_date_str,
                'current_price': float(df.iloc[-1]['收盘']),
                'match_price': float(date_map[base_date_str]['收盘']) if base_date_str in date_map else 0
            }
            return detail
        except Exception as e:
            return None
    
