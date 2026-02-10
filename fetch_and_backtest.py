#!/usr/bin/env python3
"""
拉取最近一个月的数据并执行回测策略
"""
# 必须在导入其他模块之前设置，避免 ProxyError
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

from datetime import datetime, timedelta
from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_recent_month_data(fetcher, max_workers=20):
    """拉取最近一个月的数据（使用高效更新方法）"""
    print("=" * 70)
    print("开始拉取/更新最近一个月的数据...")
    print("=" * 70)
    
    # 使用 update_caches_with_today_data 方法，它会：
    # 1. 为没有缓存的股票创建缓存（拉取近50天数据，覆盖一个月）
    # 2. 更新已有缓存到最新交易日
    # 这个方法更高效，因为它会利用现有缓存，只更新缺失的部分
    fetcher.update_caches_with_today_data(max_workers=max_workers)
    
    print("\n数据更新完成！")
    return True

def run_backtest():
    """执行回测策略"""
    print("\n" + "=" * 70)
    print("开始执行回测策略...")
    print("=" * 70)
    
    fetcher = DataFetcher()
    engine = StrategyEngine(fetcher, max_workers=50)
    
    # 根据新策略说明.md配置的策略
    strategy = {
        'conditions': [
            {'type': 'limit_up', 'date1': -5},
            {'type': 'pct_change_gt', 'date1': -4, 'value': 0},
            {'type': 'pct_change_lt', 'date1': -3, 'value': 0},
            {'type': 'volume_ratio', 'date1': -4, 'date2': -3, 'ratio': 1},
            {'type': 'volume_ratio', 'date1': 0, 'date2': -3, 'ratio': 1},
            {'type': 'pct_change_gt', 'date1': 0, 'value': 0}
        ],
        'timeRange': 30  # 回测最近30个交易日
    }
    
    results = engine.backtest(strategy)
    
    print("\n" + "=" * 70)
    print("回测完成！")
    print("=" * 70)
    
    if results:
        print(f"\n找到 {len(results)} 只符合条件的股票：\n")
        for i, r in enumerate(results, 1):
            pct = ((r['current_price'] - r['match_price']) / r['match_price'] * 100) if r.get('match_price') and r.get('match_price') > 0 else 0
            print(f"{i}. {r['code']} {r['name']} | 匹配日: {r['match_date']} | 匹配价: {r['match_price']:.2f} | 现价: {r['current_price']:.2f} | 涨跌: {pct:+.2f}%")
    else:
        print('\n未找到符合条件的股票')
    
    return results

if __name__ == '__main__':
    import sys
    
    # 检查是否在后台运行
    if len(sys.argv) > 1 and sys.argv[1] == '--background':
        # 后台运行模式：将输出重定向到文件
        import subprocess
        log_file = f"fetch_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        print(f"后台运行模式，日志文件: {log_file}")
        subprocess.Popen(
            [sys.executable, __file__],
            stdout=open(log_file, 'w'),
            stderr=subprocess.STDOUT
        )
        print(f"进程已在后台启动，PID: {subprocess.Popen.pid if hasattr(subprocess.Popen, 'pid') else 'N/A'}")
        sys.exit(0)
    
    try:
        # 第一步：拉取最近一个月的数据
        print("提示：数据拉取可能需要较长时间（约10-30分钟），请耐心等待...")
        fetcher = DataFetcher()
        fetch_recent_month_data(fetcher, max_workers=20)
        
        # 第二步：执行回测策略
        run_backtest()
        
        print("\n" + "=" * 70)
        print("全部完成！")
        print("=" * 70)
    except KeyboardInterrupt:
        print("\n\n用户中断操作")
    except Exception as e:
        print(f"\n\n发生错误: {e}")
        import traceback
        traceback.print_exc()
