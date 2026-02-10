#!/usr/bin/env python3
"""
快速回测：先更新数据到最新，然后执行回测
（回测引擎会在需要时自动拉取缺失的数据）
"""
# 必须在导入其他模块之前设置，避免 ProxyError
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher

if __name__ == '__main__':
    print("=" * 70)
    print("快速回测模式")
    print("=" * 70)
    print("提示：回测引擎会在需要时自动拉取缺失的数据")
    print()
    
    # 先快速更新已有缓存到最新交易日
    print("步骤1: 更新已有缓存到最新交易日...")
    fetcher = DataFetcher()
    fetcher.update_caches_with_today_data(max_workers=20)
    
    print("\n步骤2: 执行回测策略...")
    print("（如果遇到缺失数据，会自动拉取）")
    print()
    
    engine = StrategyEngine(fetcher, max_workers=20)
    
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
