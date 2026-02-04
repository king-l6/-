#!/usr/bin/env python3
"""
执行策略回测 - 绕过代理直连获取数据
"""
# 必须在导入其他模块之前设置，避免 ProxyError
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher

if __name__ == '__main__':
    fetcher = DataFetcher()
    engine = StrategyEngine(fetcher, max_workers=30)

    strategy = {
        'conditions': [
            {'type': 'limit_up', 'date1': -5},
            {'type': 'pct_change_gt', 'date1': -4, 'value': 0},
            {'type': 'pct_change_lt', 'date1': -3, 'value': 0},
            {'type': 'volume_ratio', 'date1': -4, 'date2': -3, 'ratio': 1},
            {'type': 'volume_ratio', 'date1': 0, 'date2': -3, 'ratio': 1},
            {'type': 'pct_change_gt', 'date1': 0, 'value': 0}
        ],
        'timeRange': 30
    }

    print('开始回测策略...')
    results = engine.backtest(strategy)

    print()
    print('=' * 70)
    print('回测完成！')
    print('=' * 70)
    if results:
        for i, r in enumerate(results, 1):
            pct = ((r['current_price'] - r['match_price']) / r['match_price'] * 100) if r.get('match_price') else 0
            print(f"{i}. {r['code']} {r['name']} | 匹配日: {r['match_date']} | 匹配价: {r['match_price']:.2f} | 现价: {r['current_price']:.2f} | 涨跌: {pct:+.2f}%")
    else:
        print('未找到符合条件的股票')
