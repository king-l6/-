#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日任务：先拉取今天新数据，再回测近一个月
"""
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

from datetime import datetime, timedelta

from data_fetcher import DataFetcher
from strategy_engine import StrategyEngine


def fetch_if_needed(fetcher):
    """若本地缓存最新日期不是最近交易日，则只补齐缺失天数（增量），不全量拉取"""
    last_trade = fetcher._get_last_trading_day()
    cache_latest = fetcher.get_local_cache_latest_date()
    if not fetcher.need_fetch_recent_data():
        print(f'本地数据已是最新（缓存最新: {cache_latest} = 最近交易日: {last_trade}），跳过拉取\n')
        return

    print(f'本地缓存最新: {cache_latest}，最近交易日: {last_trade}，需要补齐缺失数据')
    print('=' * 60)
    print('步骤1: 增量补齐至最近交易日（只拉未拉取的天数）')
    print('=' * 60)
    fetcher.update_caches_with_today_data(max_workers=100)
    print()


def run_backtest(fetcher):
    """回测近一个月"""
    print('=' * 60)
    print('步骤2: 回测近一个月')
    print('=' * 60)

    engine = StrategyEngine(fetcher, max_workers=50)

    strategy = {
        'conditions': [
            {'type': 'limit_up', 'date1': -3},
            {'type': 'pct_change_gt', 'date1': -2, 'value': 0},
            {'type': 'pct_change_lt', 'date1': -1, 'value': 0},
            {'type': 'volume_ratio', 'date1': -2, 'date2': -1, 'ratio': 1},
            {'type': 'volume_ratio', 'date1': 0, 'date2': -1, 'ratio': 1},
            {'type': 'pct_change_gt', 'date1': 0, 'value': 0}
        ],
        'exclude': {'kcb': True, 'cyb': True, 'bjs': True, 'st': True, 'delist': True},
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
    return results


if __name__ == '__main__':
    print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 开始每日任务\n')
    fetcher = DataFetcher()
    fetcher.remove_duplicate_cache()
    fetcher.get_stock_list()
    print('步骤1: 拉取今日数据并入已有缓存')
    fetcher.update_caches_with_today_data(max_workers=100)
    fetch_if_needed(fetcher)
    run_backtest(fetcher)
    print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 每日任务完成\n')
