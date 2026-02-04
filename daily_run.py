#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日任务：先拉取今天新数据，再回测近一个月
"""
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from data_fetcher import DataFetcher
from strategy_engine import StrategyEngine


def fetch_one(stock, start_date_str, end_date_str, fetcher, force_refresh=False):
    """获取单只股票数据"""
    code = stock['code']
    try:
        df = fetcher.get_stock_data(code, start_date_str, end_date_str, force_refresh=force_refresh)
        return {'code': code, 'success': df is not None and not df.empty}
    except Exception:
        return {'code': code, 'success': False}


def fetch_if_needed(fetcher):
    """若本地缓存最新日期不是最近交易日，则拉取近一个月数据"""
    last_trade = fetcher._get_last_trading_day()
    cache_latest = fetcher.get_local_cache_latest_date()
    if not fetcher.need_fetch_recent_data():
        print(f'本地数据已是最新（缓存最新: {cache_latest} = 最近交易日: {last_trade}），跳过拉取\n')
        return

    print(f'本地缓存最新: {cache_latest}，最近交易日: {last_trade}，需要拉取新数据')
    print('=' * 60)
    print('步骤1: 拉取近一个月数据')
    print('=' * 60)

    stocks = fetcher.get_stock_list()
    total = len(stocks)
    print(f'\n共 {total} 只主板股票')

    today = datetime.now()
    start_date = today - timedelta(days=50)  # 覆盖近一个月
    start_str = start_date.strftime('%Y%m%d')
    end_str = today.strftime('%Y%m%d')
    print(f'日期范围: {start_str} ~ {end_str}\n')

    success_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_one, s, start_str, end_str, fetcher, True): s  # 强制从网络拉取
            for s in stocks
        }
        for i, future in enumerate(as_completed(futures)):
            r = future.result()
            if r['success']:
                success_count += 1
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                print(f'进度: {i+1}/{total} | 成功: {success_count} | {elapsed:.1f}秒', flush=True)

    elapsed = time.time() - start_time
    print(f'\n数据拉取完成: 成功 {success_count}/{total}, 耗时 {elapsed:.1f} 秒\n')


def run_backtest(fetcher):
    """回测近一个月"""
    print('=' * 60)
    print('步骤2: 回测近一个月')
    print('=' * 60)

    engine = StrategyEngine(fetcher, max_workers=30)

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
    fetcher.update_caches_with_today_data(max_workers=10)
    fetch_if_needed(fetcher)
    run_backtest(fetcher)
    print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 每日任务完成\n')
