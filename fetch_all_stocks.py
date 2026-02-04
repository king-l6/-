#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量获取所有主板股票近一个月的数据（使用 AKShare）
"""

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from data_fetcher import DataFetcher


def fetch_one(stock, start_date_str, end_date_str, fetcher):
    """获取单只股票数据"""
    code = stock['code']
    try:
        df = fetcher.get_stock_data(code, start_date_str, end_date_str)
        return {'code': code, 'success': df is not None and not df.empty}
    except Exception:
        return {'code': code, 'success': False}


def main():
    print('=' * 60)
    print('批量获取主板股票近一个月数据（AKShare）')
    print('=' * 60)

    fetcher = DataFetcher()
    stocks = fetcher.get_stock_list()
    total = len(stocks)
    print(f'\n共 {total} 只主板股票')

    today = datetime.now()
    start_date = today - timedelta(days=30)
    start_str = start_date.strftime('%Y%m%d')
    end_str = today.strftime('%Y%m%d')
    print(f'日期范围: {start_str} ~ {end_str}\n')

    success_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_one, s, start_str, end_str, fetcher): s
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
    print(f'\n完成: 成功 {success_count}/{total}, 耗时 {elapsed:.1f} 秒')


if __name__ == '__main__':
    main()
