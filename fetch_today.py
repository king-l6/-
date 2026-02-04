#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仅拉取今日数据并合并到已有缓存 json 文件中
"""
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

from datetime import datetime
from data_fetcher import DataFetcher

if __name__ == '__main__':
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 拉取今日数据并入缓存\n')
    fetcher = DataFetcher()
    fetcher.remove_duplicate_cache()
    fetcher.update_caches_with_today_data(max_workers=10)
    print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 完成')
