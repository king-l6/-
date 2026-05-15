#!/usr/bin/env python3
"""
快速工作流程测试
只测试少量股票，快速验证流程
"""
import time
import sys
import os
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from data_fetcher import DataFetcher
from strategy_engine import StrategyEngine

def test_quick():
    """快速测试完整流程"""
    print("="*60)
    print("🚀 快速工作流程测试")
    print("="*60)

    fetcher = DataFetcher()

    # 1. 测试单只股票拉取速度
    print("\n📊 1. 数据源速度对比")
    print("-"*40)

    test_code = '600519'
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

    results = {}
    for source in ['sina', 'tencent', 'eastmoney']:
        os.environ['DATA_FETCH_STOCK_HIST_SOURCE'] = source
        # 重建 fetcher 以清除缓存
        fetcher = DataFetcher()

        start_time = time.time()
        try:
            df = fetcher.get_stock_data(test_code, start_date, end_date, force_refresh=True)
            elapsed = time.time() - start_time
            success = df is not None and not df.empty
            rows = len(df) if success else 0
            results[source] = {'success': success, 'elapsed': elapsed, 'rows': rows}
            status = "✅" if success else "❌"
            print(f"{status} {source:<12} | {elapsed:.2f}s | {rows} 行")
        except Exception as e:
            elapsed = time.time() - start_time
            results[source] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:50]}
            print(f"❌ {source:<12} | {elapsed:.2f}s | 错误: {str(e)[:50]}")

    # 2. 测试增量回测（使用最快的数据源）
    print("\n📊 2. 增量回测测试（最近1个交易日）")
    print("-"*40)

    # 选择最快的成功数据源
    successful = {k: v for k, v in results.items() if v['success']}
    if successful:
        fastest = min(successful.items(), key=lambda x: x[1]['elapsed'])
        os.environ['DATA_FETCH_STOCK_HIST_SOURCE'] = fastest[0]
        print(f"使用最快数据源: {fastest[0]}")
    else:
        os.environ['DATA_FETCH_STOCK_HIST_SOURCE'] = 'sina'

    fetcher = DataFetcher()
    engine = StrategyEngine(fetcher, max_workers=10)

    # 简单策略：涨幅大于 3%
    strategy = {
        'name': '涨幅大于3%',
        'conditions': [
            {'type': 'pct_change_gt', 'date1': 0, 'value': 3}
        ],
        'timeRange': 1  # 增量回测：1个交易日
    }

    last_trade = fetcher._get_last_trading_day()
    print(f"最近交易日: {last_trade}")

    start_time = time.time()
    try:
        results = engine.backtest_single_day(
            strategy,
            strategy_name='增量测试_涨幅3%',
            trading_date=last_trade
        )
        elapsed = time.time() - start_time
        print(f"✅ 增量回测完成 | 耗时: {elapsed:.2f}s | 找到: {len(results)} 只股票")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ 增量回测失败 | 耗时: {elapsed:.2f}s | 错误: {str(e)[:100]}")

    # 3. 测试全量回测（最近5个交易日）
    print("\n📊 3. 全量回测测试（最近5个交易日）")
    print("-"*40)

    strategy['timeRange'] = 5  # 全量回测：5个交易日

    start_time = time.time()
    try:
        results = engine.backtest(
            strategy,
            strategy_name='全量测试_涨幅3%'
        )
        elapsed = time.time() - start_time
        print(f"✅ 全量回测完成 | 耗时: {elapsed:.2f}s | 找到: {len(results)} 条记录")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ 全量回测失败 | 耗时: {elapsed:.2f}s | 错误: {str(e)[:100]}")

    # 4. 测试新增策略
    print("\n📊 4. 新增策略回测测试")
    print("-"*40)

    new_strategy = {
        'name': '涨停回调',
        'conditions': [
            {'type': 'recent_limit_up', 'date1': -1, 'days': 10},
            {'type': 'pct_change_lt', 'date1': 0, 'value': -2}
        ],
        'timeRange': 10  # 10个交易日
    }

    print("策略: 近10日有涨停 + 今日跌幅>2%")

    start_time = time.time()
    try:
        results = engine.backtest(
            new_strategy,
            strategy_name='新策略_涨停回调'
        )
        elapsed = time.time() - start_time
        print(f"✅ 新策略回测完成 | 耗时: {elapsed:.2f}s | 找到: {len(results)} 条记录")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ 新策略回测失败 | 耗时: {elapsed:.2f}s | 错误: {str(e)[:100]}")

    # 汇总
    print("\n" + "="*60)
    print("📈 测试汇总")
    print("="*60)
    print(f"数据源推荐: {fastest[0] if successful else 'sina'}")
    print(f"环境变量: DATA_FETCH_STOCK_HIST_SOURCE=auto (自动降级)")
    print("\n降级顺序: 新浪 → 腾讯 → 东财")

if __name__ == '__main__':
    test_quick()
