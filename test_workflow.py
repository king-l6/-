#!/usr/bin/env python3
"""
工作流程测试脚本
测试增量拉取、全量拉取、新增策略回测等场景
"""
import time
import sys
import os
import json
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.dirname(__file__))

from data_fetcher import DataFetcher
from strategy_engine import StrategyEngine

def test_incremental_fetch():
    """测试增量数据拉取"""
    print("\n" + "="*60)
    print("📊 测试 1: 增量数据拉取")
    print("="*60)

    fetcher = DataFetcher()

    # 获取股票列表
    stocks = fetcher.get_stock_list()
    print(f"股票总数: {len(stocks)}")

    # 测试增量更新（只更新最近交易日）
    print("\n开始增量更新...")
    start_time = time.time()
    fetcher.update_caches_with_today_data(max_workers=20)
    elapsed = time.time() - start_time
    print(f"增量更新完成 | 耗时: {elapsed:.2f}s")

def test_full_fetch(days=30):
    """测试全量数据拉取"""
    print("\n" + "="*60)
    print(f"📊 测试 2: 全量数据拉取（最近 {days} 个交易日）")
    print("="*60)

    fetcher = DataFetcher()

    print(f"\n开始全量拉取（{days} 个交易日）...")
    start_time = time.time()
    fetcher.ensure_sufficient_data(time_range=days, max_workers=50)
    elapsed = time.time() - start_time
    print(f"全量拉取完成 | 耗时: {elapsed:.2f}s")

def test_incremental_backtest():
    """测试增量回测（只检查最近交易日）"""
    print("\n" + "="*60)
    print("📊 测试 3: 增量回测（最近交易日）")
    print("="*60)

    fetcher = DataFetcher()
    engine = StrategyEngine(fetcher, max_workers=20)

    # 简单策略：涨幅大于 5%
    strategy = {
        'name': '涨幅大于5%',
        'conditions': [
            {'type': 'pct_change_gt', 'date1': 0, 'value': 5}
        ]
    }

    # 获取最近交易日
    last_trade = fetcher._get_last_trading_day()
    print(f"最近交易日: {last_trade}")

    print("\n开始增量回测...")
    start_time = time.time()
    results = engine.backtest(
        strategy,
        strategy_name='增量回测_涨幅5%',
        time_range=1,
        only_t_date=last_trade
    )
    elapsed = time.time() - start_time
    print(f"增量回测完成 | 耗时: {elapsed:.2f}s | 找到: {len(results)} 只股票")

    return results

def test_full_backtest(time_range=30):
    """测试全量回测"""
    print("\n" + "="*60)
    print(f"📊 测试 4: 全量回测（最近 {time_range} 个交易日）")
    print("="*60)

    fetcher = DataFetcher()
    engine = StrategyEngine(fetcher, max_workers=20)

    # 简单策略：涨幅大于 5%
    strategy = {
        'name': '涨幅大于5%',
        'conditions': [
            {'type': 'pct_change_gt', 'date1': 0, 'value': 5}
        ]
    }

    print(f"\n开始全量回测（{time_range} 个交易日）...")
    start_time = time.time()
    results = engine.backtest(
        strategy,
        strategy_name='全量回测_涨幅5%',
        time_range=time_range,
        write_results=False
    )
    elapsed = time.time() - start_time
    print(f"全量回测完成 | 耗时: {elapsed:.2f}s | 找到: {len(results)} 条记录")

    return results

def test_new_strategy_backtest():
    """测试新增策略回测"""
    print("\n" + "="*60)
    print("📊 测试 5: 新增策略全量回测")
    print("="*60)

    fetcher = DataFetcher()
    engine = StrategyEngine(fetcher, max_workers=20)

    # 新增策略：连续涨停后回调
    strategy = {
        'name': '涨停回调',
        'conditions': [
            {'type': 'recent_limit_up', 'date1': -1, 'days': 10},  # 近10日有涨停
            {'type': 'pct_change_lt', 'date1': 0, 'value': -3}     # 今日跌幅 > 3%
        ]
    }

    print("策略条件:")
    print("  - 近10个交易日有涨停")
    print("  - 今日跌幅 > 3%")

    print("\n开始新策略全量回测（20个交易日）...")
    start_time = time.time()
    results = engine.backtest(
        strategy,
        strategy_name='新策略_涨停回调',
        time_range=20,
        write_results=True
    )
    elapsed = time.time() - start_time
    print(f"新策略回测完成 | 耗时: {elapsed:.2f}s | 找到: {len(results)} 条记录")

    return results

def main():
    """主测试流程"""
    print("="*60)
    print("🚀 量化回测系统 - 工作流程测试")
    print("="*60)

    # 设置数据源（可选：auto, sina, tencent, eastmoney）
    os.environ['DATA_FETCH_STOCK_HIST_SOURCE'] = 'auto'

    # 测试 1: 增量拉取
    test_incremental_fetch()

    # 测试 2: 全量拉取（10个交易日，减少测试时间）
    test_full_fetch(days=10)

    # 测试 3: 增量回测
    test_incremental_backtest()

    # 测试 4: 全量回测（5个交易日，减少测试时间）
    test_full_backtest(time_range=5)

    # 测试 5: 新增策略回测
    test_new_strategy_backtest()

    print("\n" + "="*60)
    print("✅ 所有测试完成")
    print("="*60)

if __name__ == '__main__':
    main()
