#!/usr/bin/env python3
"""
数据源速度测试脚本
测试东财、新浪、腾讯三个数据源的拉取速度
"""
import time
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(__file__))

from data_fetcher import DataFetcher

def test_single_source(code, start_date, end_date, source_name):
    """测试单个数据源的速度"""
    # 设置环境变量指定数据源
    os.environ['DATA_FETCH_STOCK_HIST_SOURCE'] = source_name

    fetcher = DataFetcher()

    print(f"\n{'='*50}")
    print(f"测试数据源: {source_name}")
    print(f"股票代码: {code}")
    print(f"时间范围: {start_date} ~ {end_date}")
    print(f"{'='*50}")

    # 测试拉取速度
    start_time = time.time()
    try:
        df = fetcher.get_stock_data(code, start_date, end_date, force_refresh=True)
        elapsed = time.time() - start_time

        if df is not None and not df.empty:
            print(f"✅ 成功 | 耗时: {elapsed:.2f}s | 数据行数: {len(df)}")
            print(f"   最早日期: {df['日期'].min()}")
            print(f"   最新日期: {df['日期'].max()}")
            return {
                'source': source_name,
                'success': True,
                'elapsed': elapsed,
                'rows': len(df),
                'min_date': str(df['日期'].min()),
                'max_date': str(df['日期'].max())
            }
        else:
            print(f"❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            return {'source': source_name, 'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ 失败: {str(e)[:100]} | 耗时: {elapsed:.2f}s")
        return {'source': source_name, 'success': False, 'elapsed': elapsed, 'error': str(e)[:100]}

def test_batch_speed(code_list, start_date, end_date, source_name, max_workers=10):
    """测试批量拉取速度"""
    os.environ['DATA_FETCH_STOCK_HIST_SOURCE'] = source_name

    fetcher = DataFetcher()
    fetcher.max_workers = max_workers

    print(f"\n{'='*50}")
    print(f"批量测试: {source_name} | {len(code_list)} 只股票 | {max_workers} 并发")
    print(f"{'='*50}")

    start_time = time.time()
    success_count = 0
    fail_count = 0

    for code in code_list:
        try:
            df = fetcher.get_stock_data(code, start_date, end_date, force_refresh=False)
            if df is not None and not df.empty:
                success_count += 1
            else:
                fail_count += 1
        except Exception:
            fail_count += 1

    elapsed = time.time() - start_time
    print(f"完成 | 耗时: {elapsed:.2f}s | 成功: {success_count} | 失败: {fail_count}")
    print(f"平均每只: {elapsed/len(code_list):.2f}s")

    return {
        'source': source_name,
        'total': len(code_list),
        'success': success_count,
        'fail': fail_count,
        'elapsed': elapsed,
        'avg_per_stock': elapsed / len(code_list)
    }

def main():
    """主测试函数"""
    # 测试用的股票代码（主板股票）
    test_codes = ['600519', '000858', '601318', '000001', '600036']

    # 时间范围：最近 30 个交易日（约 45 天）
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    print("="*60)
    print("A股数据源速度测试")
    print("="*60)

    # 1. 测试单只股票各数据源速度
    print("\n\n📊 单只股票拉取速度测试")
    single_results = []
    for source in ['eastmoney', 'sina', 'tencent']:
        result = test_single_source('600519', start_date, end_date, source)
        single_results.append(result)

    # 2. 测试批量拉取速度
    print("\n\n📊 批量拉取速度测试（5只股票）")
    batch_results = []
    for source in ['eastmoney', 'sina', 'tencent']:
        result = test_batch_speed(test_codes, start_date, end_date, source, max_workers=5)
        batch_results.append(result)

    # 3. 汇总报告
    print("\n\n" + "="*60)
    print("📈 测试结果汇总")
    print("="*60)

    print("\n单只股票拉取:")
    print(f"{'数据源':<12} {'状态':<8} {'耗时':<10} {'数据行数':<10}")
    print("-"*40)
    for r in single_results:
        status = "✅ 成功" if r['success'] else "❌ 失败"
        elapsed = f"{r['elapsed']:.2f}s"
        rows = r.get('rows', '-')
        print(f"{r['source']:<12} {status:<8} {elapsed:<10} {rows:<10}")

    print("\n批量拉取（5只股票）:")
    print(f"{'数据源':<12} {'成功/总数':<12} {'总耗时':<10} {'平均每只':<10}")
    print("-"*44)
    for r in batch_results:
        success_ratio = f"{r['success']}/{r['total']}"
        elapsed = f"{r['elapsed']:.2f}s"
        avg = f"{r['avg_per_stock']:.2f}s"
        print(f"{r['source']:<12} {success_ratio:<12} {elapsed:<10} {avg:<10}")

    # 4. 推荐配置
    print("\n\n🎯 推荐配置:")
    successful = [r for r in single_results if r['success']]
    if successful:
        fastest = min(successful, key=lambda x: x['elapsed'])
        print(f"最快数据源: {fastest['source']} ({fastest['elapsed']:.2f}s)")

    print("\n环境变量配置:")
    print("  DATA_FETCH_STOCK_HIST_SOURCE=auto    # 自动降级（默认）")
    print("  DATA_FETCH_STOCK_HIST_SOURCE=eastmoney  # 仅东财")
    print("  DATA_FETCH_STOCK_HIST_SOURCE=sina       # 仅新浪")
    print("  DATA_FETCH_STOCK_HIST_SOURCE=tencent    # 仅腾讯")

if __name__ == '__main__':
    main()
