#!/usr/bin/env python3
"""
板块/概念联动数据源速度测试
测试东财、同花顺、新浪的数据源速度
"""
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import akshare as ak
from sector_linkage import (
    load_top_concept_boards,
    load_top_industry_boards,
    retry_call
)

def test_concept_spot_sources():
    """测试概念板块 spot 数据源"""
    print("\n" + "="*60)
    print("📊 概念板块 Spot 数据源测试")
    print("="*60)

    results = {}

    # 1. 东财概念 spot
    print("\n1. 东财概念 spot (stock_board_concept_name_em)")
    start_time = time.time()
    try:
        df = retry_call(ak.stock_board_concept_name_em)
        elapsed = time.time() - start_time
        if df is not None and not df.empty:
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 板块数: {len(df)}")
            results['eastmoney'] = {'success': True, 'elapsed': elapsed, 'count': len(df)}
        else:
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    # 2. 新浪概念 spot
    print("\n2. 新浪概念 spot (stock_sector_spot)")
    start_time = time.time()
    try:
        df = retry_call(lambda: ak.stock_sector_spot(indicator="概念"))
        elapsed = time.time() - start_time
        if df is not None and not df.empty:
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 板块数: {len(df)}")
            results['sina'] = {'success': True, 'elapsed': elapsed, 'count': len(df)}
        else:
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['sina'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['sina'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    return results

def test_industry_spot_sources():
    """测试行业板块 spot 数据源"""
    print("\n" + "="*60)
    print("📊 行业板块 Spot 数据源测试")
    print("="*60)

    results = {}

    # 1. 东财行业 spot
    print("\n1. 东财行业 spot (stock_board_industry_name_em)")
    start_time = time.time()
    try:
        df = retry_call(ak.stock_board_industry_name_em)
        elapsed = time.time() - start_time
        if df is not None and not df.empty:
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 板块数: {len(df)}")
            results['eastmoney'] = {'success': True, 'elapsed': elapsed, 'count': len(df)}
        else:
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    # 2. 新浪行业 spot
    print("\n2. 新浪行业 spot (stock_sector_spot)")
    start_time = time.time()
    try:
        df = retry_call(lambda: ak.stock_sector_spot(indicator="行业"))
        elapsed = time.time() - start_time
        if df is not None and not df.empty:
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 板块数: {len(df)}")
            results['sina'] = {'success': True, 'elapsed': elapsed, 'count': len(df)}
        else:
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['sina'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['sina'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    return results

def test_concept_daily_sources():
    """测试概念板块日K数据源（按 match_date 对齐）"""
    print("\n" + "="*60)
    print("📊 概念板块日K数据源测试（按 match_date）")
    print("="*60)

    results = {}

    # 1. 同花顺概念日K（默认）
    print("\n1. 同花顺概念日K (stock_board_concept_name_ths + stock_board_concept_index_ths)")
    start_time = time.time()
    try:
        # 获取概念列表
        df_name = retry_call(ak.stock_board_concept_name_ths)
        if df_name is not None and not df_name.empty:
            # 只测试前3个概念
            test_concepts = df_name.head(3)
            total_rows = 0
            for _, row in test_concepts.iterrows():
                concept_name = row.get('concept_name', row.get('name', ''))
                if concept_name:
                    try:
                        df_idx = retry_call(lambda: ak.stock_board_concept_index_ths(symbol=concept_name))
                        if df_idx is not None:
                            total_rows += len(df_idx)
                    except Exception:
                        pass
            elapsed = time.time() - start_time
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 测试3个概念, 共{total_rows}条数据")
            results['ths'] = {'success': True, 'elapsed': elapsed, 'count': len(df_name)}
        else:
            elapsed = time.time() - start_time
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['ths'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['ths'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    # 2. 东财概念日K
    print("\n2. 东财概念日K (stock_board_concept_hist_em)")
    start_time = time.time()
    try:
        # 获取概念列表
        df_name = retry_call(ak.stock_board_concept_name_em)
        if df_name is not None and not df_name.empty:
            # 只测试前3个概念
            test_concepts = df_name.head(3)
            total_rows = 0
            for _, row in test_concepts.iterrows():
                concept_name = row.get('板块名称', '')
                if concept_name:
                    try:
                        df_hist = retry_call(lambda: ak.stock_board_concept_hist_em(symbol=concept_name, period="日k", start_date="20260501", end_date="20260515"))
                        if df_hist is not None:
                            total_rows += len(df_hist)
                    except Exception:
                        pass
            elapsed = time.time() - start_time
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 测试3个概念, 共{total_rows}条数据")
            results['eastmoney'] = {'success': True, 'elapsed': elapsed, 'count': len(df_name)}
        else:
            elapsed = time.time() - start_time
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    return results

def test_industry_daily_sources():
    """测试行业板块日K数据源（按 match_date 对齐）"""
    print("\n" + "="*60)
    print("📊 行业板块日K数据源测试（按 match_date）")
    print("="*60)

    results = {}

    # 1. 同花顺行业日K（默认）
    print("\n1. 同花顺行业日K (stock_board_industry_name_ths + stock_board_industry_index_ths)")
    start_time = time.time()
    try:
        # 获取行业列表
        df_name = retry_call(ak.stock_board_industry_name_ths)
        if df_name is not None and not df_name.empty:
            # 只测试前3个行业
            test_industries = df_name.head(3)
            total_rows = 0
            for _, row in test_industries.iterrows():
                industry_name = row.get('industry_name', row.get('name', ''))
                if industry_name:
                    try:
                        df_idx = retry_call(lambda: ak.stock_board_industry_index_ths(symbol=industry_name))
                        if df_idx is not None:
                            total_rows += len(df_idx)
                    except Exception:
                        pass
            elapsed = time.time() - start_time
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 测试3个行业, 共{total_rows}条数据")
            results['ths'] = {'success': True, 'elapsed': elapsed, 'count': len(df_name)}
        else:
            elapsed = time.time() - start_time
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['ths'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['ths'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    # 2. 东财行业日K
    print("\n2. 东财行业日K (stock_board_industry_hist_em)")
    start_time = time.time()
    try:
        # 获取行业列表
        df_name = retry_call(ak.stock_board_industry_name_em)
        if df_name is not None and not df_name.empty:
            # 只测试前3个行业
            test_industries = df_name.head(3)
            total_rows = 0
            for _, row in test_industries.iterrows():
                industry_name = row.get('板块名称', '')
                if industry_name:
                    try:
                        df_hist = retry_call(lambda: ak.stock_board_industry_hist_em(symbol=industry_name, period="日k", start_date="20260501", end_date="20260515"))
                        if df_hist is not None:
                            total_rows += len(df_hist)
                    except Exception:
                        pass
            elapsed = time.time() - start_time
            print(f"   ✅ 成功 | 耗时: {elapsed:.2f}s | 测试3个行业, 共{total_rows}条数据")
            results['eastmoney'] = {'success': True, 'elapsed': elapsed, 'count': len(df_name)}
        else:
            elapsed = time.time() - start_time
            print(f"   ❌ 失败: 返回空数据 | 耗时: {elapsed:.2f}s")
            results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': '空数据'}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ 失败: {str(e)[:80]} | 耗时: {elapsed:.2f}s")
        results['eastmoney'] = {'success': False, 'elapsed': elapsed, 'error': str(e)[:80]}

    return results

def main():
    """主测试函数"""
    print("="*60)
    print("🚀 板块/概念联动数据源速度测试")
    print("="*60)

    # 测试各类数据源
    concept_spot_results = test_concept_spot_sources()
    industry_spot_results = test_industry_spot_sources()
    concept_daily_results = test_concept_daily_sources()
    industry_daily_results = test_industry_daily_sources()

    # 汇总报告
    print("\n" + "="*60)
    print("📈 测试结果汇总")
    print("="*60)

    print("\n【概念板块 Spot】")
    print(f"{'数据源':<12} {'状态':<8} {'耗时':<10} {'板块数':<10}")
    print("-"*40)
    for source, r in concept_spot_results.items():
        status = "✅" if r['success'] else "❌"
        elapsed = f"{r['elapsed']:.2f}s"
        count = r.get('count', '-')
        print(f"{source:<12} {status:<8} {elapsed:<10} {count:<10}")

    print("\n【行业板块 Spot】")
    print(f"{'数据源':<12} {'状态':<8} {'耗时':<10} {'板块数':<10}")
    print("-"*40)
    for source, r in industry_spot_results.items():
        status = "✅" if r['success'] else "❌"
        elapsed = f"{r['elapsed']:.2f}s"
        count = r.get('count', '-')
        print(f"{source:<12} {status:<8} {elapsed:<10} {count:<10}")

    print("\n【概念板块日K（按 match_date）】")
    print(f"{'数据源':<12} {'状态':<8} {'耗时':<10} {'板块数':<10}")
    print("-"*40)
    for source, r in concept_daily_results.items():
        status = "✅" if r['success'] else "❌"
        elapsed = f"{r['elapsed']:.2f}s"
        count = r.get('count', '-')
        print(f"{source:<12} {status:<8} {elapsed:<10} {count:<10}")

    print("\n【行业板块日K（按 match_date）】")
    print(f"{'数据源':<12} {'状态':<8} {'耗时':<10} {'板块数':<10}")
    print("-"*40)
    for source, r in industry_daily_results.items():
        status = "✅" if r['success'] else "❌"
        elapsed = f"{r['elapsed']:.2f}s"
        count = r.get('count', '-')
        print(f"{source:<12} {status:<8} {elapsed:<10} {count:<10}")

    # 推荐配置
    print("\n" + "="*60)
    print("🎯 推荐配置")
    print("="*60)

    # 概念 spot 推荐
    concept_spot_ok = [k for k, v in concept_spot_results.items() if v['success']]
    if concept_spot_ok:
        fastest = min(concept_spot_ok, key=lambda x: concept_spot_results[x]['elapsed'])
        print(f"\n概念 Spot: {fastest} ({concept_spot_results[fastest]['elapsed']:.2f}s)")

    # 行业 spot 推荐
    industry_spot_ok = [k for k, v in industry_spot_results.items() if v['success']]
    if industry_spot_ok:
        fastest = min(industry_spot_ok, key=lambda x: industry_spot_results[x]['elapsed'])
        print(f"行业 Spot: {fastest} ({industry_spot_results[fastest]['elapsed']:.2f}s)")

    # 概念日K 推荐
    concept_daily_ok = [k for k, v in concept_daily_results.items() if v['success']]
    if concept_daily_ok:
        fastest = min(concept_daily_ok, key=lambda x: concept_daily_results[x]['elapsed'])
        print(f"概念日K: {fastest} ({concept_daily_results[fastest]['elapsed']:.2f}s)")

    # 行业日K 推荐
    industry_daily_ok = [k for k, v in industry_daily_results.items() if v['success']]
    if industry_daily_ok:
        fastest = min(industry_daily_ok, key=lambda x: industry_daily_results[x]['elapsed'])
        print(f"行业日K: {fastest} ({industry_daily_results[fastest]['elapsed']:.2f}s)")

    print("\n环境变量配置:")
    print("  SECTOR_LINKAGE_CONCEPT_DAILY=ths        # 同花顺概念日K（默认，慢但稳）")
    print("  SECTOR_LINKAGE_CONCEPT_DAILY=eastmoney  # 东财概念日K（快但易出错）")
    print("  SECTOR_LINKAGE_INDUSTRY_DAILY=ths       # 同花顺行业日K（默认）")
    print("  SECTOR_LINKAGE_INDUSTRY_DAILY=eastmoney # 东财行业日K")
    print("  SECTOR_LINKAGE_MATCH_DATE=0             # 禁用按 match_date 对齐")
    print("  SECTOR_LINKAGE_OFFLINE=1                # 禁用HTTP请求（仅用缓存）")

if __name__ == '__main__':
    main()
