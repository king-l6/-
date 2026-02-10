#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查任务进度脚本
"""
import os
import glob
import json
import subprocess
from datetime import datetime
from data_fetcher import DataFetcher

def check_task_status():
    """检查任务是否在运行"""
    try:
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True
        )
        lines = result.stdout.split('\n')
        running = False
        for line in lines:
            if 'daily_run.py' in line and 'grep' not in line:
                running = True
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[1]
                    print(f'✓ 任务正在运行 (进程 ID: {pid})')
                break
        if not running:
            print('✗ 任务未运行')
        return running
    except Exception:
        print('✗ 无法检查任务状态')
        return False

def check_cache_progress():
    """检查缓存文件进度"""
    fetcher = DataFetcher()
    stocks = fetcher.get_stock_list()
    total_stocks = len(stocks)
    
    cache_files = glob.glob(os.path.join(fetcher.stock_data_cache_dir, '*.json'))
    cache_codes = set()
    for f in cache_files:
        name = os.path.basename(f)
        if '_' in name and name.endswith('.json'):
            parts = name[:-5].split('_')
            if len(parts) == 3:
                code = parts[0]
                if len(code) == 6:
                    cache_codes.add(code)
    
    cached_count = len(cache_codes)
    missing_count = total_stocks - cached_count
    
    print(f'\n📊 缓存文件统计:')
    print(f'  股票总数: {total_stocks}')
    print(f'  已有缓存: {cached_count} ({cached_count*100//total_stocks if total_stocks > 0 else 0}%)')
    print(f'  缺少缓存: {missing_count}')
    
    return cached_count, total_stocks

def check_data_freshness():
    """检查数据新鲜度"""
    fetcher = DataFetcher()
    last_trade = fetcher._get_last_trading_day()
    cache_latest = fetcher.get_local_cache_latest_date()
    need_update = fetcher.need_fetch_recent_data()
    
    print(f'\n📅 数据状态:')
    print(f'  最近交易日: {last_trade}')
    print(f'  缓存最新日期: {cache_latest if cache_latest else "无数据"}')
    
    if cache_latest:
        if cache_latest == last_trade:
            print(f'  ✓ 数据已是最新')
        else:
            print(f'  ⚠ 数据需要更新 (落后 {last_trade} - {cache_latest} = {(datetime.strptime(last_trade, "%Y-%m-%d") - datetime.strptime(cache_latest, "%Y-%m-%d")).days} 天)')
    else:
        print(f'  ⚠ 无缓存数据')
    
    return need_update

def check_latest_results():
    """检查最新回测结果"""
    results_dir = os.path.join(os.path.dirname(__file__), 'results')
    if not os.path.exists(results_dir):
        print(f'\n📋 回测结果: 无结果文件')
        return
    
    result_files = glob.glob(os.path.join(results_dir, '*.jsonl'))
    if not result_files:
        print(f'\n📋 回测结果: 无结果文件')
        return
    
    latest_file = max(result_files, key=os.path.getmtime)
    file_time = datetime.fromtimestamp(os.path.getmtime(latest_file))
    
    print(f'\n📋 最新回测结果:')
    print(f'  文件: {os.path.basename(latest_file)}')
    print(f'  时间: {file_time.strftime("%Y-%m-%d %H:%M:%S")}')
    
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        data_lines = [l for l in lines if l.strip() and not l.strip().startswith('{"_meta')]
        print(f'  找到股票数: {len(data_lines)}')
        
        if data_lines:
            # 显示前5只股票
            print(f'\n  前5只股票:')
            for i, line in enumerate(data_lines[:5], 1):
                data = json.loads(line)
                pct = ((data['current_price'] - data['match_price']) / data['match_price'] * 100) if data.get('match_price') else 0
                print(f'    {i}. {data["code"]} {data["name"]} | 匹配日: {data["match_date"]} | 涨跌: {pct:+.2f}%')
    except Exception as e:
        print(f'  读取结果文件失败: {e}')

def main():
    print('=' * 70)
    print(f'任务进度检查 - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 70)
    
    # 检查任务状态
    print('\n🔄 任务状态:')
    is_running = check_task_status()
    
    # 检查缓存进度
    cached_count, total_stocks = check_cache_progress()
    
    # 检查数据新鲜度
    need_update = check_data_freshness()
    
    # 检查最新结果
    check_latest_results()
    
    # 总结
    print('\n' + '=' * 70)
    if is_running:
        progress = cached_count * 100 // total_stocks if total_stocks > 0 else 0
        print(f'📈 总体进度: {progress}% ({cached_count}/{total_stocks})')
        print('💡 提示: 任务正在运行中，请稍候...')
    else:
        if cached_count == total_stocks and not need_update:
            print('✅ 所有任务已完成！')
        else:
            print('⚠️  任务未完成，请运行 daily_run.py')
    print('=' * 70)

if __name__ == '__main__':
    main()
