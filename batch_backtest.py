#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量执行多个常用策略回测（支持并行执行）

使用方法：
1. 执行所有策略：
   python batch_backtest.py

2. 执行指定策略（通过策略名称）：
   python batch_backtest.py --strategies "龙头战法" "断板反包"

3. 使用自定义策略文件：
   python batch_backtest.py --config custom_strategies.json

4. 并行执行（默认）：
   python batch_backtest.py --parallel

5. 串行执行：
   python batch_backtest.py --no-parallel

6. 仅本地缓存回测（不访问 AkShare / 不拉 K 线）：
   python batch_backtest.py --cache-only --no-parallel

7. 指定单个/若干策略时默认仅用本地缓存（不补数）；需联网补 K 线时加 --fetch：
   python batch_backtest.py --strategies "昨摸板今涨停"

7. 全量回测默认即窗口内「每个命中日」各一行（同股可多日多行；日频图与当日扫描口径一致）。
"""

import json
import argparse
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher


def load_strategies(config_file='common_strategies.json'):
    """加载策略配置"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('strategies', [])
    except FileNotFoundError:
        print(f'[ERROR] 策略配置文件不存在: {config_file}')
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f'[ERROR] 策略配置文件格式错误: {e}')
        sys.exit(1)


def filter_strategies(strategies, strategy_names):
    """根据策略名称过滤策略"""
    if not strategy_names:
        return strategies
    
    filtered = []
    for strategy in strategies:
        if strategy['name'] in strategy_names:
            filtered.append(strategy)
    
    # 检查是否有未找到的策略
    found_names = {s['name'] for s in filtered}
    not_found = set(strategy_names) - found_names
    if not_found:
        print(f'[WARNING] 未找到以下策略: {", ".join(not_found)}')
    
    return filtered


def print_summary(results):
    """打印汇总信息"""
    print('\n' + '=' * 80)
    print('批量回测汇总')
    print('=' * 80)
    
    total_strategies = len(results)
    success_count = sum(1 for r in results if r['success'])
    total_stocks = sum(r['count'] for r in results)
    
    print(f'总策略数: {total_strategies}')
    print(f'成功执行: {success_count}')
    print(f'失败数量: {total_strategies - success_count}')
    print(f'找到股票总数: {total_stocks}')
    print('\n详细结果:')
    print('-' * 80)
    
    for result in results:
        status = '✓' if result['success'] else '✗'
        print(f'{status} {result["name"]}: {result["count"]} 只股票', end='')
        if not result['success']:
            print(f' (错误: {result.get("error", "未知错误")})')
        else:
            print()
            # 显示前3只股票
            if result['results']:
                print('  示例股票: ', end='')
                stocks = result['results'][:3]
                stock_strs = [f"{s['code']} {s['name']}" for s in stocks]
                print(', '.join(stock_strs))
    
    print('=' * 80)


def execute_single_strategy(strategy_config, worker_id, total_workers, timeout=None, cache_only=False):
    """执行单个策略（用于并行执行）"""
    strategy_name = strategy_config['name']
    strategy = strategy_config['strategy']
    start_time = datetime.now()
    
    # 每个策略使用独立的引擎实例（避免线程冲突）
    fetcher = DataFetcher()
    if cache_only:
        fetcher.cache_only = True
    engine = StrategyEngine(fetcher, max_workers=total_workers)
    
    try:
        print(f'[线程 {worker_id}] 开始执行策略: {strategy_name}', flush=True)
        
        # 执行策略
        strategy_results = engine.backtest(strategy, strategy_name=strategy_name)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f'[线程 {worker_id}] ✓ 策略 "{strategy_name}" 完成，找到 {len(strategy_results)} 只股票，耗时 {elapsed:.2f} 秒', flush=True)
        return {
            'name': strategy_name,
            'strategy': strategy,
            'results': strategy_results,
            'count': len(strategy_results),
            'success': True,
            'elapsed_seconds': elapsed
        }
    except Exception as e:
        import traceback
        error_msg = str(e)
        elapsed = (datetime.now() - start_time).total_seconds()
        error_type = type(e).__name__
        print(f'[线程 {worker_id}] ✗ 策略 "{strategy_name}" 执行失败: {error_type}: {error_msg} (耗时 {elapsed:.2f} 秒)', flush=True)
        if 'Timeout' in error_type:
            print(f'[线程 {worker_id}] 策略 "{strategy_name}" 执行超时', flush=True)
        return {
            'name': strategy_name,
            'strategy': strategy,
            'results': [],
            'count': 0,
            'success': False,
            'error': f'{error_type}: {error_msg}',
            'elapsed_seconds': elapsed
        }


def main():
    parser = argparse.ArgumentParser(description='批量执行多个策略回测（支持并行执行）')
    parser.add_argument('--config', default='common_strategies.json',
                       help='策略配置文件路径（默认: common_strategies.json）')
    parser.add_argument('--strategies', nargs='+',
                       help='要执行的策略名称列表（可选）')
    parser.add_argument('--workers', type=int, default=50,
                       help='每个策略的并发线程数（默认: 50）')
    parser.add_argument('--parallel', action='store_true', default=True,
                       help='并行执行多个策略（默认: True）')
    parser.add_argument('--no-parallel', dest='parallel', action='store_false',
                       help='串行执行多个策略')
    parser.add_argument('--max-parallel', type=int, default=None,
                       help='最大并行策略数（默认: 策略总数，即全部并行）')
    parser.add_argument('--timeout', type=int, default=None,
                       help='单个策略的超时时间（秒），默认无超时')
    parser.add_argument('--verbose', action='store_true',
                       help='显示详细错误信息')
    parser.add_argument(
        '--cache-only',
        action='store_true',
        help='仅使用本地 cache/stock_data 与 stock_list.json 回测，不访问 AkShare（ensure_sufficient_data 跳过）',
    )
    parser.add_argument(
        '--fetch',
        '--online',
        dest='fetch_online',
        action='store_true',
        help='允许访问 AkShare 补数（与 --strategies 联用时覆盖默认的仅缓存模式）',
    )
    parser.add_argument(
        '--all-match-dates',
        action='store_true',
        help='已废弃：全量默认即输出窗口内每个命中日，此参数无效果（保留仅为兼容旧命令行）',
    )

    args = parser.parse_args()

    # 指定策略名时默认只读本地缓存，避免新策略全量回测前先跑 ensure_sufficient_data 拉全网
    if args.strategies and not args.fetch_online and not args.cache_only:
        args.cache_only = True

    print('=' * 80)
    print('批量策略回测工具')
    print('=' * 80)
    print(f'配置文件: {args.config}')
    print(f'每个策略的并发线程数: {args.workers}')
    print(f'执行模式: {"并行" if args.parallel else "串行"}')
    if args.max_parallel:
        print(f'最大并行策略数: {args.max_parallel}')
    if args.cache_only:
        if args.strategies and not args.fetch_online:
            print('数据模式: 仅本地缓存（指定策略默认；需联网补数请加 --fetch）')
        else:
            print('数据模式: 仅本地缓存（--cache-only，不拉网）')
    print()
    
    # 加载策略
    all_strategies = load_strategies(args.config)
    
    # 过滤策略
    if args.strategies:
        strategies = filter_strategies(all_strategies, args.strategies)
        print(f'执行指定策略: {", ".join(args.strategies)}')
    else:
        strategies = all_strategies
        print(f'执行所有策略: {len(strategies)} 个')
    
    if not strategies:
        print('[ERROR] 没有可执行的策略')
        sys.exit(1)
    
    print()
    
    # 执行回测
    results = []
    start_time = datetime.now()
    
    if args.parallel:
        # 并行执行
        max_parallel = args.max_parallel if args.max_parallel else len(strategies)
        print(f'使用 {max_parallel} 个线程并行执行策略...\n')
        
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            # 提交所有任务
            future_to_strategy = {
                executor.submit(
                    execute_single_strategy,
                    strategy_config,
                    idx + 1,
                    args.workers,
                    args.timeout,
                    args.cache_only,
                ): strategy_config
                for idx, strategy_config in enumerate(strategies)
            }
            
            # 收集结果（按完成顺序）
            completed = {}
            total_strategies = len(future_to_strategy)
            
            print(f'[INFO] 等待 {total_strategies} 个策略完成...', flush=True)
            
            for future in as_completed(future_to_strategy):
                strategy_config = future_to_strategy[future]
                try:
                    # 对每个 future 设置超时（如果指定了超时时间）
                    if args.timeout:
                        try:
                            result = future.result(timeout=args.timeout)
                        except Exception as timeout_error:
                            # 超时或其他错误
                            error_type = type(timeout_error).__name__
                            error_msg = str(timeout_error)
                            print(f'[ERROR] 策略 "{strategy_config["name"]}" 获取结果超时或异常: {error_type}: {error_msg}', flush=True)
                            result = {
                                'name': strategy_config['name'],
                                'strategy': strategy_config['strategy'],
                                'results': [],
                                'count': 0,
                                'success': False,
                                'error': f'{error_type}: {error_msg}'
                            }
                    else:
                        # 没有超时限制，直接获取结果
                        result = future.result()
                    
                    completed[strategy_config['name']] = result
                    remaining = total_strategies - len(completed)
                    print(f'[进度] 已完成 {len(completed)}/{total_strategies} 个策略，剩余 {remaining} 个', flush=True)
                    
                except Exception as e:
                    import traceback
                    error_type = type(e).__name__
                    error_msg = str(e)
                    print(f'[ERROR] 策略 "{strategy_config["name"]}" 执行异常: {error_type}: {error_msg}', flush=True)
                    if args.verbose:
                        print(traceback.format_exc(), flush=True)
                    completed[strategy_config['name']] = {
                        'name': strategy_config['name'],
                        'strategy': strategy_config['strategy'],
                        'results': [],
                        'count': 0,
                        'success': False,
                        'error': f'{error_type}: {error_msg}'
                    }
                    remaining = total_strategies - len(completed)
                    print(f'[进度] 已完成 {len(completed)}/{total_strategies} 个策略，剩余 {remaining} 个', flush=True)
            
            # 确保所有策略都有结果
            if len(completed) < total_strategies:
                print(f'[WARNING] 有 {total_strategies - len(completed)} 个策略未完成', flush=True)
                # 为未完成的策略添加失败记录
                for strategy_config in strategies:
                    if strategy_config['name'] not in completed:
                        print(f'[WARNING] 策略 "{strategy_config["name"]}" 未完成，标记为失败', flush=True)
                        completed[strategy_config['name']] = {
                            'name': strategy_config['name'],
                            'strategy': strategy_config['strategy'],
                            'results': [],
                            'count': 0,
                            'success': False,
                            'error': '策略执行未完成'
                        }
            
            # 按原始顺序排序结果
            for strategy_config in strategies:
                if strategy_config['name'] in completed:
                    results.append(completed[strategy_config['name']])
    else:
        # 串行执行
        print('串行执行策略...\n')
        for idx, strategy_config in enumerate(strategies, 1):
            strategy_name = strategy_config['name']
            strategy = strategy_config['strategy']
            
            print(f'\n[{idx}/{len(strategies)}] 执行策略: {strategy_name}')
            print(f'描述: {strategy_config.get("description", "无描述")}')
            print('-' * 80)
            
            # 每个策略使用独立的引擎实例
            fetcher = DataFetcher()
            if args.cache_only:
                fetcher.cache_only = True
            engine = StrategyEngine(fetcher, max_workers=args.workers)
            
            try:
                strategy_results = engine.backtest(strategy, strategy_name=strategy_name)
                results.append({
                    'name': strategy_name,
                    'strategy': strategy,
                    'results': strategy_results,
                    'count': len(strategy_results),
                    'success': True
                })
                print(f'✓ 策略 "{strategy_name}" 完成，找到 {len(strategy_results)} 只股票')
            except Exception as e:
                import traceback
                error_msg = str(e)
                print(f'✗ 策略 "{strategy_name}" 执行失败: {error_msg}')
                if args.verbose:
                    print(traceback.format_exc())
                results.append({
                    'name': strategy_name,
                    'strategy': strategy,
                    'results': [],
                    'count': 0,
                    'success': False,
                    'error': error_msg
                })
    
    # 打印汇总
    elapsed = (datetime.now() - start_time).total_seconds()
    print('\n' + '=' * 80)
    print_summary(results)
    print(f'\n总耗时: {elapsed:.2f} 秒')
    if args.parallel and len(strategies) > 1:
        # 估算串行执行时间（假设每个策略平均耗时相同）
        success_results = [r for r in results if r['success']]
        if success_results:
            # 简单估算：假设串行执行时间 = 单个策略平均时间 * 策略数
            # 这里只是粗略估算，实际可能因策略复杂度不同而不同
            print(f'并行执行模式: 多个策略同时运行，充分利用多核CPU和I/O等待时间')
    
    # 保存结果到文件
    output_file = f'batch_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'run_at': datetime.now().isoformat(),
            'elapsed_seconds': elapsed,
            'parallel': args.parallel,
            'results': results
        }, f, ensure_ascii=False, indent=2)
    print(f'\n结果已保存到: {output_file}')
    
    # 保存结果到文件
    output_file = f'batch_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'run_at': datetime.now().isoformat(),
            'elapsed_seconds': elapsed,
            'results': results
        }, f, ensure_ascii=False, indent=2)
    print(f'\n结果已保存到: {output_file}')


if __name__ == '__main__':
    main()
