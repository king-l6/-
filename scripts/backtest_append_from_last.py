#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按「战法结果文件」中最后一个数据的日期，只回测该日期之后到今天的 T 日，并把结果追加到**同一个战法结果文件**中。

步骤：
1. 读取预设战法（如：龙头战法、断板反包、均线上穿、情绪周期、三连板、筑底突破、月内三连板+首板涨停）的结果文件
   （主文件 策略名_结果.jsonl + 按日文件 策略名_YYYYMMDD_结果.jsonl）
2. 每个策略取最大的 match_date，再在这些日期中取最小值作为「最后日期」
   （若某策略无数据则跳过，只从有数据的策略中取最小最后日期）
3. 若存在最后日期，则只回测 [最后日期+1 日, 今天] 之间的所有交易日 T
4. **按股票并发**：每只股票只加载一次 K 线，在该数据上对所有 T 日×所有策略做检查，再按策略聚合写回各自 策略名_结果.jsonl（比「每 T 日×每策略」全量扫一遍快得多）。

使用方法（在项目根目录）：
  python scripts/backtest_append_from_last.py                    # 自动检测最后日期并回测补齐（全部预设战法）
  python scripts/backtest_append_from_last.py --strategies \"月内三连板+首板涨停\"   # 只回测指定战法
  python scripts/backtest_append_from_last.py --no-check-cache   # 不检查缓存是否最新
  python scripts/backtest_append_from_last.py --workers 50
"""

import os
import sys
import json
import re
import argparse
import glob
import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ.setdefault('NO_PROXY', '*')
os.environ.setdefault('no_proxy', '*')


# 预设战法名称（与 common_strategies.json 及前端一致）
STRATEGY_NAMES = [
    '龙头战法',
    '断板反包',
    '均线上穿',
    '情绪周期',
    '三连板',
    '筑底突破',
    '月内三连板+首板涨停',
]


def _read_one_results_file(filepath):
    """读取单个 .jsonl 结果文件，返回 (meta_dict, results_list[仅日期字符串])。"""
    meta = None
    results = []
    if not os.path.isfile(filepath):
        return meta, results
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return meta, results
    if not lines:
        return meta, results
    try:
        first = json.loads(lines[0].strip())
        if '_meta' in first:
            meta = first['_meta']
            lines = lines[1:]
    except Exception:
        pass
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            if 'code' not in data:
                continue
            match_date = data.get('match_date')
            if match_date is None:
                continue
            s = str(match_date).strip()
            if len(s) >= 10 and s[4] == '-' and s[7] == '-':
                results.append(s[:10])
        except Exception:
            continue
    return meta, results


def get_last_date_for_strategy(results_dir, strategy_name):
    """获取该策略在主文件 + 按日文件中出现的最大 match_date，无则返回 None。"""
    main_file = os.path.join(results_dir, f"{strategy_name}_结果.jsonl")
    pattern = re.compile(re.escape(strategy_name) + r'_(\d{8})_结果\.jsonl$')
    all_dates = []

    if os.path.isfile(main_file):
        _, rows = _read_one_results_file(main_file)
        all_dates.extend(rows)

    for filename in os.listdir(results_dir):
        if not filename.endswith('.jsonl'):
            continue
        if pattern.match(filename):
            filepath = os.path.join(results_dir, filename)
            if os.path.isfile(filepath):
                _, rows = _read_one_results_file(filepath)
                all_dates.extend(rows)

    if not all_dates:
        return None
    return max(all_dates)


def get_common_last_date(results_dir, strategy_names=None):
    """
    给定策略列表中，每个策略取最大 match_date，再取这些中的最小值。
    若某个策略无数据则跳过；若所有策略都无数据则返回 None。
    """
    names = strategy_names or STRATEGY_NAMES
    last_dates = []
    for name in names:
        d = get_last_date_for_strategy(results_dir, name)
        if d is not None:
            last_dates.append(d)
    if not last_dates:
        return None
    return min(last_dates)


def next_trading_day(date_str):
    """给定 YYYY-MM-DD，返回下一日的 YYYY-MM-DD（仅加一天，不判断是否交易日）。"""
    try:
        d = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        return (d + timedelta(days=1)).strftime('%Y-%m-%d')
    except Exception:
        return None


def weekdays_between(start_yyyy_mm_dd, end_yyyy_mm_dd):
    """生成 start 到 end 之间所有工作日（周一=0 到周五=4）的日期列表，含首尾。格式 YYYY-MM-DD。"""
    try:
        start_d = datetime.strptime(start_yyyy_mm_dd[:10], '%Y-%m-%d').date()
        end_d = datetime.strptime(end_yyyy_mm_dd[:10], '%Y-%m-%d').date()
        if start_d > end_d:
            return []
        out = []
        d = start_d
        while d <= end_d:
            if d.weekday() < 5:  # 0-4 为周一到周五
                out.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        return out
    except Exception:
        return []


def load_strategies(config_file='common_strategies.json', allowed_names=None):
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            strategies = data.get('strategies', [])
        names_set = set(allowed_names or STRATEGY_NAMES)
        return [s for s in strategies if s.get('name') in names_set]
    except FileNotFoundError:
        print(f'[ERROR] 策略配置文件不存在: {config_file}')
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f'[ERROR] 策略配置文件格式错误: {e}')
        sys.exit(1)


def _read_full_results_file(filepath):
    """读取结果文件，返回 (meta_dict, results_list[完整记录])。与 app._read_one_results_file 类似。"""
    meta = None
    results = []
    if not os.path.isfile(filepath):
        return meta, results
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return meta, results
    if not lines:
        return meta, results
    try:
        first = json.loads(lines[0].strip())
        if '_meta' in first:
            meta = first['_meta']
            lines = lines[1:]
    except Exception:
        pass
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            if 'code' not in data:
                continue
            results.append(data)
        except Exception:
            continue
    return meta, results


def _normalize_date_str(value):
    """将日期字段统一为 YYYY-MM-DD 字符串（若可能）。"""
    if value is None:
        return ''
    s = str(value).strip()
    if not s:
        return ''
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        return s[:10]
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return s


def append_results_to_main_file(results_dir, strategy_name, new_results, strategy_engine=None):
    """
    将新增结果追加到同一个 {策略名}_结果.jsonl 中：
    - 读取原文件所有记录 + 新记录
    - 按 (code, match_date, name) 去重
    - 所有策略统一：连续三个 A 股交易日内同股只保留第一次（需传入 strategy_engine）
    - 按 match_date, code 排序
    - 重写同一个文件
    """
    filepath = os.path.join(results_dir, f"{strategy_name}_结果.jsonl")
    _, old_results = _read_full_results_file(filepath)

    all_results = []
    if old_results:
        all_results.extend(old_results)
    if new_results:
        all_results.extend(new_results)

    # 去重
    seen = set()
    unique = []
    for r in all_results:
        match_date = _normalize_date_str(r.get('match_date'))
        key = (r.get('code', ''), match_date, r.get('name', ''))
        if key in seen:
            continue
        seen.add(key)
        r = dict(r)
        r['match_date'] = match_date
        unique.append(r)

    # 排序
    unique.sort(key=lambda x: (_normalize_date_str(x.get('match_date', '9999-99-99')), x.get('code', '')))

    # 所有策略统一：连续三个 A 股交易日内同股只保留第一次
    if strategy_engine is not None:
        unique = strategy_engine._dedupe_same_stock_within_three_trading_days(unique, trading_days=3)

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            meta = {
                '_meta': {
                    'strategy_name': strategy_name,
                    'run_at': datetime.now().isoformat(),
                    'count': len(unique),
                    'incremental': True,
                }
            }
            f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
            for r in unique:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')
        return filepath
    except Exception as e:
        print(f"[WARNING] 写入 {filepath} 失败: {e}")
        return None


def write_shard_results(results_dir, shard_id, task_index, all_results):
    """
    将当前分片计算得到的增量结果写入分片结果文件，不直接改动主结果文件。
    目录结构：
        results/incremental_shards/{shard_id}/{策略名}_task{task_index}.jsonl
    """
    shard_root = os.path.join(results_dir, 'incremental_shards', str(shard_id))
    os.makedirs(shard_root, exist_ok=True)

    total = 0
    for strategy_name, rows in all_results.items():
        if not rows:
            continue
        filepath = os.path.join(shard_root, f"{strategy_name}_task{task_index}.jsonl")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                meta = {
                    '_meta': {
                        'strategy_name': strategy_name,
                        'shard_id': shard_id,
                        'task_index': int(task_index),
                        'created_at': datetime.now().isoformat(),
                        'count': len(rows),
                    }
                }
                f.write(json.dumps(meta, ensure_ascii=False, default=str) + '\n')
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False, default=str) + '\n')
            print(f'[INFO] 分片结果已写入: {os.path.relpath(filepath, results_dir)} ({len(rows)} 条)')
            total += len(rows)
        except Exception as e:
            print(f'[WARNING] 写入分片结果文件失败: {filepath} - {e}')

    if total == 0:
        print('[INFO] 当前分片无任何新增结果（all_results 为空），未生成分片文件。')
    else:
        print(f'[INFO] 当前分片共写入 {total} 条结果到分片目录: incremental_shards/{shard_id}/')


def merge_shard_results(results_dir, shard_id, config_file):
    """
    将指定 shard_id 下所有分片结果文件合并回各自主结果文件，使用 append_results_to_main_file
    进行去重和「三交易日内同股只保留一次」规则，然后清理分片文件。
    """
    from data_fetcher import DataFetcher
    from strategy_engine import StrategyEngine

    shard_root = os.path.join(results_dir, 'incremental_shards', str(shard_id))
    if not os.path.isdir(shard_root):
        print(f'[ERROR] 分片目录不存在: {shard_root}')
        return

    strategies = load_strategies(config_file)
    if not strategies:
        print('[ERROR] 未从配置文件中加载到任何策略，无法合并分片结果。')
        return

    engine = StrategyEngine(DataFetcher(), max_workers=50)
    merged_total = 0

    print()
    print('=' * 60)
    print(f'合并分片增量结果：shard_id={shard_id}')
    print('=' * 60)

    for s in strategies:
        name = s.get('name')
        if not name:
            continue
        pattern = os.path.join(shard_root, f"{name}_task*.jsonl")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f'{name}: 未找到任何分片文件，跳过')
            continue

        shard_rows = []
        for fp in files:
            _, rows = _read_full_results_file(fp)
            if rows:
                shard_rows.extend(rows)

        if not shard_rows:
            print(f'{name}: 分片文件中无有效数据，跳过')
            continue

        path = append_results_to_main_file(results_dir, name, shard_rows, strategy_engine=engine)
        if path:
            print(f'{name}: 合并 {len(shard_rows)} 条分片结果 → {os.path.basename(path)}')
            merged_total += len(shard_rows)

    # 清理分片文件
    try:
        removed_files = 0
        for root, _, files in os.walk(shard_root, topdown=False):
            for filename in files:
                fp = os.path.join(root, filename)
                try:
                    os.remove(fp)
                    removed_files += 1
                except Exception:
                    pass
            try:
                if not os.listdir(root):
                    os.rmdir(root)
            except Exception:
                pass
        if removed_files:
            print(f'[INFO] 已清理分片目录 {shard_root} 下 {removed_files} 个文件')
    except Exception as e:
        print(f'[WARNING] 清理分片目录失败: {e}')

    print()
    print('=' * 60)
    print(f'分片合并完成，总计合并 {merged_total} 条记录')
    print('=' * 60)


def main():
    parser = argparse.ArgumentParser(
        description='按六个战法结果文件的最后日期，只回测之后到今天的 T 日并追加到结果文件'
    )
    parser.add_argument('--config', default='common_strategies.json', help='策略配置文件路径')
    parser.add_argument('--workers', type=int, default=50, help='并发线程数')
    parser.add_argument('--no-check-cache', action='store_true', help='不检查缓存是否最新，直接跑')
    parser.add_argument('--skip-ensure-data', action='store_true',
                        help='跳过预拉取 ensure_sufficient_data，仅使用已有缓存（适合每日轻量回测）')
    parser.add_argument('--task-index', type=int, default=None,
                        help='多任务分片的当前任务下标（从 0 开始）；与 --task-count、--shard-id 搭配使用')
    parser.add_argument('--task-count', type=int, default=None,
                        help='多任务分片的总任务数（>1 生效）；与 --task-index、--shard-id 搭配使用')
    parser.add_argument('--shard-id', type=str, default=None,
                        help='分片运行的批次标识（任意非空字符串），用于区分不同一轮分片增量回测')
    parser.add_argument('--merge-shards', action='store_true',
                        help='仅合并指定 shard-id 下的所有分片结果到主结果文件，不重新回测')
    parser.add_argument('--auto-shard-count', type=int, default=None,
                        help='自动开启多进程分片数量（>=2），父进程负责预拉取与合并，子进程负责各自分片回测')
    parser.add_argument('--strategies', nargs='+',
                        help='只回测指定策略名称列表（可选），不填则默认回测所有预设战法')
    parser.add_argument('--from-days-ago', type=int, default=None,
                        help='直接按最近 N 个交易日回测（忽略结果文件中的最后日期），如 90 表示最近 90 个交易日')
    args = parser.parse_args()

    from data_fetcher import DataFetcher
    from strategy_engine import StrategyEngine

    results_dir = os.path.join(PROJECT_ROOT, 'results')
    os.makedirs(results_dir, exist_ok=True)

    # 参数合法性检查（分片 & 合并模式）
    if args.merge_shards:
        if not args.shard_id:
            print('[ERROR] 合并分片模式下必须提供 --shard-id')
            sys.exit(1)
        if args.task_index is not None or args.task_count is not None:
            print('[ERROR] 合并分片时不得同时指定 --task-index / --task-count')
            sys.exit(1)
        merge_shard_results(results_dir, args.shard_id, args.config)
        return

    if args.auto_shard_count is not None:
        if args.auto_shard_count <= 1:
            print('[ERROR] --auto-shard-count 必须大于 1')
            sys.exit(1)
        if args.task_index is not None or args.task_count is not None:
            print('[ERROR] --auto-shard-count 不能与 --task-index / --task-count 同时使用')
            sys.exit(1)

    if (args.task_index is None) ^ (args.task_count is None):
        print('[ERROR] 使用多任务分片时，必须同时提供 --task-index 与 --task-count，或都不提供')
        sys.exit(1)
    if args.task_count is not None and args.task_count <= 1:
        print('[ERROR] --task-count 必须大于 1')
        sys.exit(1)
    if args.task_index is not None and args.task_index < 0:
        print('[ERROR] --task-index 不能为负数')
        sys.exit(1)
    if args.task_index is not None and not args.shard_id:
        print('[ERROR] 分片模式下必须提供 --shard-id 以标识本轮增量回测')
        sys.exit(1)

    sharding = args.task_index is not None and args.task_count is not None

    # 处理策略名称过滤
    if args.strategies:
        selected_names = [name for name in args.strategies if name in STRATEGY_NAMES]
        missing = [name for name in args.strategies if name not in STRATEGY_NAMES]
        if missing:
            print(f'[WARN] 以下策略名称不在预设战法列表中，将被忽略: {missing}')
        if not selected_names:
            print('[ERROR] --strategies 中的名称均不在预设战法列表中，无法回测')
            sys.exit(1)
        effective_names = selected_names
    else:
        effective_names = STRATEGY_NAMES

    fetcher = DataFetcher()
    cache_latest = fetcher.get_local_cache_latest_date()
    last_trade = fetcher._get_last_trading_day_available()

    print()
    print('=' * 60)
    if args.from_days_ago:
        print('按最近 N 个交易日全量回测（忽略结果文件最后日期）')
    else:
        print('按最后结果日期增量回测（只补 T 日并追加）')
    print('=' * 60)
    print(f'缓存最新日期: {cache_latest or "无"}')
    print(f'最近交易日:   {last_trade}')

    # 计算需要回测的 T 日列表
    if args.from_days_ago and args.from_days_ago > 0:
        # 直接取最近 N 个交易日
        approx_start = (datetime.strptime(last_trade, '%Y-%m-%d') - timedelta(days=args.from_days_ago * 2)).strftime('%Y-%m-%d')
        all_days = fetcher.get_trading_days_between(approx_start, last_trade)
        if not all_days:
            all_days = weekdays_between(approx_start, last_trade)
        trading_days = all_days[-args.from_days_ago:] if all_days else []
        if not trading_days:
            print('[INFO] 未查询到需回测的交易日。')
            return
        start_date = trading_days[0]
        print(f'本次回测 T 日范围: {trading_days[0]} ~ {trading_days[-1]}，共 {len(trading_days)} 个交易日')
        print()
    else:
        # 1) 查看选中战法结果中的最后日期
        common_last = get_common_last_date(results_dir, strategy_names=effective_names)
        print(f'选中战法结果中「最后日期」: {common_last or "无（任一策略均无数据）"}')

        if common_last is None:
            print()
            print('[INFO] 未检测到任何战法结果数据，将仅回测最近一个交易日（与 incremental_backtest 行为一致）。')
            start_date = last_trade
            trading_days = [last_trade]
        else:
            start_date = next_trading_day(common_last)
            if not start_date or start_date > last_trade:
                print()
                print('[INFO] 最后日期已为最近交易日或更晚，无需回测。')
                return
            trading_days = fetcher.get_trading_days_between(start_date, last_trade)
            fallback_days = weekdays_between(start_date, last_trade)
            # 若 Baostock 未返回或返回天数明显少于「工作日」数，用工作日列表兜底，保证回测所有中间交易日
            if not trading_days:
                trading_days = fallback_days
            elif fallback_days and len(trading_days) < len(fallback_days):
                trading_days = fallback_days
            if not trading_days:
                print()
                print('[INFO] 未查询到需回测的交易日。')
                return
            print(f'本次回测 T 日范围: {trading_days[0]} ~ {trading_days[-1]}，共 {len(trading_days)} 个交易日')
        print()

        if not args.no_check_cache and cache_latest:
            if trading_days and cache_latest < trading_days[-1]:
                print(f'[WARN] 缓存最新日期 {cache_latest} 早于回测截止日 {trading_days[-1]}，建议先运行 update_cache_and_backtest.py 补齐数据。')
                print('       若坚持继续，可加 --no-check-cache')
        print()

    strategies = load_strategies(args.config, allowed_names=effective_names)
    if not strategies:
        print(f'[ERROR] 在配置中未找到任何选中战法，请检查 common_strategies.json')
        sys.exit(1)
    if len(strategies) != len(effective_names):
        print(f'[WARN] 配置中缺少部分选中战法，仅回测: {[s["name"] for s in strategies]}')
    # [(strategy_name, strategy_dict), ...] 供「按股票只加载一次」的增量接口使用
    strategies_list = [(s['name'], s['strategy']) for s in strategies]

    # 若指定自动多进程分片，由父进程统一进行一次数据预拉取
    if args.auto_shard_count is not None:
        if not args.skip_ensure_data:
            fetcher.ensure_sufficient_data(
                max((s['strategy'].get('timeRange', 30) for s in strategies), default=30),
                max_workers=100
            )
        shard_id = args.shard_id or datetime.now().strftime('auto_%Y%m%d_%H%M%S')
        print()
        print('=' * 60)
        print(f'自动多进程分片增量回测：共 {args.auto_shard_count} 个分片，shard-id={shard_id}')
        print('=' * 60)
        # 构造子进程命令模板
        procs = []
        for idx in range(args.auto_shard_count):
            cmd = [
                sys.executable,
                os.path.join(PROJECT_ROOT, 'scripts', 'backtest_append_from_last.py'),
                '--config', args.config,
                '--workers', str(args.workers),
                '--task-index', str(idx),
                '--task-count', str(args.auto_shard_count),
                '--shard-id', shard_id,
                '--skip-ensure-data',
                '--no-check-cache',
            ]
            if args.strategies:
                cmd.append('--strategies')
                cmd.extend(effective_names)
            print(f'[INFO] 启动分片进程 {idx+1}/{args.auto_shard_count}: {" ".join(cmd)}')
            procs.append(subprocess.Popen(cmd))

        # 等待所有分片完成
        fail = False
        for idx, p in enumerate(procs):
            code = p.wait()
            if code != 0:
                print(f'[ERROR] 分片进程 task-index={idx} 退出码非 0: {code}')
                fail = True
        if fail:
            print('[ERROR] 存在分片进程执行失败，请检查日志后重试；本次不自动合并分片结果。')
            sys.exit(1)

        # 全部分片成功后，父进程直接调用合并逻辑
        merge_shard_results(results_dir, shard_id, args.config)
        return

    if not args.skip_ensure_data:
        fetcher.ensure_sufficient_data(
            max((s['strategy'].get('timeRange', 30) for s in strategies), default=30),
            max_workers=100
        )
    stocks = fetcher.get_stock_list()

    # 分片模式下，根据 task_index / task_count 对股票列表进行切片
    if sharding:
        stocks_sorted = sorted(stocks, key=lambda s: s['code'])
        n = len(stocks_sorted)
        if n == 0:
            print('[INFO] 股票列表为空，当前分片无需处理')
            return
        size = (n + args.task_count - 1) // args.task_count
        start = args.task_index * size
        end = min(n, (args.task_index + 1) * size)
        if start >= n:
            print(f'[INFO] 当前分片 (task_index={args.task_index}) 在股票列表范围之外，无需处理')
            return
        stocks = stocks_sorted[start:end]
        print(f'[INFO] 多任务分片模式：task_index={args.task_index}, task_count={args.task_count}, '
              f'本分片负责 {len(stocks)} / {n} 只股票')
    else:
        print(f'共 {len(stocks)} 只股票，{len(trading_days)} 个 T 日 × {len(strategies)} 个策略 → 按股票并发（每只股票只加载一次 K 线）')
    print()

    engine = StrategyEngine(fetcher, max_workers=args.workers)
    # 按策略名聚合：all_results[name] = [result_rows]
    all_results = {s['name']: [] for s in strategies}
    total_stocks = len(stocks)
    processed = [0]  # 闭包用

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_stock = {
            executor.submit(engine.run_incremental_for_stock, stock, trading_days, strategies_list): stock
            for stock in stocks
        }
        for future in as_completed(future_to_stock):
            processed[0] += 1
            if processed[0] % 100 == 0 or processed[0] == total_stocks:
                pct = 100 * processed[0] // total_stocks if total_stocks else 0
                print(f'进度: {processed[0]}/{total_stocks} ({pct}%)', flush=True)
            try:
                per_stock = future.result(timeout=60)
                for name, rows in per_stock.items():
                    all_results[name].extend(rows)
            except Exception as e:
                if processed[0] % 500 == 0:
                    print(f'[WARN] 某只股票处理异常: {e}', flush=True)

    total_count = 0
    if sharding:
        # 分片模式：将结果写入分片文件，后续再用 --merge-shards 合并
        write_shard_results(results_dir, args.shard_id, args.task_index, all_results)
        for s in strategies:
            name = s['name']
            rows = all_results.get(name, [])
            total_count += len(rows)
        print()
        print('=' * 60)
        print(f'分片增量回测完成（shard_id={args.shard_id}, task_index={args.task_index}），'
              f'共 {len(trading_days)} 个 T 日 × {len(strategies)} 个策略，本分片合计 {total_count} 条记录')
        print('后续请在所有分片运行完成后，执行一次：')
        print(f'  python scripts/backtest_append_from_last.py --merge-shards --shard-id {args.shard_id}')
        print('=' * 60)
    else:
        for s in strategies:
            name = s['name']
            rows = all_results.get(name, [])
            if rows:
                path = append_results_to_main_file(results_dir, name, rows, strategy_engine=engine)
                if path:
                    print(f'{name}: {len(rows)} 条 → {os.path.basename(path)}')
                total_count += len(rows)
            else:
                print(f'{name}: 0 条')

        print()
        print('=' * 60)
        print(f'增量回测完成，共 {len(trading_days)} 个 T 日 × {len(strategies)} 个策略，合计 {total_count} 条记录')
        print('=' * 60)


if __name__ == '__main__':
    main()
