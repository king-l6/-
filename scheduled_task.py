#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务：每天固定时间仅执行「本地 cache 上的增量回测」（不拉全市场 K 线）。
K 线请先自行跑 scripts/update_cache_and_backtest.py --no-backtest 或等价入口补齐。

支持两种运行方式：
1. 作为守护进程运行（需要保持进程运行）
2. 作为一次性任务运行（配合系统定时任务使用，例如 macOS launchd）
"""
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

# 尝试从.env文件加载环境变量
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 如果没有安装python-dotenv，跳过

import schedule
import time
import subprocess
import sys
import json
from datetime import datetime
from urllib import request, error

# 从环境变量读取企业微信 Webhook URL
WECHAT_WEBHOOK = os.getenv('WECHAT_WEBHOOK_URL', '')

# 从配置文件动态加载策略名称
def _load_strategy_names():
    """从 common_strategies.json 加载策略名称列表"""
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'common_strategies.json')
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return [s['name'] for s in data.get('strategies', [])]
    except Exception:
        # 如果加载失败，返回默认列表
        return ['主力建仓', '断板反包', '筑底突破', '连阳上影', '四连阳摸板']

STRATEGY_NAMES = _load_strategy_names()


def _detect_latest_match_date(script_dir, strategy_name):
    """从结果文件中检测该策略最新的 match_date（YYYY-MM-DD），无则返回 None。"""
    results_file = os.path.join(script_dir, 'results', f'{strategy_name}_结果.jsonl')
    if not os.path.isfile(results_file):
        return None
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return None
    if not lines:
        return None
    # 跳过首行 _meta
    try:
        first = json.loads(lines[0].strip())
        if isinstance(first, dict) and '_meta' in first:
            lines = lines[1:]
    except Exception:
        pass
    latest = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except Exception:
            continue
        d = str(data.get('match_date') or '').strip()
        if not d:
            continue
        if len(d) >= 10 and d[4] == '-' and d[7] == '-':
            d = d[:10]
        if latest is None or d > latest:
            latest = d
    return latest


def _load_strategy_records_for_date(script_dir, strategy_name, trade_date):
    """读取指定策略在 trade_date 当天的结果记录列表。"""
    results_file = os.path.join(script_dir, 'results', f'{strategy_name}_结果.jsonl')
    records = []
    if not os.path.isfile(results_file):
        return records
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return records
    if not lines:
        return records
    # 跳过首行 _meta
    try:
        first = json.loads(lines[0].strip())
        if isinstance(first, dict) and '_meta' in first:
            lines = lines[1:]
    except Exception:
        pass
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except Exception:
            continue
        md = str(data.get('match_date') or '').strip()
        if len(md) >= 10 and md[4] == '-' and md[7] == '-':
            md = md[:10]
        if md == trade_date:
            records.append(data)
    return records


def _format_wechat_markdown(strategy_name, trade_date, records):
    """格式化为企业微信 markdown 表格内容。"""
    title = f"【量化回测】{strategy_name}（{trade_date}）"
    if not records:
        return title + "\n\n今日无符合条件的结果。"

    lines = [
        title,
        "",
        "| 序号 | 代码 | 名称 | 匹配价 | 当前价 | T+1涨跌幅 |",
        "|------|------|------|--------|--------|-----------|",
    ]
    for idx, r in enumerate(records, 1):
        code = str(r.get('code', '') or '')
        name = str(r.get('name', '') or '')
        match_price = r.get('match_price')
        current_price = r.get('current_price')
        day1_change_pct = r.get('day1_change_pct')
        mp = f"{match_price:.2f}" if isinstance(match_price, (int, float)) else "-"
        cp = f"{current_price:.2f}" if isinstance(current_price, (int, float)) else "-"
        pct = f"{day1_change_pct:+.2f}%" if isinstance(day1_change_pct, (int, float)) else "-"
        lines.append(f"| {idx} | {code} | {name} | {mp} | {cp} | {pct} |")
    return "\n".join(lines)


def _send_wechat_message(content):
    """通过企业微信机器人 webhook 发送 markdown 消息。"""
    if not WECHAT_WEBHOOK:
        print("未配置企业微信 webhook，跳过发送。")
        return
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": content
        }
    }
    data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = request.Request(
        WECHAT_WEBHOOK,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    try:
        with request.urlopen(req, timeout=10) as resp:
            resp_text = resp.read().decode('utf-8', errors='ignore')
        print("企业微信发送结果:", resp_text)
    except error.HTTPError as e:
        print("企业微信发送失败（HTTPError）:", e.code, e.reason)
    except error.URLError as e:
        print("企业微信发送失败（URLError）:", getattr(e, 'reason', e))
    except Exception as e:
        print("企业微信发送失败（其他错误）:", e)

def run_daily_task(backtest_workers: int = 100):
    """执行每日任务：仅用本地 K 线缓存做六个策略增量回测 + 发企微（不调用补数脚本）。"""
    print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 开始执行定时任务')
    print('=' * 70)
    
    try:
        # 获取脚本所在目录（项目根目录）
        script_dir = os.path.dirname(os.path.abspath(__file__))
        python_bin = sys.executable

        print(
            '步骤1: 六个策略增量回测（仅本地 cache：--cache-only --no-check-cache --skip-ensure-data）'
        )
        backtest_script = os.path.join(script_dir, 'scripts', 'backtest_append_from_last.py')
        result2 = subprocess.run(
            [
                python_bin,
                backtest_script,
                '--cache-only',
                '--no-check-cache',
                '--skip-ensure-data',
                '--workers',
                str(backtest_workers),
            ],
            cwd=script_dir,
            capture_output=False,
            text=True
        )
        if result2.returncode == 0:
            print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务执行成功')
            # 2. 发送「六个战法」本次最新交易日的结果到企业微信（按文档中表格形式）
            for strategy_name in STRATEGY_NAMES:
                trade_date = _detect_latest_match_date(script_dir, strategy_name)
                if not trade_date:
                    print(f'策略 {strategy_name}: 未找到任何 match_date，跳过发送。')
                    continue
                records = _load_strategy_records_for_date(script_dir, strategy_name, trade_date)
                print(f'准备发送企业微信通知，策略 {strategy_name}，{trade_date} 数量: {len(records)}')
                content = _format_wechat_markdown(strategy_name, trade_date, records)
                _send_wechat_message(content)
        else:
            print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 增量回测执行失败，返回码: {result2.returncode}')
            
    except Exception as e:
        print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务执行出错: {e}')
        import traceback
        traceback.print_exc()
    
    print('=' * 70)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='定时任务：每天固定时间仅用本地缓存增量回测（不拉 K 线）')
    parser.add_argument('--once', action='store_true', help='立即执行一次任务（用于测试）')
    parser.add_argument('--time', default='15:30', help='执行时间，格式 HH:MM（默认: 15:30）')
    parser.add_argument('--workers', type=int, default=100, help='增量回测并发线程数（默认 100）')
    args = parser.parse_args()
    
    if args.once:
        # 立即执行一次（用于测试）
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 立即执行任务（测试模式）')
        run_daily_task(backtest_workers=args.workers)
    else:
        # 定时执行
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务服务启动')
        print(f'任务计划: 每天 {args.time} 执行本地缓存增量回测（不拉 K 线）')
        print('按 Ctrl+C 停止服务\n')
        
        # 设置定时任务
        schedule.every().day.at(args.time).do(run_daily_task, backtest_workers=args.workers)
        
        # 运行调度器
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务服务已停止')
