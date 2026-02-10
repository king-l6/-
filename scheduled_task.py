#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务：每天下午3:30执行更新数据和回测
支持两种运行方式：
1. 作为守护进程运行（需要保持进程运行）
2. 作为一次性任务运行（配合系统定时任务使用）
"""
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

import schedule
import time
import subprocess
import sys
from datetime import datetime

def run_daily_task():
    """执行每日任务：更新数据 + 回测"""
    print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 开始执行定时任务')
    print('=' * 70)
    
    try:
        # 获取脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        daily_run_script = os.path.join(script_dir, 'daily_run.py')
        
        # 执行 daily_run.py
        result = subprocess.run(
            [sys.executable, daily_run_script],
            cwd=script_dir,
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务执行成功')
        else:
            print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务执行失败，返回码: {result.returncode}')
            
    except Exception as e:
        print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务执行出错: {e}')
        import traceback
        traceback.print_exc()
    
    print('=' * 70)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='定时任务：每天15:30执行更新数据和回测')
    parser.add_argument('--once', action='store_true', help='立即执行一次任务（用于测试）')
    parser.add_argument('--time', default='15:30', help='执行时间，格式 HH:MM（默认: 15:30）')
    args = parser.parse_args()
    
    if args.once:
        # 立即执行一次（用于测试）
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 立即执行任务（测试模式）')
        run_daily_task()
    else:
        # 定时执行
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务服务启动')
        print(f'任务计划: 每天 {args.time} 执行更新数据和回测')
        print('按 Ctrl+C 停止服务\n')
        
        # 设置定时任务
        schedule.every().day.at(args.time).do(run_daily_task)
        
        # 运行调度器
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] 定时任务服务已停止')
