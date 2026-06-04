#!/usr/bin/env python3
"""
回测3个条件：
1. 原始MACD柱加速（无额外过滤）
2. 信号强度 >= 1000
3. MACD柱 >= 2.5
"""
import json
import glob
import os
from datetime import datetime
import sys

# 策略参数
HOLD_DAYS = 15
STOP_LOSS = 0.10
INITIAL_CAPITAL = 40000

# 计算MACD从klines
def calculate_macd(klines):
    results = []
    ema12 = None
    ema26 = None
    dea = None
    
    for i, kline in enumerate(klines):
        close = kline['收盘']
        
        if i == 0:
            ema12 = close
            ema26 = close
            dif = 0
            dea = dif
            macd_bar = 0
        else:
            ema12 = ema12 * 11/13 + close * 2/13
            ema26 = ema26 * 25/27 + close * 2/27
            dif = ema12 - ema26
            dea = dea * 8/10 + dif * 2/10
            macd_bar = 2 * (dif - dea)
        
        results.append({
            'date': kline['日期'],
            'close': close,
            'open': kline['开盘'],
            'high': kline['最高'],
            'low': kline['最低'],
            'dif': dif,
            'dea': dea,
            'macd_bar': macd_bar
        })
    
    return results

# 计算信号强度（最近5天MACD柱之和*100）
def calculate_signal_strength(results, idx):
    strength = 0
    for i in range(max(0, idx-4), idx+1):
        if results[i]['macd_bar'] > 0:
            strength += int(results[i]['macd_bar'] * 100)
    return strength

# 检查MACD柱加速（连续3天变长且>0）
def check_macd_acceleration(results, idx):
    if idx < 3:
        return False
    
    today = results[idx]['macd_bar']
    yesterday = results[idx-1]['macd_bar']
    day_before = results[idx-2]['macd_bar']
    
    if today > 0 and yesterday > 0 and day_before > 0:
        if today > yesterday > day_before:
            return True
    return False

# 检查涨停（涨幅>=9.8%）
def check_limit_up(results, idx):
    if idx < 1:
        return False
    prev_close = results[idx-1]['close']
    today_close = results[idx]['close']
    if prev_close <= 0:
        return False
    change = (today_close - prev_close) / prev_close
    return change >= 0.098

# 运行回测
def run_backtest(results, filter_type='none'):
    trades = []
    capital = INITIAL_CAPITAL
    position = None
    
    for i in range(3, len(results)):
        # 检查是否有持仓
        if position:
            days_held = i - position['buy_idx']
            current_price = results[i]['close']
            buy_price = position['buy_price']
            pnl = (current_price - buy_price) / buy_price
            
            should_sell = False
            sell_reason = ''
            
            if pnl <= -STOP_LOSS:
                should_sell = True
                sell_reason = '止损'
            elif days_held >= HOLD_DAYS:
                should_sell = True
                sell_reason = '持仓到期'
            
            if should_sell:
                shares = position['shares']
                sell_value = current_price * shares
                buy_value = buy_price * shares
                profit = sell_value - buy_value
                capital += sell_value
                
                trades.append({
                    'buy_date': position['buy_date'],
                    'buy_price': buy_price,
                    'sell_date': results[i]['date'],
                    'sell_price': current_price,
                    'shares': shares,
                    'profit': profit,
                    'pnl': pnl,
                    'reason': sell_reason,
                    'hold_days': days_held
                })
                position = None
        
        # 检查是否该买
        if not position and capital >= 10000:
            if not check_macd_acceleration(results, i):
                continue
            
            if check_limit_up(results, i):
                continue
            
            # 应用过滤条件
            if filter_type == 'signal_1000':
                strength = calculate_signal_strength(results, i)
                if strength < 1000:
                    continue
            elif filter_type == 'macd_bar_2.5':
                if results[i]['macd_bar'] < 2.5:
                    continue
            
            # 买入
            if i+1 < len(results):
                buy_price = results[i+1]['open']
                buy_idx = i+1
                buy_date = results[i+1]['date']
            else:
                buy_price = results[i]['close']
                buy_idx = i
                buy_date = results[i]['date']
            
            shares = int(capital / buy_price / 100) * 100
            
            if shares <= 0:
                continue
            
            cost = buy_price * shares
            capital -= cost
            
            position = {
                'buy_date': buy_date,
                'buy_idx': buy_idx,
                'buy_price': buy_price,
                'shares': shares
            }
    
    return trades

# 主程序
def main():
    filter_type = sys.argv[1] if len(sys.argv) > 1 else 'all'
    
    cache_dir = "/Users/bilibili/Desktop/test/量化/cache/stock_data"
    files = glob.glob(f"{cache_dir}/*.json")
    
    print(f"开始回测 {len(files)} 只股票，过滤类型: {filter_type}")
    
    all_trades = {'none': [], 'signal_1000': [], 'macd_bar_2.5': []}
    filters_to_run = ['none', 'signal_1000', 'macd_bar_2.5'] if filter_type == 'all' else [filter_type]
    
    count = 0
    for filepath in files:
        count += 1
        if count % 500 == 0:
            print(f"进度: {count}/{len(files)}")
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
        except:
            continue
        
        if 'data' not in data or len(data['data']) < 30:
            continue
        
        results = calculate_macd(data['data'])
        
        for ft in filters_to_run:
            trades = run_backtest(results, ft)
            all_trades[ft].extend(trades)
    
    # 输出结果
    print("\n" + "="*60)
    print("回测结果汇总（近1年）")
    print("="*60)
    
    for ft in filters_to_run:
        trades = all_trades[ft]
        if not trades:
            print(f"\n【{ft}】无交易记录")
            continue
        
        wins = [t for t in trades if t['profit'] > 0]
        losses = [t for t in trades if t['profit'] <= 0]
        
        total_profit = sum(t['profit'] for t in trades)
        win_rate = len(wins) / len(trades) * 100 if trades else 0
        
        avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t['pnl'] for t in losses) / len(losses) if losses else 0
        profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        
        final_capital = INITIAL_CAPITAL + total_profit
        total_return = (final_capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
        
        # 计算最大回撤
        capital_history = [INITIAL_CAPITAL]
        for t in trades:
            capital_history.append(capital_history[-1] + t['profit'])
        max_capital = capital_history[0]
        max_drawdown = 0
        for c in capital_history:
            if c > max_capital:
                max_capital = c
            drawdown = (max_capital - c) / max_capital
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 按月统计
        monthly = {}
        for t in trades:
            month = t['buy_date'][:7]
            if month not in monthly:
                monthly[month] = {'count': 0, 'profit': 0}
            monthly[month]['count'] += 1
            monthly[month]['profit'] += t['profit']
        
        print(f"\n【{ft}】")
        print(f"  总交易数: {len(trades)} 笔")
        print(f"  胜率: {win_rate:.1f}%")
        print(f"  盈亏比: {profit_loss_ratio:.2f}")
        print(f"  总收益: {total_profit:.0f} 元 ({total_return:.1f}%)")
        print(f"  最终资金: {final_capital:.0f} 元")
        print(f"  最大回撤: {max_drawdown*100:.1f}%")
        print(f"  月均交易: {len(trades)/12:.1f} 笔")
        
        # 保存详细交易记录
        output_file = f"/Users/bilibili/Desktop/test/量化/backtest_results/trades_{ft}.json"
        with open(output_file, 'w') as f:
            json.dump({
                'filter_type': ft,
                'total_trades': len(trades),
                'win_rate': win_rate,
                'profit_loss_ratio': profit_loss_ratio,
                'total_return': total_return,
                'max_drawdown': max_drawdown,
                'trades': trades
            }, f, ensure_ascii=False, indent=2)
        print(f"  交易明细已保存: {output_file}")

if __name__ == '__main__':
    main()
