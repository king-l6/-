'''
Author: v_liuhaoran v_liuhaoran@bilibili.com
Date: 2026-02-02 15:04:40
LastEditors: v_liuhaoran v_liuhaoran@bilibili.com
LastEditTime: 2026-02-02 15:19:19
FilePath: /量化/app.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from datetime import datetime, timedelta
import json
import os
from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher

app = Flask(__name__)
CORS(app)

# 尝试从.env文件加载环境变量
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 如果没有安装python-dotenv，跳过

# 初始化数据获取器和策略引擎
# 使用 AKShare（免费、数据准确）
data_fetcher = DataFetcher()
# 使用30个并发线程加速回测（提高速度）
strategy_engine = StrategyEngine(data_fetcher, max_workers=30)

@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')

@app.route('/api/backtest', methods=['POST'])
def backtest():
    """策略回测API"""
    try:
        data = request.json
        strategy = data.get('strategy', {})
        strategy_name = data.get('strategy_name', None)  # 可选：策略名称
        
        # 执行回测
        results = strategy_engine.backtest(strategy, strategy_name=strategy_name)
        
        return jsonify({
            'success': True,
            'data': results,
            'count': len(results)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stocks', methods=['GET'])
def get_stocks():
    """获取股票列表"""
    try:
        stocks = data_fetcher.get_stock_list()
        return jsonify({
            'success': True,
            'data': stocks
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8086)
