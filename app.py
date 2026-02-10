'''
Author: v_liuhaoran v_liuhaoran@bilibili.com
Date: 2026-02-02 15:04:40
LastEditors: v_liuhaoran v_liuhaoran@bilibili.com
LastEditTime: 2026-02-02 15:19:19
FilePath: /量化/app.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_cors import CORS
from datetime import datetime, timedelta
import json
import os
from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher

# 检测是否存在前端构建产物（生产环境）
FRONTEND_DIST_PATH = os.path.join(os.path.dirname(__file__), 'static')
HAS_FRONTEND_BUILD = os.path.exists(os.path.join(FRONTEND_DIST_PATH, 'index.html'))

if HAS_FRONTEND_BUILD:
    # 生产环境：使用前端构建的静态文件
    app = Flask(__name__, static_folder=FRONTEND_DIST_PATH, static_url_path='')
else:
    # 开发环境：使用 templates
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
# 回测并发 50（过高会因线程开销变慢）
strategy_engine = StrategyEngine(data_fetcher, max_workers=50)

# 策略结果缓存：相同策略同日 5 分钟内秒返
_backtest_cache = {}
_BACKTEST_CACHE_TTL = 300  # 秒

@app.route('/')
def index():
    """主页面"""
    if HAS_FRONTEND_BUILD:
        # 生产环境：返回静态文件
        return send_from_directory(FRONTEND_DIST_PATH, 'index.html')
    else:
        # 开发环境：使用模板
        return render_template('index.html')

@app.route('/api/backtest', methods=['POST'])
def backtest():
    """策略回测API"""
    try:
        data = request.json
        strategy = data.get('strategy', {})
        strategy_name = data.get('strategy_name', None)  # 可选：策略名称

        def _sorted_results(rows):
            """按 match_date 升序、code 升序排列"""
            return sorted(rows, key=lambda r: (r.get('match_date', '9999-99-99'), r.get('code', '')))

        # 命中缓存则秒返（相同策略、同日、5 分钟内）
        cache_key = (json.dumps(strategy, sort_keys=True), datetime.now().strftime('%Y-%m-%d'))
        if cache_key in _backtest_cache:
            results, ts = _backtest_cache[cache_key]
            if (datetime.now() - ts).total_seconds() < _BACKTEST_CACHE_TTL:
                return jsonify({'success': True, 'data': _sorted_results(results), 'count': len(results), '_cached': True})

        # 执行回测
        results = strategy_engine.backtest(strategy, strategy_name=strategy_name)
        results = _sorted_results(results)
        _backtest_cache[cache_key] = (results, datetime.now())
        if len(_backtest_cache) > 50:
            _backtest_cache.clear()

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
    # 支持通过环境变量配置端口，默认 8086
    port = int(os.getenv('FLASK_PORT', 8086))
    # 生产环境关闭 debug 模式
    debug = os.getenv('FLASK_ENV', 'development') == 'development'
    app.run(debug=debug, host='0.0.0.0', port=port)
