'''
Author: v_liuhaoran v_liuhaoran@bilibili.com
Date: 2026-02-02 15:04:40
LastEditors: v_liuhaoran v_liuhaoran@bilibili.com
LastEditTime: 2026-02-02 15:19:19
FilePath: /量化/app.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from datetime import datetime, timedelta
import json
import os
from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher

# 使用前端构建产物（构建到 static 目录）
FRONTEND_DIST_PATH = os.path.join(os.path.dirname(__file__), 'static')
app = Flask(__name__, static_folder=FRONTEND_DIST_PATH, static_url_path='')

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
    index_path = os.path.join(FRONTEND_DIST_PATH, 'index.html')
    if not os.path.exists(index_path):
        return jsonify({
            'error': '前端未构建，请先运行: cd frontend && npm install && npm run build'
        }), 404
    return send_from_directory(FRONTEND_DIST_PATH, 'index.html')

@app.route('/api/backtest', methods=['POST'])
def backtest():
    """策略回测API"""
    try:
        data = request.json
        strategy = data.get('strategy', {})
        strategy_name = data.get('strategy_name', None)  # 可选：策略名称

        # 打印接收到的策略信息
        print(f'收到回测请求，策略名称: {strategy_name}')
        print(f'策略条件数量: {len(strategy.get("conditions", []))}')
        print(f'回测时间范围: {strategy.get("timeRange", 30)} 个交易日')

        def _sorted_results(rows):
            """按 match_date 升序、code 升序排列"""
            return sorted(rows, key=lambda r: (r.get('match_date', '9999-99-99'), r.get('code', '')))

        # 命中缓存则秒返（相同策略、同日、5 分钟内）
        cache_key = (json.dumps(strategy, sort_keys=True), datetime.now().strftime('%Y-%m-%d'))
        if cache_key in _backtest_cache:
            results, ts = _backtest_cache[cache_key]
            if (datetime.now() - ts).total_seconds() < _BACKTEST_CACHE_TTL:
                print(f'返回缓存结果，共 {len(results)} 条')
                return jsonify({'success': True, 'data': _sorted_results(results), 'count': len(results), '_cached': True})

        # 执行回测
        print(f'开始执行回测...')
        results = strategy_engine.backtest(strategy, strategy_name=strategy_name)
        results = _sorted_results(results)
        _backtest_cache[cache_key] = (results, datetime.now())
        if len(_backtest_cache) > 50:
            _backtest_cache.clear()

        print(f'回测完成，找到 {len(results)} 只符合条件的股票')
        return jsonify({
            'success': True,
            'data': results,
            'count': len(results)
        })
    except Exception as e:
        import traceback
        error_msg = str(e)
        error_traceback = traceback.format_exc()
        print(f'回测出错: {error_msg}')
        print(f'错误堆栈: {error_traceback}')
        return jsonify({
            'success': False,
            'error': error_msg,
            'traceback': error_traceback if app.debug else None
        }), 500

@app.route('/api/backtest/batch', methods=['POST'])
def batch_backtest():
    """批量执行多个策略回测API"""
    try:
        data = request.json
        strategies = data.get('strategies', [])  # 策略列表
        strategy_names = data.get('strategy_names', [])  # 策略名称列表（可选）
        
        if not strategies:
            return jsonify({
                'success': False,
                'error': '请提供至少一个策略'
            }), 400
        
        print(f'收到批量回测请求，共 {len(strategies)} 个策略')
        
        results = []
        for idx, strategy in enumerate(strategies):
            strategy_name = strategy_names[idx] if idx < len(strategy_names) else f"策略_{idx+1}"
            print(f'[API] 执行策略 {idx+1}/{len(strategies)}: {strategy_name}')
            
            try:
                # 执行单个策略回测
                strategy_results = strategy_engine.backtest(strategy, strategy_name=strategy_name)
                results.append({
                    'strategy_name': strategy_name,
                    'strategy': strategy,
                    'results': strategy_results,
                    'count': len(strategy_results),
                    'success': True
                })
                print(f'[API] 策略 {strategy_name} 完成，找到 {len(strategy_results)} 只股票')
            except Exception as e:
                import traceback
                error_traceback = traceback.format_exc()
                print(f'[API] 策略 {strategy_name} 执行失败: {str(e)}')
                results.append({
                    'strategy_name': strategy_name,
                    'strategy': strategy,
                    'results': [],
                    'count': 0,
                    'success': False,
                    'error': str(e)
                })
        
        # 统计汇总
        total_stocks = sum(r['count'] for r in results)
        success_count = sum(1 for r in results if r['success'])
        
        print(f'批量回测完成，成功 {success_count}/{len(strategies)} 个策略，共找到 {total_stocks} 只股票')
        
        return jsonify({
            'success': True,
            'data': results,
            'summary': {
                'total_strategies': len(strategies),
                'success_count': success_count,
                'total_stocks': total_stocks
            }
        })
    except Exception as e:
        import traceback
        error_msg = str(e)
        error_traceback = traceback.format_exc()
        print(f'批量回测出错: {error_msg}')
        print(f'错误堆栈: {error_traceback}')
        return jsonify({
            'success': False,
            'error': error_msg,
            'traceback': error_traceback if app.debug else None
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

@app.route('/api/results/list', methods=['GET'])
def get_results_list():
    """获取 results 目录下的所有文件列表"""
    try:
        results_dir = os.path.join(os.path.dirname(__file__), 'results')
        if not os.path.exists(results_dir):
            return jsonify({
                'success': True,
                'data': []
            })
        
        files = []
        for filename in os.listdir(results_dir):
            if filename.endswith('.jsonl'):
                filepath = os.path.join(results_dir, filename)
                if os.path.isfile(filepath):
                    stat = os.stat(filepath)
                    files.append({
                        'filename': filename,
                        'size': stat.st_size,
                        'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
        
        # 战法名称列表（用于置顶），按优先级排序
        strategy_names = ['龙头战法', '断板反包', '均线上穿', '情绪周期', '三连板']
        
        # 判断文件是否属于战法策略（精确匹配，避免匹配到组合名称）
        def is_strategy_file(filename):
            # 移除.jsonl后缀和"结果"等后缀，只保留核心名称
            base_name = filename.replace('_结果.jsonl', '').replace('.jsonl', '')
            # 检查是否完全匹配某个战法名称，或者以战法名称开头
            for name in strategy_names:
                if base_name == name or base_name.startswith(name + '_') or base_name.startswith(name + '-'):
                    return True
            return False
        
        # 分离战法文件和其他文件
        strategy_files = []
        other_files = []
        for file in files:
            if is_strategy_file(file['filename']):
                strategy_files.append(file)
            else:
                other_files.append(file)
        
        # 战法文件内部按固定顺序排序（优先匹配纯战法名称）
        def get_strategy_index(filename):
            base_name = filename.replace('_结果.jsonl', '').replace('.jsonl', '')
            # 优先匹配完全匹配的战法名称
            for idx, name in enumerate(strategy_names):
                if base_name == name:
                    return idx * 100  # 完全匹配优先级更高
            # 其次匹配以战法名称开头的
            for idx, name in enumerate(strategy_names):
                if base_name.startswith(name + '_') or base_name.startswith(name + '-'):
                    return idx * 100 + 50  # 部分匹配优先级稍低
            return len(strategy_names) * 100
        
        strategy_files.sort(key=lambda x: get_strategy_index(x['filename']))
        
        # 其他文件按修改时间倒序排列（最新的在前）
        other_files.sort(key=lambda x: x['modified'], reverse=True)
        
        # 合并：战法文件在前，其他文件在后
        files = strategy_files + other_files
        
        
        return jsonify({
            'success': True,
            'data': files
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/results/file', methods=['GET'])
def get_results_file():
    """获取指定 results 文件的内容"""
    try:
        filename = request.args.get('filename')
        if not filename:
            return jsonify({
                'success': False,
                'error': '缺少 filename 参数'
            }), 400
        
        # 安全检查：防止路径遍历攻击
        if '..' in filename or '/' in filename or '\\' in filename:
            return jsonify({
                'success': False,
                'error': '无效的文件名'
            }), 400
        
        if not filename.endswith('.jsonl'):
            return jsonify({
                'success': False,
                'error': '只支持 .jsonl 文件'
            }), 400
        
        results_dir = os.path.join(os.path.dirname(__file__), 'results')
        filepath = os.path.join(results_dir, filename)
        
        if not os.path.exists(filepath):
            return jsonify({
                'success': False,
                'error': '文件不存在'
            }), 404
        
        # 读取文件内容
        meta = None
        results = []
        
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            # 解析第一行（可能是元数据）
            if lines:
                try:
                    first_line = json.loads(lines[0].strip())
                    if '_meta' in first_line:
                        meta = first_line['_meta']
                        lines = lines[1:]
                except:
                    pass
            
            # 解析数据行
            seen_keys = set()  # 用于去重：code + match_date + name
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    if 'code' in data:
                        # 生成唯一键：code + match_date + name
                        key = f"{data.get('code', '')}-{data.get('match_date', '')}-{data.get('name', '')}"
                        if key not in seen_keys:
                            seen_keys.add(key)
                            results.append(data)
                except:
                    continue
        
        return jsonify({
            'success': True,
            'data': {
                'meta': meta,
                'results': results,
                'count': len(results)
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/<path:path>')
def serve_static(path):
    """处理前端路由（Vue Router SPA）- 必须在所有 API 路由之后"""
    # API 路由不处理（虽然理论上不会到这里，但保险起见）
    if path.startswith('api/'):
        return jsonify({'error': 'Not Found'}), 404
    
    # 尝试返回静态文件
    file_path = os.path.join(FRONTEND_DIST_PATH, path)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return send_from_directory(FRONTEND_DIST_PATH, path)
    
    # 如果是目录或文件不存在，返回 index.html（让 Vue Router 处理）
    index_path = os.path.join(FRONTEND_DIST_PATH, 'index.html')
    if os.path.exists(index_path):
        return send_from_directory(FRONTEND_DIST_PATH, 'index.html')
    
    return jsonify({
        'error': '前端未构建，请先运行: cd frontend && npm install && npm run build'
    }), 404

if __name__ == '__main__':
    # 支持通过环境变量配置端口，默认 8086
    port = int(os.getenv('FLASK_PORT', 8086))
    # 生产环境关闭 debug 模式
    debug = os.getenv('FLASK_ENV', 'development') == 'development'
    app.run(debug=debug, host='0.0.0.0', port=port)
