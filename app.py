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
import threading
import subprocess
import re
from strategy_engine import StrategyEngine
from data_fetcher import DataFetcher
from emotion_cycle_service import analyze_emotion_cycle, get_emotion_cycle_health

# 使用前端构建产物（构建到 static 目录）
FRONTEND_DIST_PATH = os.path.join(os.path.dirname(__file__), 'static')
# 不把 static 挂到根路径，避免 Flask 默认静态路由拦截 SPA 子路径导致 404
app = Flask(__name__, static_folder=None, static_url_path=None)

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

# 结果文件缓存：减少文件I/O
_results_cache = {}
_RESULTS_CACHE_TTL = 300  # 5分钟缓存

# 缓存补齐后台任务状态（单任务）
_cache_update_lock = threading.Lock()
_cache_update_task = {
    'running': False,
    'started_at': None,
    'ended_at': None,
    'exit_code': None,
    'last_lines': [],
    'progress': None,
    'error': None,
}


def _append_cache_update_line(line: str):
    line = (line or '').rstrip('\n')
    if not line:
        return
    _cache_update_task['last_lines'].append(line)
    if len(_cache_update_task['last_lines']) > 200:
        _cache_update_task['last_lines'] = _cache_update_task['last_lines'][-200:]

    # 解析常见进度格式：进度: 123/456 | 已更新: 100
    m = re.search(r'进度[:：]\s*(\d+)\s*/\s*(\d+)', line)
    if m:
        try:
            cur = int(m.group(1))
            total = int(m.group(2))
            percent = round(cur / total * 100, 2) if total > 0 else 0.0
            _cache_update_task['progress'] = {
                'current': cur,
                'total': total,
                'percent': percent,
                'line': line,
            }
        except Exception:
            pass


def _run_cache_update_task():
    project_dir = os.path.dirname(__file__)
    cmd = ['python3', 'scripts/update_cache_and_backtest.py', '--no-backtest']
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
        )
        if proc.stdout:
            for line in proc.stdout:
                with _cache_update_lock:
                    _append_cache_update_line(line)
        code = proc.wait()
        with _cache_update_lock:
            _cache_update_task['running'] = False
            _cache_update_task['ended_at'] = datetime.now().isoformat()
            _cache_update_task['exit_code'] = code
            if code != 0 and not _cache_update_task.get('error'):
                _cache_update_task['error'] = f'任务异常退出，exit_code={code}'
    except Exception as e:
        with _cache_update_lock:
            _cache_update_task['running'] = False
            _cache_update_task['ended_at'] = datetime.now().isoformat()
            _cache_update_task['error'] = str(e)
            _cache_update_task['exit_code'] = -1
    finally:
        if proc and proc.stdout:
            try:
                proc.stdout.close()
            except Exception:
                pass

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


@app.route('/api/stock-daily', methods=['GET'])
def api_stock_daily():
    """单只股票日 K（来自本地缓存 / Baostock），供多股复盘图等使用。"""
    code = (request.args.get('code') or '').strip()
    if len(code) != 6 or not code.isdigit():
        return jsonify({'success': False, 'error': 'code 须为 6 位数字'}), 400
    start = (request.args.get('start') or '').strip().replace('-', '')[:8] or None
    end = (request.args.get('end') or '').strip().replace('-', '')[:8] or None
    try:
        df = data_fetcher.get_stock_data(code, start_date=start, end_date=end, force_refresh=False)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    if df is None or getattr(df, 'empty', True):
        return jsonify({'success': False, 'error': '暂无 K 线，请先补该股缓存或调整日期范围'}), 404
    rows = []
    for _, r in df.iterrows():
        d = r['日期']
        if hasattr(d, 'strftime'):
            ds = d.strftime('%Y-%m-%d')
        else:
            ds = str(d)[:10]
        rows.append({
            'date': ds,
            'open': float(r['开盘']),
            'high': float(r['最高']),
            'low': float(r['最低']),
            'close': float(r['收盘']),
            'volume': float(r['成交量']),
        })
    return jsonify({'success': True, 'data': {'code': code, 'rows': rows}})

@app.route('/api/strategies', methods=['GET'])
def get_strategies():
    """获取所有策略配置（从 common_strategies.json 读取）"""
    try:
        config_file = os.path.join(os.path.dirname(__file__), 'common_strategies.json')
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify({
            'success': True,
            'data': data.get('strategies', [])
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/trading-days', methods=['GET'])
def get_trading_days():
    """获取指定日期范围内的所有交易日（来自本地缓存 K 线），用于图表按日补全（无数据日显示 0）。"""
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        if not start or not end:
            return jsonify({'success': False, 'error': '缺少 start 或 end 参数（格式 YYYY-MM-DD）'}), 400
        start_s = start.strip()[:10]
        end_s = end.strip()[:10]
        days = data_fetcher.get_trading_days_between(start_s, end_s)
        if not days:
            days = data_fetcher.get_trading_days_from_cache(start_s, end_s)
        return jsonify({'success': True, 'data': days})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/results/list', methods=['GET'])
def get_results_list():
    """获取 results 目录下的所有文件列表"""
    try:
        def _count_one_results_file(filepath: str) -> int:
            """统计单个 .jsonl 结果文件的数据条数（跳过 _meta 行与无效行）。"""
            cnt = 0
            checked_first_non_empty = False
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if not checked_first_non_empty:
                            checked_first_non_empty = True
                            try:
                                first = json.loads(line)
                                if isinstance(first, dict) and '_meta' in first:
                                    continue
                                data = first
                            except Exception:
                                continue
                            if isinstance(data, dict) and 'code' in data:
                                name = (data.get('name') or '').strip()
                                if len(name) <= 4:
                                    cnt += 1
                            continue

                        try:
                            data = json.loads(line)
                        except Exception:
                            continue
                        if not isinstance(data, dict) or 'code' not in data:
                            continue
                        name = (data.get('name') or '').strip()
                        if len(name) > 4:
                            continue
                        cnt += 1
            except Exception:
                return 0
            return cnt

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
                        'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        'count': _count_one_results_file(filepath)
                    })
        
        # 战法名称列表（用于置顶），按优先级排序
        strategy_names = ['龙头战法', '断板反包', '均线上穿', '情绪周期', '三连板', '超跌反弹+量能确认']
        
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

# 结果文件缓存获取函数
def _get_results_with_cache(cache_key, fetch_func):
    """带缓存的结果获取，减少文件I/O"""
    if cache_key in _results_cache:
        data, ts = _results_cache[cache_key]
        if (datetime.now() - ts).total_seconds() < _RESULTS_CACHE_TTL:
            return data, True  # 返回数据和缓存命中标记

    data = fetch_func()
    _results_cache[cache_key] = (data, datetime.now())

    # 缓存大小控制
    if len(_results_cache) > 20:
        # 清理最旧的10个
        sorted_keys = sorted(_results_cache.keys(),
                           key=lambda k: _results_cache[k][1])
        for k in sorted_keys[:10]:
            del _results_cache[k]

    return data, False

def _read_one_results_file(filepath):
    """读取单个 .jsonl 结果文件，返回 (meta_dict, results_list)。"""
    meta = None
    results = []
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
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
            name = (data.get('name') or '').strip()
            if len(name) > 4:
                continue
            match_date = data.get('match_date')
            if match_date is not None:
                if hasattr(match_date, 'strftime'):
                    data = dict(data)
                    data['match_date'] = match_date.strftime('%Y-%m-%d')
                else:
                    s = str(match_date).strip()
                    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
                        data = dict(data)
                        data['match_date'] = s[:10]
            results.append(data)
        except Exception:
            continue
    return meta, results

@app.route('/api/results/strategy', methods=['GET'])
def get_results_by_strategy():
    """按策略名聚合：合并「策略名_结果.jsonl」与所有「策略名_YYYYMMDD_结果.jsonl」，按 match_date 排序返回。（带缓存）"""
    try:
        name = request.args.get('name')
        if not name or not name.strip():
            return jsonify({'success': False, 'error': '缺少 name 参数'}), 400
        strategy_name = name.strip()
        # 安全检查：仅允许策略名（不含路径、不含 ..）
        if '..' in strategy_name or '/' in strategy_name or '\\' in strategy_name:
            return jsonify({'success': False, 'error': '无效的策略名'}), 400

        # 使用缓存
        cache_key = f"strategy:{strategy_name}"

        def fetch():
            results_dir = os.path.join(os.path.dirname(__file__), 'results')
            if not os.path.isdir(results_dir):
                return {'meta': {'strategy_name': strategy_name}, 'results': [], 'count': 0}

            import re
            # 主文件：策略名_结果.jsonl
            main_file = os.path.join(results_dir, f"{strategy_name}_结果.jsonl")
            # 按日文件：策略名_YYYYMMDD_结果.jsonl
            pattern = re.compile(re.escape(strategy_name) + r'_(\d{8})_结果\.jsonl$')
            all_results = []
            meta_merged = {'strategy_name': strategy_name, 'aggregated': True}

            if os.path.isfile(main_file):
                m, rows = _read_one_results_file(main_file)
                if m:
                    meta_merged.update({k: v for k, v in m.items() if k != 'strategy_name'})
                all_results.extend(rows)

            for filename in os.listdir(results_dir):
                if not filename.endswith('.jsonl'):
                    continue
                if pattern.match(filename):
                    filepath = os.path.join(results_dir, filename)
                    if os.path.isfile(filepath):
                        m, rows = _read_one_results_file(filepath)
                        all_results.extend(rows)

            # 去重：code + match_date + name
            seen = set()
            unique = []
            for r in all_results:
                key = (r.get('code', ''), r.get('match_date', ''), r.get('name', ''))
                if key in seen:
                    continue
                seen.add(key)
                unique.append(r)
            unique.sort(key=lambda x: (x.get('match_date', '9999-99-99'), x.get('code', '')))

            # 所有策略统一：连续三个 A 股交易日内同股只保留第一次出现的日期
            try:
                fetcher = DataFetcher()
                engine = StrategyEngine(fetcher)
                unique = engine._dedupe_same_stock_within_three_trading_days(unique, trading_days=3)
                unique.sort(key=lambda x: (x.get('match_date', '9999-99-99'), x.get('code', '')))
            except Exception:
                pass

            return {'meta': meta_merged, 'results': unique, 'count': len(unique)}

        data, cached = _get_results_with_cache(cache_key, fetch)

        return jsonify({
            'success': True,
            'data': data,
            '_cached': cached
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/results/file', methods=['GET'])
def get_results_file():
    """获取指定 results 文件的内容（带缓存）"""
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

        # 使用缓存读取文件
        def fetch():
            meta, results = _read_one_results_file(filepath)
            return {'meta': meta, 'results': results, 'count': len(results)}

        data, cached = _get_results_with_cache(filename, fetch)

        return jsonify({
            'success': True,
            'data': data,
            '_cached': cached
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/emotion-cycle', methods=['GET'])
def get_emotion_cycle():
    """获取情绪周期分析结果（全市场温度 + 龙头节奏等）"""
    try:
        days = int(request.args.get('days', 120))
        stock_code = request.args.get('stock_code', '').strip()
        force_q = (request.args.get('force') or request.args.get('force_refresh') or '').strip().lower()
        force_refresh = force_q in ('1', 'true', 'yes', 'force')
        if days <= 0:
            days = 120
        if days > 500:
            days = 500
        report = analyze_emotion_cycle(
            days=days,
            stock_code=stock_code,
            force_refresh=force_refresh,
        )
        return jsonify({
            'success': True,
            'data': report
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/emotion-cycle/health', methods=['GET'])
def get_emotion_cycle_health_api():
    """情绪周期数据自检信息（本地日线缓存截面）。"""
    try:
        health = get_emotion_cycle_health()
        return jsonify({'success': True, 'data': health})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cache-update/start', methods=['POST'])
def start_cache_update_task():
    """启动缓存补齐后台任务（update_cache_and_backtest.py --no-backtest）。"""
    try:
        with _cache_update_lock:
            if _cache_update_task['running']:
                return jsonify({
                    'success': True,
                    'data': {
                        'message': '任务已在运行中',
                        'task': _cache_update_task
                    }
                })
            _cache_update_task['running'] = True
            _cache_update_task['started_at'] = datetime.now().isoformat()
            _cache_update_task['ended_at'] = None
            _cache_update_task['exit_code'] = None
            _cache_update_task['last_lines'] = []
            _cache_update_task['progress'] = None
            _cache_update_task['error'] = None
            _append_cache_update_line('已启动缓存补齐任务...')

        t = threading.Thread(target=_run_cache_update_task, daemon=True)
        t.start()
        return jsonify({'success': True, 'data': {'message': '已启动', 'task': _cache_update_task}})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cache-update/status', methods=['GET'])
def get_cache_update_status():
    """查询缓存补齐后台任务状态。"""
    try:
        with _cache_update_lock:
            task = dict(_cache_update_task)
            task['last_lines'] = list(_cache_update_task.get('last_lines', []))
            progress = _cache_update_task.get('progress')
            task['progress'] = dict(progress) if isinstance(progress, dict) else progress
        return jsonify({'success': True, 'data': task})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

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
