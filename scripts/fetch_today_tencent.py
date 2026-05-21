#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用腾讯API拉取今日实时行情，写入本地K线缓存
"""
import json
import os
import sys
import glob
import time
import urllib.request
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CACHE_DIR = os.path.join(PROJECT_ROOT, "cache", "stock_data")


def parse_tencent_line(line: str):
    """解析腾讯行情数据"""
    # v_sh600000="1~浦发银行~600000~8.95~9.08~9.08~1274569~..."
    if '="' not in line:
        return None
    key, val = line.split('="', 1)
    val = val.rstrip('"')
    parts = val.split("~")
    if len(parts) < 35:
        return None
    try:
        code = parts[2]
        name = parts[1]
        price = float(parts[3]) if parts[3] else 0
        yesterday_close = float(parts[4]) if parts[4] else 0
        open_price = float(parts[5]) if parts[5] else 0
        volume = float(parts[6]) if parts[6] else 0  # 成交量(手)
        high = float(parts[33]) if parts[33] else 0
        low = float(parts[34]) if parts[34] else 0
        turnover = float(parts[37]) if len(parts) > 37 and parts[37] else 0  # 成交额(万)
        
        if price <= 0 or open_price <= 0:
            return None
        
        # 计算涨跌幅
        pct_change = ((price - yesterday_close) / yesterday_close * 100) if yesterday_close > 0 else 0
        change_amount = price - yesterday_close
        amplitude = ((high - low) / yesterday_close * 100) if yesterday_close > 0 and low > 0 else 0
        turnover_rate = 0  # 换手率需要流通股本，暂时为0
        
        return {
            "code": code,
            "name": name,
            "price": price,
            "open": open_price,
            "high": high,
            "low": low,
            "close": price,
            "volume": volume,
            "turnover": turnover,
            "yesterday_close": yesterday_close,
            "pct_change": round(pct_change, 2),
            "change_amount": round(change_amount, 4),
            "amplitude": round(amplitude, 2),
        }
    except (ValueError, IndexError):
        return None


def fetch_batch(codes: list):
    """批量查询腾讯行情"""
    # 转换为腾讯格式: 600000->sh600000, 000001->sz000001
    tencent_codes = []
    for code in codes:
        if code.startswith("6"):
            tencent_codes.append(f"sh{code}")
        else:
            tencent_codes.append(f"sz{code}")
    
    url = f"http://qt.gtimg.cn/q={','.join(tencent_codes)}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode("gbk")
        results = {}
        for line in data.split(";"):
            line = line.strip()
            if not line:
                continue
            parsed = parse_tencent_line(line)
            if parsed:
                results[parsed["code"]] = parsed
        return results
    except Exception as e:
        return {}


def update_cache(today_str: str, today_compact: str):
    """更新缓存"""
    # 获取所有股票代码
    files = glob.glob(os.path.join(CACHE_DIR, "*_20250414.json"))
    codes = [os.path.basename(f).split("_")[0] for f in files]
    print(f"共 {len(codes)} 只股票需要更新", flush=True)
    
    # 检查哪些已经有今天的数据
    existing = set()
    for f in glob.glob(os.path.join(CACHE_DIR, f"*_{today_compact}.json")):
        code = os.path.basename(f).split("_")[0]
        existing.add(code)
    
    need_update = [c for c in codes if c not in existing]
    print(f"已有 {len(existing)} 只，需更新 {len(need_update)} 只", flush=True)
    
    if not need_update:
        print("所有股票已是最新", flush=True)
        return
    
    # 批量查询
    batch_size = 10
    total = len(need_update)
    success = 0
    fail = 0
    
    for i in range(0, total, batch_size):
        batch = need_update[i:i+batch_size]
        results = fetch_batch(batch)
        
        for code in batch:
            if code not in results:
                fail += 1
                continue
            
            r = results[code]
            
            # 读取最新缓存文件
            cache_files = glob.glob(os.path.join(CACHE_DIR, f"{code}_*.json"))
            if not cache_files:
                fail += 1
                continue
            
            latest_file = sorted(cache_files)[-1]
            try:
                with open(latest_file, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)
            except Exception:
                fail += 1
                continue
            
            # 检查是否已经有今天的数据
            has_today = False
            for row in cache_data.get("data", []):
                if row.get("日期", "").startswith(today_str):
                    has_today = True
                    break
            
            if has_today:
                success += 1
                continue
            
            # 添加今天的数据
            new_row = {
                "日期": today_str,
                "开盘": r["open"],
                "收盘": r["close"],
                "最高": r["high"],
                "最低": r["low"],
                "成交量": r["volume"],
                "成交额": r["turnover"],
                "振幅": r["amplitude"],
                "涨跌幅": r["pct_change"],
                "涨跌额": r["change_amount"],
                "换手率": 0,
            }
            
            # 增量计算MACD
            ema12_seed = cache_data.get("ema12_seed")
            ema26_seed = cache_data.get("ema26_seed")
            dea_seed = cache_data.get("dea_seed")
            
            if ema12_seed is not None and ema26_seed is not None and dea_seed is not None:
                close = r["close"]
                k12 = 2 / 13
                k26 = 2 / 27
                k9 = 2 / 10
                
                new_ema12 = close * k12 + ema12_seed * (1 - k12)
                new_ema26 = close * k26 + ema26_seed * (1 - k26)
                new_dif = new_ema12 - new_ema26
                new_dea = new_dif * k9 + dea_seed * (1 - k9)
                new_bar = 2 * (new_dif - new_dea)
                
                new_row["DIF"] = round(new_dif, 4)
                new_row["DEA"] = round(new_dea, 4)
                new_row["MACD_BAR"] = round(new_bar, 4)
                
                # 更新种子值
                cache_data["ema12_seed"] = round(new_ema12, 6)
                cache_data["ema26_seed"] = round(new_ema26, 6)
                cache_data["dea_seed"] = round(new_dea, 6)
                
                # 计算信号强度
                data_rows = cache_data["data"]
                if len(data_rows) >= 3:
                    pct3 = (close / data_rows[-3]["收盘"] - 1) * 100
                    cache_data["last_signal_strength"] = round(abs(new_bar) * (100 + abs(pct3)), 1)
            
            cache_data["data"].append(new_row)
            
            # 写回原缓存文件（更新 end_date、cache_time，追加数据行）
            new_file = latest_file  # 直接覆盖原文件
            cache_data["end_date"] = today_compact
            cache_data["cache_time"] = datetime.now().isoformat()
            try:
                with open(new_file, "w", encoding="utf-8") as f:
                    json.dump(cache_data, f, ensure_ascii=False)
                success += 1
            except Exception:
                fail += 1
        
        progress = min(i + batch_size, total)
        if progress % 100 == 0 or progress == total:
            print(f"进度: {progress}/{total} (成功:{success}, 失败:{fail})", flush=True)
        
        time.sleep(0.05)
    
    print(f"\n完成: 成功 {success}, 失败 {fail}", flush=True)


def main():
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    today_compact = today.strftime("%Y%m%d")
    
    print(f"今日日期: {today_str}", flush=True)
    update_cache(today_str, today_compact)


if __name__ == "__main__":
    main()
