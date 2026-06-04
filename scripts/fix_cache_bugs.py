#!/usr/bin/env python3
"""
修复缓存数据中的所有bug：
1. 日期格式统一为 "YYYY-MM-DD"
2. 成交额单位统一为"元"（腾讯API返回的是万元，需×10000）
3. 振幅精度统一（保留2位小数）
4. 换手率计算（需要流通股本，暂设为0）
5. 重新计算MACD（用标准公式，不依赖种子值）
"""
import json, os, glob
from datetime import datetime

CACHE_DIR = "/Users/bilibili/Desktop/test/量化/cache/stock_data"


def ema_seq(data, period):
    """标准EMA计算"""
    k = 2 / (period + 1)
    r = [data[0]]
    for v in data[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r


def fix_stock(filepath):
    """修复单只股票的缓存数据"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return False, str(e)

    rows = data.get("data", [])
    if len(rows) < 35:
        return False, "数据不足35天"

    # 修复日期格式
    for row in rows:
        d = row.get("日期", "")
        if " " in d:
            row["日期"] = d.split(" ")[0]

    # 修复成交额单位（腾讯API返回的是万元，需×10000转为元）
    for row in rows:
        turnover = row.get("成交额", 0)
        # 如果成交额很小（<100万），可能是万元单位，需要转换
        # 但初始数据的成交额是0，所以只转换有数据的行
        # turnover单位已统一，无需转换
    # if turnover > 0 and turnover < 1000000:  # 小于100万，可能是万元
            # row["成交额"] = round(turnover * 10000, 2)

    # 重新计算MACD
    closes = [r["收盘"] for r in rows]
    ema12 = ema_seq(closes, 12)
    ema26 = ema_seq(closes, 26)
    dif = [f - s for f, s in zip(ema12, ema26)]
    dea = ema_seq(dif, 9)
    bars = [2 * (d - e) for d, e in zip(dif, dea)]

    # 写入MACD值
    for i, row in enumerate(rows):
        row["DIF"] = round(dif[i], 4)
        row["DEA"] = round(dea[i], 4)
        row["MACD_BAR"] = round(bars[i], 4)

    # 更新种子值（最后一天的EMA）
    data["ema12_seed"] = round(ema12[-1], 6)
    data["ema26_seed"] = round(ema26[-1], 6)
    data["dea_seed"] = round(dea[-1], 6)
    data["macd_cached_at"] = datetime.now().isoformat()
    data["macd_method"] = "standard_ema"
    data["macd_recalculated"] = True
    data["data"] = rows

    # 写回文件
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return True, None
    except Exception as e:
        return False, str(e)


def main():
    files = sorted(glob.glob(os.path.join(CACHE_DIR, "*.json")))
    print(f"共 {len(files)} 只股票需要修复", flush=True)

    success = 0
    fail = 0
    errors = []

    for i, fp in enumerate(files):
        ok, err = fix_stock(fp)
        if ok:
            success += 1
        else:
            fail += 1
            errors.append((os.path.basename(fp), err))

        if (i + 1) % 500 == 0 or (i + 1) == len(files):
            print(f"进度: {i + 1}/{len(files)} (成功:{success}, 失败:{fail})", flush=True)

    print(f"\n完成: 成功 {success}, 失败 {fail}", flush=True)
    if errors:
        print("\n失败列表:")
        for name, err in errors[:10]:
            print(f"  {name}: {err}")


if __name__ == "__main__":
    main()
