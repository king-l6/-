#!/usr/bin/env python3
"""
从前端常用策略页面的五个策略结果中，提取相同的 code+match_date 组合。
五个策略：龙头战法、断板反包、均线上穿、情绪周期、三连板

输出：
1. 五策略交集_code日期.jsonl - 必须同时满足五个策略（交集）
2. 五策略多策略重合_code日期.jsonl - 至少出现在2个策略中，附 strategy_count 字段
"""

import json
from pathlib import Path
from collections import defaultdict

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
COMMON_STRATEGY_FILES = [
    "龙头战法_结果.jsonl",
    "断板反包_结果.jsonl",
    "均线上穿_结果.jsonl",
    "情绪周期_结果.jsonl",
    "三连板_结果.jsonl",
]
STRATEGY_NAMES = [f.replace("_结果.jsonl", "") for f in COMMON_STRATEGY_FILES]


def load_code_dates(filepath: Path) -> set[tuple[str, str]]:
    """从 jsonl 文件加载 (code, match_date) 集合，跳过 _meta 行"""
    result = set()
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            if "_meta" in data:
                continue
            code = data.get("code")
            match_date = data.get("match_date")
            if code and match_date:
                result.add((code, match_date))
    return result


def load_all_records() -> dict[tuple[str, str], dict]:
    """从所有文件中加载记录，同一 key 以第一个遇到的为准"""
    records: dict[tuple[str, str], dict] = {}
    for filename in COMMON_STRATEGY_FILES:
        filepath = RESULTS_DIR / filename
        if not filepath.exists():
            continue
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                if "_meta" in data:
                    continue
                code = data.get("code")
                match_date = data.get("match_date")
                if code and match_date and (code, match_date) not in records:
                    records[(code, match_date)] = data
    return records


def main():
    sets_list: list[set[tuple[str, str]]] = []
    for filename in COMMON_STRATEGY_FILES:
        filepath = RESULTS_DIR / filename
        if not filepath.exists():
            print(f"警告: 文件不存在 {filepath}")
            continue
        s = load_code_dates(filepath)
        sets_list.append(s)
        print(f"{filename}: {len(s)} 条 (code, date)")

    if not sets_list:
        print("没有找到任何策略结果文件")
        return

    # 1. 五策略交集（必须全部满足）
    common = sets_list[0]
    for s in sets_list[1:]:
        common = common & s
    print(f"\n五策略交集: {len(common)} 条")

    # 2. 统计每个 (code, date) 出现在几个策略中
    count_by_key: dict[tuple[str, str], int] = defaultdict(int)
    for s in sets_list:
        for key in s:
            count_by_key[key] += 1
    multi_match = {k: v for k, v in count_by_key.items() if v >= 2}
    print(f"至少2个策略重合: {len(multi_match)} 条")
    for n in range(3, 6):
        cnt = sum(1 for v in count_by_key.values() if v >= n)
        print(f"至少{n}个策略重合: {cnt} 条")

    records = load_all_records()

    def write_output(items: list[tuple[str, str]], out_path: Path, meta_desc: str, extra_field: dict | None = None):
        with open(out_path, "w", encoding="utf-8") as f:
            meta = {
                "_meta": {
                    "source": "五策略提取",
                    "strategies": STRATEGY_NAMES,
                    "count": len(items),
                    "description": meta_desc,
                }
            }
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for code, match_date in sorted(items, key=lambda x: (x[1], x[0])):
                row = records.get((code, match_date), {"code": code, "match_date": match_date}).copy()
                if extra_field:
                    row.update(extra_field.get((code, match_date), {}))
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"已保存: {out_path}")

    # 输出1: 五策略交集
    write_output(
        sorted(common, key=lambda x: (x[1], x[0])),
        RESULTS_DIR / "五策略交集_code日期.jsonl",
        "龙头战法、断板反包、均线上穿、情绪周期、三连板 五个策略共同的 code+date",
    )

    # 输出2: 至少2个策略重合，附 strategy_count
    extra = {(code, date): {"strategy_count": count_by_key[(code, date)]} for code, date in multi_match}
    write_output(
        sorted(multi_match, key=lambda x: (-count_by_key[x], x[1], x[0])),
        RESULTS_DIR / "五策略多策略重合_code日期.jsonl",
        "至少出现在2个策略中的 code+date，按重合数降序",
        extra_field=extra,
    )


if __name__ == "__main__":
    main()
