# A 股策略回测系统

面向**沪深主板**的本地量化回测与结果展示：拉取日 K 缓存、多策略条件回测、板块/概念联动 enrich、Web 前端查看与导出。

## 功能概览

- **数据**：基于 AkShare 聚合接口拉取主板股票列表与日 K，落盘 `cache/stock_list.json`、`cache/stock_data/`（详见根目录 `DATA_SOURCE.md`）。
- **策略**：策略定义在 `common_strategies.json`，由 `strategy_engine.py` 解析条件并回测；支持全量、增量追加、按单日增量等脚本。
- **结果**：默认写入 `results/策略名_结果.jsonl`（该路径已在 `.gitignore` 中忽略，勿把个人回测大文件提交进仓库）。
- **板块联动**：按 `match_date` 对齐的日快照在 `cache/sector_linkage/daily/`，回测后可通过 `scripts/enrich_sector_linkage.py` 写入 `linkage_*` 字段。
- **前端**：Vue 3 + Vite + Ant Design Vue，构建产物输出到 `static/`，由 Flask 提供页面与 `/api/*`。

## 环境要求

- Python 3.10+（推荐用项目 `venv`）
- Node.js 18+ 与 npm（仅构建前端时需要）

主要 Python 依赖见 `requirements.txt`（Flask、pandas、akshare 等）。

## 快速启动（本机）

在项目根目录：

```bash
./run.sh
```

脚本会：创建/激活 `venv`、安装依赖、构建前端、启动 Flask（默认 **http://localhost:8086**）。

仅后端（不重建前端）时：

```bash
source venv/bin/activate
python app.py
```

## 常用命令

更全的说明见 **`docs/常用命令.md`**，例如：

| 场景 | 命令 |
|------|------|
| 仅补日 K、不回测 | `./venv/bin/python scripts/update_cache_and_backtest.py --no-backtest` |
| 补缓存 + 全量回测 | `./venv/bin/python scripts/update_cache_and_backtest.py` |
| 增量追加回测 | `./venv/bin/python scripts/backtest_append_from_last.py --cache-only --no-check-cache --skip-ensure-data` |
| 指定 T 日增量（按日文件） | `./venv/bin/python scripts/incremental_backtest.py --date YYYY-MM-DD` |

## 定时任务（可选）

`install_scheduled_task.sh` 用于在 macOS **launchd** 注册每日链路（示例：拉数 → 板块日快照 → 增量回测），具体以仓库内 plist 与脚本为准。日志在 `logs/`。

## 扩展策略与口径说明

- 主策略列表：`common_strategies.json`。
- **实体阳线（东财常见「连续阳线」口径）** 与「涨跌幅连涨」不同；独立配置见 `strategies_连阳实体阳线.json`，可用同一套 `scripts/incremental_backtest.py --config ...` 跑按日文件。

## 目录结构（摘要）

```
├── app.py                 # Flask 入口
├── data_fetcher.py        # 拉数与缓存
├── strategy_engine.py     # 策略条件与回测引擎
├── batch_backtest.py      # 多策略批跑入口
├── common_strategies.json # 默认策略配置
├── scheduled_task.py      # 定时增量回测（可与 launchd 配合）
├── scripts/               # 补数、增量、enrich 等脚本
├── frontend/              # Vue 源码
├── static/                # 构建后的前端资源（由 npm run build 生成）
├── docs/                  # 部署说明、常用命令等
├── DATA_SOURCE.md         # 数据源与脚本约定
└── cache/                 # 本地缓存（一般不提交）
```

## 规范与文档

- 前端/UI 约定：`docs/前端设计.md`
- 后端/API 约定：`docs/后端规范.md`
- 数据源与拉数：`DATA_SOURCE.md`

## 免责声明

本项目仅供学习与研究，不构成投资建议。行情数据来自第三方公开接口，其准确性、完整性与时效性请自行甄别。
