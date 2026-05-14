# 数据源说明

## 选用原则（快且准）

- **股票列表（仅代码与简称）**：**AkShare** 优先拉交易所公开主板清单（沪市「主板A股」、深市「A股列表」中板块为「主板」）；失败或结果过少时依次尝试东财 `stock_zh_a_spot_em`（必要时 `stock_sh_a_spot_em` / `stock_sz_a_spot_em`）、新浪 `stock_zh_a_spot`；均与项目内主板规则一致；再失败则用本地 `cache/stock_list.json` 过期缓存（若有）。
- **日 K 线（开高低收、量额、涨跌幅等）**：**AkShare** 优先 `stock_zh_a_hist`（东方财富，不复权）；失败时依次尝试 `stock_zh_a_daily`（新浪）、`stock_zh_a_hist_tx`（腾讯，若当前版本提供）；统一落成与缓存一致的中文列。腾讯路径无官方成交额字段时成交额记为 0，由下游 `_normalize_hist_df` 等仍可读数。**熔断**：批量拉取时若东财对「单只股票单次调用」累计失败达到 `data_fetcher.EASTMONEY_HIST_CB_THRESHOLD`（默认 50）只，本会话内不再请求东财，后续股票直接走新浪/腾讯。**仅新浪日 K**：设环境变量 `DATA_FETCH_STOCK_HIST_SOURCE=sina`（或 `新浪`）时只走 `stock_zh_a_daily`，不再尝试东财/腾讯；批量清空并重拉近 N 个交易日（新浪日 K + 同花顺概念/行业日线快照）可用 `scripts/rebuild_cache_recent_sina_ths.py`。
- **板块/概念联动（enrich 脚本）**：默认按每条结果的 **`match_date`（T 日）** 对齐——**概念**侧默认用 **同花顺概念指数** 计算该交易日涨跌幅并排序（较慢、较稳；环境变量 `SECTOR_LINKAGE_CONCEPT_DAILY=eastmoney` 或 `--concept-daily eastmoney` 可改回东财概念日 K）；**行业**侧默认用 **同花顺行业指数**（`SECTOR_LINKAGE_INDUSTRY_DAILY=eastmoney` 或 `--industry-daily eastmoney` 可改东财行业日 K）。**每条结果**用其自身的 `match_date` 取对应日的 `daily` 快照后再与成份求交。快照在 `cache/sector_linkage/daily/YYYY-MM-DD.json`（v2 含 `industry_daily`）；**空的 daily 文件不再当作有效缓存**（会强制重建）。概念侧东财与同花顺之间：同一源连续 3 次瞬时失败会自动换另一源；仍失败可调大 `--hist-sleep`（或 `SECTOR_LINKAGE_HIST_SLEEP`）、`--no-proxy`，或 `--max-concept-scan` 减轻压力。当本地日 K 缓存最新日与 `daily/` 下最大快照日相同时，默认**不再发起概念侧网络拉取**（`SECTOR_LINKAGE_SKIP_CONCEPT_WHEN_SYNCED=0` 可关闭）。成份股仍用东财/新浪 **当前** 成份接口与个股求交（与 T 日历史上真实成份可能有偏差；**名次与涨跌幅为 T 日板块指数口径**）。仅统计「当日强势榜 topN（及 min_pct 过滤）」内的命中：若该股所属概念都不在该范围内，则 `linkage_concepts` 可能为空（属口径限制，非日期错误）。关闭按日对齐：`SECTOR_LINKAGE_MATCH_DATE=0` 或 `--no-match-date`。未启用按日对齐时：`SECTOR_LINKAGE_SOURCE` 控制 spot 数据源。  
  **全量回测**（`strategy_engine` / `batch_backtest`）在写入各策略 `策略名_结果.jsonl` 后会自动 enrich；**增量按日** `scripts/incremental_backtest.py`、**按最后日期追加** `scripts/backtest_append_from_last.py` 在主文件重写后也会 enrich。上述入口统一为 `enrich_sector_linkage.enrich_results_jsonl_after_backtest`（多策略并行 batch 时 **互斥**，避免 enrich 竞态导致整文件无 `linkage_*`）。可用 `SKIP_SECTOR_LINKAGE_ENRICH=1` 关闭；`SECTOR_LINKAGE_SOURCE=auto` 恢复东财优先（**仅在不使用按日对齐时**影响 spot 排行）；`SECTOR_LINKAGE_CLEAR_PROXY=1` 清除代理；默认 **新浪优先**（仅影响未按日对齐时的 spot）。**多策略同日聚合**从各策略行合并已有 `linkage_*`。
- **交易日历 / 「最近可用交易日」**：用 AkShare 拉 **000001** 日 K 的日期序列推断（与旧版用基准股探测思路一致）。
- **情绪周期页 / 已落盘缓存**：计算时只读本地 `cache/stock_data`，**最快**；**准度**等于当前缓存里已写入的交易日数据（见下文「当日数据何时可用」）。

## 使用的数据源

本项目 A 股**列表与日 K** 均使用 **AkShare**（开源聚合接口；**列表**以交易所页为主、东财/新浪 spot 为备用；**日 K** 以东财 `stock_zh_a_hist` 为主，新浪/腾讯为备用，底层为各站点公开数据）。

- **类型**：免费源  
- **用途**：主板股票列表；日 K（开高低收、成交量等）；交易日探测  
- **范围**：沪市主板（`60xxxx`）、深市主板（`000`–`003` 段，与深交所「主板」板块一致），不含科创板、创业板、北交所  
- **代码**：`stock_list_sources.py`、`data_fetcher.py` 写入本地 `cache/stock_list.json` 与 `cache/stock_data/`

## 当日数据何时可用

日 K 一般在**收盘后一段时间**由数据源侧完成入库（具体以数据源为准，常见为 **17:00–18:00** 左右）。

- **交易日当天**：收盘后不久接口里才会出现「当天」的日 K。  
- **盘中或刚收盘**：`_get_last_trading_day_available()` 会探测 000001 当日是否有 K 线；若无则用前一有数据的交易日，避免拉到空数据。
- **中午等盘中拉缓存**：数据源可能对「今天」已返回一行不完整日 K，易被误判为已更新。`DataFetcher` 默认在**本地时间未到 `DATA_FETCH_EOD_HHMM`（默认 15:10）**时，不把**日历当天**当作最后一根完整日 K（回退到上一交易日），并在写缓存时**剔除**已落盘中、日期晚于该参照日的行；收盘后再跑则会自然对齐到当天完整 K。`get_stock_data` 的默认 `end_date` 与右端裁剪同样基于 `_get_last_trading_day_available()`（与 `update_caches_with_today_data` 一致），避免仅用工作日历导致盘中多拉了「当天」不完整 K。若需恢复旧行为（完全依赖接口探测）：`DATA_FETCH_DISABLE_INTRADAY_CAP=1`。收盘参照时刻可改：`DATA_FETCH_EOD_HHMM=15:05` 等。

因此：

- 若希望回测**包含今天**的数据，建议在 **当天收盘后稍晚**（或次日）再跑拉数/回测脚本（例如 `scripts/update_cache_and_backtest.py` 或定时任务设在 **17:35 或 18:00** 等）。
- 若在数据未就绪时跑，脚本会使用「最近有数据的交易日」，不会出错，只是可能不包含今天。

## 与本项目脚本的关系

- **拉取数据**：`scripts/update_cache_and_backtest.py`、`data_fetcher.update_caches_with_today_data()` 等都会用 `_get_last_trading_day_available()` 作为「最近交易日」，因此会自然避开「当日数据尚未更新」的情况。需「删 cache + 新浪-only 日 K + 同花顺概念/行业 daily」时：`./venv/bin/python scripts/rebuild_cache_recent_sina_ths.py --days 30`（详见脚本内 `--help`；多分片补个股后需再跑一次 `--board-only`）。
- **定时任务**：若设在 17:00，第一次跑可能仍拉不到当天；若希望尽量包含当天，可将时间改为 **17:35 或 18:00** 等。

## 参考

- AkShare 文档与接口说明：[https://akshare.akfamily.xyz/](https://akshare.akfamily.xyz/)
