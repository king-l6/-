# 数据源说明

## 选用原则（快且准）

- **股票列表（仅代码与简称）**：默认 **AkShare 拉交易所公开主板清单**（与「主板」板块定义一致、通常更快）；失败或为空时 **自动回退 Baostock** `query_all_stock` + 与本项目相同的规则过滤。需要强制与回测旧环境一致时，设环境变量 **`STOCK_LIST_SOURCE=baostock`** 仅用 Baostock。
- **日 K 线（开高低收、量额、涨跌幅等）**：统一 **Baostock**，与回测、缓存文件名及字段一致；不因「换源快」混用其它日 K 源，避免同一套策略口径不一致。
- **情绪周期页 / 已落盘缓存**：计算时只读本地 `cache/stock_data`，**最快**；**准度**等于当前缓存里已写入的交易日数据（见下文「当日数据何时可用」）。

## 使用的数据源

本项目使用 **Baostock**（[www.baostock.com](http://www.baostock.com)）作为 A 股**日 K 线**数据源；**沪深主板普通股代码表**默认用 **AkShare** 从交易所公开清单汇总（沪市「主板A股」、深市「A股列表」中板块为「主板」），失败时回退 Baostock `query_all_stock` + 规则过滤。环境变量 `STOCK_LIST_SOURCE=baostock` 可强制仅用 Baostock 拉列表。

- **类型**：Baostock / AkShare 均为免费源  
- **用途**：AkShare/Baostock 股票列表；Baostock 日 K（开高低收、成交量等）  
- **范围**：沪市主板（`60xxxx`）、深市主板（`000`–`003` 段，与深交所「主板」板块一致），不含科创板、创业板、北交所  
- **代码**：`stock_list_sources.py`、`data_fetcher.py` 写入本地 `cache/stock_list.json` 与 `cache/stock_data/`

## 当日数据何时可用

Baostock 的**日 K 线数据**按以下时间更新：

| 内容         | 更新时间（自然日） |
|--------------|----------------------|
| **日 K 线**  | **当日 17:30** 左右  |
| 复权因子     | 当日 18:00 左右      |
| 分钟 K 线    | 下一自然日 11:00     |

也就是说：

- **交易日当天**：收盘后约 **17:30** 才会有当天的日 K 数据。
- **盘中或收盘后不久**（17:30 前）：接口里“最新交易日”仍是**上一交易日**，本项目的 `_get_last_trading_day_available()` 会检测当日是否有数据，没有则用前一交易日，避免拉到空数据。

因此：

- 若希望回测**包含今天**的数据，建议在 **当天 17:30 之后**（或次日）再跑拉数/回测脚本（例如 `scripts/update_cache_and_backtest.py` 或每天下午 5 点的定时任务）。
- 若在 17:30 前跑，脚本会使用“最近有数据的交易日”（通常是昨天），不会出错，只是不包含今天。

## 与本项目脚本的关系

- **拉取数据**：`scripts/update_cache_and_backtest.py`、`data_fetcher.update_caches_with_today_data()` 等都会用 `_get_last_trading_day_available()` 作为“最近交易日”，因此会自然避开“当日数据尚未更新”的情况。
- **每天下午 5 点定时任务**：若设为 17:00，第一次跑可能仍拉不到当天（需 17:30 后）；若希望尽量包含当天，可将定时时间改为 **17:35 或 18:00** 等。

## 参考

- Baostock 官网与文档：[www.baostock.com](http://www.baostock.com)  
- 日 K 更新时间：当前交易日 17:30 完成日 K 线数据入库（以官方最新说明为准）
