// 策略条件类型
export type ConditionType =
  | 'limit_up'
  | 'pct_change_gt'
  | 'pct_change_lt'
  | 'pct_change_between'
  | 'volume_ratio'
  | 'three_limit_up'
  | 'recent_limit_up'
  | 'ma_cross_up'
  | 'bottoming_breakout'
  | 'touch_limit_not_close'
  | 'high_is_limit_up'
  | 'recent_n_day_pct_change_lt'
  | 'close_below_ma_deviation'
  | 'rsi_lt'
  | 'stop_fall_signal'
  | 'listed_days_gte'
  | 'avg_amount_gte'
  | 'main_force_build_position'
  | 'consecutive_up_days_gte'
  | 'upper_shadow_pct_gt'

export interface StrategyCondition {
  type: ConditionType
  date1?: number // 交易日偏移（负数=往前推）
  value?: number // 涨幅值（用于 pct_change_gt/lt）
  minValue?: number // 最小涨幅值（用于 pct_change_between）
  maxValue?: number // 最大涨幅值（用于 pct_change_between）
  date2?: number // 交易日偏移2（用于 volume_ratio）
  ratio?: number // 比例（用于 volume_ratio）
  days?: number // 检查天数范围（用于 three_limit_up/recent_limit_up，默认30/10）
  shortPeriod?: number // 短期均线周期（用于 ma_cross_up，默认5）
  longPeriod?: number // 长期均线周期（用于 ma_cross_up，默认10）
  period?: number // 周期参数（用于 close_below_ma_deviation / rsi_lt）
  deviation?: number // 乖离比例（用于 close_below_ma_deviation，如 0.06）
  lowerShadowRatio?: number // 下影线占比阈值（用于 stop_fall_signal）
  volumeDays?: number // 成交量均线周期（用于 stop_fall_signal）
  volumeRatio?: number // 放量阈值（用于 stop_fall_signal）
  windowDays?: number // 窗口天数（用于 main_force_build_position）
  consecutiveDays?: number // 连阳天数阈值（用于 consecutive_up_days_gte）
  requireMa5GtMa10?: boolean // 连阳窗口内是否要求每一天都满足5日线>10日线
}

export interface ExcludeRules {
  kcb: boolean // 排除科创板
  cyb: boolean // 排除创业板
  bjs: boolean // 排除北交所
  st: boolean // 排除ST股
  delist: boolean // 排除退市股
}

export interface Strategy {
  name?: string
  conditions: StrategyCondition[]
  exclude: ExcludeRules
  timeRange: number // 回测时间范围（交易日数）
}

export interface BacktestRequest {
  strategy: Strategy
  strategy_name?: string
}

export interface StockResult {
  code: string
  name: string
  match_date?: string
  match_price?: number
  current_price?: number
  day1_amplitude?: number   // 匹配日振幅（(收盘-开盘)/开盘*100）
  day1_change_pct?: number // 匹配日涨跌幅（(收盘-前收)/前收*100）
  day2_amplitude?: number   // 次日振幅（收盘-开盘）/开盘*100
  day2_change_pct?: number  // 次日涨跌幅（(收盘-前收盘)/前收盘*100）
  day3_amplitude?: number   // 第三日振幅
  day3_change_pct?: number  // 第三日涨跌幅
  day2_buy_10d_max_gain_pct?: number | null // 次日开盘买入后，10个交易日内最高涨幅(%)
  day2_buy_10d_close_pct?: number | null // 次日开盘买入后，第10个交易日收盘涨跌幅(%)
  day2_buy_hit_5pct_day?: number | null // 次日开盘买入后，首次达到5%是第几天（买入日=1）
  /** 月内三连板+首板策略中特殊标记：T日最高价触及涨停但收盘未涨停 */
  touch_limit_not_close?: boolean
  /** 主力建仓打标（不要求T日涨停；要求T-1非涨停+T-10至T满足均线多头收涨结构） */
  main_force_build_tag?: boolean
  /** T日是否涨停（仅用于前端筛选标记） */
  main_force_t_limit_up_tag?: boolean
  /** 主力建仓命中收涨日个数（T-10至T：收盘>前一交易日收盘） */
  main_force_bullish_days?: number
  /** 主力建仓命中收涨日中「5/10日均线斜率均向上」的个数 */
  main_force_slope_up_days?: number
  /** T 日向前连续阳线天数（含 T 日） */
  consecutive_up_days?: number
  /** T 日上影线幅度：最高涨幅-收盘涨幅（%） */
  upper_shadow_pct?: number
  /** 连阳区间内是否出现过最高价触及涨停价（触板/涨停） */
  consecutive_up_has_limit_touch?: boolean
  /**
   * 板块/概念联动文案（由 `scripts/enrich_sector_linkage.py` 写入；含概念/行业板块当日涨跌幅；与 match_date 无历史对齐）
   */
  linkage_text?: string
  linkage_concepts?: { name: string; pct: number; rank?: number }[]
  /** 命中全部强势概念的「涨幅榜名次」算术平均（与 linkage_concepts 条数一致口径） */
  linkage_concept_rank_avg?: number | null
  /** 命中全部强势概念的板块当日涨跌幅（%）算术平均 */
  linkage_concept_pct_avg?: number | null
  linkage_industry?: string
  /** 联动行业板块当日涨跌幅（%），与 linkage_industry 对应 */
  linkage_industry_pct?: number | null
  /** 联动行业在当前排序板块列表中的涨幅名次（1-based） */
  linkage_industry_rank?: number | null
  linkage_fetched_at?: string
  /** 多策略同日重叠 jsonl（scripts/aggregate_same_day_multi_strategy.py） */
  strategy_count?: number
  overlap_strategies?: string[]
  overlap_strategies_text?: string
  overlap_summary?: string
  strategies?: string[]
  strategies_joined?: string
}

export interface BacktestResponse {
  success: boolean
  data: StockResult[]
  count: number
  _cached?: boolean
  error?: string
}

export interface Stock {
  code: string
  name: string
}

export interface StocksResponse {
  success: boolean
  data: Stock[]
  error?: string
}

export interface ResultFile {
  filename: string
  size: number
  modified: string
  count?: number
}

export interface ResultsListResponse {
  success: boolean
  data: ResultFile[]
  error?: string
}

export interface ResultFileData {
  meta?: {
    strategy_name?: string
    run_at?: string
    count?: number
  }
  results: StockResult[]
  count: number
}

export interface ResultFileResponse {
  success: boolean
  data: ResultFileData
  error?: string
}

export interface MarketLeaderDayRow {
  date: string
  limit_up_count: number
  top1: { code: string; name: string; consecutive_boards: number; pct_change: number; volume: number } | null
  leaders: Array<{ code: string; name: string; consecutive_boards: number; pct_change: number; volume: number }>
}

export interface MarketLeaderSegmentRow {
  start_date: string
  end_date: string
  code: string
  name: string
  days: number
  /** 该段内主线标的出现的最高连板数 */
  max_consecutive_boards?: number
}

export interface MarketLeaderRotation {
  daily: MarketLeaderDayRow[]
  segments: MarketLeaderSegmentRow[]
  limit_pct: number
  top_k: number
  note?: string
}

export interface EmotionCycleReport {
  date: string
  market_metrics: {
    total: number
    limit_up_count: number
    strong_count: number
    big_drop_count: number
    avg_pct_change: number
    limit_up_ratio_pct: number
    strong_ratio_pct: number
    big_drop_ratio_pct: number
  }
  scores: {
    market_score: number
    total_score: number
  }
  cycle: string
  timeline: Array<{
    date: string
    market_score: number
    total_score: number
    cycle: string
    limit_up_count: number
    strong_count: number
    big_drop_count: number
    avg_pct_change: number
  }>
  /** 仅当请求带 stock_code 时返回个股 K 线周期（页面已改用市场龙头周期） */
  stock_cycle?: {
    code: string
    name?: string
    periods: Array<{
      start_date: string
      end_date: string
      days: number
      gain_pct: number
      label: string
    }>
    phase_segments: Array<{
      start_date: string
      end_date: string
      label: string
      kind: 'rally' | 'warmup' | 'range' | 'cooldown'
    }>
    series: Array<{
      date: string
      pct_change: number
      open: number
      high: number
      low: number
      close: number
      volume: number
    }>
  }
  market_leader_rotation?: MarketLeaderRotation
  generated_at: string
  /** 温度图按日快照：磁盘滚动缓存命中情况（由后端 emotion_cycle_rolling.json 维护） */
  timeline_rolling_meta?: {
    rolling_file: string
    files_merged: number
    files_skipped: number
    force_refresh: boolean
    dates_in_store: number
  }
}

export interface EmotionCycleResponse {
  success: boolean
  data?: EmotionCycleReport
  error?: string
}

export interface EmotionCycleHealth {
  cache_dir: string
  cache_file_count: number
  latest_date: string
  latest_snapshot_size: number
  sample_codes: string[]
  sample_stocks?: Array<{
    code: string
    name: string
  }>
}

export interface EmotionCycleHealthResponse {
  success: boolean
  data?: EmotionCycleHealth
  error?: string
}

export interface CacheUpdateTaskStatus {
  running: boolean
  started_at: string | null
  ended_at: string | null
  exit_code: number | null
  last_lines: string[]
  progress: {
    current: number
    total: number
    percent: number
    line: string
  } | null
  error: string | null
}

export interface CacheUpdateTaskResponse {
  success: boolean
  data?: {
    message: string
    task: CacheUpdateTaskStatus
  } | CacheUpdateTaskStatus
  error?: string
}

/** 日 K 单行（/api/stock-daily） */
export interface StockDailyBar {
  date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface StockDailyResponse {
  success: boolean
  data?: {
    code: string
    rows: StockDailyBar[]
  }
  error?: string
}
