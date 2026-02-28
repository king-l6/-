// 策略条件类型
export type ConditionType = 'limit_up' | 'pct_change_gt' | 'pct_change_lt' | 'pct_change_between' | 'volume_ratio' | 'three_limit_up' | 'ma_cross_up' | 'bottoming_breakout'

export interface StrategyCondition {
  type: ConditionType
  date1: number // 交易日偏移（负数=往前推）
  value?: number // 涨幅值（用于 pct_change_gt/lt）
  minValue?: number // 最小涨幅值（用于 pct_change_between）
  maxValue?: number // 最大涨幅值（用于 pct_change_between）
  date2?: number // 交易日偏移2（用于 volume_ratio）
  ratio?: number // 比例（用于 volume_ratio）
  days?: number // 检查天数范围（用于 three_limit_up，默认30）
  shortPeriod?: number // 短期均线周期（用于 ma_cross_up，默认5）
  longPeriod?: number // 长期均线周期（用于 ma_cross_up，默认10）
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
