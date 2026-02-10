import type { StrategyCondition } from '@/types'

export interface StrategyTemplate {
  id: string
  name: string
  description: string
  conditions: StrategyCondition[]
  timeRange: number
  exclude?: {
    kcb?: boolean
    cyb?: boolean
    bjs?: boolean
    st?: boolean
    delist?: boolean
  }
}

/**
 * 龙头战法策略
 * 特征：
 * 1. T-2日涨停（启动日）
 * 2. T-1日涨停（连续涨停，确认龙头地位）
 * 3. T日继续上涨（涨幅>0，保持强势）
 * 4. T-1日成交量放大（成交量/T-2日成交量>1.2，资金关注度高）
 * 5. T日成交量继续放大（成交量/T-2日成交量>1.5，持续关注）
 */
export const dragonHeadStrategy: StrategyTemplate = {
  id: 'dragon_head',
  name: '龙头战法',
  description: '捕捉连续涨停的龙头股，适合追涨策略。要求T-2日和T-1日连续涨停，T日继续上涨，成交量持续放大。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -2 }, // T-2日涨停
    { type: 'limit_up', date1: -1 }, // T-1日涨停
    { type: 'pct_change_gt', date1: 0, value: 0 }, // T日涨幅>0
    { type: 'volume_ratio', date1: -1, date2: -2, ratio: 1.2 }, // T-1日成交量/T-2日成交量>1.2
    { type: 'volume_ratio', date1: 0, date2: -2, ratio: 1.5 } // T日成交量/T-2日成交量>1.5
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true
  }
}

/**
 * 断板反包策略
 * 特征：
 * 1. T-2日涨停（启动日）
 * 2. T-1日断板（涨幅<0，回调）
 * 3. T日反包涨停（重新涨停，确认强势回归）
 * 4. T-1日成交量放大（回调时成交量放大，洗盘）
 * 5. T日成交量继续放大（反包时成交量放大，资金回流）
 */
export const breakAndRecoverStrategy: StrategyTemplate = {
  id: 'break_and_recover',
  name: '断板反包',
  description: '捕捉涨停后回调再反包的股票，适合低吸策略。要求T-2日涨停，T-1日回调，T日反包涨停。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -2 }, // T-2日涨停
    { type: 'pct_change_lt', date1: -1, value: 0 }, // T-1日涨幅<0（断板）
    { type: 'limit_up', date1: 0 }, // T日涨停（反包）
    { type: 'volume_ratio', date1: -1, date2: -2, ratio: 1.1 }, // T-1日成交量/T-2日成交量>1.1
    { type: 'volume_ratio', date1: 0, date2: -2, ratio: 1.3 } // T日成交量/T-2日成交量>1.3
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true
  }
}

/**
 * 情绪周期策略
 * 特征：
 * 1. T-3日涨停（启动日）
 * 2. T-2日涨幅>3%（继续上涨，情绪升温）
 * 3. T-1日涨幅>0（保持上涨，情绪延续）
 * 4. T日涨停（情绪高潮，追涨机会）
 * 5. 成交量持续放大（资金持续流入）
 */
export const emotionCycleStrategy: StrategyTemplate = {
  id: 'emotion_cycle',
  name: '情绪周期',
  description: '捕捉情绪周期中的上涨股票，适合情绪追涨策略。要求连续上涨，成交量放大，T日涨停。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -3 }, // T-3日涨停
    { type: 'pct_change_gt', date1: -2, value: 3 }, // T-2日涨幅>3%
    { type: 'pct_change_gt', date1: -1, value: 0 }, // T-1日涨幅>0
    { type: 'limit_up', date1: 0 }, // T日涨停
    { type: 'volume_ratio', date1: -1, date2: -3, ratio: 1.2 }, // T-1日成交量/T-3日成交量>1.2
    { type: 'volume_ratio', date1: 0, date2: -3, ratio: 1.5 } // T日成交量/T-3日成交量>1.5
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true
  }
}

/**
 * 三连板策略
 * 特征：
 * 1. T-2日涨停
 * 2. T-1日涨停
 * 3. T日涨停（三连板）
 * 4. 成交量持续放大
 */
export const threeLimitUpStrategy: StrategyTemplate = {
  id: 'three_limit_up',
  name: '三连板',
  description: '捕捉连续三个交易日涨停的股票，适合强势追涨策略。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -2 }, // T-2日涨停
    { type: 'limit_up', date1: -1 }, // T-1日涨停
    { type: 'limit_up', date1: 0 }, // T日涨停
    { type: 'volume_ratio', date1: -1, date2: -2, ratio: 1.1 }, // T-1日成交量/T-2日成交量>1.1
    { type: 'volume_ratio', date1: 0, date2: -2, ratio: 1.2 } // T日成交量/T-2日成交量>1.2
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true
  }
}

/**
 * 均线上穿策略
 * 特征：
 * 1. T-1日涨停（启动）
 * 2. T日5日均线上穿10日均线（技术面确认）
 * 3. T日涨幅>0（保持上涨）
 * 4. 成交量放大
 */
export const maCrossUpStrategy: StrategyTemplate = {
  id: 'ma_cross_up',
  name: '均线上穿',
  description: '捕捉涨停后均线上穿的股票，结合技术面确认。要求T-1日涨停，T日5日均线上穿10日均线。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -1 }, // T-1日涨停
    { type: 'ma_cross_up', date1: 0, shortPeriod: 5, longPeriod: 10 }, // T日5日均线上穿10日均线
    { type: 'pct_change_gt', date1: 0, value: 0 }, // T日涨幅>0
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.1 } // T日成交量/T-1日成交量>1.1
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true
  }
}

// 所有策略模板（所有战法置顶）
export const allStrategyTemplates: StrategyTemplate[] = [
  // 所有战法策略（置顶）
  dragonHeadStrategy,        // 龙头战法
  breakAndRecoverStrategy,   // 断板反包
  maCrossUpStrategy,         // 均线上穿
  emotionCycleStrategy,      // 情绪周期
  threeLimitUpStrategy       // 三连板
]
