import type { StrategyCondition } from '@/types';

export interface StrategyTemplate {
  id: string;
  name: string;
  description: string;
  conditions: StrategyCondition[];
  timeRange: number;
  exclude?: {
    kcb?: boolean;
    cyb?: boolean;
    bjs?: boolean;
    st?: boolean;
    delist?: boolean;
  };
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
  description:
    '捕捉连续涨停的龙头股，适合追涨策略。要求T-2日和T-1日连续涨停，T日继续上涨，成交量持续放大。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -2 }, // T-2日涨停
    { type: 'limit_up', date1: -1 }, // T-1日涨停
    { type: 'pct_change_gt', date1: 0, value: 0 }, // T日涨幅>0
    { type: 'volume_ratio', date1: -1, date2: -2, ratio: 1.2 }, // T-1日成交量/T-2日成交量>1.2
    { type: 'volume_ratio', date1: 0, date2: -2, ratio: 1.5 }, // T日成交量/T-2日成交量>1.5
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

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
  description:
    '捕捉涨停后回调再反包的股票，适合低吸策略。要求T-2日涨停，T-1日回调，T日反包涨停。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -2 }, // T-2日涨停
    { type: 'pct_change_lt', date1: -1, value: 0 }, // T-1日涨幅<0（断板）
    { type: 'limit_up', date1: 0 }, // T日涨停（反包）
    { type: 'volume_ratio', date1: -1, date2: -2, ratio: 1.1 }, // T-1日成交量/T-2日成交量>1.1
    { type: 'volume_ratio', date1: 0, date2: -2, ratio: 1.3 }, // T日成交量/T-2日成交量>1.3
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

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
  description:
    '捕捉情绪周期中的上涨股票，适合情绪追涨策略。要求连续上涨，成交量放大，T日涨停。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -3 }, // T-3日涨停
    { type: 'pct_change_gt', date1: -2, value: 3 }, // T-2日涨幅>3%
    { type: 'pct_change_gt', date1: -1, value: 0 }, // T-1日涨幅>0
    { type: 'limit_up', date1: 0 }, // T日涨停
    { type: 'volume_ratio', date1: -1, date2: -3, ratio: 1.2 }, // T-1日成交量/T-3日成交量>1.2
    { type: 'volume_ratio', date1: 0, date2: -3, ratio: 1.5 }, // T日成交量/T-3日成交量>1.5
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

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
    { type: 'volume_ratio', date1: 0, date2: -2, ratio: 1.2 }, // T日成交量/T-2日成交量>1.2
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 摸板首板策略
 * 特征：
 * 1. T日最高价触及涨停价，但收盘未涨停（摸板未封）
 * 2. T-1日涨幅<9.8%（前一日不是涨停）
 * 3. 近30个交易日内出现过三连板
 * 4. 近10个交易日内至少有1天涨停
 */
export const touchLimitNotCloseWithThreeLimitStrategy: StrategyTemplate = {
  id: 'touch_limit_not_close_with_three_limit',
  name: '摸板首板',
  description:
    'T日最高价摸到涨停价但收盘未涨停，T-1日非涨停，且近30个交易日内走出过三连板、近10个交易日内有涨停，用于捕捉强势股的摸板首板机会。',
  timeRange: 90,
  conditions: [
    { type: 'touch_limit_not_close', date1: 0 }, // T日最高价是涨停价，T日未涨停
    { type: 'pct_change_lt', date1: -1, value: 9.8 }, // T-1日涨幅<9.8%
    { type: 'three_limit_up', date1: -1, days: 30 }, // 近30个交易日内三连板
    { type: 'recent_limit_up', date1: -1, days: 10 }, // 近10个交易日内有涨停
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 月内三连板 + 首板涨停策略
 * 特征：
 * 1. T日前一个月内（近30个交易日）出现过三连板
 * 2. T日为首板涨停（T日涨停，且T-1日非涨停）
 */
export const monthThreeLimitUpWithFirstBoardStrategy: StrategyTemplate = {
  id: 'month_three_limit_up_first_board',
  name: '月内三连板+首板涨停',
  description:
    'T日前一个月内出现过三连板，且T日为首板涨停（T日涨停且T-1日非涨停）。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: 0 }, // T日涨停（首板）
    { type: 'pct_change_lt', date1: -1, value: 9.8 }, // T-1日非涨停（涨幅<9.8%）
    { type: 'three_limit_up', date1: -1, days: 30 }, // 从T-1日往前30个交易日内出现三连板
    { type: 'recent_limit_up', date1: -1, days: 10 }, // 从T-1日往前10个交易日内至少有1天涨停
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

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
  description:
    '捕捉涨停后均线上穿的股票，结合技术面确认。要求T-1日涨停，T日5日均线上穿10日均线。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -1 }, // T-1日涨停
    { type: 'ma_cross_up', date1: 0, shortPeriod: 5, longPeriod: 10 }, // T日5日均线上穿10日均线
    { type: 'pct_change_gt', date1: 0, value: 0 }, // T日涨幅>0
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.1 }, // T日成交量/T-1日成交量>1.1
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 二次筑底突破策略（双底/W底）
 * 形态：涨一波→回调低点1→涨一小波→再回调低点2（二次筑底）→放量上涨那天=买点
 */
export const bottomingBreakoutStrategy: StrategyTemplate = {
  id: 'bottoming_breakout',
  name: '筑底突破',
  description:
    '捕捉双底形态突破买点。形态：前面涨一波→回调形成低点→涨一小波→再回调形成二次筑底→放量上涨那天符合买点。',
  timeRange: 90,
  conditions: [{ type: 'bottoming_breakout' }],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 超跌反弹 + 量能确认策略（T+1）
 * 特征：
 * 1. 近5日累计跌幅 <= -12%
 * 2. T日收盘显著低于20日均线（默认乖离 >= 6%）
 * 3. 出现止跌迹象：长下影或放量（成交量 > 5日均量 * 1.3）
 * 4. RSI(6) < 25（超卖）
 * 5. 上市满120天，近20日日均成交额 >= 2亿
 */
export const oversoldReboundVolumeConfirmStrategy: StrategyTemplate = {
  id: 'oversold_rebound_volume_confirm',
  name: '超跌反弹+量能确认',
  description:
    '急跌后的技术性反抽策略：近5日大跌、收盘偏离20日线、出现长下影或放量止跌且RSI超卖，偏向T+1快进快出。',
  timeRange: 90,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 200000000 },
    { type: 'recent_n_day_pct_change_lt', date1: 0, days: 5, value: -12 },
    { type: 'close_below_ma_deviation', date1: 0, period: 20, deviation: 0.06 },
    { type: 'stop_fall_signal', date1: 0, lowerShadowRatio: 0.4, volumeDays: 5, volumeRatio: 1.3 },
    { type: 'rsi_lt', date1: 0, period: 6, value: 25 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 打板二版策略
 * 特征：
 * 1. T-1日首板涨停（T-2日非涨停）
 * 2. T日最高价触及涨停价
 * 用于观察次日涨跌幅
 */
export const secondBoardStrategy: StrategyTemplate = {
  id: 'second_board',
  name: '打板二版',
  description:
    '上个交易日首板涨停，今天最高价触及涨停价。T-1日首板涨停（T-2日非涨停），T日最高价触及涨停价，观察次日涨跌幅。',
  timeRange: 90,
  conditions: [
    { type: 'limit_up', date1: -1 }, // T-1日涨停（首板）
    { type: 'pct_change_lt', date1: -2, value: 9.8 }, // T-2日非涨停
    { type: 'high_is_limit_up', date1: 0 }, // T日最高价触及涨停价
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 主力建仓策略
 * 特征：
 * 1. T-1 日非涨停（涨幅<9.8%）
 * 2. T-10 至 T（含）至少 5 日收涨（收盘>前一交易日收盘，即涨跌幅>0），且收盘在 5 日线上方
 * 3. 计入收涨当日满足 5 日线 > 10 日线 > 20 日线
 * 4. 计入收涨日中至少一半满足 5/10 日线斜率均向上
 * 5. T 日涨停仅作为后端打标，供前端筛选
 */
export const mainForceBuildPositionStrategy: StrategyTemplate = {
  id: 'main_force_build_position',
  name: '主力建仓',
  description:
    'T-1日涨幅<9.8%；T-10至T日（含）至少5日收涨（收盘>昨收、涨跌幅>0）且收盘站上5日线，且计入当日5日线在10日线上方、10日线在20日线上方，且命中收涨日里至少一半满足5日线和10日线均向上（斜率>0）。T日涨停仅作打标供前端筛选。',
  timeRange: 120,
  conditions: [{ type: 'main_force_build_position', date1: 0, windowDays: 10 }],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 连阳上影策略
 * 特征：
 * 1. T 日向前连续阳线天数 >= N（默认 3）
 * 2. 连阳这 N 天每一天都满足 5 日线 > 10 日线
 * 2. 连阳区间内至少 1 天上影线幅度 > 阈值（默认 2%）
 * 上影线幅度 = 当日（最高涨幅 - 收盘涨幅）
 */
export const consecutiveUpUpperShadowStrategy: StrategyTemplate = {
  id: 'consecutive_up_upper_shadow',
  name: '连阳上影',
  description:
    '筛选连续连阳后的上影线个股：默认要求T日向前至少连阳3天，且连阳3天都满足5日线>10日线，并且这3天里至少1天上影线幅度大于2%。',
  timeRange: 120,
  conditions: [
    { type: 'consecutive_up_days_gte', date1: 0, consecutiveDays: 3, requireMa5GtMa10: true },
    { type: 'upper_shadow_pct_gt', date1: 0, days: 3, value: 2 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 游资合力接力策略
 * 特征：
 * 1. 近10日内有涨停（体现活跃度与辨识度）
 * 2. T-1日分歧回踩（小阴/小阳，避免连续加速末端）
 * 3. T日放量涨停（分歧转一致）
 * 4. 20日日均成交额 >= 5亿（保证流动性，便于大资金参与）
 */
export const yzrConsensusRelayStrategy: StrategyTemplate = {
  id: 'yzr_consensus_relay',
  name: '游资合力接力',
  description:
    '游资风格的分歧转一致接力：近10日有涨停识别度，T-1日小幅回踩，T日放量涨停并满足流动性门槛。',
  timeRange: 120,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 500000000 },
    { type: 'recent_limit_up', date1: -1, days: 10 },
    { type: 'pct_change_between', date1: -1, minValue: -5, maxValue: 3 },
    { type: 'limit_up', date1: 0 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.3 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 游资三件套一：首板试错策略
 * 特征：
 * 1. T日首板涨停（T-1日非涨停）
 * 2. 近20日日均成交额 >= 3亿（保证流动性）
 * 3. T日相对T-1日放量（资金主动性）
 */
export const yzrFirstBoardProbeStrategy: StrategyTemplate = {
  id: 'yzr_first_board_probe',
  name: '游资首板试错',
  description:
    '游资试错型策略：T日首板涨停，前一日非涨停，且满足中高流动性与当日放量，用于捕捉题材启动首板。',
  timeRange: 120,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 300000000 },
    { type: 'pct_change_lt', date1: -1, value: 9.8 },
    { type: 'limit_up', date1: 0 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.2 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 游资三件套二：分歧转一致策略
 * 特征：
 * 1. 近10日有涨停，代表票有辨识度
 * 2. T-1日小幅分歧回踩
 * 3. T日放量涨停转一致
 */
export const yzrDisagreementToConsensusStrategy: StrategyTemplate = {
  id: 'yzr_disagreement_to_consensus',
  name: '游资分歧转一致',
  description:
    '游资主流接力型策略：近10日有涨停，T-1日分歧回踩，T日放量涨停确认一致性。',
  timeRange: 120,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 500000000 },
    { type: 'recent_limit_up', date1: -1, days: 10 },
    { type: 'pct_change_between', date1: -1, minValue: -5, maxValue: 3 },
    { type: 'limit_up', date1: 0 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.3 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 游资三件套三：二波加速策略
 * 特征：
 * 1. 近30日内出现过三连板（历史龙头辨识度）
 * 2. T-1日非涨停（避免连板末端）
 * 3. T日再次涨停且放量，捕捉二波加速
 */
export const yzrSecondWaveAccelerationStrategy: StrategyTemplate = {
  id: 'yzr_second_wave_acceleration',
  name: '游资二波加速',
  description:
    '游资二波策略：近30日出现过三连板，T-1日非涨停，T日再次涨停并放量，尝试捕捉龙头二波启动。',
  timeRange: 120,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 600000000 },
    { type: 'three_limit_up', date1: -1, days: 30 },
    { type: 'pct_change_lt', date1: -1, value: 9.8 },
    { type: 'limit_up', date1: 0 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.2 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 市场状态开关：弱势策略包
 * 思路：偏防守，只做分歧转一致里的高流动性、深回踩、强放量。
 */
export const yzrPackWeakMarketStrategy: StrategyTemplate = {
  id: 'yzr_pack_weak_market',
  name: '游资策略包-弱势',
  description:
    '弱势防守版：提高流动性门槛，要求更充分分歧与更强放量后再做转一致，减少噪音交易。',
  timeRange: 120,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 800000000 },
    { type: 'recent_limit_up', date1: -1, days: 10 },
    { type: 'pct_change_between', date1: -1, minValue: -6, maxValue: 2 },
    { type: 'limit_up', date1: 0 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.5 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 市场状态开关：中性策略包
 * 思路：均衡配置，使用分歧转一致的标准参数。
 */
export const yzrPackNeutralMarketStrategy: StrategyTemplate = {
  id: 'yzr_pack_neutral_market',
  name: '游资策略包-中性',
  description:
    '中性均衡版：使用标准分歧转一致参数，在胜率与样本数量之间平衡。',
  timeRange: 120,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 500000000 },
    { type: 'recent_limit_up', date1: -1, days: 10 },
    { type: 'pct_change_between', date1: -1, minValue: -5, maxValue: 3 },
    { type: 'limit_up', date1: 0 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.3 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

/**
 * 市场状态开关：强势策略包
 * 思路：进攻型，聚焦二波加速（历史龙头二次启动）。
 */
export const yzrPackStrongMarketStrategy: StrategyTemplate = {
  id: 'yzr_pack_strong_market',
  name: '游资策略包-强势',
  description:
    '强势进攻版：聚焦历史龙头二波加速，适合主升阶段追求弹性。',
  timeRange: 120,
  conditions: [
    { type: 'listed_days_gte', date1: 0, days: 120 },
    { type: 'avg_amount_gte', date1: 0, days: 20, value: 600000000 },
    { type: 'three_limit_up', date1: -1, days: 30 },
    { type: 'pct_change_lt', date1: -1, value: 9.8 },
    { type: 'limit_up', date1: 0 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1.2 },
  ],
  exclude: {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true,
  },
};

// 所有策略模板（所有战法置顶）
export const allStrategyTemplates: StrategyTemplate[] = [
  // 所有战法策略（置顶）
  dragonHeadStrategy, // 龙头战法
  breakAndRecoverStrategy, // 断板反包
  maCrossUpStrategy, // 均线上穿
  emotionCycleStrategy, // 情绪周期
  threeLimitUpStrategy, // 三连板
  monthThreeLimitUpWithFirstBoardStrategy, // 月内三连板+首板涨停
  oversoldReboundVolumeConfirmStrategy, // 超跌反弹+量能确认
  yzrFirstBoardProbeStrategy, // 游资首板试错
  yzrDisagreementToConsensusStrategy, // 游资分歧转一致
  yzrSecondWaveAccelerationStrategy, // 游资二波加速
  yzrPackWeakMarketStrategy, // 游资策略包-弱势
  yzrPackNeutralMarketStrategy, // 游资策略包-中性
  yzrPackStrongMarketStrategy, // 游资策略包-强势
  yzrConsensusRelayStrategy, // 游资合力接力
  mainForceBuildPositionStrategy, // 主力建仓
  consecutiveUpUpperShadowStrategy, // 连阳上影
  bottomingBreakoutStrategy, // 筑底突破
  touchLimitNotCloseWithThreeLimitStrategy, // 摸板首板
  secondBoardStrategy, // 打板二版
];
