import axios from 'axios'
import type {
  BacktestRequest,
  BacktestResponse,
  StocksResponse,
  StockDailyResponse,
  ResultsListResponse,
  ResultFileResponse,
  EmotionCycleResponse,
  EmotionCycleHealthResponse,
  CacheUpdateTaskResponse
} from '@/types'

const api = axios.create({
  baseURL: '/api',
  timeout: 300000, // 5分钟超时（回测可能需要较长时间）
  headers: {
    'Content-Type': 'application/json'
  }
})

// 请求拦截器
api.interceptors.request.use(
  (config) => {
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// 响应拦截器
api.interceptors.response.use(
  (response) => {
    return response.data
  },
  (error) => {
    const message = error.response?.data?.error || error.message || '请求失败'
    return Promise.reject(new Error(message))
  }
)

/**
 * 策略回测
 */
export function backtest(data: BacktestRequest): Promise<BacktestResponse> {
  return api.post('/backtest', data)
}

/**
 * 获取股票列表
 */
export function getStocks(): Promise<StocksResponse> {
  return api.get('/stocks')
}

/** 单只股票日 K（本地缓存），用于多股复盘图 */
export function getStockDaily(params: {
  code: string
  start?: string
  end?: string
}): Promise<StockDailyResponse> {
  return api.get('/stock-daily', {
    params: {
      code: params.code,
      start: params.start,
      end: params.end
    }
  })
}

/**
 * 获取 results 文件列表
 */
export function getResultsList(): Promise<ResultsListResponse> {
  return api.get('/results/list')
}

/**
 * 获取指定 results 文件内容
 */
export function getResultsFile(filename: string): Promise<ResultFileResponse> {
  return api.get('/results/file', { params: { filename } })
}

/**
 * 按策略名聚合：合并该策略的主文件与所有按日文件，按 match_date 排序（用于历史结果按日期聚合展示）
 */
export function getResultsByStrategy(strategyName: string): Promise<ResultFileResponse> {
  return api.get('/results/strategy', { params: { name: strategyName } })
}

/** 获取指定日期范围内的所有交易日（来自本地缓存），用于图表按日补全（无数据日显示 0） */
export function getTradingDays(start: string, end: string): Promise<{ success: boolean; data: string[] }> {
  return api.get('/trading-days', { params: { start, end } })
}

/** 获取情绪周期分析结果 */
export function getEmotionCycle(params?: {
  days?: number
  stock_code?: string
  /** 为 true 时传 force=1，忽略滚动缓存并全量重扫股票缓存 */
  force_refresh?: boolean
}): Promise<EmotionCycleResponse> {
  if (!params) return api.get('/emotion-cycle')
  const { force_refresh, ...rest } = params
  const query: Record<string, unknown> = { ...rest }
  if (force_refresh) query.force = 1
  return api.get('/emotion-cycle', { params: query })
}

/** 情绪周期数据自检 */
export function getEmotionCycleHealth(): Promise<EmotionCycleHealthResponse> {
  return api.get('/emotion-cycle/health')
}

/** 启动缓存补齐后台任务 */
export function startCacheUpdateTask(): Promise<CacheUpdateTaskResponse> {
  return api.post('/cache-update/start')
}

/** 查询缓存补齐后台任务状态 */
export function getCacheUpdateTaskStatus(): Promise<CacheUpdateTaskResponse> {
  return api.get('/cache-update/status')
}
