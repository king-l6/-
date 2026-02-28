import axios from 'axios'
import type { BacktestRequest, BacktestResponse, StocksResponse, ResultsListResponse, ResultFileResponse } from '@/types'

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
