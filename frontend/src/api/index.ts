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
