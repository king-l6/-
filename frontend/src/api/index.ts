import axios from 'axios'
import type { BacktestRequest, BacktestResponse, StocksResponse } from '@/types'

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
