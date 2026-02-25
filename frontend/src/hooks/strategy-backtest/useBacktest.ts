import { useStrategyStore } from '@/store/modules/strategy'
import { backtest } from '@/api'
import { message } from 'ant-design-vue'

export function useBacktest() {
  const strategyStore = useStrategyStore()

  async function executeBacktest() {
    if (strategyStore.conditions.length === 0) {
      message.warning('请至少添加一个策略条件')
      return
    }

    strategyStore.setLoading(true)
    strategyStore.setError(null)

    try {
      const strategy = strategyStore.getStrategy()
      const response = await backtest({
        strategy,
        strategy_name: strategy.name
      })

      if (response.success) {
        strategyStore.setResults(response.data)
        message.success(`回测完成，找到 ${response.count} 只符合条件的股票${response._cached ? '（缓存）' : ''}`)
      } else {
        strategyStore.setError(response.error || '回测失败')
        message.error(response.error || '回测失败')
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : '回测请求失败'
      strategyStore.setError(errorMessage)
      message.error(errorMessage)
    } finally {
      strategyStore.setLoading(false)
    }
  }

  return {
    executeBacktest
  }
}
