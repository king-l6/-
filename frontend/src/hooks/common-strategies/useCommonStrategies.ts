import { ref } from 'vue'
import { message } from 'ant-design-vue'
import { useStrategyStore } from '@/store/modules/strategy'
import { useBacktest } from '@/hooks/strategy-backtest/useBacktest'
import { allStrategyTemplates, type StrategyTemplate } from '@/config/strategyTemplates'
import type { StrategyCondition } from '@/types'

export function useCommonStrategies() {
  const strategyStore = useStrategyStore()
  const { executeBacktest } = useBacktest()
  const strategies = ref<StrategyTemplate[]>(allStrategyTemplates)
  const loadingStrategyId = ref<string | null>(null)

  function formatCondition(condition: StrategyCondition): string {
    const dateOffset = condition.date1 ?? 0
    const dateStr = dateOffset === 0 ? 'T日' : dateOffset < 0 ? `T${dateOffset}日` : `T+${dateOffset}日`

    switch (condition.type) {
      case 'limit_up':
        return `${dateStr}涨停`
      case 'pct_change_gt':
        return `${dateStr}涨幅>${condition.value ?? 0}%`
      case 'pct_change_lt':
        return `${dateStr}涨幅<${condition.value ?? 0}%`
      case 'pct_change_between':
        return `${dateStr}涨幅在${condition.minValue ?? 0}%-${condition.maxValue ?? 0}%之间`
      case 'volume_ratio': {
        const date2Offset = condition.date2 ?? 0
        const date2Str = date2Offset === 0 ? 'T日' : date2Offset < 0 ? `T${date2Offset}日` : `T+${date2Offset}日`
        return `${dateStr}成交量/${date2Str}成交量>${condition.ratio ?? 1}`
      }
      case 'three_limit_up':
        return `近${condition.days ?? 30}个交易日内三连板`
      case 'recent_limit_up':
        return `近${condition.days ?? 10}个交易日内有涨停`
      case 'ma_cross_up':
        return `${dateStr}${condition.shortPeriod ?? 5}日均线上穿${condition.longPeriod ?? 10}日均线`
      case 'bottoming_breakout':
        return '涨一波→回调低点1→涨一小波→二次筑底→放量上涨=买点'
      default:
        return `${dateStr}未知条件`
    }
  }

  async function handleLoadStrategy(
    strategy: StrategyTemplate,
    onLoaded?: () => void
  ) {
    if (loadingStrategyId.value) return

    loadingStrategyId.value = strategy.id

    try {
      strategyStore.setStrategyName(strategy.name)
      strategyStore.setTimeRange(strategy.timeRange)
      if (strategy.exclude) {
        strategyStore.setExclude({
          kcb: strategy.exclude.kcb ?? true,
          cyb: strategy.exclude.cyb ?? true,
          bjs: strategy.exclude.bjs ?? true,
          st: strategy.exclude.st ?? true,
          delist: strategy.exclude.delist ?? true
        })
      }
      strategyStore.setConditions([...strategy.conditions])

      message.info(`正在执行回测：${strategy.name}（近${strategy.timeRange}个交易日）...`)
      await executeBacktest()
      onLoaded?.()
    } catch (error) {
      console.error('回测失败:', error)
    } finally {
      loadingStrategyId.value = null
    }
  }

  return {
    strategies,
    loadingStrategyId,
    formatCondition,
    handleLoadStrategy
  }
}
