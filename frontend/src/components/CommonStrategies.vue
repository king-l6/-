<template>
  <Card title="常用策略" class="h-full">
    <div class="space-y-2">
      <Alert
        message="点击策略卡片或「开始回测」按钮，将自动执行回测并显示结果（近90个交易日）"
        type="info"
        show-icon
        closable
        class="mb-2"
        size="small"
      />
      
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
        <Card
          v-for="strategy in strategies"
          :key="strategy.id"
          :hoverable="true"
          class="cursor-pointer transition-all hover:shadow-lg"
          @click="handleLoadStrategy(strategy)"
        >
          <template #title>
            <div class="flex items-center justify-between">
              <span class="text-sm font-semibold">{{ strategy.name }}</span>
              <Button
                type="primary"
                size="small"
                :loading="loadingStrategyId === strategy.id"
                @click.stop="handleLoadStrategy(strategy)"
              >
                {{ loadingStrategyId === strategy.id ? '回测中...' : '开始回测' }}
              </Button>
            </div>
          </template>
          
          <div class="space-y-1">
            <p class="text-xs text-gray-600 mb-1">{{ strategy.description }}</p>
            
            <div class="border-t pt-1">
              <div class="text-xs text-gray-500 mb-1">策略条件：</div>
              <div class="space-y-0.5">
                <div
                  v-for="(condition, index) in strategy.conditions"
                  :key="index"
                  class="text-xs text-gray-700 bg-gray-50 px-1.5 py-0.5 rounded"
                >
                  {{ formatCondition(condition) }}
                </div>
              </div>
            </div>
            
            <div class="flex items-center justify-between text-xs text-gray-500 mt-1 pt-1 border-t">
              <span>回测范围：{{ strategy.timeRange }}个交易日</span>
            </div>
          </div>
        </Card>
      </div>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { Card, Button, Alert, message } from 'ant-design-vue'
import { useStrategyStore } from '@/store/modules/strategy'
import { useBacktest } from '@/hooks/useBacktest'
import { allStrategyTemplates, type StrategyTemplate } from '@/config/strategyTemplates'
import type { StrategyCondition } from '@/types'

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
    case 'volume_ratio':
      const date2Offset = condition.date2 ?? 0
      const date2Str = date2Offset === 0 ? 'T日' : date2Offset < 0 ? `T${date2Offset}日` : `T+${date2Offset}日`
      return `${dateStr}成交量/${date2Str}成交量>${condition.ratio ?? 1}`
    case 'three_limit_up':
      return `近${condition.days ?? 30}个交易日内三连板`
    case 'ma_cross_up':
      return `${dateStr}${condition.shortPeriod ?? 5}日均线上穿${condition.longPeriod ?? 10}日均线`
    case 'bottoming_breakout':
      return '涨一波→回调低点1→涨一小波→二次筑底→放量上涨=买点'
    default:
      return `${dateStr}未知条件`
  }
}

async function handleLoadStrategy(strategy: StrategyTemplate) {
  // 如果正在加载，则忽略
  if (loadingStrategyId.value) {
    return
  }
  
  loadingStrategyId.value = strategy.id
  
  try {
    // 更新策略名称
    strategyStore.setStrategyName(strategy.name)
    
    // 更新回测时间范围
    strategyStore.setTimeRange(strategy.timeRange)
    
    // 更新排除规则
    if (strategy.exclude) {
      strategyStore.setExclude({
        kcb: strategy.exclude.kcb ?? true,
        cyb: strategy.exclude.cyb ?? true,
        bjs: strategy.exclude.bjs ?? true,
        st: strategy.exclude.st ?? true,
        delist: strategy.exclude.delist ?? true
      })
      }
    
    // 更新策略条件
    strategyStore.setConditions([...strategy.conditions])
    
    message.info(`正在执行回测：${strategy.name}（近${strategy.timeRange}个交易日）...`)
    
    // 自动执行回测
    await executeBacktest()
    
    // 切换到策略回测tab（通过事件通知父组件）
    emit('strategy-loaded')
  } catch (error) {
    console.error('回测失败:', error)
  } finally {
    loadingStrategyId.value = null
  }
}

const emit = defineEmits<{
  (e: 'strategy-loaded'): void
}>()
</script>

<style scoped>
:deep(.ant-card) {
  transition: all 0.3s ease;
}

:deep(.ant-card:hover) {
  transform: translateY(-2px);
}
</style>
