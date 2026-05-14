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
          @click="handleLoadStrategy(strategy, onStrategyLoaded)"
        >
          <template #title>
            <div class="flex items-center justify-between">
              <span class="text-sm font-semibold">{{ strategy.name }}</span>
              <Button
                type="primary"
                size="small"
                :loading="loadingStrategyId === strategy.id"
                @click.stop="handleLoadStrategy(strategy, onStrategyLoaded)"
              >
                {{ loadingStrategyId === strategy.id ? '回测中...' : '开始回测' }}
              </Button>
            </div>
          </template>
          
          <div class="space-y-1">
            <p class="mb-1 text-xs text-gray-600 dark:text-neutral-300">{{ strategy.description }}</p>
            
            <div class="border-t border-gray-100 pt-1 dark:border-neutral-700">
              <div class="mb-1 text-xs text-gray-500 dark:text-neutral-400">策略条件：</div>
              <div class="space-y-0.5">
                <div
                  v-for="(condition, index) in strategy.conditions"
                  :key="index"
                  class="rounded bg-gray-50 px-1.5 py-0.5 text-xs text-gray-700 dark:bg-neutral-900/90 dark:text-neutral-200"
                >
                  {{ formatCondition(condition) }}
                </div>
              </div>
            </div>
            
            <div class="mt-1 flex items-center justify-between border-t border-gray-100 pt-1 text-xs text-gray-500 dark:border-neutral-700 dark:text-neutral-400">
              <span>回测范围：{{ strategy.timeRange }}个交易日</span>
            </div>
          </div>
        </Card>
      </div>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { Card, Button, Alert } from 'ant-design-vue'
import { useCommonStrategies } from '@/hooks/common-strategies/useCommonStrategies'

const emit = defineEmits<{
  (e: 'strategy-loaded'): void
}>()

const { strategies, loadingStrategyId, formatCondition, handleLoadStrategy } = useCommonStrategies()

function onStrategyLoaded() {
  emit('strategy-loaded')
}
</script>

<style scoped>
:deep(.ant-card) {
  transition: all 0.3s ease;
}

:deep(.ant-card:hover) {
  transform: translateY(-2px);
}
</style>
