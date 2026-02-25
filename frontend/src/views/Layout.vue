<template>
  <div class="min-h-screen bg-gray-50">
    <div class="container mx-auto px-3 py-2">
      <header class="mb-2">
        <h1 class="text-xl font-semibold text-gray-900 mb-0.5">股票策略回测系统</h1>
        <p class="text-xs text-gray-600">自定义策略条件，回测A股市场</p>
      </header>

      <Tabs v-model:activeKey="activeKey" type="line" size="small" @change="onTabChange">
        <TabPane key="strategy-backtest" tab="策略回测" />
        <TabPane key="common-strategies" tab="常用策略" />
        <TabPane key="history-results" tab="历史回测数据" />
      </Tabs>

      <div class="mt-2">
        <router-view />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { Tabs, TabPane } from 'ant-design-vue'

const router = useRouter()
const route = useRoute()

const routeToKey: Record<string, string> = {
  StrategyBacktest: 'strategy-backtest',
  CommonStrategies: 'common-strategies',
  HistoryResults: 'history-results'
}

const keyToPath: Record<string, string> = {
  'strategy-backtest': '/strategy-backtest',
  'common-strategies': '/common-strategies',
  'history-results': '/history-results'
}

const activeKey = computed({
  get: () => routeToKey[route.name as string] || 'strategy-backtest',
  set: (key: string) => {
    const path = keyToPath[key]
    if (path && route.path !== path) router.push(path)
  }
})

function onTabChange(key: string) {
  const path = keyToPath[key]
  if (path) router.push(path)
}
</script>
