<template>
  <div class="min-h-screen bg-gray-50">
    <div class="container mx-auto px-3 py-2">
      <Tabs v-model:activeKey="activeKey" type="line" size="small" @change="onTabChange">
        <TabPane key="strategy-backtest" tab="策略回测" />
        <TabPane key="common-strategies" tab="常用策略" />
        <TabPane key="history-results" tab="历史回测数据" />
        <TabPane key="emotion-cycle" tab="情绪周期" />
        <TabPane key="multi-kline" tab="多股复盘K" />
      </Tabs>

      <div class="mt-2">
        <router-view v-slot="{ Component, route: r }">
          <keep-alive :include="['EmotionCycle', 'MultiKline']">
            <component :is="Component" :key="r.name" />
          </keep-alive>
        </router-view>
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
  HistoryResults: 'history-results',
  EmotionCycle: 'emotion-cycle',
  MultiKline: 'multi-kline'
}

const keyToPath: Record<string, string> = {
  'strategy-backtest': '/strategy-backtest',
  'common-strategies': '/common-strategies',
  'history-results': '/history-results',
  'emotion-cycle': '/emotion-cycle',
  'multi-kline': '/multi-kline'
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
