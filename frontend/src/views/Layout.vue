<template>
  <div class="min-h-screen bg-gray-50 text-gray-900 transition-colors duration-200 dark:bg-neutral-950 dark:text-neutral-100">
    <div class="container mx-auto px-3 py-2">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between sm:gap-3">
        <Tabs
          v-model:activeKey="activeKey"
          type="line"
          size="small"
          class="min-w-0 flex-1"
          @change="onTabChange"
        >
          <TabPane key="strategy-backtest" tab="策略回测" />
          <TabPane key="common-strategies" tab="常用策略" />
          <TabPane key="history-results" tab="历史回测数据" />
          <TabPane key="emotion-cycle" tab="情绪周期" />
          <TabPane key="multi-kline" tab="多股复盘K" />
          <TabPane key="sector-ranking" tab="板块排行" />
        </Tabs>
        <div class="flex shrink-0 items-center gap-2">
          <span class="hidden text-xs text-gray-500 dark:text-neutral-400 sm:inline">外观</span>
          <Segmented
            size="small"
            :value="themeStore.scheme"
            :options="[
              { label: '浅色', value: 'light' },
              { label: '深色', value: 'dark' }
            ]"
            @change="onSchemeChange"
          />
        </div>
      </div>

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
import { Tabs, TabPane, Segmented } from 'ant-design-vue'
import { useThemeStore } from '@/store/modules/theme'
import type { ColorScheme } from '@/store/modules/theme'

const themeStore = useThemeStore()
const router = useRouter()
const route = useRoute()

const routeToKey: Record<string, string> = {
  StrategyBacktest: 'strategy-backtest',
  CommonStrategies: 'common-strategies',
  HistoryResults: 'history-results',
  EmotionCycle: 'emotion-cycle',
  MultiKline: 'multi-kline',
  SectorRanking: 'sector-ranking'
}

const keyToPath: Record<string, string> = {
  'strategy-backtest': '/strategy-backtest',
  'common-strategies': '/common-strategies',
  'history-results': '/history-results',
  'emotion-cycle': '/emotion-cycle',
  'multi-kline': '/multi-kline',
  'sector-ranking': '/sector-ranking'
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

function onSchemeChange(val: string | number) {
  if (val === 'light' || val === 'dark') themeStore.setScheme(val as ColorScheme)
}
</script>
