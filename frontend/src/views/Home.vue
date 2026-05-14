<template>
  <div class="min-h-screen bg-gray-50 text-gray-900 transition-colors dark:bg-neutral-950 dark:text-neutral-100">
    <div class="container mx-auto px-3 py-2">
      <Tabs v-model:activeKey="activeTab" type="line" size="small">
        <TabPane key="backtest" tab="策略回测">
          <div class="grid grid-cols-1 lg:grid-cols-2 gap-2 mt-2">
            <div class="lg:col-span-1">
              <StrategyForm />
            </div>
            <div class="lg:col-span-1">
              <ResultsTable />
            </div>
          </div>
        </TabPane>
        <TabPane key="common" tab="常用策略">
          <div class="mt-2">
            <CommonStrategies @strategy-loaded="handleStrategyLoaded" />
          </div>
        </TabPane>
        <TabPane key="history" tab="历史回测数据">
          <div class="mt-2">
            <HistoryResults />
          </div>
        </TabPane>
      </Tabs>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { Tabs, TabPane } from 'ant-design-vue'
import StrategyForm from '@/components/StrategyForm.vue'
import ResultsTable from '@/components/ResultsTable.vue'
import HistoryResults from '@/components/HistoryResults.vue'
import CommonStrategies from '@/components/CommonStrategies.vue'

const activeTab = ref('backtest')

function handleStrategyLoaded() {
  // 当策略加载后，切换到策略回测tab
  activeTab.value = 'backtest'
}
</script>
