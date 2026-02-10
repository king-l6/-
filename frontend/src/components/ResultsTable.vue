<template>
  <Card title="回测结果" class="h-full">
    <template v-if="loading" #extra>
      <Spin />
    </template>
    
    <div v-if="error" class="py-4">
      <Alert
        :message="error"
        type="error"
        show-icon
        closable
        @close="handleCloseError"
      />
    </div>
    
    <div v-else-if="!hasResults" class="py-12 text-center text-gray-500">
      暂无回测结果
    </div>
    
    <div v-else>
      <div v-if="hasResults" class="mb-4 text-lg font-semibold text-primary">
        找到 {{ resultsCount }} 只符合条件的股票
      </div>
      
      <Table
        :columns="columns"
        :data-source="results"
        :loading="loading"
        :pagination="{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `共 ${total} 条` }"
        row-key="code"
        :scroll="{ x: 'max-content' }"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'pctChange'">
            <span :class="getPctChangeClass(record)">
              {{ getPctChange(record) }}
            </span>
          </template>
        </template>
      </Table>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Card, Table, Spin, Alert } from 'ant-design-vue'
import { useStrategyStore } from '@/store/modules/strategy'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult } from '@/types'

const strategyStore = useStrategyStore()

const loading = computed(() => strategyStore.loading)
const error = computed(() => strategyStore.error)
const results = computed(() => strategyStore.results)
const hasResults = computed(() => strategyStore.hasResults)
const resultsCount = computed(() => strategyStore.resultsCount)

const columns: ColumnsType<StockResult> = [
  {
    title: '代码',
    dataIndex: 'code',
    key: 'code',
    width: 100,
    fixed: 'left'
  },
  {
    title: '名称',
    dataIndex: 'name',
    key: 'name',
    width: 120
  },
  {
    title: '匹配日期',
    dataIndex: 'match_date',
    key: 'match_date',
    width: 120
  },
  {
    title: '匹配价格',
    dataIndex: 'match_price',
    key: 'match_price',
    width: 120,
    customRender: ({ text }) => text ? text.toFixed(2) : '-'
  },
  {
    title: '当前价格',
    dataIndex: 'current_price',
    key: 'current_price',
    width: 120,
    customRender: ({ text }) => text ? text.toFixed(2) : '-'
  },
  {
    title: '涨跌幅',
    key: 'pctChange',
    width: 120,
    fixed: 'right'
  }
]

function getPctChange(stock: StockResult): string {
  if (!stock.current_price || !stock.match_price) {
    return '-'
  }
  const pct = ((stock.current_price - stock.match_price) / stock.match_price * 100).toFixed(2)
  return `${pct}%`
}

function getPctChangeClass(stock: StockResult): string {
  if (!stock.current_price || !stock.match_price) {
    return ''
  }
  const pct = (stock.current_price - stock.match_price) / stock.match_price
  return pct >= 0 ? 'text-green-600 font-semibold' : 'text-red-600 font-semibold'
}

function handleCloseError() {
  strategyStore.setError(null)
}
</script>
