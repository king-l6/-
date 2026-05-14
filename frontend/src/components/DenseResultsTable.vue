<template>
  <Card title="回测结果（密集表格）" class="h-full">
    <template #extra>
      <Space>
        <Button size="small" @click="handleExport">导出CSV</Button>
        <Button size="small" @click="handleClearFilters">清除筛选</Button>
      </Space>
    </template>
    
    <div v-if="error" class="py-2">
      <Alert
        :message="error"
        type="error"
        show-icon
        closable
        @close="handleCloseError"
        size="small"
      />
    </div>
    
    <div v-else-if="!hasResults" class="py-6 text-center text-gray-500 dark:text-neutral-400">
      暂无回测结果
    </div>
    
    <div v-else>
      <div class="mb-2 flex items-center justify-between">
        <div class="text-sm font-semibold text-primary">
          找到 {{ filteredResults.length }} 只符合条件的股票
        </div>
        <Input
          v-model:value="searchText"
          placeholder="搜索代码或名称"
          allow-clear
          size="small"
          style="width: 180px"
          @input="handleSearch"
        >
          <template #prefix>
            <SearchOutlined />
          </template>
        </Input>
      </div>
      <Table
        :columns="columns"
        :data-source="filteredResults"
        :loading="loading"
        :pagination="{
          pageSize: 200,
          showSizeChanger: true,
          pageSizeOptions: ['20', '50', '100', '200'],
          showTotal: (total) => `共 ${total} 条`,
          showQuickJumper: true
        }"
        :row-key="(record) => `${record.code}-${record.match_date || ''}`"
        :scroll="{ x: 'max-content', y: 600 }"
        size="small"
        bordered
        :row-class-name="getRowClassName"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'code_name'">
            <div class="flex items-center gap-1 min-w-0">
              <span class="font-mono text-xs font-semibold shrink-0">{{ record.code }}</span>
              <span class="text-xs truncate">{{ record.name }}</span>
            </div>
          </template>
          <template v-else-if="column.key === 'match_date'">
            <span class="text-xs">{{ formatDate(record.match_date) }}</span>
          </template>
          <template v-else-if="column.key === 'match_price'">
            <span class="font-mono text-xs">{{ formatPrice(record.match_price) }}</span>
          </template>
          <template v-else-if="column.key === 'current_price'">
            <span class="font-mono text-xs">{{ formatPrice(record.current_price) }}</span>
          </template>
        </template>
      </Table>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { Card, Table, Alert, Button, Space, Input } from 'ant-design-vue'
import { SearchOutlined } from '@ant-design/icons-vue'
import { useStrategyStore } from '@/store/modules/strategy'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult } from '@/types'
import {
  STOCK_CODE_NAME_COLUMN_KEY,
  STOCK_CODE_NAME_COLUMN_TITLE
} from '@/constants/stockTable'

const strategyStore = useStrategyStore()

const loading = computed(() => strategyStore.loading)
const error = computed(() => strategyStore.error)
const results = computed(() => strategyStore.results)
const hasResults = computed(() => strategyStore.hasResults)

const searchText = ref('')

// 筛选后的结果（确保返回新数组，避免被 Table 组件修改）
const filteredResults = computed(() => {
  let sourceData = results.value
  // 如果有搜索条件，先筛选
  if (searchText.value) {
    const search = searchText.value.toLowerCase()
    sourceData = sourceData.filter(item => 
      item.code.toLowerCase().includes(search) || 
      item.name.toLowerCase().includes(search)
    )
  }
  // 返回数组的副本，避免 Table 组件修改原始数据
  return [...sourceData]
})

const columns: ColumnsType<StockResult> = [
  {
    title: STOCK_CODE_NAME_COLUMN_TITLE,
    key: STOCK_CODE_NAME_COLUMN_KEY,
    width: 160,
    fixed: 'left',
    ellipsis: true,
    sorter: (a, b) =>
      String(a.code || '').localeCompare(String(b.code || '')) ||
      String(a.name || '').localeCompare(String(b.name || ''))
  },
  {
    title: '匹配日期',
    dataIndex: 'match_date',
    key: 'match_date',
    width: 100,
    sortDirections: ['ascend', 'descend'],
    sorter: (a, b) => {
      const dateA = a.match_date || ''
      const dateB = b.match_date || ''
      if (!dateA && !dateB) return 0
      if (!dateA) return 1
      if (!dateB) return -1
      return dateA.localeCompare(dateB)
    },
    customRender: ({ text }) => formatDate(text ?? '')
  },
  {
    title: '匹配价',
    dataIndex: 'match_price',
    key: 'match_price',
    width: 80,
    align: 'right',
    sorter: (a, b) => (a.match_price || 0) - (b.match_price || 0)
  },
  {
    title: '当前价',
    dataIndex: 'current_price',
    key: 'current_price',
    width: 80,
    align: 'right',
    sorter: (a, b) => (a.current_price || 0) - (b.current_price || 0)
  },
  {
    title: '次日涨跌',
    dataIndex: 'day2_change_pct',
    key: 'day2_change_pct',
    width: 85,
    fixed: 'right',
    align: 'right',
    sorter: (a, b) => (a.day2_change_pct || 0) - (b.day2_change_pct || 0),
    customRender: ({ text }) => {
      if (text == null) return '-'
      const val = text.toFixed(2)
      return {
        children: `${val >= 0 ? '+' : ''}${val}%`,
        props: { style: { color: text >= 0 ? '#dc2626' : '#16a34a', fontWeight: 600 } }
      }
    }
  },
  {
    title: '第三日涨跌',
    dataIndex: 'day3_change_pct',
    key: 'day3_change_pct',
    width: 85,
    fixed: 'right',
    align: 'right',
    sorter: (a, b) => (a.day3_change_pct || 0) - (b.day3_change_pct || 0),
    customRender: ({ text }) => {
      if (text == null) return '-'
      const val = text.toFixed(2)
      return {
        children: `${val >= 0 ? '+' : ''}${val}%`,
        props: { style: { color: text >= 0 ? '#dc2626' : '#16a34a', fontWeight: 600 } }
      }
    }
  }
]

function formatDate(date?: string): string {
  if (!date) return '-'
  const s = String(date).trim()
  if (s.length >= 10 && s[4] === '-' && s[7] === '-') return s.slice(0, 10)
  try {
    return new Date(s).toISOString().slice(0, 10)
  } catch {
    return s
  }
}

function formatPrice(price?: number): string {
  if (!price) return '-'
  return price.toFixed(2)
}

function getRowClassName(record: StockResult): string {
  if (!record.current_price || !record.match_price) {
    return ''
  }
  const pct = (record.current_price - record.match_price) / record.match_price
  return pct >= 0 ? 'bg-red-50' : 'bg-green-50'
}

function handleSearch() {
  // 搜索逻辑已在 computed 中处理
}

function handleClearFilters() {
  searchText.value = ''
}

function handleExport() {
  const data = filteredResults.value
  if (data.length === 0) {
    return
  }

  // 按日期排序（日期早的在前，同日期按代码排序）
  const sortedData = [...data].sort((a, b) => {
    const dateA = a.match_date || ''
    const dateB = b.match_date || ''
    const dateCompare = dateA.localeCompare(dateB)
    if (dateCompare !== 0) {
      return dateCompare
    }
    // 日期相同，按代码排序
    return (a.code || '').localeCompare(b.code || '')
  })

  // 构建 CSV 内容
  const headers = ['代码·名称', '匹配日期', '匹配价', '当前价', '次日涨跌%', '第三日涨跌%']
  const rows = sortedData.map(item => [
    `${item.code ?? ''} ${item.name ?? ''}`.trim(),
    formatDate(item.match_date),
    item.match_price?.toFixed(2) || '',
    item.current_price?.toFixed(2) || '',
    item.day2_change_pct != null ? item.day2_change_pct.toFixed(2) : '',
    item.day3_change_pct != null ? item.day3_change_pct.toFixed(2) : ''
  ])

  const csvContent = [
    headers.join(','),
    ...rows.map(row => row.join(','))
  ].join('\n')

  // 添加 BOM 以支持中文
  const BOM = '\uFEFF'
  const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  const url = URL.createObjectURL(blob)
  link.setAttribute('href', url)
  link.setAttribute('download', `回测结果_${new Date().toISOString().slice(0, 10)}.csv`)
  link.style.visibility = 'hidden'
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}

function handleCloseError() {
  strategyStore.setError(null)
}
</script>

<style scoped>
::deep(.ant-table-small) {
  font-size: 12px;
}

::deep(.ant-table-thead > tr > th) {
  padding: 8px 4px;
  font-size: 12px;
  font-weight: 600;
}

::deep(.ant-table-tbody > tr > td) {
  padding: 4px;
  font-size: 12px;
}

::deep(.bg-green-50) {
  background-color: #f0fdf4;
}

::deep(.bg-red-50) {
  background-color: #fef2f2;
}

/* 涨跌幅颜色样式 */
::deep(.text-red-600) {
  color: #dc2626 !important;
}

::deep(.text-green-600) {
  color: #16a34a !important;
}
</style>

