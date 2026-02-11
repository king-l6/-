<template>
  <Card title="回测结果" class="h-full">
    <template #extra>
      <Space>
        <Button size="small" @click="isDense = !isDense">
          {{ isDense ? '普通模式' : '密集模式' }}
        </Button>
        <Button v-if="hasResults" size="small" @click="handleExport">导出CSV</Button>
        <Input
          v-if="hasResults && isDense"
          v-model:value="searchText"
          placeholder="搜索代码或名称"
          allow-clear
          size="small"
          style="width: 150px"
        >
          <template #prefix>
            <SearchOutlined />
          </template>
        </Input>
        <Spin v-if="loading" />
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
    
    <div v-else-if="!hasResults" class="py-6 text-center text-gray-500">
      暂无回测结果
    </div>
    
    <div v-else>
      <div class="mb-2 flex items-center justify-between">
        <div class="text-sm font-semibold text-primary">
          找到 {{ filteredResults.length }} 只符合条件的股票
        </div>
      </div>
      
      <Table
        :columns="isDense ? denseColumns : columns"
        :data-source="filteredResults"
        :loading="loading"
        :pagination="isDense ? {
          pageSize: 50,
          showSizeChanger: true,
          pageSizeOptions: ['20', '50', '100', '200'],
          showTotal: (total) => `共 ${total} 条`,
          showQuickJumper: true
        } : {
          pageSize: 20,
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 条`
        }"
        :row-key="(record) => `${record.code}-${record.match_date || ''}`"
        :scroll="isDense ? { x: 'max-content', y: 600 } : { x: 'max-content' }"
        :size="isDense ? 'small' : 'middle'"
        :bordered="isDense"
        :row-class-name="isDense ? getRowClassName : undefined"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'pctChange'">
            <span 
              :class="getPctChangeClass(record as any as StockResult)"
              :style="getPctChangeStyle(record as any as StockResult)"
            >
              {{ getPctChange(record as any as StockResult) }}
            </span>
          </template>
          <template v-else-if="column.key === 'day2_amplitude'">
            <span :class="getAmplitudeClass((record as any).day2_amplitude)">
              {{ formatAmplitude((record as any).day2_amplitude) }}
            </span>
          </template>
          <template v-else-if="column.key === 'day2_change_pct'">
            <span :class="getDayPctClass((record as any).day2_change_pct)">
              {{ formatDayPct((record as any).day2_change_pct) }}
            </span>
          </template>
          <template v-else-if="column.key === 'day3_amplitude'">
            <span :class="getAmplitudeClass((record as any).day3_amplitude)">
              {{ formatAmplitude((record as any).day3_amplitude) }}
            </span>
          </template>
          <template v-else-if="column.key === 'day3_change_pct'">
            <span :class="getDayPctClass((record as any).day3_change_pct)">
              {{ formatDayPct((record as any).day3_change_pct) }}
            </span>
          </template>
          <template v-else-if="isDense && column.key === 'match_date'">
            <span class="text-xs">{{ formatDate((record as any).match_date) }}</span>
          </template>
          <template v-else-if="isDense && column.key === 'match_price'">
            <span class="font-mono text-xs">{{ formatPrice((record as any).match_price) }}</span>
          </template>
          <template v-else-if="isDense && column.key === 'current_price'">
            <span class="font-mono text-xs">{{ formatPrice((record as any).current_price) }}</span>
          </template>
          <template v-else-if="isDense && column.key === 'code'">
            <span class="font-mono text-xs font-semibold">{{ (record as any).code }}</span>
          </template>
          <template v-else-if="isDense && column.key === 'day2_amplitude'">
            <span :class="getAmplitudeClass((record as any).day2_amplitude)" class="text-xs">
              {{ formatAmplitude((record as any).day2_amplitude) }}
            </span>
          </template>
          <template v-else-if="isDense && column.key === 'day2_change_pct'">
            <span :class="getDayPctClass((record as any).day2_change_pct)" class="text-xs">
              {{ formatDayPct((record as any).day2_change_pct) }}
            </span>
          </template>
          <template v-else-if="isDense && column.key === 'day3_amplitude'">
            <span :class="getAmplitudeClass((record as any).day3_amplitude)" class="text-xs">
              {{ formatAmplitude((record as any).day3_amplitude) }}
            </span>
          </template>
          <template v-else-if="isDense && column.key === 'day3_change_pct'">
            <span :class="getDayPctClass((record as any).day3_change_pct)" class="text-xs">
              {{ formatDayPct((record as any).day3_change_pct) }}
            </span>
          </template>
        </template>
      </Table>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { Card, Table, Spin, Alert, Button, Space, Input } from 'ant-design-vue'
import { SearchOutlined } from '@ant-design/icons-vue'
import { useStrategyStore } from '@/store/modules/strategy'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult } from '@/types'

const strategyStore = useStrategyStore()

const loading = computed(() => strategyStore.loading)
const error = computed(() => strategyStore.error)
const results = computed(() => strategyStore.results)
const hasResults = computed(() => strategyStore.hasResults)

const isDense = ref(true)
const searchText = ref('')

// 筛选后的结果（确保返回新数组，避免被 Table 组件修改）
const filteredResults = computed(() => {
  let sourceData = results.value
  // 如果有搜索条件，先筛选
  if (isDense.value && searchText.value) {
    const search = searchText.value.toLowerCase()
    sourceData = sourceData.filter(item => 
      item.code.toLowerCase().includes(search) || 
      item.name.toLowerCase().includes(search)
    )
  }
  // 返回数组的副本，避免 Table 组件修改原始数据
  return [...sourceData]
})

function formatDate(date?: string): string {
  if (!date) return '-'
  return date
}

function formatPrice(price?: number): string {
  if (!price) return '-'
  return price.toFixed(2)
}

function getPctChange(stock: StockResult): string {
  if (!stock.current_price || !stock.match_price) {
    return '-'
  }
  const pct = ((stock.current_price - stock.match_price) / stock.match_price * 100).toFixed(2)
  return `${pct}%`
}

function getPctChangeValue(stock: StockResult): number {
  if (!stock.current_price || !stock.match_price) {
    return 0
  }
  return (stock.current_price - stock.match_price) / stock.match_price * 100
}

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
    width: 120
  },
  {
    title: '次日振幅',
    dataIndex: 'day2_amplitude',
    key: 'day2_amplitude',
    width: 100
  },
  {
    title: '次日涨跌幅',
    dataIndex: 'day2_change_pct',
    key: 'day2_change_pct',
    width: 100
  },
  {
    title: '第三日振幅',
    dataIndex: 'day3_amplitude',
    key: 'day3_amplitude',
    width: 100
  },
  {
    title: '第三日涨跌幅',
    dataIndex: 'day3_change_pct',
    key: 'day3_change_pct',
    width: 100,
    fixed: 'right'
  }
]

const denseColumns: ColumnsType<StockResult> = [
  {
    title: '代码',
    dataIndex: 'code',
    key: 'code',
    width: 80,
    fixed: 'left',
    sorter: (a, b) => a.code.localeCompare(b.code)
  },
  {
    title: '名称',
    dataIndex: 'name',
    key: 'name',
    width: 100,
    sorter: (a, b) => a.name.localeCompare(b.name)
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
      // 处理空值：空值排在最后
      if (!dateA && !dateB) return 0
      if (!dateA) return 1
      if (!dateB) return -1
      // 使用日期字符串比较（格式：YYYY-MM-DD，可以直接用字符串比较）
      return dateA.localeCompare(dateB)
    }
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
    title: '涨跌幅',
    key: 'pctChange',
    width: 90,
    align: 'right',
    sorter: (a, b) => {
      const pctA = getPctChangeValue(a)
      const pctB = getPctChangeValue(b)
      return pctA - pctB
    }
  },
  {
    title: '次日振幅',
    dataIndex: 'day2_amplitude',
    key: 'day2_amplitude',
    width: 85,
    align: 'right',
    sorter: (a, b) => (a.day2_amplitude || 0) - (b.day2_amplitude || 0)
  },
  {
    title: '次日涨跌幅',
    dataIndex: 'day2_change_pct',
    key: 'day2_change_pct',
    width: 90,
    align: 'right',
    sorter: (a, b) => (a.day2_change_pct || 0) - (b.day2_change_pct || 0)
  },
  {
    title: '第三日振幅',
    dataIndex: 'day3_amplitude',
    key: 'day3_amplitude',
    width: 85,
    align: 'right',
    sorter: (a, b) => (a.day3_amplitude || 0) - (b.day3_amplitude || 0)
  },
  {
    title: '第三日涨跌幅',
    dataIndex: 'day3_change_pct',
    key: 'day3_change_pct',
    width: 90,
    align: 'right',
    fixed: 'right',
    sorter: (a, b) => (a.day3_change_pct || 0) - (b.day3_change_pct || 0)
  }
]

function getPctChangeClass(stock: StockResult): string {
  if (!stock.current_price || !stock.match_price) {
    return ''
  }
  const pct = (stock.current_price - stock.match_price) / stock.match_price
  return pct >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'
}

function getPctChangeStyle(stock: StockResult): Record<string, string> {
  if (!stock.current_price || !stock.match_price) {
    return {}
  }
  const pct = (stock.current_price - stock.match_price) / stock.match_price
  return {
    color: pct >= 0 ? '#dc2626' : '#16a34a',
    fontWeight: '600'
  }
}

function formatDayPct(pct?: number): string {
  if (pct === undefined || pct === null) return '-'
  return `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
}

function getDayPctClass(pct?: number): string {
  if (pct === undefined || pct === null) return ''
  return pct >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'
}

function formatAmplitude(amplitude?: number): string {
  if (amplitude === undefined || amplitude === null) return '-'
  return `${amplitude >= 0 ? '+' : ''}${amplitude.toFixed(2)}%`
}

function getAmplitudeClass(amplitude?: number): string {
  if (amplitude === undefined || amplitude === null) return ''
  return amplitude >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'
}

function getRowClassName(record: StockResult): string {
  if (!record.current_price || !record.match_price) {
    return ''
  }
  const pct = (record.current_price - record.match_price) / record.match_price
  return pct >= 0 ? 'bg-red-50' : 'bg-green-50'
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
  const headers = ['代码', '名称', '匹配日期', '匹配价格', '当前价格', '涨跌幅(%)', '次日振幅', '次日涨跌幅(%)', '第三日振幅', '第三日涨跌幅(%)']
  const rows = sortedData.map(item => {
    const pct = getPctChangeValue(item)
    return [
      item.code,
      item.name,
      item.match_date || '',
      item.match_price?.toFixed(2) || '',
      item.current_price?.toFixed(2) || '',
      pct.toFixed(2),
      item.day2_amplitude !== undefined ? item.day2_amplitude.toFixed(2) : '',
      item.day2_change_pct !== undefined ? item.day2_change_pct.toFixed(2) : '',
      item.day3_amplitude !== undefined ? item.day3_amplitude.toFixed(2) : '',
      item.day3_change_pct !== undefined ? item.day3_change_pct.toFixed(2) : ''
    ]
  })
  
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
:deep(.ant-table-small) {
  font-size: 12px;
}

:deep(.ant-table-small .ant-table-thead > tr > th) {
  padding: 8px 4px;
  font-size: 12px;
  font-weight: 600;
}

:deep(.ant-table-small .ant-table-tbody > tr > td) {
  padding: 4px;
  font-size: 12px;
}

:deep(.bg-green-50) {
  background-color: #f0fdf4;
}

:deep(.bg-red-50) {
  background-color: #fef2f2;
}

/* 涨跌幅颜色样式 */
:deep(.text-red-600) {
  color: #dc2626 !important;
}

:deep(.text-green-600) {
  color: #16a34a !important;
}
</style>
