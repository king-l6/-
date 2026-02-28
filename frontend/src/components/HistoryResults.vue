<template>
  <Card title="历史回测数据" class="h-full">
    <template #extra>
      <Space wrap class="w-full md:w-auto">
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
          class="w-32 sm:w-36 md:w-[150px]"
        >
          <template #prefix>
            <SearchOutlined />
          </template>
        </Input>
        <Button size="small" @click="loadFileList" :loading="loadingFiles">
          刷新列表
        </Button>
      </Space>
    </template>
    
    <div v-if="loadingFiles" class="py-2 text-center">
      <Spin /> 加载文件列表...
    </div>
    
    <div v-else-if="fileList.length === 0" class="py-6 text-center text-gray-500">
      <template v-if="error">
        <div class="text-red-600 mb-2">{{ error }}</div>
        <div class="text-sm">请确认后端服务已启动（如 <code class="bg-gray-100 px-1">python app.py</code>），并点击「刷新列表」重试。</div>
      </template>
      <template v-else>暂无历史回测数据文件</template>
    </div>
    
    <div v-else class="flex flex-col md:flex-row gap-0 h-full min-h-0" style="min-height: 320px;">
      <!-- 移动端：文件选择下拉（仅小屏显示） -->
      <div class="md:hidden flex-shrink-0 p-2 bg-gray-50 border-b border-gray-200">
        <div class="flex items-center gap-2">
          <span class="text-sm font-semibold text-gray-700 whitespace-nowrap">选择文件：</span>
          <Select
            v-model:value="activeFile"
            placeholder="请选择回测文件"
            class="flex-1 min-w-0"
            :options="fileList.map(f => ({ label: `${formatFileName(f.filename)}${typeof f.count === 'number' ? ` (${f.count}条)` : ''}`, value: f.filename }))"
            :loading="loadingFiles"
            @change="onMobileFileSelect"
            allow-clear
            show-search
            :filter-option="(input: string, opt: any) => (opt?.label ?? '').toLowerCase().includes(input.toLowerCase())"
          />
        </div>
      </div>

      <!-- 左侧导航栏（仅 md 及以上显示） -->
      <div class="hidden md:block w-64 flex-shrink-0 bg-gray-50 border-r-2 border-gray-300 pr-0">
        <div class="p-2 border-b border-gray-200 bg-white">
          <div class="flex items-center justify-between mb-1">
            <span class="text-sm font-semibold text-gray-800">文件列表</span>
            <span class="text-xs text-gray-500 bg-gray-100 px-1.5 py-0.5 rounded">共 {{ fileList.length }} 个</span>
          </div>
        </div>
        <div class="overflow-y-auto" style="max-height: calc(100vh - 180px);">
          <Menu
            v-model:selectedKeys="selectedKeys"
            mode="inline"
            class="border-0 bg-transparent"
            @select="({ key }) => handleFileChange(key as string)"
          >
            <MenuItem
              v-for="file in fileList"
              :key="file.filename"
              :class="['file-menu-item', { 'strategy-file': isStrategyFile(file.filename) }]"
            >
              <div class="flex flex-col w-full">
                <span class="text-xs font-medium text-gray-800 truncate" :title="file.filename">
                  {{ formatFileName(file.filename) }}
                </span>
                <div class="mt-0.5 flex items-center justify-between gap-2">
                  <span class="text-xs text-gray-500 truncate" :title="formatFullDate(file.modified)">
                    {{ formatFileDate(file.modified) }}
                  </span>
                  <span v-if="typeof file.count === 'number'" class="text-xs text-gray-500 whitespace-nowrap">
                    {{ file.count }} 条
                  </span>
                </div>
              </div>
            </MenuItem>
          </Menu>
        </div>
      </div>
      
      <!-- 右侧内容区：小屏全宽、可横向滚动 -->
      <div class="flex-1 min-w-0 overflow-auto pl-2 pr-2 md:pl-3">

      <!-- 元数据信息 -->
      <div v-if="metaInfo" class="mb-2 p-2 bg-blue-50 border-l-4 border-blue-500 rounded">
        <div class="text-xs text-gray-700">
          <span class="font-semibold">策略名称:</span> {{ metaInfo.strategy_name || '未知' }} | 
          <span class="font-semibold">运行时间:</span> {{ formatDate(metaInfo.run_at) }} | 
          <span class="font-semibold">数据条数:</span> {{ metaInfo.count || results.length }}
        </div>
      </div>

      <!-- 错误提示 -->
      <div v-if="error" class="py-2">
        <Alert
          :message="error"
          type="error"
          show-icon
          closable
          @close="error = ''"
        />
      </div>
      
      <!-- 加载中 -->
      <div v-else-if="loading" class="py-6 text-center">
        <Spin /> 加载数据中...
      </div>
      
      <!-- 无数据 -->
      <div v-else-if="!hasResults" class="py-6 text-center text-gray-500">
        请选择一个文件查看数据
      </div>
      
      <!-- 数据区域：先展示图表，再 Tabs -->
      <div v-else>
        <!-- 每日趋势图：数据加载好后直接渲染在可见区域 -->
        <Card title="每个交易日匹配数量" size="small" class="mb-3">
          <div class="relative w-full min-w-0" style="height: 280px;">
            <div :ref="setChartRef" class="w-full h-full" style="min-width: 300px;"></div>
            <div v-if="dailyChartData.dates.length === 0" class="absolute inset-0 flex items-center justify-center bg-gray-50 text-gray-500 text-sm">
              暂无按日数据，无法绘制趋势图
            </div>
          </div>
        </Card>

        <Tabs v-model:activeKey="contentTabKey" type="card" size="small" class="mb-3">
          <TabPane key="favorites" :tab="`我的自选${collectedItems.length > 0 ? ` (${collectedItems.length})` : ''}`">
            <Card :title="`我的自选${collectedItems.length > 0 ? ` (${collectedItems.length} 条)` : ''}`" size="small" class="mb-0">
              <div v-if="collectedItems.length === 0" class="py-6 text-center text-gray-500 text-sm">
                点击「数据列表」中表格「名称」列前的 ＋ 图标可加入自选
              </div>
              <Table
                v-else
                :columns="collectedColumns"
                :data-source="collectedItems"
                :pagination="false"
                :row-key="(r) => `${r.code}-${r.match_date || ''}`"
                size="small"
                bordered
              >
                <template #bodyCell="{ column, record }">
                  <template v-if="column.key === 'action'">
                    <Button type="link" danger size="small" @click="handleRemove(record as StockResult)">删除</Button>
                  </template>
                </template>
              </Table>
            </Card>
          </TabPane>
          <TabPane :key="'table'" :tab="`数据列表 (${filteredResults.length} 只)`">
            <div class="mb-2 flex flex-wrap items-center justify-between gap-2">
              <div class="text-sm font-semibold text-primary">
                找到 {{ filteredResults.length }} 只符合条件的股票
              </div>
              <Checkbox v-model:checked="filterDay2Strong" class="text-sm">
                当日涨幅&gt;3% 或 振幅&gt;3%
              </Checkbox>
            </div>
            <div :class="['overflow-x-auto w-full -mx-2 px-2 md:mx-0 md:px-0', { 'history-table-mobile': isNarrowScreen }]">
              <Table
                :columns="isDense ? displayDenseColumns : displayColumns"
                :data-source="filteredResults"
                :loading="loading"
                :pagination="isDense ? paginationDense : paginationNormal"
                @change="(pag: any) => onTableChange(pag)"
                :row-key="(record, index) => `${record.code}-${record.match_date || ''}-${record.name || ''}-${index}`"
                :scroll="tableScroll"
                :size="isDense ? 'small' : 'middle'"
                :bordered="isDense"
                :row-class-name="isDense ? getRowClassName : undefined"
              >
                <template #bodyCell="{ column, record }">
                  <template v-if="column.key === 'name' || column.dataIndex === 'name'">
                    <div class="flex items-center gap-1.5">
                      <span
                        class="cursor-pointer text-primary hover:opacity-80 inline-flex items-center"
                        title="加入自选"
                        @click.stop="handleAddToCollection(record as StockResult)"
                      >
                        <PlusOutlined />
                      </span>
                      <span>{{ record.name }}</span>
                    </div>
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
                </template>
              </Table>
            </div>
          </TabPane>
        </Tabs>
      </div>
      </div>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { computed, ref, watch, onMounted, onUnmounted } from 'vue'
import { Card, Table, Spin, Alert, Button, Space, Input, Menu, MenuItem, Select, Tabs, TabPane, Checkbox } from 'ant-design-vue'
import { SearchOutlined, PlusOutlined } from '@ant-design/icons-vue'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult } from '@/types'
import { useHistoryResults } from '@/hooks/history-results/useHistoryResults'
import { getTradingDays } from '@/api'
import * as echarts from 'echarts'

const contentTabKey = ref<'favorites' | 'table'>('table')
const chartRef = ref<HTMLElement | null>(null)
const tradingDaysList = ref<string[]>([]) // 当前结果日期范围内的全部交易日（来自后端缓存），用于图表无数据日显示 0
let chartInstance: echarts.ECharts | null = null

function setChartRef(el: unknown) {
  const div = el instanceof HTMLElement ? el : null
  chartRef.value = div
  if (div && hasResults.value) {
    if (chartInstance) chartInstance.dispose()
    chartInstance = echarts.init(div)
    updateChart()
    setTimeout(() => chartInstance?.resize(), 50)
  }
  if (!div && chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
}

const {
  loadingFiles,
  loading,
  error,
  fileList,
  activeFile,
  results,
  metaInfo,
  isDense,
  searchText,
  selectedKeys,
  tableScroll,
  collectedItems,
  paginationDense,
  paginationNormal,
  hasResults,
  filteredResults,
  loadFileList,
  handleFileChange,
  onMobileFileSelect,
  onTableChange,
  handleAddToCollection,
  handleRemove,
  formatFileName,
  formatFileDate,
  formatFullDate,
  formatDate,
  formatPrice,
  getRowClassName,
  isStrategyFile,
  handleExport,
  isNarrowScreen,
  filterDay2Strong
} = useHistoryResults()

const collectedColumns: ColumnsType<StockResult> = [
  { title: '代码', dataIndex: 'code', key: 'code', width: 80 },
  { title: '名称', dataIndex: 'name', key: 'name', width: 100 },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 100, customRender: ({ text }) => formatDate(text ?? '') },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' },
  { title: '操作', key: 'action', width: 70, fixed: 'right' }
]

const columns: ColumnsType<StockResult> = [
  { title: '代码', dataIndex: 'code', key: 'code', width: 100, fixed: 'left' },
  { title: '名称', dataIndex: 'name', key: 'name', width: 120 },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 120, customRender: ({ text }) => formatDate(text ?? '') },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 120, customRender: ({ text }) => text ? text.toFixed(2) : '-' },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 120, customRender: ({ text }) => text ? text.toFixed(2) : '-' }
]

const denseColumns: ColumnsType<StockResult> = [
  { title: '代码', dataIndex: 'code', key: 'code', width: 80, fixed: 'left', sorter: (a, b) => a.code.localeCompare(b.code) },
  { title: '名称', dataIndex: 'name', key: 'name', width: 100, sorter: (a, b) => a.name.localeCompare(b.name) },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 100, sortDirections: ['ascend', 'descend'], sorter: (a, b) => ((a.match_date || '').trim()).localeCompare((b.match_date || '').trim()), customRender: ({ text }) => formatDate(text ?? '') },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 80, align: 'right', sorter: (a, b) => (a.match_price || 0) - (b.match_price || 0) },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 80, align: 'right', sorter: (a, b) => (a.current_price || 0) - (b.current_price || 0) }
]

const displayColumns = computed(() => {
  if (!isNarrowScreen.value) return columns
  return columns.map(col => {
    const { width, fixed, ...rest } = col
    return { ...rest, fixed: undefined, width: undefined }
  })
})
const displayDenseColumns = computed(() => {
  if (!isNarrowScreen.value) return denseColumns
  return denseColumns.map(col => {
    const { width, fixed, ...rest } = col
    return { ...rest, fixed: undefined, width: undefined }
  })
})

// 按 match_date 聚合每日匹配数量，用于趋势图；若有交易日列表则按全部交易日展示，无数据日显示 0
const dailyChartData = computed(() => {
  const list = results.value || []
  const map = new Map<string, number>()
  for (const r of list) {
    const raw = (r as any).match_date ?? (r as any).matchDate ?? (r as any).date
    if (raw == null) continue
    const date = String(raw).trim().slice(0, 10)
    if (!date || date.length < 10) continue
    map.set(date, (map.get(date) ?? 0) + 1)
  }
  const allDays = tradingDaysList.value
  if (allDays.length > 0) {
    return { dates: allDays, counts: allDays.map(d => map.get(d) ?? 0) }
  }
  const dates = Array.from(map.keys()).sort()
  const counts = dates.map(d => map.get(d)!)
  return { dates, counts }
})

function updateChart() {
  if (!chartInstance) return
  const { dates, counts } = dailyChartData.value
  if (dates.length === 0) {
    chartInstance.setOption({
      graphic: {
        type: 'text',
        left: 'center',
        top: 'middle',
        style: { text: '暂无按日数据', fontSize: 14, fill: '#999' }
      }
    }, { notMerge: true })
    return
  }
  const option: echarts.EChartsOption = {
    title: { text: '每个交易日匹配数量', left: 'center', textStyle: { fontSize: 14 } },
    tooltip: { trigger: 'axis' },
    grid: { left: 48, right: 24, top: 40, bottom: 48 },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: { rotate: dates.length > 12 ? 45 : 0, fontSize: 11 }
    },
    yAxis: { type: 'value', name: '数量', minInterval: 1 },
    series: [
      { type: 'line', smooth: true, data: counts, areaStyle: {}, symbol: 'circle', symbolSize: 6 }
    ]
  }
  chartInstance.setOption(option, { notMerge: true })
}

onMounted(() => {})

onUnmounted(() => {
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
})

// 数据变化时更新图表（创建在 setChartRef 里完成）
watch(
  () => ({ dates: dailyChartData.value.dates, counts: dailyChartData.value.counts }),
  () => {
    if (chartInstance) {
      updateChart()
      setTimeout(() => chartInstance?.resize(), 50)
    }
  },
  { deep: true }
)

// 有结果时拉取该日期范围内的全部交易日，用于图表按日补全（无数据日显示 0）
watch(
  () => results.value,
  async (list) => {
    const arr = list || []
    if (arr.length === 0) {
      tradingDaysList.value = []
      return
    }
    let minD = ''
    let maxD = ''
    for (const r of arr) {
      const raw = (r as any).match_date ?? (r as any).matchDate ?? (r as any).date
      if (raw == null) continue
      const date = String(raw).trim().slice(0, 10)
      if (date.length >= 10 && date[4] === '-' && date[7] === '-') {
        if (!minD || date < minD) minD = date
        if (!maxD || date > maxD) maxD = date
      }
    }
    if (!minD || !maxD) {
      tradingDaysList.value = []
      return
    }
    try {
      const res = await getTradingDays(minD, maxD)
      if (res?.success && Array.isArray(res.data) && res.data.length > 0) {
        tradingDaysList.value = res.data
      } else {
        tradingDaysList.value = []
      }
    } catch {
      tradingDaysList.value = []
    }
  },
  { immediate: true, deep: true }
)
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

/* 移动端：列宽按内容（最长文字）自适应，表格整体横向滚动 */
.history-table-mobile :deep(.ant-table-content) {
  overflow-x: auto !important;
}
.history-table-mobile :deep(.ant-table-content table) {
  table-layout: auto;
  width: max-content;
  min-width: 100%;
}
.history-table-mobile :deep(.ant-table-thead > tr > th),
.history-table-mobile :deep(.ant-table-tbody > tr > td) {
  white-space: nowrap;
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

/* 左侧导航栏样式 */
:deep(.ant-menu) {
  background: transparent;
  border-right: none;
}

:deep(.ant-menu-item) {
  margin: 2px 4px;
  padding: 6px 10px;
  height: auto;
  min-height: 48px;
  line-height: 1.4;
  border-radius: 4px;
  transition: all 0.2s;
  border: 1px solid transparent;
}

:deep(.ant-menu-item:hover) {
  background-color: #f0f0f0;
  border-color: #d9d9d9;
}

:deep(.ant-menu-item-selected) {
  background-color: #e6f7ff;
  color: #1890ff;
  font-weight: 600;
  border-color: #1890ff;
  box-shadow: 0 2px 4px rgba(24, 144, 255, 0.1);
}

:deep(.ant-menu-item-selected::after) {
  display: none;
}

/* 战法文件背景颜色 */
:deep(.strategy-file) {
  background-color: #fff7e6 !important;
  border-left: 3px solid #faad14 !important;
}

:deep(.strategy-file:hover) {
  background-color: #ffecc7 !important;
}

:deep(.strategy-file.ant-menu-item-selected) {
  background-color: #fff1b8 !important;
  border-color: #faad14 !important;
  border-left-width: 3px !important;
}

/* 文件菜单项样式 */
.file-menu-item {
  width: 100%;
}

.file-menu-item :deep(.ant-menu-title-content) {
  width: 100%;
}
</style>