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
      暂无历史回测数据文件
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
            :options="fileList.map(f => ({ label: formatFileName(f.filename), value: f.filename }))"
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
                <span class="text-xs text-gray-500 mt-0.5" :title="formatFullDate(file.modified)">
                  {{ formatFileDate(file.modified) }}
                </span>
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
      
      <!-- 数据表格 -->
      <div v-else>
        <!-- 我的自选：放在表格上方，添加后一眼能看到 -->
        <Card :title="`我的自选${collectedItems.length > 0 ? ` (${collectedItems.length} 条)` : ''}`" class="mb-3" size="small">
          <div v-if="collectedItems.length === 0" class="py-2 text-center text-gray-500 text-sm">
            点击下方表格「名称」列前的 ＋ 图标可加入自选
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
              <template v-else-if="column.key === 'pctChange'">
                <span :class="getPctChangeClass(record as StockResult)">{{ getPctChange(record as StockResult) }}</span>
              </template>
            </template>
          </Table>
        </Card>

        <div class="mb-2 flex items-center justify-between">
          <div class="text-sm font-semibold text-primary">
            找到 {{ filteredResults.length }} 只符合条件的股票
          </div>
        </div>
        
        <!-- 小屏下整块可横向滑动，保证表格数据可见；移动端列宽按内容、最后一列不固定 -->
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
            <template v-else-if="column.key === 'pctChange'">
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
            <template v-else-if="isDense && column.key === 'code'">
              <span class="font-mono text-xs font-semibold">{{ (record as any).code }}</span>
            </template>
          </template>
          </Table>
        </div>
      </div>
      </div>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Card, Table, Spin, Alert, Button, Space, Input, Menu, MenuItem, Select } from 'ant-design-vue'
import { SearchOutlined, PlusOutlined } from '@ant-design/icons-vue'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult } from '@/types'
import { useHistoryResults } from '@/hooks/history-results/useHistoryResults'

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
  getPctChange,
  getPctChangeValue,
  getPctChangeClass,
  getPctChangeStyle,
  formatDayPct,
  getDayPctClass,
  formatAmplitude,
  getAmplitudeClass,
  getRowClassName,
  isStrategyFile,
  handleExport,
  isNarrowScreen
} = useHistoryResults()

const collectedColumns: ColumnsType<StockResult> = [
  { title: '代码', dataIndex: 'code', key: 'code', width: 80 },
  { title: '名称', dataIndex: 'name', key: 'name', width: 100 },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 100 },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' },
  { title: '涨跌幅', key: 'pctChange', width: 90 },
  { title: '操作', key: 'action', width: 70, fixed: 'right' }
]

const columns: ColumnsType<StockResult> = [
  { title: '代码', dataIndex: 'code', key: 'code', width: 100, fixed: 'left' },
  { title: '名称', dataIndex: 'name', key: 'name', width: 120 },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 120 },
  { title: '匹配价格', dataIndex: 'match_price', key: 'match_price', width: 120, customRender: ({ text }) => text ? text.toFixed(2) : '-' },
  { title: '当前价格', dataIndex: 'current_price', key: 'current_price', width: 120, customRender: ({ text }) => text ? text.toFixed(2) : '-' },
  { title: '涨跌幅', key: 'pctChange', width: 120 },
  { title: '次日振幅', dataIndex: 'day2_amplitude', key: 'day2_amplitude', width: 100 },
  { title: '次日涨跌幅', dataIndex: 'day2_change_pct', key: 'day2_change_pct', width: 100 },
  { title: '第三日振幅', dataIndex: 'day3_amplitude', key: 'day3_amplitude', width: 100 },
  { title: '第三日涨跌幅', dataIndex: 'day3_change_pct', key: 'day3_change_pct', width: 100, fixed: 'right' }
]

const denseColumns: ColumnsType<StockResult> = [
  { title: '代码', dataIndex: 'code', key: 'code', width: 80, fixed: 'left', sorter: (a, b) => a.code.localeCompare(b.code) },
  { title: '名称', dataIndex: 'name', key: 'name', width: 100, sorter: (a, b) => a.name.localeCompare(b.name) },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 100, sortDirections: ['ascend', 'descend'], sorter: (a, b) => ((a.match_date || '').trim()).localeCompare((b.match_date || '').trim()) },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 80, align: 'right', sorter: (a, b) => (a.match_price || 0) - (b.match_price || 0) },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 80, align: 'right', sorter: (a, b) => (a.current_price || 0) - (b.current_price || 0) },
  { title: '涨跌幅', key: 'pctChange', width: 90, align: 'right', sorter: (a, b) => getPctChangeValue(a) - getPctChangeValue(b) },
  { title: '次日振幅', dataIndex: 'day2_amplitude', key: 'day2_amplitude', width: 85, align: 'right', sorter: (a, b) => (a.day2_amplitude || 0) - (b.day2_amplitude || 0) },
  { title: '次日涨跌幅', dataIndex: 'day2_change_pct', key: 'day2_change_pct', width: 90, align: 'right', sorter: (a, b) => (a.day2_change_pct || 0) - (b.day2_change_pct || 0) },
  { title: '第三日振幅', dataIndex: 'day3_amplitude', key: 'day3_amplitude', width: 85, align: 'right', sorter: (a, b) => (a.day3_amplitude || 0) - (b.day3_amplitude || 0) },
  { title: '第三日涨跌幅', dataIndex: 'day3_change_pct', key: 'day3_change_pct', width: 90, align: 'right', fixed: 'right', sorter: (a, b) => (a.day3_change_pct || 0) - (b.day3_change_pct || 0) }
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