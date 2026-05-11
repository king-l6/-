<template>
  <Card title="历史回测数据" class="h-full">
    <template #extra>
      <Space wrap class="w-full md:w-auto">
        <Button size="small" @click="isDense = !isDense">
          {{ isDense ? '普通模式' : '密集模式' }}
        </Button>
        <Button v-if="hasResults" size="small" @click="handleExport">导出CSV</Button>
        <Input
          v-if="hasResults && (isDense || isMultiStrategyOverlapFile)"
          v-model:value="searchText"
          :placeholder="isMultiStrategyOverlapFile ? '搜索代码、名称或策略名' : '搜索代码或名称'"
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

      <!-- 左侧导航栏（仅 md 及以上显示，偏窄以让右侧表格更早露出） -->
      <div class="hidden md:block w-48 lg:w-52 flex-shrink-0 bg-gray-50 border-r border-gray-300 pr-0">
        <div class="px-2 py-1.5 border-b border-gray-200 bg-white">
          <div class="flex items-center justify-between gap-1">
            <span class="text-xs font-semibold text-gray-800 truncate">文件列表</span>
            <span class="text-[10px] text-gray-500 bg-gray-100 px-1 py-0.5 rounded shrink-0">共{{ fileList.length }}</span>
          </div>
        </div>
        <div class="overflow-y-auto" style="max-height: calc(100vh - 160px);">
          <Menu
            v-model:selectedKeys="selectedKeys"
            mode="inline"
            class="border-0 bg-transparent"
            @select="({ key }) => handleFileChange(key as string)"
          >
            <MenuItem
              v-for="file in fileList"
              :key="file.filename"
              :class="[
                'file-menu-item',
                {
                  'strategy-file': isStrategyFile(file.filename),
                  'strategy-file-main-force': isMainForceBuildHistoryFile(file.filename)
                }
              ]"
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
      <div class="flex-1 min-w-0 overflow-auto pl-1.5 pr-2 md:pl-2">

      <!-- 元数据信息 -->
      <div v-if="metaInfo" class="mb-1.5 py-1 px-2 bg-blue-50 border-l-4 border-blue-500 rounded">
        <div v-if="isMultiStrategyOverlapFile" class="text-[11px] leading-snug text-gray-700 space-y-0.5">
          <div v-if="(metaInfo as any).note" class="text-gray-800">{{ (metaInfo as any).note }}</div>
          <div>
            <span class="font-semibold">类型:</span> 多策略同日汇总（{{ (metaInfo as any).kind || 'overlap' }}） |
            <span class="font-semibold">数据条数:</span> {{ metaInfo.count ?? results.length }}
            <template v-if="(metaInfo as any).run_at">
              | <span class="font-semibold">生成时间:</span> {{ formatDate((metaInfo as any).run_at) }}
            </template>
          </div>
        </div>
        <div v-else class="text-[11px] leading-snug text-gray-700">
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
        <Card title="每个交易日匹配数量" size="small" class="mb-2" :body-style="{ padding: '8px 10px' }">
          <div class="relative w-full min-w-0" style="height: 200px;">
            <div :ref="setChartRef" class="w-full h-full" style="min-width: 300px;"></div>
            <div v-if="dailyChartData.dates.length === 0" class="absolute inset-0 flex items-center justify-center bg-gray-50 text-gray-500 text-sm">
              暂无按日数据，无法绘制趋势图
            </div>
          </div>
          <p v-if="!isMultiStrategyOverlapFile" class="text-[11px] text-gray-500 mt-1 leading-snug">
            按每条结果的 <span class="font-mono">match_date</span> 计数。默认全量 jsonl 常为「每只股票只保留窗口内最后一次命中」，日频不等于「当日全市场真实命中数」；需要后者时请用
            <span class="font-mono text-[10px]">batch_backtest.py --all-match-dates</span> 重新生成结果文件。
          </p>
          <p v-else class="text-[11px] text-gray-500 mt-1 leading-snug">
            按 <span class="font-mono">match_date</span> 计数；本文件为「同日多策略重叠」汇总，表格中「重叠策略」列为当日同时命中的策略名。
          </p>
        </Card>

        <Tabs v-model:activeKey="contentTabKey" type="card" size="small" class="mb-2">
          <TabPane key="favorites" :tab="`我的自选${collectedItems.length > 0 ? ` (${collectedItems.length})` : ''}`">
            <Card :title="`我的自选${collectedItems.length > 0 ? ` (${collectedItems.length} 条)` : ''}`" size="small" class="mb-0">
              <div v-if="collectedItems.length === 0" class="py-6 text-center text-gray-500 text-sm">
                点击「数据列表」中表格「代码·名称」列前的 ＋ 图标可加入自选
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
                  <template v-else-if="column.key === 'linkage_text'">
                    <Tooltip :title="formatLinkageTableCell(record as StockResult)">
                      <LinkageTableCell
                        :record="record as StockResult"
                        text-class="text-xs text-gray-800 max-w-[14rem] inline-block truncate align-top"
                      />
                    </Tooltip>
                  </template>
                </template>
              </Table>
            </Card>
          </TabPane>
          <TabPane :key="'table'" :tab="`数据列表 (${filteredResults.length} 只)`">
            <div class="mb-1.5 flex flex-wrap items-center justify-between gap-x-2 gap-y-1">
              <div class="text-sm font-semibold text-primary">
                找到 {{ filteredResults.length }} 只符合条件的股票
              </div>
              <div class="text-xs text-slate-600" v-if="!isMultiStrategyOverlapFile && tenDayHitStats.validCount > 0">
                次日开盘买入后10个交易日内最高涨幅&gt;5% 概率：
                <span class="font-semibold text-rose-600">
                  {{ tenDayHitStats.hitCount }}/{{ tenDayHitStats.validCount }} ({{ tenDayHitStats.hitRateText }})
                </span>
              </div>
              <Checkbox v-if="!isMultiStrategyOverlapFile" v-model:checked="filterDay2Strong" class="text-sm">
                当日涨幅&gt;3% 或 振幅&gt;3%
              </Checkbox>
              <InputNumber
                v-if="showMainForceBullishFilter"
                v-model:value="minMainForceBullishDays"
                :min="0"
                :max="11"
                size="small"
                class="w-[148px]"
                placeholder="收涨日数>="
              />
              <Button v-if="!isMultiStrategyOverlapFile" size="small" @click="openColumnModal">
                <template #icon><MenuOutlined /></template>
                列设置
              </Button>
            </div>
            <div :class="['overflow-x-auto w-full -mx-2 px-2 md:mx-0 md:px-0', { 'history-table-mobile': isNarrowScreen }]">
              <Table
                :columns="listTableColumns"
                :data-source="filteredResults"
                :loading="loading"
                :pagination="isDense ? paginationDense : paginationNormal"
                @change="(pag: any) => onTableChange(pag)"
                :row-key="(record, index) => `${record.code}-${record.match_date || ''}-${record.name || ''}-${index}`"
                :scroll="tableScroll"
                :size="isDense ? 'small' : 'middle'"
                :bordered="isDense"
                :row-class-name="(record) => {
                  const base = isDense ? getRowClassName(record as StockResult) : ''
                  const leader = isDayLeader(record as StockResult) ? 'row-day-leader' : ''
                  return [base, leader].filter(Boolean).join(' ') || ''
                }"
              >
                <template #bodyCell="{ column, record }">
                  <template v-if="column.key === 'code_name'">
                    <div class="flex items-center gap-1.5 flex-wrap min-w-0">
                      <span
                        class="cursor-pointer text-primary hover:opacity-80 inline-flex items-center shrink-0"
                        title="加入自选"
                        @click.stop="handleAddToCollection(record as StockResult)"
                      >
                        <PlusOutlined />
                      </span>
                      <span
                        v-if="getDayLeaderLabel(record as StockResult)"
                        class="text-[10px] px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-700 font-medium shrink-0"
                        :title="getDayLeaderLabel(record as StockResult)"
                      >
                        {{ getDayLeaderLabel(record as StockResult) }}
                      </span>
                      <span
                        v-if="(record as any).touch_limit_not_close"
                        class="text-[10px] px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-700 font-medium shrink-0"
                        title="T日最高价到涨停，收盘未封板；且近30日三连板、近10日有涨停"
                      >
                        摸板未封
                      </span>
                      <span
                        class="font-mono font-semibold shrink-0"
                        :class="isDense ? 'text-xs' : 'text-sm'"
                      >{{ (record as any).code }}</span>
                      <span class="min-w-0" :class="isDense ? 'text-xs' : ''">{{ (record as any).name }}</span>
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
                  <template v-else-if="column.key === 'overlap_strategies'">
                    <div class="flex flex-wrap gap-1 max-w-[min(100vw-4rem,28rem)]">
                      <Tag
                        v-for="(s, i) in overlapStrategyLabels(record as StockResult)"
                        :key="`${(record as any).code}-${i}`"
                        color="processing"
                        class="m-0"
                      >
                        {{ s }}
                      </Tag>
                      <span v-if="overlapStrategyLabels(record as StockResult).length === 0" class="text-gray-400 text-xs">-</span>
                    </div>
                  </template>
                  <template v-else-if="column.key === 'overlap_summary'">
                    <Tooltip :title="String((record as any).overlap_summary || '').trim() || undefined">
                      <span class="text-xs text-gray-700 line-clamp-2 cursor-default">
                        {{ (record as any).overlap_summary || '-' }}
                      </span>
                    </Tooltip>
                  </template>
                  <template v-else-if="column.key === 'linkage_text'">
                    <Tooltip :title="formatLinkageTableCell(record as StockResult)">
                      <LinkageTableCell
                        :record="record as StockResult"
                        text-class="text-xs text-gray-800 max-w-[min(48rem,88vw)] inline-block truncate align-top"
                      />
                    </Tooltip>
                  </template>
                </template>
              </Table>
            </div>
          </TabPane>
        </Tabs>
      </div>
      </div>
    </div>

    <Modal
      v-model:open="columnModalOpen"
      title="数据列表 — 列展示与顺序"
      :width="420"
      destroy-on-close
    >
      <p class="text-xs text-gray-500 mb-2">
        拖拽左侧手柄调整列顺序；取消勾选可隐藏列（「代码·名称」不可隐藏）。设置按「密集 / 普通」分别保存。
      </p>
      <draggable
        v-model="draftColumnRows"
        item-key="key"
        handle=".col-pref-drag-handle"
        :animation="160"
        class="border border-gray-200 rounded max-h-[55vh] overflow-y-auto"
      >
        <template #item="{ element }">
          <div
            class="flex items-center gap-2 px-2 py-1.5 border-b border-gray-100 last:border-b-0 bg-white hover:bg-gray-50"
          >
            <HolderOutlined class="col-pref-drag-handle text-gray-400 shrink-0 cursor-move text-sm" />
            <Checkbox
              :checked="element.visible"
              :disabled="element.required"
              @update:checked="(v: boolean | string) => onDraftVisibleChange(element, !!v)"
            />
            <span class="text-sm text-gray-800 flex-1 min-w-0 truncate" :title="element.title">{{ element.title }}</span>
            <span v-if="element.required" class="text-[10px] text-gray-400 shrink-0">必选</span>
          </div>
        </template>
      </draggable>
      <template #footer>
        <div class="flex flex-wrap items-center justify-between gap-2">
          <Button size="small" @click="restoreDraftColumnDefault">恢复默认</Button>
          <div class="flex gap-2">
            <Button size="small" @click="columnModalOpen = false">取消</Button>
            <Button type="primary" size="small" @click="applyColumnModal">确定</Button>
          </div>
        </div>
      </template>
    </Modal>
  </Card>
</template>

<script setup lang="ts">
import { computed, ref, watch, onMounted, onUnmounted, h } from 'vue'
import { Card, Table, Spin, Alert, Button, Space, Input, InputNumber, Menu, MenuItem, Select, Tabs, TabPane, Checkbox, Modal, Tag, Tooltip } from 'ant-design-vue'
import { SearchOutlined, PlusOutlined, HolderOutlined, MenuOutlined } from '@ant-design/icons-vue'
import draggable from 'vuedraggable'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult } from '@/types'
import { formatLinkageTableCell } from '@/utils/linkageDisplay'
import LinkageTableCell from '@/components/LinkageTableCell.vue'
import {
  STOCK_CODE_NAME_COLUMN_KEY,
  STOCK_CODE_NAME_COLUMN_TITLE
} from '@/constants/stockTable'
import { useHistoryResults } from '@/hooks/history-results/useHistoryResults'
import {
  applyHistoryColumnPrefs,
  buildDraftFromBase,
  getColumnKey,
  useHistoryTableColumnPrefs,
  type HistoryColumnDraftRow,
  type HistoryColumnPrefs
} from '@/hooks/history-results/useHistoryTableColumnPrefs'
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
  isMainForceBuildHistoryFile,
  minMainForceBullishDays,
  showMainForceBullishFilter,
  isDayLeader,
  getDayLeaderLabel,
  handleExport,
  isNarrowScreen,
  filterDay2Strong,
  isMultiStrategyOverlapFile
} = useHistoryResults()

const { densePrefs, normalPrefs, setDensePrefs, setNormalPrefs, resetDensePrefs, resetNormalPrefs } =
  useHistoryTableColumnPrefs()

const columnModalOpen = ref(false)
/** 打开弹窗时快照：密集 / 普通 各一套偏好 */
const columnModalIsDense = ref(false)
const draftColumnRows = ref<HistoryColumnDraftRow[]>([])

function stripColumnWidths<T extends Record<string, unknown>>(cols: ColumnsType<T>): ColumnsType<T> {
  return cols.map((col) => {
    const { width, fixed, ...rest } = col
    return { ...rest, fixed: undefined, width: undefined }
  })
}

function openColumnModal() {
  columnModalIsDense.value = isDense.value
  const base = isDense.value ? denseColumns.value : columns.value
  const prefs = isDense.value ? densePrefs.value : normalPrefs.value
  draftColumnRows.value = buildDraftFromBase(base, prefs)
  columnModalOpen.value = true
}

function onDraftVisibleChange(row: HistoryColumnDraftRow, checked: boolean) {
  if (row.required) return
  row.visible = checked
}

function restoreDraftColumnDefault() {
  const base = columnModalIsDense.value ? denseColumns.value : columns.value
  draftColumnRows.value = buildDraftFromBase(base, null)
}

function draftMatchesDefaultBase(draft: HistoryColumnDraftRow[], base: ColumnsType<StockResult>): boolean {
  const keys = base.map(getColumnKey).filter(Boolean)
  if (draft.length !== keys.length) return false
  if (!draft.every((r) => r.visible)) return false
  return draft.every((r, i) => r.key === keys[i])
}

function applyColumnModal() {
  const base = columnModalIsDense.value ? denseColumns.value : columns.value
  if (draftMatchesDefaultBase(draftColumnRows.value, base)) {
    if (columnModalIsDense.value) resetDensePrefs()
    else resetNormalPrefs()
  } else {
    const hidden = draftColumnRows.value.filter((r) => !r.visible && !r.required).map((r) => r.key)
    const order = draftColumnRows.value.map((r) => r.key)
    const prefs: HistoryColumnPrefs = { order, hidden }
    if (columnModalIsDense.value) setDensePrefs(prefs)
    else setNormalPrefs(prefs)
  }
  columnModalOpen.value = false
}

const collectedColumns = computed<ColumnsType<StockResult>>(() => {
  const head: ColumnsType<StockResult> = [
    {
      title: STOCK_CODE_NAME_COLUMN_TITLE,
      key: STOCK_CODE_NAME_COLUMN_KEY,
      width: 168,
      ellipsis: true,
      customRender: ({ record }) => {
        const r = record as StockResult
        return `${r.code ?? ''} ${r.name ?? ''}`.trim() || '-'
      }
    },
    { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 100, customRender: ({ text }) => formatDate(text ?? '') },
    {
      title: '联动',
      dataIndex: 'linkage_text',
      key: 'linkage_text',
      width: 200,
      ellipsis: true,
      customRender: ({ record }) =>
        h(LinkageTableCell, { record: record as StockResult })
    },
    { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' },
    { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' }
  ]
  const mid = hasMainForceResultColumns.value ? historyMainForceDenseColumns : []
  const tail: ColumnsType<StockResult> = [{ title: '操作', key: 'action', width: 70, fixed: 'right' }]
  return [...head, ...mid, ...tail]
})

function renderDay2BuyHitCell(record: StockResult): string {
  if (record.day2_buy_hit_5pct_day != null) return `第${record.day2_buy_hit_5pct_day}天`
  const day10ClosePct = record.day2_buy_10d_close_pct
  if (day10ClosePct == null) return '-'
  const sign = day10ClosePct >= 0 ? '+' : ''
  return `10日收盘 ${sign}${day10ClosePct.toFixed(2)}%`
}

/** 历史 jsonl 含主力建仓字段时展示（与 ResultsTable 一致口径） */
const hasMainForceResultColumns = computed(() => {
  const sn = String(metaInfo.value?.strategy_name || '').trim()
  if (sn === '主力建仓') return true
  return results.value.some((r) => {
    const row = r as StockResult
    return (
      row.main_force_bullish_days != null ||
      typeof row.main_force_build_tag === 'boolean' ||
      typeof row.main_force_t_limit_up_tag === 'boolean'
    )
  })
})

const historyMainForceColumns: ColumnsType<StockResult> = [
  {
    title: 'T日涨停标记',
    dataIndex: 'main_force_t_limit_up_tag',
    key: 'main_force_t_limit_up_tag',
    width: 110,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '主力建仓结构',
    dataIndex: 'main_force_build_tag',
    key: 'main_force_build_tag',
    width: 110,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '收涨个数(T-10~T)',
    dataIndex: 'main_force_bullish_days',
    key: 'main_force_bullish_days',
    width: 130,
    align: 'right',
    sorter: (a, b) => (a.main_force_bullish_days || 0) - (b.main_force_bullish_days || 0),
    customRender: ({ text }) => (text != null ? String(text) : '-')
  },
  {
    title: '斜率向上收涨数',
    dataIndex: 'main_force_slope_up_days',
    key: 'main_force_slope_up_days',
    width: 130,
    align: 'right',
    sorter: (a, b) => (a.main_force_slope_up_days || 0) - (b.main_force_slope_up_days || 0),
    customRender: ({ text }) => (text != null ? String(text) : '-')
  }
]

const historyMainForceDenseColumns: ColumnsType<StockResult> = [
  {
    title: 'T涨停',
    dataIndex: 'main_force_t_limit_up_tag',
    key: 'main_force_t_limit_up_tag',
    width: 72,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '建仓',
    dataIndex: 'main_force_build_tag',
    key: 'main_force_build_tag',
    width: 64,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '收涨数',
    dataIndex: 'main_force_bullish_days',
    key: 'main_force_bullish_days',
    width: 78,
    align: 'right',
    sorter: (a, b) => (a.main_force_bullish_days || 0) - (b.main_force_bullish_days || 0),
    customRender: ({ text }) => (text != null ? String(text) : '-')
  },
  {
    title: '斜率涨',
    dataIndex: 'main_force_slope_up_days',
    key: 'main_force_slope_up_days',
    width: 78,
    align: 'right',
    sorter: (a, b) => (a.main_force_slope_up_days || 0) - (b.main_force_slope_up_days || 0),
    customRender: ({ text }) => (text != null ? String(text) : '-')
  }
]

const historyHeadColumns: ColumnsType<StockResult> = [
  {
    title: STOCK_CODE_NAME_COLUMN_TITLE,
    key: STOCK_CODE_NAME_COLUMN_KEY,
    width: 168,
    fixed: 'left',
    ellipsis: true,
    sorter: (a, b) =>
      String(a.code || '').localeCompare(String(b.code || '')) ||
      String(a.name || '').localeCompare(String(b.name || ''))
  },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 120, customRender: ({ text }) => formatDate(text ?? '') },
  {
    title: '板块/概念联动',
    dataIndex: 'linkage_text',
    key: 'linkage_text',
    width: 280,
    ellipsis: true,
    customRender: ({ record }) =>
      h(LinkageTableCell, { record: record as StockResult })
  },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 120, customRender: ({ text }) => text ? text.toFixed(2) : '-' },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 120, customRender: ({ text }) => text ? text.toFixed(2) : '-' }
]

const historyDayColumns: ColumnsType<StockResult> = [
  {
    title: '次日涨跌',
    dataIndex: 'day2_change_pct',
    key: 'day2_change_pct',
    width: 100,
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
    width: 100,
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
  },
  {
    title: '次日买入达5%',
    dataIndex: 'day2_buy_hit_5pct_day',
    key: 'day2_buy_hit_5pct_day',
    width: 130,
    align: 'center',
    sorter: (a, b) => (a.day2_buy_hit_5pct_day || 999) - (b.day2_buy_hit_5pct_day || 999),
    customRender: ({ record }) => {
      const row = record as StockResult
      return renderDay2BuyHitCell(row)
    }
  }
]

const historyHeadDenseColumns: ColumnsType<StockResult> = [
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
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 100, sortDirections: ['ascend', 'descend'], sorter: (a, b) => ((a.match_date || '').trim()).localeCompare((b.match_date || '').trim()), customRender: ({ text }) => formatDate(text ?? '') },
  {
    title: '联动',
    dataIndex: 'linkage_text',
    key: 'linkage_text',
    width: 200,
    ellipsis: true,
    customRender: ({ record }) =>
      h(LinkageTableCell, { record: record as StockResult })
  },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 80, align: 'right', sorter: (a, b) => (a.match_price || 0) - (b.match_price || 0) },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 80, align: 'right', sorter: (a, b) => (a.current_price || 0) - (b.current_price || 0) }
]

const historyDayDenseColumns: ColumnsType<StockResult> = [
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
  },
  {
    title: '次日买入达5%',
    dataIndex: 'day2_buy_hit_5pct_day',
    key: 'day2_buy_hit_5pct_day',
    width: 110,
    align: 'center',
    sorter: (a, b) => (a.day2_buy_hit_5pct_day || 999) - (b.day2_buy_hit_5pct_day || 999),
    customRender: ({ record }) => {
      const row = record as StockResult
      return renderDay2BuyHitCell(row)
    }
  }
]

const columns = computed<ColumnsType<StockResult>>(() => {
  const mid = hasMainForceResultColumns.value ? historyMainForceColumns : []
  return [...historyHeadColumns, ...mid, ...historyDayColumns]
})

const denseColumns = computed<ColumnsType<StockResult>>(() => {
  const mid = hasMainForceResultColumns.value ? historyMainForceDenseColumns : []
  return [...historyHeadDenseColumns, ...mid, ...historyDayDenseColumns]
})

const tenDayHitStats = computed(() => {
  const rows = filteredResults.value || []
  let validCount = 0
  let hitCount = 0
  rows.forEach((item) => {
    const maxGain = item.day2_buy_10d_max_gain_pct
    if (typeof maxGain !== 'number') return
    validCount += 1
    if (maxGain > 5) hitCount += 1
  })
  const hitRate = validCount > 0 ? (hitCount / validCount) * 100 : 0
  return {
    validCount,
    hitCount,
    hitRateText: `${hitRate.toFixed(2)}%`
  }
})

const displayColumns = computed(() => {
  const cols = applyHistoryColumnPrefs(columns.value, normalPrefs.value)
  if (!isNarrowScreen.value) return cols
  return stripColumnWidths(cols)
})
const displayDenseColumns = computed(() => {
  const cols = applyHistoryColumnPrefs(denseColumns.value, densePrefs.value)
  if (!isNarrowScreen.value) return cols
  return stripColumnWidths(cols)
})

function overlapStrategyLabels(record: StockResult): string[] {
  const r = record as any
  if (Array.isArray(r.overlap_strategies) && r.overlap_strategies.length) {
    return r.overlap_strategies.map((s: unknown) => String(s).trim()).filter(Boolean)
  }
  const t = r.overlap_strategies_text || r.strategies_joined
  if (t && typeof t === 'string') {
    return t.split(/[、,，]/).map((s: string) => s.trim()).filter(Boolean)
  }
  return []
}

/** 多策略同日_*.jsonl：固定列，展示当日重叠的策略名 */
const multiStrategyOverlapColumns: ColumnsType<StockResult> = [
  {
    title: '匹配日期',
    dataIndex: 'match_date',
    key: 'match_date',
    width: 110,
    sortDirections: ['ascend', 'descend'],
    sorter: (a, b) => (String(a.match_date || '').trim()).localeCompare(String(b.match_date || '').trim()),
    customRender: ({ text }) => formatDate(text ?? '')
  },
  {
    title: STOCK_CODE_NAME_COLUMN_TITLE,
    key: STOCK_CODE_NAME_COLUMN_KEY,
    width: 160,
    ellipsis: true,
    sorter: (a, b) =>
      String(a.code || '').localeCompare(String(b.code || '')) ||
      String(a.name || '').localeCompare(String(b.name || '')),
    customRender: ({ record }) => {
      const r = record as StockResult
      return `${r.code ?? ''} ${r.name ?? ''}`.trim() || '-'
    }
  },
  {
    title: '板块/概念联动',
    dataIndex: 'linkage_text',
    key: 'linkage_text',
    width: 280,
    ellipsis: true,
    customRender: ({ record }) =>
      h(LinkageTableCell, { record: record as StockResult })
  },
  {
    title: '命中策略数',
    dataIndex: 'strategy_count',
    key: 'strategy_count',
    width: 105,
    align: 'right',
    sorter: (a, b) => ((a as any).strategy_count ?? 0) - ((b as any).strategy_count ?? 0),
    customRender: ({ text }) => (text != null && text !== '' ? String(text) : '-')
  },
  { title: '重叠策略', key: 'overlap_strategies', width: 300 },
  { title: '说明', key: 'overlap_summary', width: 220 }
]

const listTableColumns = computed<ColumnsType<StockResult>>(() => {
  if (isMultiStrategyOverlapFile.value) {
    return isNarrowScreen.value ? stripColumnWidths(multiStrategyOverlapColumns) : multiStrategyOverlapColumns
  }
  return isDense.value ? displayDenseColumns.value : displayColumns.value
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
    tooltip: { trigger: 'axis' },
    grid: { left: 42, right: 12, top: 8, bottom: 36 },
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

/* 当日涨幅或振幅最高：左侧强调条 + 浅琥珀底 */
:deep(.row-day-leader) {
  border-left: 3px solid #f59e0b !important;
  background-color: #fffbeb !important;
}
:deep(.row-day-leader.bg-red-50) {
  background-color: #fef3e8 !important;
}
:deep(.row-day-leader.bg-green-50) {
  background-color: #f0fdf0 !important;
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
  margin: 1px 2px;
  padding: 4px 6px;
  height: auto;
  min-height: 40px;
  line-height: 1.35;
  border-radius: 3px;
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

/* 主力建仓：置顶项单独绿色系，与其它战法橙色区分 */
:deep(.strategy-file.strategy-file-main-force) {
  background-color: #f6ffed !important;
  border-left-color: #52c41a !important;
}

:deep(.strategy-file.strategy-file-main-force:hover) {
  background-color: #d9f7be !important;
}

:deep(.strategy-file.strategy-file-main-force.ant-menu-item-selected) {
  background-color: #b7eb8f !important;
  border-color: #52c41a !important;
}

/* 文件菜单项样式 */
.file-menu-item {
  width: 100%;
}

.file-menu-item :deep(.ant-menu-title-content) {
  width: 100%;
}
</style>