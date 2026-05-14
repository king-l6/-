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
        <Switch
          v-if="hasResults"
          v-model:checked="onlyMainForceTag"
          size="small"
          checked-children="T日涨停"
          un-checked-children="全部"
        />
        <InputNumber
          v-if="hasResults && currentStrategyName !== '连阳超五无涨停'"
          v-model:value="minMainForceBullishDays"
          :min="0"
          :max="11"
          size="small"
          style="width: 140px"
          placeholder="收涨日数>="
        />
        <InputNumber
          v-if="hasResults"
          v-model:value="minConsecutiveUpDays"
          :min="1"
          :max="30"
          size="small"
          style="width: 140px"
          placeholder="连阳天数>="
        />
        <InputNumber
          v-if="hasResults"
          v-model:value="minUpperShadowPct"
          :min="0"
          :step="0.1"
          :precision="2"
          size="small"
          style="width: 160px"
          placeholder="上影线幅度>%"
        />
        <Switch
          v-if="hasResults"
          v-model:checked="onlyConsecutiveLimitTouch"
          size="small"
          checked-children="连阳触板"
          un-checked-children="连阳不限"
        />
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
    
    <div v-else-if="!hasResults" class="py-6 text-center text-gray-500 dark:text-neutral-400">
      暂无回测结果
    </div>
    
    <div v-else>
      <!-- 我的自选：放在表格上方，添加后一眼能看到 -->
      <Card :title="`我的自选${collectedItems.length > 0 ? ` (${collectedItems.length} 条)` : ''}`" class="mb-3" size="small">
        <div v-if="collectedItems.length === 0" class="py-2 text-center text-sm text-gray-500 dark:text-neutral-400">
          点击下方表格「代码·名称」列前的 ＋ 图标可加入自选
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

      <div class="mb-2 flex items-center justify-between">
        <div class="text-sm font-semibold text-primary">
          找到 {{ filteredResults.length }} 只符合条件的股票
        </div>
        <div class="text-xs text-slate-600" v-if="tenDayHitStats.validCount > 0">
          次日开盘买入后10个交易日内最高涨幅&gt;5%：
          <span class="font-semibold text-rose-600">
            {{ tenDayHitStats.hitCount }}/{{ tenDayHitStats.validCount }} ({{ tenDayHitStats.hitRateText }})
          </span>
        </div>
      </div>
      
      <Table
        :columns="isDense ? denseColumns : columns"
        :data-source="filteredResults"
        :loading="loading"
        :pagination="isDense ? paginationDense : paginationNormal"
        @change="(pag: any) => onTableChange(pag)"
        :row-key="(record) => `${record.code}-${record.match_date || ''}`"
        :scroll="isDense ? { x: 'max-content', y: 600 } : { x: 'max-content' }"
        :size="isDense ? 'small' : 'middle'"
        :bordered="isDense"
        :row-class-name="isDense ? getRowClassName : undefined"
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
        </template>
      </Table>
    </div>
    <ConsecutiveUpVolumeChartModal v-model:open="volumeChartOpen" :record="volumeChartRecord" />
  </Card>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, h, watch } from 'vue'
import { Card, Table, Spin, Alert, Button, Space, Input, InputNumber, Switch, message } from 'ant-design-vue'
import { SearchOutlined, PlusOutlined } from '@ant-design/icons-vue'
import { useStrategyStore } from '@/store/modules/strategy'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult } from '@/types'
import { formatLinkageTableCell } from '@/utils/linkageDisplay'
import LinkageTableCell from '@/components/LinkageTableCell.vue'
import ConsecutiveUpVolumeChartModal from '@/components/ConsecutiveUpVolumeChartModal.vue'
import {
  STOCK_CODE_NAME_COLUMN_KEY,
  STOCK_CODE_NAME_COLUMN_TITLE
} from '@/constants/stockTable'

const strategyStore = useStrategyStore()
const currentStrategyName = computed(() => String(strategyStore.strategyName || '').trim())

const loading = computed(() => strategyStore.loading)
const error = computed(() => strategyStore.error)
const results = computed(() => strategyStore.results)
const hasResults = computed(() => strategyStore.hasResults)

const volumeChartOpen = ref(false)
const volumeChartRecord = ref<StockResult | null>(null)
function openConsecutiveUpVolumeChart(record: StockResult) {
  volumeChartRecord.value = record
  volumeChartOpen.value = true
}

const isDense = ref(true)
const searchText = ref('')
const onlyMainForceTag = ref(false)
const minMainForceBullishDays = ref<number | null>(null)
const minConsecutiveUpDays = ref<number | null>(3)
const minUpperShadowPct = ref<number | null>(2)
const onlyConsecutiveLimitTouch = ref(false)

watch(currentStrategyName, (name) => {
  if (name === '连阳超五无涨停') {
    minMainForceBullishDays.value = null
    minConsecutiveUpDays.value = null
  }
})
const STORAGE_KEY_FAVORITES = 'backtest-favorites'
const collectedItems = ref<StockResult[]>([])

function loadFavoritesFromStorage() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY_FAVORITES)
    if (raw) {
      const parsed = JSON.parse(raw) as StockResult[]
      if (Array.isArray(parsed)) collectedItems.value = parsed
    }
  } catch (_) {}
}

function saveFavoritesToStorage() {
  try {
    localStorage.setItem(STORAGE_KEY_FAVORITES, JSON.stringify(collectedItems.value))
  } catch (_) {}
}

const paginationDense = ref({
  current: 1,
  pageSize: 200,
  showSizeChanger: true,
  pageSizeOptions: ['20', '50', '100', '200'],
  showTotal: (total: number) => `共 ${total} 条`,
  showQuickJumper: true
})
const paginationNormal = ref({
  current: 1,
  pageSize: 200,
  showSizeChanger: true,
  showTotal: (total: number) => `共 ${total} 条`
})

function onTableChange(pag: { current?: number; pageSize?: number }) {
  if (!pag) return
  if (isDense.value) {
    paginationDense.value = { ...paginationDense.value, ...pag }
  } else {
    paginationNormal.value = { ...paginationNormal.value, ...pag }
  }
}

const collectedColumns: ColumnsType<StockResult> = [
  {
    title: STOCK_CODE_NAME_COLUMN_TITLE,
    key: STOCK_CODE_NAME_COLUMN_KEY,
    width: 160,
    ellipsis: true,
    customRender: ({ record }) => {
      const r = record as StockResult
      return `${r.code ?? ''} ${r.name ?? ''}`.trim() || '-'
    }
  },
  { title: '匹配日期', dataIndex: 'match_date', key: 'match_date', width: 100 },
  { title: '匹配价', dataIndex: 'match_price', key: 'match_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' },
  { title: '当前价', dataIndex: 'current_price', key: 'current_price', width: 80, customRender: ({ text }) => text != null ? text.toFixed(2) : '-' },
  { title: '操作', key: 'action', width: 70, fixed: 'right' }
]

function handleAddToCollection(record: StockResult) {
  const key = `${record.code}-${record.match_date || ''}`
  if (collectedItems.value.some(r => `${r.code}-${r.match_date || ''}` === key)) {
    message.info('已在自选中，未重复添加')
    return
  }
  collectedItems.value = [...collectedItems.value, { ...record }]
  saveFavoritesToStorage()
  message.success(`已加入自选：${record.name} (${record.code})`)
}

function handleRemove(record: StockResult) {
  const key = `${record.code}-${record.match_date || ''}`
  collectedItems.value = collectedItems.value.filter(r => `${r.code}-${r.match_date || ''}` !== key)
  saveFavoritesToStorage()
}

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
  if (onlyMainForceTag.value) {
    sourceData = sourceData.filter(item => !!item.main_force_t_limit_up_tag)
  }
  if (minMainForceBullishDays.value != null && currentStrategyName.value !== '连阳超五无涨停') {
    sourceData = sourceData.filter(item => (item.main_force_bullish_days ?? 0) >= minMainForceBullishDays.value!)
  }
  if (minConsecutiveUpDays.value != null) {
    sourceData = sourceData.filter(item => (item.consecutive_up_days ?? 0) >= minConsecutiveUpDays.value!)
  }
  if (minUpperShadowPct.value != null) {
    sourceData = sourceData.filter(item => (item.upper_shadow_pct ?? 0) > minUpperShadowPct.value!)
  }
  if (onlyConsecutiveLimitTouch.value) {
    sourceData = sourceData.filter(item => !!item.consecutive_up_has_limit_touch)
  }
  // 返回数组的副本，避免 Table 组件修改原始数据
  return [...sourceData]
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

function renderDay2BuyHitCell(record: StockResult): string {
  if (record.day2_buy_hit_5pct_day != null) return `第${record.day2_buy_hit_5pct_day}天`
  const day10ClosePct = record.day2_buy_10d_close_pct
  if (day10ClosePct == null) return '-'
  const sign = day10ClosePct >= 0 ? '+' : ''
  return `10日收盘 ${sign}${day10ClosePct.toFixed(2)}%`
}

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
    width: 120,
    customRender: ({ text }) => formatDate(text ?? '')
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
    title: '匹配价',
    dataIndex: 'match_price',
    key: 'match_price',
    width: 120,
    customRender: ({ text }) => text ? text.toFixed(2) : '-'
  },
  {
    title: '当前价',
    dataIndex: 'current_price',
    key: 'current_price',
    width: 120,
    customRender: ({ text }) => text ? text.toFixed(2) : '-'
  },
  {
    title: 'T日涨停标记',
    dataIndex: 'main_force_t_limit_up_tag',
    key: 'main_force_t_limit_up_tag',
    width: 120,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '主力建仓结构',
    dataIndex: 'main_force_build_tag',
    key: 'main_force_build_tag',
    width: 100,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '收涨个数',
    dataIndex: 'main_force_bullish_days',
    key: 'main_force_bullish_days',
    width: 100,
    align: 'right',
    sorter: (a, b) => (a.main_force_bullish_days || 0) - (b.main_force_bullish_days || 0)
  },
  {
    title: '连阳天数',
    dataIndex: 'consecutive_up_days',
    key: 'consecutive_up_days',
    width: 100,
    align: 'right',
    sorter: (a, b) => (a.consecutive_up_days || 0) - (b.consecutive_up_days || 0)
  },
  {
    title: '连涨量图',
    key: 'consecutive_up_volume_chart',
    width: 92,
    align: 'center',
    customRender: ({ record }) => {
      const r = record as StockResult
      if ((r.consecutive_up_days ?? 0) < 2) return '-'
      return h(
        Button,
        { type: 'link', size: 'small', onClick: () => openConsecutiveUpVolumeChart(r) },
        () => '趋势'
      )
    }
  },
  {
    title: '上影线幅度%',
    dataIndex: 'upper_shadow_pct',
    key: 'upper_shadow_pct',
    width: 120,
    align: 'right',
    sorter: (a, b) => (a.upper_shadow_pct || 0) - (b.upper_shadow_pct || 0),
    customRender: ({ text }) => (text != null ? text.toFixed(2) : '-')
  },
  {
    title: '连阳触板',
    dataIndex: 'consecutive_up_has_limit_touch',
    key: 'consecutive_up_has_limit_touch',
    width: 100,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
  },
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
    width: 120,
    align: 'center',
    sorter: (a, b) => (a.day2_buy_hit_5pct_day || 999) - (b.day2_buy_hit_5pct_day || 999),
    customRender: ({ record }) => {
      const row = record as StockResult
      return renderDay2BuyHitCell(row)
    }
  }
]

const denseColumns: ColumnsType<StockResult> = [
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
    title: '联动',
    dataIndex: 'linkage_text',
    key: 'linkage_text',
    width: 200,
    ellipsis: true,
    customRender: ({ record }) =>
      h(LinkageTableCell, { record: record as StockResult })
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
    title: 'T日涨停标记',
    dataIndex: 'main_force_t_limit_up_tag',
    key: 'main_force_t_limit_up_tag',
    width: 120,
    align: 'center',
    filters: [
      { text: '是', value: true },
      { text: '否', value: false }
    ],
    onFilter: (value, record) => Boolean(record.main_force_t_limit_up_tag) === Boolean(value),
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '主力建仓结构',
    dataIndex: 'main_force_build_tag',
    key: 'main_force_build_tag',
    width: 90,
    align: 'center',
    filters: [
      { text: '是', value: true },
      { text: '否', value: false }
    ],
    onFilter: (value, record) => Boolean(record.main_force_build_tag) === Boolean(value),
    customRender: ({ text }) => (text ? '是' : '否')
  },
  {
    title: '收涨个数',
    dataIndex: 'main_force_bullish_days',
    key: 'main_force_bullish_days',
    width: 90,
    align: 'right',
    sorter: (a, b) => (a.main_force_bullish_days || 0) - (b.main_force_bullish_days || 0)
  },
  {
    title: '连阳',
    dataIndex: 'consecutive_up_days',
    key: 'consecutive_up_days',
    width: 70,
    align: 'right',
    sorter: (a, b) => (a.consecutive_up_days || 0) - (b.consecutive_up_days || 0)
  },
  {
    title: '量图',
    key: 'consecutive_up_volume_chart',
    width: 64,
    align: 'center',
    customRender: ({ record }) => {
      const r = record as StockResult
      if ((r.consecutive_up_days ?? 0) < 2) return '-'
      return h(
        Button,
        { type: 'link', size: 'small', onClick: () => openConsecutiveUpVolumeChart(r) },
        () => '图'
      )
    }
  },
  {
    title: '上影%',
    dataIndex: 'upper_shadow_pct',
    key: 'upper_shadow_pct',
    width: 80,
    align: 'right',
    sorter: (a, b) => (a.upper_shadow_pct || 0) - (b.upper_shadow_pct || 0),
    customRender: ({ text }) => (text != null ? text.toFixed(2) : '-')
  },
  {
    title: '触板',
    dataIndex: 'consecutive_up_has_limit_touch',
    key: 'consecutive_up_has_limit_touch',
    width: 70,
    align: 'center',
    customRender: ({ text }) => (text ? '是' : '否')
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
  },
  {
    title: '次日买入达5%',
    dataIndex: 'day2_buy_hit_5pct_day',
    key: 'day2_buy_hit_5pct_day',
    width: 95,
    fixed: 'right',
    align: 'center',
    sorter: (a, b) => (a.day2_buy_hit_5pct_day || 999) - (b.day2_buy_hit_5pct_day || 999),
    customRender: ({ record }) => {
      const row = record as StockResult
      return renderDay2BuyHitCell(row)
    }
  }
]

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
  const headers = ['代码·名称', '匹配日期', '板块概念联动', '匹配价', '当前价', '连阳天数', '上影线幅度%', '连阳触板', '次日涨跌%', '第三日涨跌%', 'T日涨停标记', '主力建仓结构', '收涨个数', '斜率向上收涨个数']
  const rows = sortedData.map(item => [
    `${item.code ?? ''} ${item.name ?? ''}`.trim(),
    formatDate(item.match_date),
    formatLinkageTableCell(item),
    item.match_price?.toFixed(2) || '',
    item.current_price?.toFixed(2) || '',
    item.consecutive_up_days ?? '',
    item.upper_shadow_pct != null ? item.upper_shadow_pct.toFixed(2) : '',
    item.consecutive_up_has_limit_touch ? '是' : '否',
    item.day2_change_pct != null ? item.day2_change_pct.toFixed(2) : '',
    item.day3_change_pct != null ? item.day3_change_pct.toFixed(2) : '',
    item.main_force_t_limit_up_tag ? '是' : '否',
    item.main_force_build_tag ? '是' : '否',
    item.main_force_bullish_days ?? '',
    item.main_force_slope_up_days ?? ''
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

onMounted(() => {
  loadFavoritesFromStorage()
})
</script>

<style scoped>
::deep(.ant-table-small) {
  font-size: 12px;
}

::deep(.ant-table-small .ant-table-thead > tr > th) {
  padding: 8px 4px;
  font-size: 12px;
  font-weight: 600;
}

::deep(.ant-table-small .ant-table-tbody > tr > td) {
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

