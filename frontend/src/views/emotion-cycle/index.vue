<template>
  <div class="space-y-3">
    <Card size="small">
      <div class="flex flex-wrap items-center gap-2">
        <InputNumber v-model:value="days" :min="30" :max="500" :step="10" class="w-[140px]" />
        <Button type="primary" :loading="loading" @click="loadData()">刷新情绪周期</Button>
        <Button :loading="loading" @click="loadData(true)">强制全量合并</Button>
        <Button :loading="healthLoading" @click="loadHealth">数据自检</Button>
        <Button type="dashed" :loading="cacheTaskStarting" @click="startCacheTask">一键补缓存</Button>
      </div>
      <div class="mt-2 text-xs text-gray-500 dark:text-neutral-400">
        `days` 为按天温度图窗口。按日序列来自本地滚动缓存（股票日线缓存文件变更时重读并合并）。
      </div>
    </Card>

    <Alert v-if="errorMsg" type="error" :message="errorMsg" show-icon />
    <Alert v-if="healthHint" type="warning" :message="healthHint" show-icon />

    <Spin :spinning="loading">
      <div v-if="report" class="space-y-3">
        <Card size="small" title="数据状态">
          <div class="grid grid-cols-1 md:grid-cols-3 gap-2 text-sm">
            <div class="rounded bg-gray-50 p-2 dark:bg-neutral-900/70 dark:text-neutral-200">缓存文件数：{{ health?.cache_file_count ?? 0 }}</div>
            <div class="rounded bg-gray-50 p-2 dark:bg-neutral-900/70 dark:text-neutral-200">缓存最新日：{{ health?.latest_date || '-' }}</div>
            <div class="rounded bg-gray-50 p-2 dark:bg-neutral-900/70 dark:text-neutral-200">当日样本数：{{ health?.latest_snapshot_size ?? 0 }}</div>
            <div v-if="report.timeline_rolling_meta" class="rounded bg-gray-50 p-2 md:col-span-4 dark:bg-neutral-900/70 dark:text-neutral-200">
              温度图合并：本次重读 {{ report.timeline_rolling_meta.files_merged }} 个股票缓存文件，跳过
              {{ report.timeline_rolling_meta.files_skipped }} 个；滚动表保留约
              {{ report.timeline_rolling_meta.dates_in_store }} 个交易日截面
            </div>
          </div>
          <div class="mt-2 rounded bg-slate-50 p-2 text-xs text-slate-700 dark:bg-neutral-900/80 dark:text-neutral-200">
            <div>
              补缓存任务：
              <span class="font-medium">{{ cacheTaskStatusText }}</span>
              <span v-if="cacheTask?.progress" class="ml-2">
                进度 {{ cacheTask.progress.current }}/{{ cacheTask.progress.total }} ({{ cacheTask.progress.percent }}%)
              </span>
            </div>
            <div v-if="cacheTaskLogLine" class="mt-1 truncate text-slate-500 dark:text-neutral-400">{{ cacheTaskLogLine }}</div>
          </div>
          <div class="mt-2 text-xs text-gray-500 dark:text-neutral-400">
            可用代码样例：
            <template v-if="(health?.sample_stocks?.length || 0) > 0">
              <span v-for="s in health?.sample_stocks || []" :key="s.code" class="mr-2">
                {{ s.code }}{{ s.name ? ` ${s.name}` : '' }}
              </span>
            </template>
            <template v-else>暂无</template>
          </div>
        </Card>

        <Card size="small">
          <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div class="rounded bg-gray-50 p-3 dark:bg-neutral-900/70">
              <div class="text-xs text-gray-500 dark:text-neutral-400">交易日</div>
              <div class="text-base font-semibold">{{ report.date }}</div>
            </div>
            <div class="rounded bg-blue-50 p-3 dark:bg-blue-950/40">
              <div class="text-xs text-gray-500 dark:text-neutral-400">市场温度分（= 综合情绪分）</div>
              <div class="text-base font-semibold text-blue-600 dark:text-blue-300">{{ report.scores.market_score }}</div>
            </div>
            <div class="rounded bg-orange-50 p-3 dark:bg-orange-950/35">
              <div class="text-xs text-gray-500 dark:text-neutral-400">周期判定</div>
              <div class="text-base font-semibold text-orange-600 dark:text-orange-300">
                {{ report.scores.total_score }} / {{ report.cycle }}
              </div>
            </div>
          </div>
          <Alert type="info" show-icon class="mt-3" message="分数含义">
            <template #description>
              <div class="space-y-2 text-sm text-gray-700 dark:text-neutral-200">
                <p>
                  当前<strong>仅按全市场主板缓存截面</strong>计算 0～100 的温度分：综合涨停占比、强势（≥5%）占比、大跌（≤-5%）占比与平均涨跌幅（详见下方「按天情绪温度」图例）。不再使用自选「代表票锚点」加权，避免小样本与公式截断造成误解。
                </p>
              </div>
            </template>
          </Alert>
        </Card>

        <Card size="small" title="市场温度指标">
          <div class="grid grid-cols-2 md:grid-cols-5 gap-2 text-sm">
            <div class="rounded bg-gray-50 p-2 dark:bg-neutral-900/70 dark:text-neutral-200">样本数：{{ report.market_metrics.total }}</div>
            <div class="rounded bg-red-50 p-2 dark:bg-red-950/30 dark:text-neutral-200">
              涨停：{{ report.market_metrics.limit_up_count }} ({{ report.market_metrics.limit_up_ratio_pct }}%)
            </div>
            <div class="rounded bg-emerald-50 p-2 dark:bg-emerald-950/25 dark:text-neutral-200">
              强势(>=5%)：{{ report.market_metrics.strong_count }} ({{ report.market_metrics.strong_ratio_pct }}%)
            </div>
            <div class="rounded bg-rose-50 p-2 dark:bg-rose-950/30 dark:text-neutral-200">
              大跌(<=-5%)：{{ report.market_metrics.big_drop_count }} ({{ report.market_metrics.big_drop_ratio_pct }}%)
            </div>
            <div class="rounded bg-blue-50 p-2 dark:bg-blue-950/35 dark:text-neutral-200">均涨跌幅：{{ report.market_metrics.avg_pct_change }}%</div>
          </div>
        </Card>

        <Card size="small" title="按天情绪温度（总分）">
          <Alert
            type="info"
            show-icon
            class="mb-3"
            message="分数说明"
            description="按日曲线为全市场涨停率、强势占比、大跌占比与均涨跌幅合成的 0～100 温度分；周期标签由该分逐日判定。"
          />
          <div class="relative w-full h-[360px]">
            <div :ref="setTempChartRef" class="w-full h-full" />
          </div>
        </Card>

        <Card size="small" title="市场龙头周期（全市场按日）">
          <Alert type="info" show-icon class="mb-3" :message="leaderNote" />
          <div v-if="leaderSegments.length === 0" class="mb-3 text-sm text-gray-500 dark:text-neutral-400">
            窗口内无连续主线龙头（常见于涨停家数极少或缓存截面不足）。
          </div>
          <Table
            v-else
            :columns="leaderSegmentColumns"
            :data-source="leaderSegments"
            :pagination="false"
            :row-key="(r: { start_date: string; code: string }) => `${r.start_date}-${r.code}`"
            size="small"
            class="mb-3"
          />
          <div class="mb-1 text-xs text-gray-500 dark:text-neutral-400">
            横轴为交易日；纵轴在「全窗口天数 + 近30日主线条数 + 末次出现日」综合排序下多留几行独立标的；仍未进前排的日主线落在「其它」，且<strong>按股票代码分色</strong>（一日一格颜色可辨轮动，不再整条同色）。主板涨停线约9.8%，300/301/688/689约20cm线；主线按连板数优先，同连板比收盘×成交量再比涨幅。
          </div>
          <div class="relative w-full h-[400px]">
            <div :ref="setLeaderChartRef" class="w-full h-full" />
          </div>
        </Card>
      </div>
    </Spin>
  </div>
</template>

<script lang="ts">
export default { name: 'EmotionCycle' }
</script>
<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'
import { Alert, Button, Card, InputNumber, Spin, Table } from 'ant-design-vue'
import type { TableColumnsType } from 'ant-design-vue'
import * as echarts from 'echarts'
import { getEmotionCycle, getEmotionCycleHealth, getCacheUpdateTaskStatus, startCacheUpdateTask } from '@/api'
import type { CacheUpdateTaskStatus, EmotionCycleHealth, EmotionCycleReport } from '@/types'
import {
  STOCK_CODE_NAME_COLUMN_KEY,
  STOCK_CODE_NAME_COLUMN_TITLE
} from '@/constants/stockTable'

const loading = ref(false)
const healthLoading = ref(false)
const errorMsg = ref('')
const healthHint = ref('')
const report = ref<EmotionCycleReport | null>(null)
const health = ref<EmotionCycleHealth | null>(null)
const cacheTask = ref<CacheUpdateTaskStatus | null>(null)
const cacheTaskStarting = ref(false)
let cacheTaskTimer: number | null = null
const days = ref(180)
let tempChart: echarts.ECharts | null = null
let leaderChart: echarts.ECharts | null = null

const leaderRotation = computed(() => report.value?.market_leader_rotation)
const leaderSegments = computed(() => leaderRotation.value?.segments || [])
const leaderNote = computed(() => leaderRotation.value?.note || '')
const leaderDaily = computed(() => leaderRotation.value?.daily || [])
const cacheTaskLogLine = computed(() => {
  const lines = cacheTask.value?.last_lines || []
  return lines.length > 0 ? lines[lines.length - 1] : ''
})
const cacheTaskStatusText = computed(() => {
  if (!cacheTask.value) return '未启动'
  if (cacheTask.value.running) return '运行中'
  if (cacheTask.value.error) return `失败: ${cacheTask.value.error}`
  if (cacheTask.value.exit_code === 0) return '已完成'
  if (cacheTask.value.exit_code != null) return `异常退出(${cacheTask.value.exit_code})`
  return '未启动'
})

const leaderSegmentColumns: TableColumnsType = [
  {
    title: STOCK_CODE_NAME_COLUMN_TITLE,
    key: STOCK_CODE_NAME_COLUMN_KEY,
    width: 168,
    ellipsis: true,
    customRender: ({ record }: { record: { code?: string; name?: string } }) =>
      `${record?.code ?? ''} ${record?.name ?? ''}`.trim() || '—'
  },
  { title: '段内最高连板', dataIndex: 'max_consecutive_boards', key: 'max_consecutive_boards', width: 120 },
  { title: '开始', dataIndex: 'start_date', key: 'start_date' },
  { title: '结束', dataIndex: 'end_date', key: 'end_date' },
  { title: '交易日数', dataIndex: 'days', key: 'days', width: 96 }
]

/** 「其它」行：按代码哈希到 HSL，避免多日不同票却连成单色长条 */
function heatColorForOtherStock(code: string): string {
  let h = 0
  for (let i = 0; i < code.length; i++) h = (Math.imul(31, h) + code.charCodeAt(i)) | 0
  const hue = Math.abs(h) % 360
  return `hsl(${hue}, 62%, 46%)`
}

const LEADER_ROW_COLORS = [
  '#ea580c',
  '#2563eb',
  '#059669',
  '#7c3aed',
  '#db2777',
  '#0d9488',
  '#ca8a04',
  '#4f46e5',
  '#b91c1c',
  '#0f766e',
  '#a16207',
  '#1e40af',
  '#6b21a8',
  '#9d174d',
  '#78716c'
]

function resizeChartsSoon() {
  void nextTick(() => {
    requestAnimationFrame(() => {
      tempChart?.resize()
      leaderChart?.resize()
    })
  })
}

function setTempChartRef(el: unknown) {
  const div = el as HTMLDivElement | null
  if (!div) return
  if (!tempChart) tempChart = echarts.init(div)
  renderTempChart()
  resizeChartsSoon()
}

function setLeaderChartRef(el: unknown) {
  const div = el as HTMLDivElement | null
  if (!div) return
  if (!leaderChart) leaderChart = echarts.init(div)
  renderLeaderChart()
  resizeChartsSoon()
}

function renderTempChart() {
  if (!tempChart || !report.value) return
  const tl = report.value.timeline || []
  if (!tl.length) {
    tempChart.setOption(
      {
        title: {
          text: '暂无按日温度数据（请先「一键补缓存」或检查股票缓存目录）',
          left: 'center',
          top: 'middle',
          textStyle: { fontSize: 14, color: '#64748b' }
        },
        xAxis: { show: false },
        yAxis: { show: false },
        series: []
      },
      { notMerge: true }
    )
    resizeChartsSoon()
    return
  }
  tempChart.setOption(
    {
      title: { show: false },
      tooltip: { trigger: 'axis' },
      legend: { data: ['市场温度分'] },
      xAxis: { type: 'category', data: tl.map((x) => x.date) },
      yAxis: { type: 'value', min: 0, max: 100 },
      series: [
        {
          name: '市场温度分',
          type: 'line',
          smooth: true,
          data: tl.map((x) => x.total_score),
          areaStyle: {}
        }
      ]
    },
    { notMerge: true }
  )
  resizeChartsSoon()
}

function renderLeaderChart() {
  if (!leaderChart || !report.value) return
  const daily = leaderDaily.value
  if (!daily.length) {
    leaderChart.clear()
    resizeChartsSoon()
    return
  }
  const xDates = daily.map((d) => d.date)
  const cnt = new Map<string, number>()
  const labelByCode = new Map<string, string>()
  const lastAppear = new Map<string, number>()
  daily.forEach((row, i) => {
    const t = row.top1
    if (t?.code) {
      cnt.set(t.code, (cnt.get(t.code) || 0) + 1)
      lastAppear.set(t.code, i)
      const raw = t.name ? `${t.code} ${t.name}` : t.code
      labelByCode.set(t.code, raw.length > 18 ? `${raw.slice(0, 16)}…` : raw)
    }
  })
  /** 近窗口主线次数 + 末次出现日（同日数时让「刚出现过」的票优先占行） */
  const RECENT_TRADING_DAYS = 30
  const MAX_Y_STOCK_ROWS = 22
  const HEAD_BY_FULL_WINDOW = 9
  const recentCnt = new Map<string, number>()
  const i0 = Math.max(0, daily.length - RECENT_TRADING_DAYS)
  for (let i = i0; i < daily.length; i++) {
    const c = daily[i].top1?.code
    if (c) recentCnt.set(c, (recentCnt.get(c) || 0) + 1)
  }
  const fullSorted = [...cnt.entries()].sort((a, b) => b[1] - a[1])
  const recentSorted = [...recentCnt.entries()].sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1]
    return (lastAppear.get(b[0]) ?? -1) - (lastAppear.get(a[0]) ?? -1)
  })
  const codes: string[] = []
  const picked = new Set<string>()
  for (const [code] of fullSorted) {
    if (codes.length >= HEAD_BY_FULL_WINDOW) break
    codes.push(code)
    picked.add(code)
  }
  for (const [code] of recentSorted) {
    if (codes.length >= MAX_Y_STOCK_ROWS) break
    if (picked.has(code)) continue
    codes.push(code)
    picked.add(code)
  }
  for (const [code] of fullSorted) {
    if (codes.length >= MAX_Y_STOCK_ROWS) break
    if (picked.has(code)) continue
    codes.push(code)
    picked.add(code)
  }
  const byLastDay = [...cnt.keys()]
    .filter((code) => !picked.has(code))
    .sort((a, b) => (lastAppear.get(b) ?? -1) - (lastAppear.get(a) ?? -1))
  for (const code of byLastDay) {
    if (codes.length >= MAX_Y_STOCK_ROWS) break
    codes.push(code)
    picked.add(code)
  }
  const yLabels = codes.map((code) => labelByCode.get(code) || code)
  codes.push('__other__')
  yLabels.push('其它（按票分色）')
  const heatData: { value: [number, number, number]; itemStyle: { color: string } }[] = []
  const otherYi = codes.length - 1
  daily.forEach((row, xi) => {
    const c = row.top1?.code
    if (!c) return
    let yi = codes.indexOf(c)
    const isOther = yi < 0
    if (isOther) yi = otherYi
    const color = isOther ? heatColorForOtherStock(c) : LEADER_ROW_COLORS[yi % LEADER_ROW_COLORS.length]
    heatData.push({ value: [xi, yi, 1], itemStyle: { color } })
  })
  if (heatData.length === 0) {
    leaderChart.setOption(
      {
        title: {
          text: '本窗口无主线（无满足涨停线的标的）',
          left: 'center',
          top: 'middle',
          textStyle: { fontSize: 14, color: '#64748b' }
        },
        xAxis: { type: 'category', data: xDates, show: false },
        yAxis: { type: 'value', show: false },
        series: []
      },
      { notMerge: true }
    )
    resizeChartsSoon()
    return
  }
  leaderChart.setOption(
    {
    title: { show: false },
    tooltip: {
      position: 'top',
      formatter: (p: { value?: number[]; data?: { value?: number[] } }) => {
        const raw = (Array.isArray(p.value) ? p.value : p.data?.value) as number[] | undefined
        const xi = raw && raw.length ? raw[0] : -1
        const row = xi >= 0 ? daily[xi] : null
        if (!row) return ''
        const tops = (row.leaders || [])
          .slice(0, 5)
          .map(
            (l) =>
              `${l.code}${l.name ? ` ${l.name}` : ''} ${l.consecutive_boards ?? '?'}连 ${l.pct_change}% 量:${l.volume}`
          )
          .join('<br/>')
        return `<div><div><strong>${row.date}</strong></div><div>涨停家数≈${row.limit_up_count}</div><div>${tops || '无近似涨停'}</div></div>`
      }
    },
    grid: { left: '14%', right: '4%', top: 28, bottom: 48 },
    xAxis: {
      type: 'category',
      data: xDates,
      splitArea: { show: true },
      axisLabel: { rotate: xDates.length > 40 ? 45 : 0, fontSize: 10 }
    },
    yAxis: { type: 'category', data: yLabels, splitArea: { show: true }, axisLabel: { fontSize: 10 } },
    visualMap: { show: false, min: 0, max: 1, calculable: false },
    series: [
      {
        name: '主线龙头',
        type: 'heatmap',
        data: heatData,
        label: { show: false },
        emphasis: { itemStyle: { shadowBlur: 6, shadowColor: 'rgba(0,0,0,0.35)' } }
      }
    ]
    },
    { notMerge: true }
  )
  resizeChartsSoon()
}

async function loadData(forceRefresh = false) {
  loading.value = true
  errorMsg.value = ''
  try {
    const res = await getEmotionCycle({
      days: days.value,
      force_refresh: forceRefresh
    })
    if (!res.success || !res.data) {
      throw new Error(res.error || '获取情绪周期失败')
    }
    report.value = res.data
    if ((res.data.timeline?.length || 0) === 0) {
      healthHint.value = '按天温度序列为空，请先补缓存数据。'
    } else {
      healthHint.value = ''
    }
    await nextTick()
    renderTempChart()
    renderLeaderChart()
    resizeChartsSoon()
  } catch (err) {
    errorMsg.value = err instanceof Error ? err.message : '请求失败'
  } finally {
    loading.value = false
    resizeChartsSoon()
  }
}

async function loadHealth() {
  healthLoading.value = true
  try {
    const res = await getEmotionCycleHealth()
    if (!res.success || !res.data) {
      throw new Error(res.error || '获取数据自检失败')
    }
    health.value = res.data
  } catch (err) {
    healthHint.value = err instanceof Error ? err.message : '数据自检失败'
  } finally {
    healthLoading.value = false
  }
}

async function pollCacheTaskStatus() {
  try {
    const res = await getCacheUpdateTaskStatus()
    if (res.success && res.data) {
      const task = ('task' in res.data ? res.data.task : res.data) as CacheUpdateTaskStatus
      cacheTask.value = task
      if (!task.running && cacheTaskTimer != null) {
        window.clearInterval(cacheTaskTimer)
        cacheTaskTimer = null
        // 任务结束后自动刷新一次
        loadHealth()
        loadData()
      }
    }
  } catch {
    // ignore
  }
}

function ensureCacheTaskPolling() {
  if (cacheTaskTimer != null) return
  cacheTaskTimer = window.setInterval(() => {
    pollCacheTaskStatus()
  }, 2000)
}

/** 仅当后台任务在跑时才轮询，避免无任务时每 2 秒打 /api/cache-update/status */
async function pollCacheTaskStatusOnce() {
  try {
    const res = await getCacheUpdateTaskStatus()
    if (!res.success || !res.data) return
    const task = ('task' in res.data ? res.data.task : res.data) as CacheUpdateTaskStatus
    cacheTask.value = task
    if (task.running) ensureCacheTaskPolling()
  } catch {
    // ignore
  }
}

async function startCacheTask() {
  cacheTaskStarting.value = true
  try {
    const res = await startCacheUpdateTask()
    if (!res.success || !res.data) {
      throw new Error(res.error || '启动缓存补齐任务失败')
    }
    const task = ('task' in res.data ? res.data.task : res.data) as CacheUpdateTaskStatus
    cacheTask.value = task
    ensureCacheTaskPolling()
    healthHint.value = '缓存补齐任务已启动，可继续使用页面，任务完成后会自动刷新。'
  } catch (err) {
    healthHint.value = err instanceof Error ? err.message : '启动缓存补齐任务失败'
  } finally {
    cacheTaskStarting.value = false
  }
}

watch(
  () => report.value?.timeline,
  () => {
    renderTempChart()
    resizeChartsSoon()
  }
)
watch(
  () => report.value?.market_leader_rotation,
  () => {
    renderLeaderChart()
    resizeChartsSoon()
  }
)

function resizeCharts() {
  tempChart?.resize()
  leaderChart?.resize()
}

onMounted(() => {
  loadHealth()
  loadData()
  void pollCacheTaskStatusOnce()
  window.addEventListener('resize', resizeCharts)
})
onUnmounted(() => {
  window.removeEventListener('resize', resizeCharts)
  if (cacheTaskTimer != null) {
    window.clearInterval(cacheTaskTimer)
    cacheTaskTimer = null
  }
  tempChart?.dispose()
  leaderChart?.dispose()
  tempChart = null
  leaderChart = null
})
</script>

