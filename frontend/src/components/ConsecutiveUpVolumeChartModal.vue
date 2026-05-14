<template>
  <Modal
    :open="open"
    :title="modalTitle"
    width="760px"
    :footer="null"
    destroy-on-close
    @update:open="(v: boolean) => emit('update:open', v)"
    @cancel="onClose"
  >
    <div v-if="loading" class="py-12 text-center text-slate-500 dark:text-neutral-400">加载日 K…</div>
    <div v-else-if="errorMsg" class="py-6 text-center text-red-600 dark:text-red-400">{{ errorMsg }}</div>
    <div v-else-if="!chartPayload" class="py-6 text-center text-slate-500 dark:text-neutral-400">无法截取连阳窗口（请补全该股缓存）</div>
    <div v-else class="space-y-2">
      <p class="text-xs text-slate-600 dark:text-neutral-300">
        连阳 {{ chartPayload.dates.length }} 日 · 截至 {{ streakEnd }} · 成交量（手）
      </p>
      <!-- 固定高度：弹窗动画阶段容器宽高常为 0，需 ResizeObserver / change 后再 init -->
      <div class="w-full overflow-hidden rounded border border-slate-200 bg-white dark:border-neutral-700 dark:bg-neutral-950" style="height: 320px">
        <div ref="chartEl" class="h-full w-full" style="min-height: 320px; min-width: 400px" />
      </div>
    </div>
  </Modal>
</template>

<script setup lang="ts">
import { ref, watch, computed, nextTick, onUnmounted } from 'vue'
import { Modal } from 'ant-design-vue'
import * as echarts from 'echarts'
import type { EChartsOption } from 'echarts'
import { getStockDaily } from '@/api'
import type { StockResult, StockDailyBar } from '@/types'
import { shiftIsoDate, sliceConsecutiveUpVolumeWindow } from '@/utils/consecutiveUpVolumeSlice'

const props = defineProps<{
  open: boolean
  record: StockResult | null
}>()

const emit = defineEmits<{
  (e: 'update:open', v: boolean): void
}>()

const loading = ref(false)
const errorMsg = ref('')
const chartPayload = ref<{ dates: string[]; volumes: number[] } | null>(null)
const chartEl = ref<HTMLElement | null>(null)
let chart: echarts.ECharts | null = null
let chartResizeObserver: ResizeObserver | null = null

const modalTitle = computed(() => {
  const r = props.record
  if (!r?.code) return '连阳成交量'
  return `连阳成交量 — ${r.code} ${r.name || ''}`.trim()
})

const streakEnd = computed(() => {
  const r = props.record
  if (!r) return ''
  const s = r.consecutive_up_streak_end_date
  if (s && String(s).length >= 10) return String(s).slice(0, 10)
  return (r.match_date || '').slice(0, 10)
})

function onClose() {
  emit('update:open', false)
}

function disconnectChartObserver() {
  chartResizeObserver?.disconnect()
  chartResizeObserver = null
}

function disposeChart() {
  disconnectChartObserver()
  if (chart) {
    chart.dispose()
    chart = null
  }
}

let paintTimer: ReturnType<typeof setTimeout> | null = null

function clearPaintTimer() {
  if (paintTimer != null) {
    clearTimeout(paintTimer)
    paintTimer = null
  }
}

function buildOption(p: { dates: string[]; volumes: number[] }): EChartsOption {
  const vols = p.volumes.map((v) => (Number.isFinite(v) ? v : 0))
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: 48, right: 24, top: 28, bottom: 40 },
    xAxis: {
      type: 'category',
      data: p.dates,
      axisLabel: { rotate: 35, fontSize: 10 }
    },
    yAxis: {
      type: 'value',
      name: '成交量',
      scale: true,
      axisLabel: { fontSize: 10 }
    },
    series: [
      {
        name: '成交量',
        type: 'bar',
        data: vols,
        itemStyle: { color: '#3b82f6', opacity: 0.85 }
      },
      {
        name: '趋势',
        type: 'line',
        data: vols,
        smooth: true,
        symbol: 'circle',
        symbolSize: 6,
        lineStyle: { width: 2, color: '#f97316' },
        itemStyle: { color: '#f97316' }
      }
    ]
  }
}

/** 容器有有效尺寸后再绘制；可重复调用以随布局 resize */
function paintChartIfReady() {
  const el = chartEl.value
  const p = chartPayload.value
  if (!el || !p || p.dates.length === 0 || !props.open) return
  const cw = Math.max(el.clientWidth || el.offsetWidth || 0, 400)
  const ch = Math.max(el.clientHeight || el.offsetHeight || 0, 280)
  if (cw < 48 || ch < 48) return

  const opt = buildOption(p)
  if (!chart) {
    chart = echarts.init(el, undefined, { renderer: 'canvas' })
  }
  chart.setOption(opt, { notMerge: true })
  chart.resize({ width: cw, height: ch })
}

function connectChartObserver() {
  disconnectChartObserver()
  const el = chartEl.value
  if (!el || typeof ResizeObserver === 'undefined') return
  chartResizeObserver = new ResizeObserver(() => {
    paintChartIfReady()
  })
  chartResizeObserver.observe(el)
}

function scheduleChartPaint() {
  clearPaintTimer()
  paintTimer = setTimeout(() => {
    paintTimer = null
    paintChartIfReady()
  }, 120)
}

watch(
  () => [props.open, props.record] as const,
  async ([isOpen, rec]) => {
    clearPaintTimer()
    disposeChart()
    errorMsg.value = ''
    chartPayload.value = null
    if (!isOpen || !rec?.code) return
    const n = rec.consecutive_up_days
    if (n == null || n < 2) {
      errorMsg.value = '连阳天数不足，无法绘制趋势'
      return
    }
    const end = streakEnd.value
    if (!end) {
      errorMsg.value = '缺少匹配日期'
      return
    }
    loading.value = true
    const requestCode = rec.code
    try {
      const start = shiftIsoDate(end, -Math.max(60, n * 8))
      const res = await getStockDaily({ code: rec.code, start, end })
      if (!props.open || props.record?.code !== requestCode) return
      if (!res.success || !res.data?.rows?.length) {
        errorMsg.value = res.error || '暂无日 K 数据'
        return
      }
      const rows = res.data.rows as StockDailyBar[]
      const sliced = sliceConsecutiveUpVolumeWindow(rows, end, n)
      if (!sliced) {
        errorMsg.value = '本地 K 线不足以覆盖连阳窗口，请先补该股缓存'
        return
      }
      chartPayload.value = sliced
      await nextTick()
      connectChartObserver()
      paintChartIfReady()
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          paintChartIfReady()
          scheduleChartPaint()
        })
      })
    } catch (e: unknown) {
      errorMsg.value = e instanceof Error ? e.message : String(e)
    } finally {
      loading.value = false
    }
  },
  { flush: 'post' }
)

watch(
  () => props.open,
  (v) => {
    if (!v) {
      clearPaintTimer()
      disposeChart()
      chartPayload.value = null
      errorMsg.value = ''
    } else if (chartPayload.value) {
      nextTick(() => {
        connectChartObserver()
        scheduleChartPaint()
      })
    }
  }
)

watch(
  () => chartPayload.value,
  (payload) => {
    if (props.open && payload) {
      nextTick(() => {
        connectChartObserver()
        paintChartIfReady()
        scheduleChartPaint()
      })
    }
  }
)

onUnmounted(() => {
  clearPaintTimer()
  disposeChart()
})
</script>
