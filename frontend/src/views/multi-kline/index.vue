<template>
  <div class="space-y-3">
    <Card size="small" title="多股复盘 K 线（情绪窗口 + 本地缓存）">
      <Alert type="info" show-icon class="mb-3" message="数据说明">
        <template #description>
          <div class="text-xs space-y-1">
            <p>
              <strong>标的</strong>：与情绪报告一致，自动模式下按各票在窗口内<strong>最后活跃日从新到旧</strong>取前 N 只（默认 5，避免整条窗里累计分高但已退场的旧龙占满叠图）；同一最后活跃日再看分段天数与日榜名次加权分。也可手动填代码。图例与 K
              线<strong>绘制顺序</strong>按各票<strong>首次与主线龙头相关</strong>的日期从早到晚（见下方规则说明）。
            </p>
            <p>
              <strong>K 线纵轴（首收 = 100）</strong>：各票绝对股价差很多，若用真实元叠在一起没法比。做法是每只票在情绪窗口里取<strong>第一根有数据的日 K 收盘</strong>当作
              <strong>100</strong>，再按比例缩放开高低收——纵轴表示<strong>相对强弱</strong>，不是真实价格。下方<strong>成交量</strong>仍是<strong>第一只叠图票</strong>的真实手数。
            </p>
          </div>
        </template>
      </Alert>

      <div class="mb-3 rounded border border-slate-200 bg-slate-50/80 px-3 py-2 text-xs text-slate-700 space-y-2">
        <div class="font-semibold text-slate-800">主线龙头是怎么认定的</div>
        <ul class="list-disc pl-4 space-y-1">
          <li>
            <strong>后端（当日谁算涨停池、谁当 top1）</strong>：在本地全市场日截面里，涨幅达到该股近似涨停阈值（主板等约 9.8%，300/301/688/689 约
            20%）的归入当日涨停池；在池内按<strong>连板数</strong>从高到低排序，同连板再比<strong>收盘×成交量</strong>、涨幅、量能等，取前
            {{ leaderMeta.top_k ?? 5 }} 只为「龙头列表」，其中<strong>第一只为当日主线 top1</strong>。连板数在滚动历史日上累计，当日未涨停或当日无该票截面则连板清零/断档。
          </li>
          <li v-if="leaderMeta.note" class="text-slate-600">{{ leaderMeta.note }}</li>
          <li>
            <strong>「龙头分段」</strong>：在情绪接口返回的窗口内，把<strong>连续多个交易日 top1 为同一只票</strong>的区间合并成一段，得到下表「开始～结束」；段内「最高连板」为该段日历日内该票作为
            top1 时连板数的最大值。
          </li>
          <li>
            <strong>本页叠 K 的 {{ maxOverlay }} 只股票（自动）</strong>：先取窗口内曾出现在龙头分段或日榜的代码，按<strong>最后活跃日</strong>（分段结束日或最后一次出现在
            <code>top1</code>/<code>leaders</code> 的交易日）<strong>从新到旧</strong>取前 {{ maxOverlay }} 只；同日再看累计分（分段主线天数 + 日榜名次加权）。手动填代码时以输入为准，仍受「叠图只数」上限；改完数字后需再点<strong>从情绪周期加载</strong>。
          </li>
          <li>
            <strong>图例与叠图顺序</strong>：对最终选中的代码，按<strong>首次龙头相关日</strong>从早到晚排序——取该代码在「龙头分段」里最早的
            <code>start_date</code>、首次成为当日 <code>top1</code>、首次进入当日 <code>leaders</code> 三者中的<strong>最早</strong>一日；都无则排在后面并保持原相对顺序。
          </li>
        </ul>
      </div>

      <div class="flex flex-wrap gap-2 items-end mb-2">
        <div>
          <div class="text-xs text-gray-500 mb-1">情绪窗口（交易日，与情绪周期页一致）</div>
          <InputNumber v-model:value="emotionDays" :min="30" :max="500" :step="10" class="w-[120px]" />
        </div>
        <div>
          <div class="text-xs text-gray-500 mb-1">叠图只数（自动选龙头时的上限）</div>
          <InputNumber v-model:value="maxOverlay" :min="1" :max="12" class="w-[88px]" />
        </div>
        <Button type="primary" :loading="loading" @click="loadFromEmotionCycle">从情绪周期加载</Button>
        <div class="flex-1 min-w-[200px]">
          <div class="text-xs text-gray-500 mb-1">覆盖写股票代码（可选，逗号分隔；留空则用龙头推导，只数受左侧上限）</div>
          <Input v-model:value="codesText" placeholder="留空自动；或 600376,002652" allow-clear />
        </div>
      </div>
      <div class="text-xs text-gray-500 mb-2">
        当前 K 线区间：<span class="font-mono">{{ dateRange?.[0] || '—' }} ~ {{ dateRange?.[1] || '—' }}</span>
      </div>

      <div v-if="segmentRows.length" class="mb-3">
        <div class="text-sm font-medium text-slate-800 mb-2">主线龙头时间段（按开始日期先后）</div>
        <Table
          size="small"
          :columns="segmentColumns"
          :data-source="segmentRows"
          :pagination="false"
          row-key="key"
          :scroll="{ x: 720 }"
          bordered
        />
      </div>
      <Alert v-else-if="!loading && !errText && loadedOnce" type="warning" show-icon class="mb-2" message="当前报告无龙头分段数据（segments 为空）" />

      <Alert v-if="errText" type="error" :message="errText" show-icon class="mb-2" />
      <MultiStockKlineWorkbench :option="builtOption" :height-px="540" />
    </Card>
  </div>
</template>

<script lang="ts">
export default { name: 'MultiKline' }
</script>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { Alert, Button, Card, Input, InputNumber, message, Table } from 'ant-design-vue'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { EChartsOption } from 'echarts'
import { getEmotionCycle, getStockDaily } from '@/api'
import type { EmotionCycleReport, MarketLeaderSegmentRow } from '@/types'
import MultiStockKlineWorkbench from '@/components/charts/MultiStockKlineWorkbench.vue'
import type { DailyBar, KlineSeriesModel, MarkerModel, PhaseModel } from '@/utils/multiKlineChart'
import {
  alignVolume,
  buildMultiKlineEChartsOption,
  buildNormalizedCandles,
  mergeTradingDates
} from '@/utils/multiKlineChart'
import {
  dateRangeFromTimeline,
  displayNameForLeaderCode,
  pickCodesFromLeaderSegments,
  sortCodesByLeaderChronology,
  sortSegmentsByStart
} from '@/utils/emotionKlineDerive'

const loading = ref(false)
const errText = ref('')
const emotionDays = ref(180)
/** 主图里最多叠几只票的 K（含手动代码）；默认 5 是历史设定，可调大至 12 */
const maxOverlay = ref(5)
const codesText = ref('')
const dateRange = ref<[string, string] | undefined>(undefined)
const loadedOnce = ref(false)

const leaderMeta = ref<{ top_k?: number; note?: string }>({})

interface SegmentRow {
  key: string
  idx: number
  start_date: string
  end_date: string
  code: string
  name: string
  days: number
  max_boards: string | number
  on_chart: string
}

const segmentRows = ref<SegmentRow[]>([])

const COLORS = [
  { up: '#b45309', down: '#78350f' },
  { up: '#1d4ed8', down: '#172554' },
  { up: '#047857', down: '#064e3b' },
  { up: '#a21caf', down: '#581c87' },
  { up: '#b91c1c', down: '#7f1d1d' },
  { up: '#0d9488', down: '#115e59' },
  { up: '#c026d3', down: '#701a75' },
  { up: '#d97706', down: '#92400e' },
  { up: '#4f46e5', down: '#312e81' },
  { up: '#15803d', down: '#14532d' },
  { up: '#e11d48', down: '#881337' },
  { up: '#0369a1', down: '#0c4a6e' }
]

const builtOption = ref<EChartsOption | null>(null)

const segmentColumns: ColumnsType<SegmentRow> = [
  { title: '#', dataIndex: 'idx', key: 'idx', width: 44, align: 'right' },
  { title: '开始', dataIndex: 'start_date', key: 'start_date', width: 112 },
  { title: '结束', dataIndex: 'end_date', key: 'end_date', width: 112 },
  { title: '代码', dataIndex: 'code', key: 'code', width: 88 },
  { title: '名称', dataIndex: 'name', key: 'name', ellipsis: true },
  { title: '天数', dataIndex: 'days', key: 'days', width: 64, align: 'right' },
  { title: '段内最高连板', dataIndex: 'max_boards', key: 'max_boards', width: 112, align: 'right' },
  { title: '本页叠K', dataIndex: 'on_chart', key: 'on_chart', width: 88, align: 'center' }
]

function setOptionFromModel(
  dates: string[],
  series: KlineSeriesModel[],
  volRows: DailyBar[],
  phases: PhaseModel[],
  markers: MarkerModel[]
) {
  const volumes = alignVolume(volRows, dates)
  builtOption.value = buildMultiKlineEChartsOption({ dates, series, volumes, phases, markers })
}

function buildSegmentRows(seg: MarketLeaderSegmentRow[] | undefined, chartCodes: string[]): SegmentRow[] {
  const sorted = sortSegmentsByStart(seg)
  const set = new Set(chartCodes.map((c) => c.trim()))
  return sorted.map((s, i) => ({
    key: `${(s.code || '').trim()}-${(s.start_date || '').slice(0, 10)}-${i}`,
    idx: i + 1,
    start_date: (s.start_date || '').slice(0, 10),
    end_date: (s.end_date || '').slice(0, 10),
    code: (s.code || '').trim(),
    name: (s.name || '').trim() || '—',
    days: Number(s.days) || 0,
    max_boards: s.max_consecutive_boards != null ? s.max_consecutive_boards : '—',
    on_chart: set.has((s.code || '').trim()) ? '是' : ''
  }))
}

async function loadFromEmotionCycle() {
  errText.value = ''
  loading.value = true
  try {
    const res = await getEmotionCycle({ days: emotionDays.value })
    if (!res.success || !res.data) throw new Error(res.error || '情绪周期接口失败')
    const report = res.data as EmotionCycleReport
    const tl = report.timeline || []
    if (!tl.length) throw new Error('情绪 timeline 为空，请先补全本地日线缓存后再试')

    const dr = dateRangeFromTimeline(tl)
    if (!dr) throw new Error('无法从 timeline 得到日期范围')
    dateRange.value = dr
    const [start, end] = dr

    const rotation = report.market_leader_rotation
    const seg = rotation?.segments
    const daily = rotation?.daily
    leaderMeta.value = {
      top_k: rotation?.top_k,
      note: (rotation?.note || '').trim() || undefined
    }

    const cap = Math.min(12, Math.max(1, Math.floor(maxOverlay.value)))
    let codes = pickCodesFromLeaderSegments(seg, daily, cap)
    const manual = codesText.value
      .split(/[,，\s]+/)
      .map((s) => s.trim())
      .filter((c) => c.length === 6 && /^\d+$/.test(c))
      .slice(0, cap)
    if (manual.length) codes = manual
    codes = sortCodesByLeaderChronology(codes, seg, daily)

    if (!codes.length) throw new Error('未能从龙头数据得到股票代码，请在上方手动填写代码')

    segmentRows.value = buildSegmentRows(seg, codes)

    const rowsList: DailyBar[][] = []
    for (const code of codes) {
      const r = await getStockDaily({ code, start, end })
      if (!r.success || !r.data?.rows?.length) {
        throw new Error(`${code}: ${r.error || '无 K 线缓存'}`)
      }
      rowsList.push(r.data.rows)
    }

    const dates = mergeTradingDates(rowsList)
    if (!dates.length) throw new Error('合并后无交易日')

    const series: KlineSeriesModel[] = rowsList.map((rows, i) => {
      const c = COLORS[i % COLORS.length]
      const label = displayNameForLeaderCode(codes[i], seg, daily)
      return {
        name: label,
        colorUp: c.up,
        colorDown: c.down,
        candle: buildNormalizedCandles(rows, dates)
      }
    })

    setOptionFromModel(dates, series, rowsList[0], [], [])
    const loadedLabels = codes.map((c) => displayNameForLeaderCode(c, seg, daily))
    message.success(`已加载：${loadedLabels.join('、')}`)
    loadedOnce.value = true
  } catch (e) {
    errText.value = e instanceof Error ? e.message : String(e)
    builtOption.value = null
    segmentRows.value = []
    leaderMeta.value = {}
    loadedOnce.value = true
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadFromEmotionCycle()
})
</script>
