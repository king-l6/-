/**
 * 多股日 K 叠加（首根有效收盘=100 便于同轴对比）+ 成交量 + 可选阶段色块 — ECharts 配置
 */
import type { EChartsOption } from 'echarts'

export interface DailyBar {
  date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface KlineSeriesModel {
  name: string
  colorUp: string
  colorDown: string
  /** 与 dates 等长；null 表示当日无该票数据 */
  candle: ([number, number, number, number] | null)[]
}

export interface PhaseModel {
  label: string
  start: string
  end: string
  border: string
  fill: string
}

export interface MarkerModel {
  date: string
  label: string
}

export function mergeTradingDates(rowsList: DailyBar[][]): string[] {
  const s = new Set<string>()
  for (const rows of rowsList) {
    for (const r of rows) s.add(r.date)
  }
  return [...s].sort()
}

function barMapByDate(rows: DailyBar[]): Map<string, DailyBar> {
  return new Map(rows.map((r) => [r.date.slice(0, 10), r]))
}

function refCloseFromMap(m: Map<string, DailyBar>, dates: string[]): number | null {
  for (const d of dates) {
    const x = m.get(d.slice(0, 10))
    if (x && x.close > 0) return x.close
  }
  return null
}

/**
 * 与主图一致：在 `dates` 从左到右第一根有数据且收盘>0 的日 K 的收盘价，作为归一基准（等价于归一收盘 100 的那天）。
 * 单点公式：归一收盘 = 当日收盘 × 100 / 该基准收盘。
 */
export function normRefCloseFromRows(rows: DailyBar[], dates: string[]): number | null {
  return refCloseFromMap(barMapByDate(rows), dates)
}

/** 某日归一收盘价（与 buildNormalizedCandles 同一基准）；无数据或无效则 null */
export function normalizedCloseForDate(rows: DailyBar[], dates: string[], date: string): number | null {
  const m = barMapByDate(rows)
  const ref = refCloseFromMap(m, dates)
  if (ref == null || ref <= 0) return null
  const x = m.get(date.slice(0, 10))
  if (!x || x.close <= 0) return null
  return (x.close * 100) / ref
}

/**
 * 在 `dates` 顺序下，所有「归一收盘 > threshold」的日期（默认 100 即相对基准涨为正）。
 * 仅包含有该票 K 的日期；基准日当天归一收盘为 100，用严格 > 时不会出现（除非改 threshold）。
 */
export function datesWhereNormalizedCloseAbove(
  rows: DailyBar[],
  dates: string[],
  threshold = 100
): string[] {
  const m = barMapByDate(rows)
  const ref = refCloseFromMap(m, dates)
  if (ref == null || ref <= 0) return []
  const out: string[] = []
  for (const d of dates) {
    const key = d.slice(0, 10)
    const x = m.get(key)
    if (!x || x.close <= 0) continue
    if ((x.close * 100) / ref > threshold) out.push(key)
  }
  return out
}

/** 以窗口内首个有效收盘为 100 缩放 OHLC：不同绝对价的票可叠在同一纵轴看相对强弱（非真实元） */
export function buildNormalizedCandles(rows: DailyBar[], dates: string[]): ([number, number, number, number] | null)[] {
  const m = barMapByDate(rows)
  const refClose = refCloseFromMap(m, dates)
  if (!refClose || refClose <= 0) return dates.map(() => null)
  const k = 100 / refClose
  return dates.map((d) => {
    const x = m.get(d.slice(0, 10))
    if (!x) return null
    return [x.open * k, x.close * k, x.low * k, x.high * k]
  })
}

export function alignVolume(rows: DailyBar[], dates: string[]): number[] {
  const m = new Map(rows.map((r) => [r.date, r]))
  return dates.map((d) => {
    const x = m.get(d)
    return x ? x.volume : 0
  })
}

function phaseLabelAtDate(phases: PhaseModel[], d: string): string | null {
  for (const p of phases) {
    if (d >= p.start && d <= p.end) return p.label
  }
  return null
}

/** 主线段模式下大量日期无 K，默认把可视范围缩到有数据的区间（百分比 0–100） */
function initialDataZoomByActiveRange(
  dates: string[],
  series: KlineSeriesModel[],
  volumes: number[]
): { start: number; end: number } {
  const n = dates.length
  if (n <= 1) return { start: 0, end: 100 }
  let lo = n
  let hi = -1
  for (let i = 0; i < n; i++) {
    let has = (volumes[i] ?? 0) > 0
    if (!has) {
      for (const s of series) {
        if (s.candle[i] != null) {
          has = true
          break
        }
      }
    }
    if (has) {
      if (i < lo) lo = i
      if (i > hi) hi = i
    }
  }
  if (hi < lo) return { start: 0, end: 100 }
  const span = hi - lo + 1
  const pad = Math.max(5, Math.ceil(span * 0.1))
  const lo2 = Math.max(0, lo - pad)
  const hi2 = Math.min(n - 1, hi + pad)
  const denom = Math.max(1, n - 1)
  let start = (lo2 / denom) * 100
  let end = (hi2 / denom) * 100
  if (end - start < 3) end = Math.min(100, start + 3)
  // 已覆盖绝大部分横轴则直接全窗，避免无意义的「假缩放」
  if (end - start > 92) return { start: 0, end: 100 }
  return { start, end }
}

export function buildMultiKlineEChartsOption(params: {
  dates: string[]
  series: KlineSeriesModel[]
  volumes: number[]
  phases: PhaseModel[]
  markers: MarkerModel[]
}): EChartsOption {
  const { dates, series, volumes, phases, markers } = params

  /** 色块表示情绪阶段；不在图上重复写阶段名（多日会叠成一团），改在 tooltip 里看「阶段」一行 */
  const markAreaData = phases.map((p) => [
    {
      name: p.label,
      xAxis: p.start,
      itemStyle: {
        color: p.fill,
        borderColor: 'transparent',
        borderWidth: 0
      },
      label: { show: false }
    },
    { xAxis: p.end }
  ])

  const markPointData = markers
    .map((mk) => {
      const i = dates.indexOf(mk.date)
      if (i < 0) return null
      const cnd = series[0]?.candle[i]
      if (!cnd) return null
      const y = cnd[1]
      return {
        name: mk.label,
        coord: [mk.date, y] as [string, number],
        value: mk.label,
        symbol: 'triangle',
        symbolSize: 8,
        symbolRotate: 180,
        itemStyle: { color: '#dc2626', opacity: 0.75 },
        label: { show: false }
      }
    })
    .filter(Boolean) as Record<string, unknown>[]

  const candleSeries = series.map((s, idx) => ({
    name: s.name,
    type: 'candlestick' as const,
    xAxisIndex: 0,
    yAxisIndex: 0,
    data: s.candle.map((c) => (c == null ? ('-' as const) : c)),
    itemStyle: {
      color: s.colorUp,
      color0: s.colorDown,
      borderColor: s.colorUp,
      borderColor0: s.colorDown
    },
    emphasis: { disabled: true },
    ...(idx === 0 && markAreaData.length
      ? {
          markArea: {
            silent: true,
            data: markAreaData
          }
        }
      : {}),
    ...(idx === 0 && markPointData.length
      ? {
          markPoint: {
            symbol: 'triangle',
            data: markPointData
          }
        }
      : {})
  }))

  const volColors = dates.map((_, i) => {
    const c = series[0]?.candle[i]
    if (!c) return '#94a3b8'
    const [o, cl] = [c[0], c[1]]
    return cl >= o ? '#f97316' : '#22c55e'
  })

  const zoom = initialDataZoomByActiveRange(dates, series, volumes)

  return {
    animation: false,
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'line',
        lineStyle: { color: '#94a3b8', width: 1, type: 'dashed' }
      },
      formatter: (items: unknown) => {
        if (!Array.isArray(items) || !items.length) return ''
        const ax = items[0] as { axisValue?: string }
        const d = ax.axisValue || ''
        const lines = [`<strong>${d}</strong>`]
        const ph = phaseLabelAtDate(phases, d)
        if (ph) lines.push(`<span style="color:#64748b">情绪阶段</span>: ${ph}`)
        for (let si = 0; si < series.length; si++) {
          const s = series[si]
          const i = dates.indexOf(d)
          const cnd = i >= 0 ? s.candle[i] : null
          if (cnd) {
            const [o, cl, lo, hi] = cnd
            lines.push(
              `${s.name}（首收=100）: O${o.toFixed(2)} C${cl.toFixed(2)} L${lo.toFixed(2)} H${hi.toFixed(2)}`
            )
          }
        }
        const vi = dates.indexOf(d)
        if (vi >= 0) {
          const volStock =
            series[0]?.name?.replace(/（主线段）$/, '')?.trim() || '首票'
          lines.push(`量(${volStock}): ${volumes[vi]?.toFixed(0) ?? 0}`)
        }
        return lines.join('<br/>')
      }
    },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    dataZoom: [
      {
        type: 'slider',
        xAxisIndex: [0, 1],
        bottom: 6,
        height: 22,
        start: zoom.start,
        end: zoom.end,
        handleStyle: { borderWidth: 1 },
        textStyle: { fontSize: 10 },
        dataBackground: { lineStyle: { opacity: 0.45 }, areaStyle: { opacity: 0.08 } }
      },
      {
        type: 'inside',
        xAxisIndex: [0, 1],
        start: zoom.start,
        end: zoom.end
      }
    ],
    grid: [
      { left: '6%', right: '4%', top: '10%', height: '50%' },
      { left: '6%', right: '4%', top: '64%', height: '14%' }
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: true,
        axisLine: { onZero: false },
        splitLine: { show: false },
        axisLabel: { fontSize: 10, rotate: dates.length > 80 ? 40 : 0 },
        gridIndex: 0
      },
      {
        type: 'category',
        data: dates,
        boundaryGap: true,
        axisLine: { onZero: false },
        axisLabel: { show: false },
        gridIndex: 1
      }
    ],
    yAxis: [
      {
        scale: true,
        name: '相对强弱(首收=100)',
        nameLocation: 'middle',
        nameGap: 40,
        nameTextStyle: { fontSize: 10, color: '#64748b' },
        splitArea: { show: true },
        axisLabel: { fontSize: 10 },
        gridIndex: 0
      },
      {
        scale: true,
        splitNumber: 2,
        axisLabel: { fontSize: 9 },
        gridIndex: 1
      }
    ],
    legend: {
      top: 0,
      data: series.map((s) => s.name),
      textStyle: { fontSize: 11 }
    },
    series: [
      ...candleSeries,
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes.map((v, i) => ({
          value: v,
          itemStyle: { color: volColors[i] }
        }))
      }
    ]
  }
}

/** 演示用：生成一段类似「多阶段 + 多箭头」的示意数据（非真实行情） */
export function buildDemoDailyBars(startDate: string, n: number): DailyBar[] {
  const out: DailyBar[] = []
  let t = new Date(startDate + 'T12:00:00')
  let base = 100
  for (let i = 0; i < n; i++) {
    let wd = t.getDay()
    while (wd === 0 || wd === 6) {
      t.setDate(t.getDate() + 1)
      wd = t.getDay()
    }
    const ds = t.toISOString().slice(0, 10)
    const wobble = Math.sin(i * 0.25) * 2 + (i % 7) * 0.15
    const o = base + wobble
    const c = o + (i % 5) * 0.4 - 0.5
    const h = Math.max(o, c) + 1.2
    const l = Math.min(o, c) - 1.0
    const v = 1e5 * (1 + (i % 11) * 0.07)
    out.push({ date: ds, open: o, high: h, low: l, close: c, volume: v })
    base = c
    t.setDate(t.getDate() + 1)
  }
  return out
}
