/**
 * 从情绪周期接口返回的 timeline / 龙头分段，推导多股 K 线页的阶段框与切换标注。
 */
import type { MarketLeaderDayRow, MarketLeaderSegmentRow } from '@/types'
import type { MarkerModel, PhaseModel } from '@/utils/multiKlineChart'

const CYCLE_STYLE: Record<string, { border: string; fill: string }> = {
  冰点: { border: '#64748b', fill: 'rgba(100, 116, 139, 0.14)' },
  弱修复: { border: '#22c55e', fill: 'rgba(34, 197, 94, 0.14)' },
  中性震荡: { border: '#ca8a04', fill: 'rgba(202, 138, 4, 0.12)' },
  强势主升: { border: '#ea580c', fill: 'rgba(234, 88, 12, 0.12)' },
  '高潮/过热': { border: '#dc2626', fill: 'rgba(220, 38, 38, 0.1)' }
}

const DEFAULT_STYLE = { border: '#94a3b8', fill: 'rgba(148, 163, 184, 0.12)' }

/** 连续相同 cycle 合并为一段（与按日温度图周期标签一致） */
export function derivePhasesFromTimeline(
  timeline: Array<{ date: string; cycle: string }>
): PhaseModel[] {
  if (!timeline?.length) return []
  const sorted = [...timeline].sort((a, b) => a.date.localeCompare(b.date))
  const out: PhaseModel[] = []
  let segStart = 0
  for (let i = 1; i <= sorted.length; i++) {
    const endIdx = i - 1
    const split = i === sorted.length || sorted[i].cycle !== sorted[segStart].cycle
    if (split) {
      const cyc = sorted[segStart].cycle
      const st = CYCLE_STYLE[cyc] ?? DEFAULT_STYLE
      out.push({
        label: cyc,
        start: sorted[segStart].date,
        end: sorted[endIdx].date,
        border: st.border,
        fill: st.fill
      })
      segStart = i
    }
  }
  return out
}

/** 周期切换日：标注 M.D（与常见复盘习惯一致） */
export function deriveMarkersFromTimeline(
  timeline: Array<{ date: string; cycle: string }>
): MarkerModel[] {
  if (timeline.length < 2) return []
  const sorted = [...timeline].sort((a, b) => a.date.localeCompare(b.date))
  const out: MarkerModel[] = []
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i].cycle === sorted[i - 1].cycle) continue
    const d = sorted[i].date
    const m = parseInt(d.slice(5, 7), 10)
    const day = parseInt(d.slice(8, 10), 10)
    out.push({ date: d, label: `${m}.${day}` })
  }
  return out
}

function isValidAshareCode(c: string): boolean {
  return c.length === 6 && /^\d+$/.test(c)
}

/**
 * 叠 K 标的代码（自动模式）：
 * 先在窗口内收集所有曾出现在龙头分段或日榜（top1 / leaders）的代码；
 * 按<strong>最后活跃日</strong>从新到旧排序（活跃日 = 分段 end_date，或当日出现在 top1/leaders 的交易日），
 * 只取前 max 只，避免整条情绪窗里「累计分最高但早已退场」的老龙头占满叠图。
 * 最后活跃日相同时，用累计分（分段天数 + 日榜名次加权）作次序。
 */
export function pickCodesFromLeaderSegments(
  segments: MarketLeaderSegmentRow[] | undefined,
  daily: MarketLeaderDayRow[] | undefined,
  max = 5
): string[] {
  const score = new Map<string, number>()
  if (segments?.length) {
    for (const s of segments) {
      const c = (s.code || '').trim()
      if (!isValidAshareCode(c)) continue
      score.set(c, (score.get(c) || 0) + (s.days || 0))
    }
  }
  if (daily?.length) {
    for (const row of daily) {
      const leaders = row.leaders || []
      const n = leaders.length
      for (let i = 0; i < n; i++) {
        const c = (leaders[i].code || '').trim()
        if (!isValidAshareCode(c)) continue
        const w = n - i
        score.set(c, (score.get(c) || 0) + w)
      }
    }
  }

  const lastSeen = new Map<string, string>()
  const bumpLast = (code: string, d: string) => {
    const c = code.trim()
    if (!isValidAshareCode(c) || d.length !== 10) return
    const prev = lastSeen.get(c) || ''
    if (d > prev) lastSeen.set(c, d)
  }

  if (segments?.length) {
    for (const s of segments) {
      const c = (s.code || '').trim()
      if (!isValidAshareCode(c)) continue
      const e = (s.end_date || '').slice(0, 10)
      const st = (s.start_date || '').slice(0, 10)
      bumpLast(c, e.length === 10 ? e : st)
    }
  }
  if (daily?.length) {
    for (const row of daily) {
      const d = (row.date || '').slice(0, 10)
      if (d.length !== 10) continue
      const top = (row.top1?.code || '').trim()
      if (isValidAshareCode(top)) bumpLast(top, d)
      for (const l of row.leaders || []) {
        const lc = (l.code || '').trim()
        if (isValidAshareCode(lc)) bumpLast(lc, d)
      }
    }
  }

  const codes = new Set<string>()
  for (const s of segments || []) {
    const c = (s.code || '').trim()
    if (isValidAshareCode(c)) codes.add(c)
  }
  for (const row of daily || []) {
    const top = (row.top1?.code || '').trim()
    if (isValidAshareCode(top)) codes.add(top)
    for (const l of row.leaders || []) {
      const lc = (l.code || '').trim()
      if (isValidAshareCode(lc)) codes.add(lc)
    }
  }

  const NO_DATE = '0000-00-00'
  return [...codes]
    .map((code) => ({
      code,
      last: lastSeen.get(code) || NO_DATE,
      sc: score.get(code) || 0
    }))
    .sort((a, b) => {
      if (a.last !== b.last) return b.last.localeCompare(a.last)
      if (a.sc !== b.sc) return b.sc - a.sc
      return a.code.localeCompare(b.code)
    })
    .map((x) => x.code)
    .slice(0, max)
}

const NO_LEADER_DATE = '9999-99-99'

/**
 * 首次与「主线龙头」相关的时间（日历日字符串，越小越早）：
 * 取「龙头分段」该代码最早 start_date」「首次成为当日 top1」「首次进入当日 leaders 榜单」三者中最早的一个。
 */
export function firstLeaderChronologyDate(
  code: string,
  segments: MarketLeaderSegmentRow[] | undefined,
  daily: MarketLeaderDayRow[] | undefined
): string {
  const c = (code || '').trim()
  if (c.length !== 6) return NO_LEADER_DATE
  const candidates: string[] = []
  if (segments?.length) {
    for (const s of segments) {
      if ((s.code || '').trim() !== c) continue
      const d = (s.start_date || '').slice(0, 10)
      if (d.length === 10) candidates.push(d)
    }
  }
  if (daily?.length) {
    for (const row of daily) {
      if ((row.top1?.code || '').trim() !== c) continue
      const d = (row.date || '').slice(0, 10)
      if (d.length === 10) candidates.push(d)
    }
    for (const row of daily) {
      for (const l of row.leaders || []) {
        if ((l.code || '').trim() !== c) continue
        const d = (row.date || '').slice(0, 10)
        if (d.length === 10) candidates.push(d)
        break
      }
    }
  }
  if (!candidates.length) return NO_LEADER_DATE
  return candidates.reduce((m, d) => (d < m ? d : m))
}

/** 叠图 / 图例顺序：按首次龙头相关日期从早到晚；无数据的保持输入相对顺序 */
export function sortCodesByLeaderChronology(
  codes: string[],
  segments: MarketLeaderSegmentRow[] | undefined,
  daily: MarketLeaderDayRow[] | undefined
): string[] {
  const ordered: string[] = []
  const seen = new Set<string>()
  for (const raw of codes) {
    const c = (raw || '').trim()
    if (c.length !== 6 || !/^\d+$/.test(c) || seen.has(c)) continue
    seen.add(c)
    ordered.push(c)
  }
  const origIdx = new Map(ordered.map((c, i) => [c, i]))
  return [...ordered].sort((a, b) => {
    const da = firstLeaderChronologyDate(a, segments, daily)
    const db = firstLeaderChronologyDate(b, segments, daily)
    if (da !== db) return da.localeCompare(db)
    return (origIdx.get(a) ?? 0) - (origIdx.get(b) ?? 0)
  })
}

/** 主线龙头分段按开始日期升序（页面时间轴展示） */
export function sortSegmentsByStart(
  segments: MarketLeaderSegmentRow[] | undefined
): MarketLeaderSegmentRow[] {
  if (!segments?.length) return []
  return [...segments].sort((a, b) => (a.start_date || '').localeCompare(b.start_date || ''))
}

/** 图例 / tooltip：从龙头分段或按日主线行解析证券简称；无则退回代码 */
export function displayNameForLeaderCode(
  code: string,
  segments: MarketLeaderSegmentRow[] | undefined,
  daily: MarketLeaderDayRow[] | undefined
): string {
  const c = (code || '').trim()
  if (!c) return code
  if (segments?.length) {
    for (const s of segments) {
      if ((s.code || '').trim() !== c) continue
      const n = (s.name || '').trim()
      if (n) return n
    }
  }
  if (daily?.length) {
    for (const row of daily) {
      if ((row.top1?.code || '').trim() === c) {
        const n = (row.top1?.name || '').trim()
        if (n) return n
      }
      const hit = row.leaders?.find((x) => (x.code || '').trim() === c)
      const n = (hit?.name || '').trim()
      if (n) return n
    }
  }
  return c
}

/** timeline 首尾日期作为拉 K 区间 */
export function dateRangeFromTimeline(
  timeline: Array<{ date: string }>
): [string, string] | null {
  if (!timeline?.length) return null
  const sorted = [...timeline].map((x) => x.date).sort()
  return [sorted[0], sorted[sorted.length - 1]]
}

/** 该代码作为市场主线的连续区间（来自 segments）；无则退化为 daily 里 top1=该代码 的日期包络；再无则用整窗 */
export function leaderSegmentWindowsForCode(
  segments: MarketLeaderSegmentRow[] | undefined,
  daily: Array<{ date: string; top1?: { code?: string } | null }> | undefined,
  code: string,
  rangeStart: string,
  rangeEnd: string
): { start: string; end: string }[] {
  const fromSeg = (segments || []).filter((s) => (s.code || '').trim() === code)
  if (fromSeg.length) {
    return fromSeg.map((s) => ({
      start: (s.start_date || '').slice(0, 10),
      end: (s.end_date || '').slice(0, 10)
    }))
  }
  const days = (daily || [])
    .filter((row) => (row.top1?.code || '').trim() === code)
    .map((row) => (row.date || '').slice(0, 10))
    .filter(Boolean)
    .sort()
  if (days.length) return [{ start: days[0], end: days[days.length - 1] }]
  return [{ start: rangeStart, end: rangeEnd }]
}

export function mergeWindowBounds(
  windows: { start: string; end: string }[],
  fallback: [string, string]
): [string, string] {
  if (!windows.length) return fallback
  let mn = windows[0].start
  let mx = windows[0].end
  for (const w of windows) {
    if (w.start < mn) mn = w.start
    if (w.end > mx) mx = w.end
  }
  return [mn, mx]
}

/** 把拉数区间限制在情绪窗内 */
export function clipDateRange(inner: [string, string], outer: [string, string]): [string, string] {
  const [a, b] = inner
  const [c, d] = outer
  const s = a < c ? c : a
  const e = b > d ? d : b
  return s <= e ? [s, e] : outer
}

export function filterBarsToWindows<T extends { date: string }>(
  rows: T[],
  windows: { start: string; end: string }[]
): T[] {
  if (!windows.length) return rows
  return rows.filter((r) => {
    const d = (r.date || '').slice(0, 10)
    return windows.some((w) => d >= w.start && d <= w.end)
  })
}
