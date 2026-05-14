import type { StockDailyBar } from '@/types'

/** 取「连阳段」最后 N 个交易日（含 streakEnd 当日）的日期与成交量；rows 须按日期升序 */
export function sliceConsecutiveUpVolumeWindow(
  rows: StockDailyBar[],
  streakEnd: string,
  n: number
): { dates: string[]; volumes: number[] } | null {
  const end = streakEnd.trim().slice(0, 10)
  if (!end || n < 1 || !rows.length) return null
  const i = rows.findIndex((r) => String(r.date).slice(0, 10) === end)
  if (i < 0) return null
  const start = i - n + 1
  if (start < 0) return null
  const slice = rows.slice(start, i + 1)
  if (slice.length !== n) return null
  return {
    dates: slice.map((r) => String(r.date).slice(0, 10)),
    volumes: slice.map((r) => Number(r.volume) || 0)
  }
}

export function shiftIsoDate(iso: string, deltaCalendarDays: number): string {
  const d = new Date(`${iso.trim().slice(0, 10)}T12:00:00`)
  d.setDate(d.getDate() + deltaCalendarDays)
  return d.toISOString().slice(0, 10)
}
