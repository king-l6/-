import type { StockResult } from '@/types'

function fmtBoardPct(p: number): string {
  const n = Number(p)
  if (Number.isNaN(n)) return '0.00%'
  const sign = n >= 0 ? '+' : ''
  return `${sign}${n.toFixed(2)}%`
}

/** 与后端 sector_linkage.concept_hits_rank_pct_averages 一致：对全部概念条求名次均值、涨幅均值 */
function conceptAveragesFromConcepts(
  lc: { pct?: number; rank?: number }[]
): { rankAvg: number; pctAvg: number } | null {
  if (!lc.length) return null
  const pcts = lc.map((c) => Number(c.pct)).filter((x) => Number.isFinite(x))
  if (!pcts.length) return null
  const ranks = lc
    .map((c) => (c.rank != null && Number.isFinite(Number(c.rank)) ? Number(c.rank) : NaN))
    .filter((x) => Number.isFinite(x)) as number[]
  const pctAvg = pcts.reduce((a, b) => a + b, 0) / pcts.length
  const rankAvg = ranks.length ? ranks.reduce((a, b) => a + b, 0) / ranks.length : 0
  return { rankAvg, pctAvg }
}

function formatConceptMeanSuffix(row: StockResult, lc: NonNullable<StockResult['linkage_concepts']>): string {
  const rAvg = row.linkage_concept_rank_avg
  const pAvg = row.linkage_concept_pct_avg
  let rankAvg: number
  let pctAvg: number
  if (
    typeof rAvg === 'number' &&
    Number.isFinite(rAvg) &&
    typeof pAvg === 'number' &&
    Number.isFinite(pAvg)
  ) {
    rankAvg = rAvg
    pctAvg = pAvg
  } else {
    const av = conceptAveragesFromConcepts(lc)
    if (!av) return ''
    rankAvg = av.rankAvg
    pctAvg = av.pctAvg
  }
  return `｜概念均值:名次均${rankAvg.toFixed(1)}·涨幅均${fmtBoardPct(pctAvg)}`
}

/**
 * 表格/导出用：优先用 linkage_concepts（含板块当日涨跌幅），避免旧 linkage_text 无涨幅时页面不显示。
 */
export function formatLinkageTableCell(row: StockResult): string {
  const parts: string[] = []
  const lc = row.linkage_concepts
  if (lc && lc.length > 0) {
    parts.push(
      '概念:' +
        lc
          .map((c) => {
            const rk =
              c.rank != null && Number.isFinite(Number(c.rank))
                ? `·涨幅第${c.rank}名`
                : ''
            return `${c.name}(${fmtBoardPct(Number(c.pct))}${rk})`
          })
          .join('、') +
        formatConceptMeanSuffix(row, lc)
    )
  }
  const ind = row.linkage_industry?.trim()
  if (ind) {
    const ip = row.linkage_industry_pct
    const ir = row.linkage_industry_rank
    const rankSuffix =
      typeof ir === 'number' && Number.isFinite(ir) ? `·涨幅第${ir}名` : ''
    if (typeof ip === 'number' && !Number.isNaN(ip)) {
      parts.push(`行业:${ind}(${fmtBoardPct(ip)}${rankSuffix})`)
    } else {
      parts.push(`行业:${ind}${rankSuffix || ''}`)
    }
  }
  if (parts.length > 0) {
    return parts.join('；')
  }
  const t = row.linkage_text?.trim()
  return t || '-'
}
