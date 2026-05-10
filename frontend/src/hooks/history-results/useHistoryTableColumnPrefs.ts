import { computed, ref } from 'vue'
import type { ColumnsType } from 'ant-design-vue/es/table'

const STORAGE_KEY = 'history-results-column-prefs-v1'

/** 不可隐藏：至少保留代码与名称 */
export const HISTORY_TABLE_REQUIRED_KEYS = ['code', 'name'] as const

/** 默认可吸右侧固定区（顺序可改，但会作为整块贴在表格右侧） */
export const HISTORY_TABLE_RIGHT_FIXED_KEYS = [
  'day2_change_pct',
  'day3_change_pct',
  'day2_buy_hit_5pct_day'
] as const

export interface HistoryColumnPrefs {
  order: string[]
  hidden: string[]
}

export interface HistoryColumnDraftRow {
  key: string
  title: string
  visible: boolean
  required: boolean
}

type Stored = {
  dense: HistoryColumnPrefs | null
  normal: HistoryColumnPrefs | null
}

function loadStored(): Stored {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return { dense: null, normal: null }
    const p = JSON.parse(raw) as Partial<Stored>
    const norm = (x: unknown): HistoryColumnPrefs | null => {
      if (!x || typeof x !== 'object') return null
      const o = x as HistoryColumnPrefs
      if (!Array.isArray(o.order)) return null
      return {
        order: o.order.map(String),
        hidden: Array.isArray(o.hidden) ? o.hidden.map(String) : []
      }
    }
    return { dense: norm(p.dense), normal: norm(p.normal) }
  } catch {
    return { dense: null, normal: null }
  }
}

function persist(s: Stored) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(s))
}

const _stored = ref<Stored>(loadStored())

export function getColumnKey(col: ColumnsType[number]): string {
  const k = col.key ?? col.dataIndex
  return k != null ? String(k) : ''
}

function titleText(col: ColumnsType[number]): string {
  const t = col.title
  if (typeof t === 'string') return t
  return getColumnKey(col) || '列'
}

/** 按偏好过滤、排序；并重算 fixed，避免拖拽后 Ant Table 固定列错位 */
export function applyHistoryColumnPrefs<T extends Record<string, unknown>>(
  baseColumns: ColumnsType<T>,
  prefs: HistoryColumnPrefs | null
): ColumnsType<T> {
  if (!prefs) return baseColumns

  const byKey = new Map<string, ColumnsType<T>[number]>()
  for (const c of baseColumns) {
    const k = getColumnKey(c)
    if (k) byKey.set(k, c)
  }

  const required = new Set<string>(HISTORY_TABLE_REQUIRED_KEYS as unknown as string[])
  const hidden = new Set(prefs.hidden.filter((k) => !required.has(k)))

  const seen = new Set<string>()
  const orderedKeys: string[] = []

  for (const k of prefs.order) {
    if (!byKey.has(k) || hidden.has(k)) continue
    if (seen.has(k)) continue
    seen.add(k)
    orderedKeys.push(k)
  }
  for (const c of baseColumns) {
    const k = getColumnKey(c)
    if (!k || hidden.has(k) || seen.has(k)) continue
    seen.add(k)
    orderedKeys.push(k)
  }

  const out: ColumnsType<T> = orderedKeys.map((k) => ({ ...byKey.get(k)! }))
  return reconcileFixedColumns(out)
}

export function reconcileFixedColumns<T extends Record<string, unknown>>(cols: ColumnsType<T>): ColumnsType<T> {
  const out = cols.map((c) => {
    const { fixed: _f, ...rest } = c as ColumnsType<T>[number] & { fixed?: unknown }
    return { ...rest } as ColumnsType<T>[number]
  })
  if (out.length === 0) return out

  if (getColumnKey(out[0]) === 'code') {
    ;(out[0] as ColumnsType<T>[number] & { fixed?: string }).fixed = 'left'
  }

  const rightSet = new Set<string>(HISTORY_TABLE_RIGHT_FIXED_KEYS as unknown as string[])
  let j = out.length - 1
  while (j >= 0 && rightSet.has(getColumnKey(out[j]))) {
    j--
  }
  for (let k = j + 1; k < out.length; k++) {
    ;(out[k] as ColumnsType<T>[number] & { fixed?: string }).fixed = 'right'
  }
  return out
}

export function buildDraftFromBase<T extends Record<string, unknown>>(
  baseColumns: ColumnsType<T>,
  prefs: HistoryColumnPrefs | null
): HistoryColumnDraftRow[] {
  const keysFromBase = baseColumns.map(getColumnKey).filter(Boolean)
  const titleByKey = new Map<string, string>()
  for (const c of baseColumns) {
    const k = getColumnKey(c)
    if (!k) continue
    titleByKey.set(k, titleText(c))
  }

  let order = [...(prefs?.order?.length ? prefs.order : keysFromBase)]
  order = order.filter((k) => keysFromBase.includes(k))
  for (const k of keysFromBase) {
    if (!order.includes(k)) order.push(k)
  }

  const hidden = new Set(prefs?.hidden ?? [])
  const req = new Set<string>(HISTORY_TABLE_REQUIRED_KEYS as unknown as string[])

  return order.map((k) => ({
    key: k,
    title: titleByKey.get(k) ?? k,
    visible: req.has(k) ? true : !hidden.has(k),
    required: req.has(k)
  }))
}

export function useHistoryTableColumnPrefs() {
  const densePrefs = computed(() => _stored.value.dense)
  const normalPrefs = computed(() => _stored.value.normal)

  function setDense(p: HistoryColumnPrefs | null) {
    _stored.value = { ..._stored.value, dense: p }
    persist(_stored.value)
  }

  function setNormal(p: HistoryColumnPrefs | null) {
    _stored.value = { ..._stored.value, normal: p }
    persist(_stored.value)
  }

  return {
    densePrefs,
    normalPrefs,
    setDensePrefs: setDense,
    setNormalPrefs: setNormal,
    resetDensePrefs: () => setDense(null),
    resetNormalPrefs: () => setNormal(null)
  }
}
