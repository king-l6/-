import { computed, ref, watch, onMounted, onUnmounted } from 'vue'
import { message } from 'ant-design-vue'
import { getResultsList, getResultsFile, getResultsByStrategy } from '@/api'
import type { StockResult, ResultFile } from '@/types'

const STORAGE_KEY_FAVORITES = 'history-favorites-by-file'
const strategyNames = ['龙头战法', '断板反包', '均线上穿', '情绪周期', '三连板', '筑底突破']

export function useHistoryResults() {
  const loadingFiles = ref(false)
  const loading = ref(false)
  const error = ref('')
  const fileList = ref<ResultFile[]>([])
  const activeFile = ref<string>('')
  const results = ref<StockResult[]>([])
  const metaInfo = ref<any>(null)

  const isDense = ref(true)
  const searchText = ref('')
  const selectedKeys = ref<string[]>([])
  const collectedByFile = ref<Record<string, StockResult[]>>({})
  /** 仅显示：次日涨幅>3% 或 次日振幅(收盘-开盘)幅度>3% */
  const filterDay2Strong = ref(false)

  const isNarrowScreen = ref(typeof window !== 'undefined' && window.innerWidth < 768)

  const tableScroll = computed(() => {
    const y = isNarrowScreen.value ? 360 : (isDense.value ? 600 : undefined)
    return isDense.value ? { x: 'max-content' as const, y } : { x: 'max-content' as const }
  })

  const collectedItems = computed(() => {
    const file = activeFile.value
    return file ? (collectedByFile.value[file] || []) : []
  })

  const paginationDense = ref({
    current: 1,
    pageSize: 50,
    showSizeChanger: true,
    pageSizeOptions: ['20', '50', '100', '200'],
    showTotal: (total: number) => `共 ${total} 条`,
    showQuickJumper: true
  })
  const paginationNormal = ref({
    current: 1,
    pageSize: 20,
    showSizeChanger: true,
    showTotal: (total: number) => `共 ${total} 条`
  })

  const hasResults = computed(() => results.value.length > 0)

  /** 每个交易日只高亮一只：当日按涨幅优先、振幅次之排序取第一；并记录是该日涨幅最高/振幅最高/两者 */
  const dayLeaderInfo = computed(() => {
    const list = filteredResults.value
    const info = new Map<string, { isMaxPct: boolean; isMaxAmp: boolean }>()
    if (!list.length) return info
    const byDate = new Map<string, typeof list>()
    for (const r of list) {
      const d = (r as any).match_date || ''
      if (!d) continue
      if (!byDate.has(d)) byDate.set(d, [])
      byDate.get(d)!.push(r)
    }
    byDate.forEach((rows) => {
      let maxPct = -Infinity
      let maxAmp = -Infinity
      for (const r of rows) {
        const pct = (r as any).day1_change_pct ?? (r as any).day2_change_pct
        const amp = (r as any).day1_amplitude ?? (r as any).day2_amplitude
        if (typeof pct === 'number' && pct > maxPct) maxPct = pct
        if (typeof amp === 'number' && Math.abs(amp) > maxAmp) maxAmp = Math.abs(amp)
      }
      const sorted = [...rows].sort((a, b) => {
        const pctA = (a as any).day1_change_pct ?? (a as any).day2_change_pct ?? -Infinity
        const pctB = (b as any).day1_change_pct ?? (b as any).day2_change_pct ?? -Infinity
        if (pctB !== pctA) return pctB - pctA
        const ampA = Math.abs((a as any).day1_amplitude ?? (a as any).day2_amplitude ?? -Infinity)
        const ampB = Math.abs((b as any).day1_amplitude ?? (b as any).day2_amplitude ?? -Infinity)
        return ampB - ampA
      })
      const winner = sorted[0]
      if (!winner) return
      const key = `${(winner as any).match_date}-${(winner as any).code}`
      const pct = (winner as any).day1_change_pct ?? (winner as any).day2_change_pct
      const amp = (winner as any).day1_amplitude ?? (winner as any).day2_amplitude
      const isMaxPct = typeof pct === 'number' && pct === maxPct && maxPct !== -Infinity
      const isMaxAmp = typeof amp === 'number' && Math.abs(amp) === maxAmp && maxAmp !== -Infinity
      info.set(key, { isMaxPct, isMaxAmp })
    })
    return info
  })

  function isDayLeader(record: StockResult): boolean {
    return dayLeaderInfo.value.has(`${record.match_date || ''}-${record.code || ''}`)
  }

  /** 仅对当日高亮的那一只返回标签：涨幅最高 / 振幅最高 / 涨幅&振幅最高 */
  function getDayLeaderLabel(record: StockResult): '' | '涨幅最高' | '振幅最高' | '涨幅&振幅最高' {
    const v = dayLeaderInfo.value.get(`${record.match_date || ''}-${record.code || ''}`)
    if (!v) return ''
    if (v.isMaxPct && v.isMaxAmp) return '涨幅&振幅最高'
    if (v.isMaxPct) return '涨幅最高'
    if (v.isMaxAmp) return '振幅最高'
    return ''
  }

  const filteredResults = computed(() => {
    let sourceData = results.value
    if (isDense.value && searchText.value) {
      const search = searchText.value.toLowerCase()
      sourceData = sourceData.filter(item =>
        item.code.toLowerCase().includes(search) ||
        item.name.toLowerCase().includes(search)
      )
    }
    if (filterDay2Strong.value) {
      sourceData = sourceData.filter(item => {
        const r = item as any
        // 优先用匹配日当天(day1)，没有则用次日(day2)兼容老数据
        const pct = r?.day1_change_pct ?? r?.day2_change_pct
        const amp = r?.day1_amplitude ?? r?.day2_amplitude
        const pctOk = typeof pct === 'number' && pct > 3
        const ampOk = typeof amp === 'number' && Math.abs(amp) > 3
        return pctOk || ampOk
      })
    }
    return [...sourceData]
  })

  function loadFavoritesFromStorage() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY_FAVORITES)
      if (raw) {
        const parsed = JSON.parse(raw) as Record<string, StockResult[]>
        if (parsed && typeof parsed === 'object') collectedByFile.value = parsed
      }
    } catch (_) {}
  }

  function saveFavoritesToStorage() {
    try {
      localStorage.setItem(STORAGE_KEY_FAVORITES, JSON.stringify(collectedByFile.value))
    } catch (_) {}
  }

  function updateNarrowScreen() {
    if (typeof window !== 'undefined') {
      isNarrowScreen.value = window.innerWidth < 768
    }
  }

  function onTableChange(pag: { current?: number; pageSize?: number }) {
    if (!pag) return
    if (isDense.value) {
      paginationDense.value = { ...paginationDense.value, ...pag }
    } else {
      paginationNormal.value = { ...paginationNormal.value, ...pag }
    }
  }

  function handleAddToCollection(record: StockResult) {
    const file = activeFile.value
    if (!file) return
    const list = collectedByFile.value[file] || []
    const key = `${record.code}-${record.match_date || ''}`
    if (list.some(r => `${r.code}-${r.match_date || ''}` === key)) {
      message.info('已在自选中，未重复添加')
      return
    }
    collectedByFile.value = { ...collectedByFile.value, [file]: [...list, { ...record }] }
    saveFavoritesToStorage()
    message.success(`已加入自选：${record.name} (${record.code})`)
  }

  function handleRemove(record: StockResult) {
    const file = activeFile.value
    if (!file) return
    const key = `${record.code}-${record.match_date || ''}`
    const list = (collectedByFile.value[file] || []).filter(r => `${r.code}-${r.match_date || ''}` !== key)
    collectedByFile.value = { ...collectedByFile.value, [file]: list }
    saveFavoritesToStorage()
  }

  watch(activeFile, (newVal) => {
    if (newVal) selectedKeys.value = [newVal]
  }, { immediate: true })

  function onMobileFileSelect(value: unknown) {
    const filename = typeof value === 'string' && value !== '' ? value : null
    if (filename) {
      handleFileChange(filename, true)
    } else {
      activeFile.value = ''
      results.value = []
      metaInfo.value = null
    }
  }

  function formatFileName(filename: string): string {
    if (filename.startsWith('__strategy__')) {
      return filename.replace('__strategy__', '')
    }
    const name = filename.replace(/\.jsonl$/, '')
    return name.length > 25 ? name.substring(0, 25) + '...' : name
  }

  function formatFileDate(dateStr: string): string {
    if (!dateStr) return ''
    try {
      const date = new Date(dateStr)
      const now = new Date()
      const diffMs = now.getTime() - date.getTime()
      const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))
      if (diffDays === 0) return '今天'
      if (diffDays === 1) return '昨天'
      if (diffDays < 7) return `${diffDays}天前`
      return date.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' })
    } catch {
      return dateStr.substring(0, 10)
    }
  }

  function formatFullDate(dateStr: string): string {
    if (!dateStr) return ''
    try {
      const date = new Date(dateStr)
      return date.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      })
    } catch {
      return dateStr
    }
  }

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

  function getPctChange(stock: StockResult): string {
    if (!stock.current_price || !stock.match_price) return '-'
    const pct = ((stock.current_price - stock.match_price) / stock.match_price * 100).toFixed(2)
    return `${pct}%`
  }

  function getPctChangeValue(stock: StockResult): number {
    if (!stock.current_price || !stock.match_price) return 0
    return (stock.current_price - stock.match_price) / stock.match_price * 100
  }

  function getPctChangeClass(stock: StockResult): string {
    if (!stock.current_price || !stock.match_price) return ''
    const pct = (stock.current_price - stock.match_price) / stock.match_price
    return pct >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'
  }

  function getPctChangeStyle(stock: StockResult): Record<string, string> {
    if (!stock.current_price || !stock.match_price) return {}
    const pct = (stock.current_price - stock.match_price) / stock.match_price
    return { color: pct >= 0 ? '#dc2626' : '#16a34a', fontWeight: '600' }
  }

  function formatDayPct(pct?: number): string {
    if (pct === undefined || pct === null) return '-'
    return `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
  }

  function getDayPctClass(pct?: number): string {
    if (pct === undefined || pct === null) return ''
    return pct >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'
  }

  function formatAmplitude(amplitude?: number): string {
    if (amplitude === undefined || amplitude === null) return '-'
    return `${amplitude >= 0 ? '+' : ''}${amplitude.toFixed(2)}%`
  }

  function getAmplitudeClass(amplitude?: number): string {
    if (amplitude === undefined || amplitude === null) return ''
    return amplitude >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'
  }

  function getRowClassName(record: StockResult): string {
    if (!record.current_price || !record.match_price) return ''
    const pct = (record.current_price - record.match_price) / record.match_price
    return pct >= 0 ? 'bg-red-50' : 'bg-green-50'
  }

  function isStrategyFile(filename: string): boolean {
    if (filename.startsWith('__strategy__')) return true
    const baseName = filename.replace('_结果.jsonl', '').replace('.jsonl', '')
    return strategyNames.some(name =>
      baseName === name ||
      baseName.startsWith(name + '_') ||
      baseName.startsWith(name + '-')
    )
  }

  /** 从文件名解析策略名：龙头战法_结果.jsonl 或 龙头战法_20260226_结果.jsonl -> 龙头战法 */
  function getStrategyNameFromFilename(filename: string): string | null {
    if (filename.startsWith('__strategy__')) return filename.replace('__strategy__', '')
    const withDate = filename.match(/^(.+)_\d{8}_结果\.jsonl$/)
    if (withDate) return strategyNames.includes(withDate[1]) ? withDate[1] : null
    const main = filename.replace(/_结果\.jsonl$/, '').replace(/\.jsonl$/, '')
    return strategyNames.includes(main) ? main : null
  }

  function sortFileList(files: ResultFile[]): ResultFile[] {
    const strategyFiles: ResultFile[] = []
    const otherFiles: ResultFile[] = []
    files.forEach(file => {
      if (isStrategyFile(file.filename)) strategyFiles.push(file)
      else otherFiles.push(file)
    })
    strategyFiles.sort((a, b) => {
      const baseNameA = a.filename.replace('_结果.jsonl', '').replace('.jsonl', '').replace(/^__strategy__/, '')
      const baseNameB = b.filename.replace('_结果.jsonl', '').replace('.jsonl', '').replace(/^__strategy__/, '')
      const getIndex = (baseName: string) => {
        for (let idx = 0; idx < strategyNames.length; idx++) {
          if (baseName === strategyNames[idx]) return idx * 100
        }
        for (let idx = 0; idx < strategyNames.length; idx++) {
          if (baseName.startsWith(strategyNames[idx] + '_') || baseName.startsWith(strategyNames[idx] + '-')) {
            return idx * 100 + 50
          }
        }
        return strategyNames.length * 100
      }
      return getIndex(baseNameA) - getIndex(baseNameB)
    })
    otherFiles.sort((a, b) => new Date(b.modified).getTime() - new Date(a.modified).getTime())
    return [...strategyFiles, ...otherFiles]
  }

  /** 在文件列表前插入「按策略聚合」虚拟项（每个策略名一条），且不再重复列出策略的实体文件 */
  function buildFileListWithAggregated(responseFiles: ResultFile[]): ResultFile[] {
    const strategyNameSet = new Set<string>()
    const strategyAgg = new Map<string, { count: number; latestModified: string }>()
    const otherFiles: ResultFile[] = []
    responseFiles.forEach(file => {
      const name = getStrategyNameFromFilename(file.filename)
      if (name) {
        strategyNameSet.add(name)
        const prev = strategyAgg.get(name) || { count: 0, latestModified: '' }
        prev.count += typeof file.count === 'number' ? file.count : 0
        if (file.modified) {
          const prevTs = prev.latestModified ? new Date(prev.latestModified).getTime() : 0
          const curTs = new Date(file.modified).getTime()
          if (!Number.isNaN(curTs) && curTs > prevTs) prev.latestModified = file.modified
        }
        strategyAgg.set(name, prev)
        // 策略文件不再单独列出，只通过「按策略聚合」入口查看
      } else {
        otherFiles.push(file)
      }
    })
    const aggregatedEntries: ResultFile[] = Array.from(strategyNameSet).sort((a, b) => {
      const ia = strategyNames.indexOf(a)
      const ib = strategyNames.indexOf(b)
      if (ia !== -1 && ib !== -1) return ia - ib
      if (ia !== -1) return -1
      if (ib !== -1) return 1
      return a.localeCompare(b)
    }).map(name => {
      const agg = strategyAgg.get(name)
      return {
        filename: `__strategy__${name}`,
        size: 0,
        modified: agg?.latestModified || '',
        count: agg?.count ?? 0
      }
    })
    return sortFileList([...aggregatedEntries, ...otherFiles])
  }

  async function loadFileList() {
    loadingFiles.value = true
    error.value = ''
    try {
      const response = await getResultsList()
      const list = response?.success && Array.isArray(response.data) ? response.data : []
      fileList.value = buildFileListWithAggregated(list)

      if (fileList.value.length > 0) {
        const firstFile = fileList.value[0].filename
        if (!activeFile.value || !fileList.value.find(f => f.filename === activeFile.value)) {
          // 不阻塞列表加载：先展示文件列表，详情请求走独立 loading
          handleFileChange(firstFile, true)
        }
      } else {
        activeFile.value = ''
        results.value = []
        metaInfo.value = null
        if (response && !response.success) {
          error.value = (response as any).error || '加载文件列表失败'
        }
      }
    } catch (e: any) {
      error.value = e?.message || '加载文件列表失败'
      fileList.value = []
    } finally {
      loadingFiles.value = false
    }
  }

  async function handleFileChange(filename: string, force = false) {
    if (!filename) return
    if (!force && filename === activeFile.value) return

    activeFile.value = filename
    loading.value = true
    error.value = ''
    results.value = []
    metaInfo.value = null

    try {
      const isAggregated = filename.startsWith('__strategy__')
      const response = isAggregated
        ? await getResultsByStrategy(filename.replace('__strategy__', ''))
        : await getResultsFile(filename)

      if (response.success) {
        const meta = response.data.meta || {}
        let newResults = Array.isArray(response.data.results) ? [...response.data.results] : []

        const seen = new Set<string>()
        newResults = newResults.filter(item => {
          const key = `${item.code}-${item.match_date || ''}-${item.name || ''}`
          if (seen.has(key)) return false
          seen.add(key)
          return true
        })

        // 按 match_date 倒序（最新在前），同一天内按涨幅、振幅降序，再按 code 升序
        newResults.sort((a, b) => {
          const dA = a.match_date || ''
          const dB = b.match_date || ''
          if (dB !== dA) return dB.localeCompare(dA)
          const pctA = (a as any).day1_change_pct ?? (a as any).day2_change_pct ?? -9999
          const pctB = (b as any).day1_change_pct ?? (b as any).day2_change_pct ?? -9999
          if (pctB !== pctA) return pctB - pctA
          const ampA = (a as any).day1_amplitude ?? (a as any).day2_amplitude ?? -9999
          const ampB = (b as any).day1_amplitude ?? (b as any).day2_amplitude ?? -9999
          if (ampB !== ampA) return ampB - ampA
          return (a.code || '').localeCompare(b.code || '')
        })

        results.value = newResults
        metaInfo.value = meta
      } else {
        error.value = response.error || '加载文件失败'
      }
    } catch (e: any) {
      error.value = e.message || '加载文件失败'
    } finally {
      loading.value = false
    }
  }

  function handleExport() {
    const data = filteredResults.value
    if (data.length === 0) return
    const sortedData = [...data].sort((a, b) => {
      const dateCompare = (a.match_date || '').localeCompare(b.match_date || '')
      if (dateCompare !== 0) return dateCompare
      return (a.code || '').localeCompare(b.code || '')
    })
    const headers = ['代码', '名称', '匹配日期', '匹配价', '当前价']
    const rows = sortedData.map(item => [
      item.code,
      item.name,
      formatDate(item.match_date),
      item.match_price?.toFixed(2) || '',
      item.current_price?.toFixed(2) || ''
    ])
    const csvContent = [headers.join(','), ...rows.map(row => row.join(','))].join('\n')
    const BOM = '\uFEFF'
    const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    link.setAttribute('href', URL.createObjectURL(blob))
    link.setAttribute('download', `回测结果_${activeFile.value.replace('.jsonl', '')}_${new Date().toISOString().slice(0, 10)}.csv`)
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
  }

  function init() {
    loadFavoritesFromStorage()
    loadFileList()
    updateNarrowScreen()
    window.addEventListener('resize', updateNarrowScreen)
  }

  function destroy() {
    window.removeEventListener('resize', updateNarrowScreen)
  }

  onMounted(init)
  onUnmounted(destroy)

  return {
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
    collectedByFile,
    filterDay2Strong,
    isNarrowScreen,
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
    getPctChange,
    getPctChangeValue,
    getPctChangeClass,
    getPctChangeStyle,
    formatDayPct,
    getDayPctClass,
    formatAmplitude,
    getAmplitudeClass,
    getRowClassName,
    isStrategyFile,
    isDayLeader,
    getDayLeaderLabel,
    handleExport
  }
}

