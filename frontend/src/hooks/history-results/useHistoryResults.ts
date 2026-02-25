import { computed, ref, watch, onMounted, onUnmounted } from 'vue'
import { message } from 'ant-design-vue'
import { getResultsList, getResultsFile } from '@/api'
import type { StockResult, ResultFile } from '@/types'

const STORAGE_KEY_FAVORITES = 'history-favorites-by-file'
const strategyNames = ['龙头战法', '断板反包', '均线上穿', '情绪周期', '三连板']

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

  const filteredResults = computed(() => {
    let sourceData = results.value
    if (isDense.value && searchText.value) {
      const search = searchText.value.toLowerCase()
      sourceData = sourceData.filter(item =>
        item.code.toLowerCase().includes(search) ||
        item.name.toLowerCase().includes(search)
      )
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
    try {
      return new Date(date).toLocaleString('zh-CN')
    } catch {
      return date
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
    const baseName = filename.replace('_结果.jsonl', '').replace('.jsonl', '')
    return strategyNames.some(name =>
      baseName === name ||
      baseName.startsWith(name + '_') ||
      baseName.startsWith(name + '-')
    )
  }

  function sortFileList(files: ResultFile[]): ResultFile[] {
    const strategyFiles: ResultFile[] = []
    const otherFiles: ResultFile[] = []
    files.forEach(file => {
      if (isStrategyFile(file.filename)) strategyFiles.push(file)
      else otherFiles.push(file)
    })
    strategyFiles.sort((a, b) => {
      const baseNameA = a.filename.replace('_结果.jsonl', '').replace('.jsonl', '')
      const baseNameB = b.filename.replace('_结果.jsonl', '').replace('.jsonl', '')
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

  async function loadFileList() {
    loadingFiles.value = true
    error.value = ''
    try {
      const response = await getResultsList()
      if (response.success) {
        fileList.value = sortFileList(response.data)
        if (fileList.value.length > 0) {
          const firstFile = fileList.value[0].filename
          if (!activeFile.value || !fileList.value.find(f => f.filename === activeFile.value)) {
            await handleFileChange(firstFile, true)
          }
        } else {
          activeFile.value = ''
          results.value = []
          metaInfo.value = null
        }
      } else {
        error.value = response.error || '加载文件列表失败'
      }
    } catch (e: any) {
      error.value = e.message || '加载文件列表失败'
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
      const response = await getResultsFile(filename)
      if (response.success) {
        let newResults = Array.isArray(response.data.results) ? [...response.data.results] : []
        const seen = new Set<string>()
        newResults = newResults.filter(item => {
          const key = `${item.code}-${item.match_date || ''}-${item.name || ''}`
          if (seen.has(key)) return false
          seen.add(key)
          return true
        })
        results.value = newResults
        metaInfo.value = response.data.meta
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
    const headers = ['代码', '名称', '匹配日期', '匹配价格', '当前价格', '涨跌幅(%)', '次日振幅', '次日涨跌幅(%)', '第三日振幅', '第三日涨跌幅(%)']
    const rows = sortedData.map(item => {
      const pct = getPctChangeValue(item)
      return [
        item.code,
        item.name,
        item.match_date || '',
        item.match_price?.toFixed(2) || '',
        item.current_price?.toFixed(2) || '',
        pct.toFixed(2),
        item.day2_amplitude !== undefined ? item.day2_amplitude.toFixed(2) : '',
        item.day2_change_pct !== undefined ? item.day2_change_pct.toFixed(2) : '',
        item.day3_amplitude !== undefined ? item.day3_amplitude.toFixed(2) : '',
        item.day3_change_pct !== undefined ? item.day3_change_pct.toFixed(2) : ''
      ]
    })
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
    handleExport
  }
}
