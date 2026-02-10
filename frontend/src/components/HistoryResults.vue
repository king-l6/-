<template>
  <Card title="历史回测数据" class="h-full">
    <template #extra>
      <Space>
        <Button size="small" @click="isDense = !isDense">
          {{ isDense ? '普通模式' : '密集模式' }}
        </Button>
        <Button v-if="hasResults" size="small" @click="handleExport">导出CSV</Button>
        <Input
          v-if="hasResults && isDense"
          v-model:value="searchText"
          placeholder="搜索代码或名称"
          allow-clear
          size="small"
          style="width: 150px"
        >
          <template #prefix>
            <SearchOutlined />
          </template>
        </Input>
        <Button size="small" @click="loadFileList" :loading="loadingFiles">
          刷新列表
        </Button>
      </Space>
    </template>
    
    <div v-if="loadingFiles" class="py-2 text-center">
      <Spin /> 加载文件列表...
    </div>
    
    <div v-else-if="fileList.length === 0" class="py-6 text-center text-gray-500">
      暂无历史回测数据文件
    </div>
    
    <div v-else class="flex gap-0 h-full" style="min-height: 500px;">
      <!-- 左侧导航栏 -->
      <div class="w-64 flex-shrink-0 bg-gray-50 border-r-2 border-gray-300 pr-0">
        <div class="p-2 border-b border-gray-200 bg-white">
          <div class="flex items-center justify-between mb-1">
            <span class="text-sm font-semibold text-gray-800">文件列表</span>
            <span class="text-xs text-gray-500 bg-gray-100 px-1.5 py-0.5 rounded">共 {{ fileList.length }} 个</span>
          </div>
        </div>
        <div class="overflow-y-auto" style="max-height: calc(100vh - 180px);">
          <Menu
            v-model:selectedKeys="selectedKeys"
            mode="inline"
            class="border-0 bg-transparent"
            @select="({ key }) => handleFileChange(key as string)"
          >
            <MenuItem
              v-for="file in fileList"
              :key="file.filename"
              :class="['file-menu-item', { 'strategy-file': isStrategyFile(file.filename) }]"
            >
              <div class="flex flex-col w-full">
                <span class="text-xs font-medium text-gray-800 truncate" :title="file.filename">
                  {{ formatFileName(file.filename) }}
                </span>
                <span class="text-xs text-gray-500 mt-0.5" :title="formatFullDate(file.modified)">
                  {{ formatFileDate(file.modified) }}
                </span>
              </div>
            </MenuItem>
          </Menu>
        </div>
      </div>
      
      <!-- 右侧内容区 -->
      <div class="flex-1 overflow-auto pl-3 pr-2">

      <!-- 元数据信息 -->
      <div v-if="metaInfo" class="mb-2 p-2 bg-blue-50 border-l-4 border-blue-500 rounded">
        <div class="text-xs text-gray-700">
          <span class="font-semibold">策略名称:</span> {{ metaInfo.strategy_name || '未知' }} | 
          <span class="font-semibold">运行时间:</span> {{ formatDate(metaInfo.run_at) }} | 
          <span class="font-semibold">数据条数:</span> {{ metaInfo.count || results.length }}
        </div>
      </div>

      <!-- 错误提示 -->
      <div v-if="error" class="py-2">
        <Alert
          :message="error"
          type="error"
          show-icon
          closable
          @close="error = ''"
        />
      </div>
      
      <!-- 加载中 -->
      <div v-else-if="loading" class="py-6 text-center">
        <Spin /> 加载数据中...
      </div>
      
      <!-- 无数据 -->
      <div v-else-if="!hasResults" class="py-6 text-center text-gray-500">
        请选择一个文件查看数据
      </div>
      
      <!-- 数据表格 -->
      <div v-else>
        <div class="mb-2 flex items-center justify-between">
          <div class="text-sm font-semibold text-primary">
            找到 {{ filteredResults.length }} 只符合条件的股票
          </div>
        </div>
        
        <Table
          :columns="isDense ? denseColumns : columns"
          :data-source="filteredResults"
          :loading="loading"
          :pagination="isDense ? {
            pageSize: 50,
            showSizeChanger: true,
            pageSizeOptions: ['20', '50', '100', '200'],
            showTotal: (total) => `共 ${total} 条`,
            showQuickJumper: true
          } : {
            pageSize: 20,
            showSizeChanger: true,
            showTotal: (total) => `共 ${total} 条`
          }"
          row-key="code"
          :scroll="isDense ? { x: 'max-content', y: 600 } : { x: 'max-content' }"
          :size="isDense ? 'small' : 'middle'"
          :bordered="isDense"
          :row-class-name="isDense ? getRowClassName : undefined"
        >
          <template #bodyCell="{ column, record }">
            <template v-if="column.key === 'pctChange'">
              <span 
                :class="getPctChangeClass(record as any as StockResult)"
                :style="getPctChangeStyle(record as any as StockResult)"
              >
                {{ getPctChange(record as any as StockResult) }}
              </span>
            </template>
            <template v-else-if="column.key === 'day2_amplitude'">
              <span :class="getAmplitudeClass((record as any).day2_amplitude)">
                {{ formatAmplitude((record as any).day2_amplitude) }}
              </span>
            </template>
            <template v-else-if="column.key === 'day2_change_pct'">
              <span :class="getDayPctClass((record as any).day2_change_pct)">
                {{ formatDayPct((record as any).day2_change_pct) }}
              </span>
            </template>
            <template v-else-if="column.key === 'day3_amplitude'">
              <span :class="getAmplitudeClass((record as any).day3_amplitude)">
                {{ formatAmplitude((record as any).day3_amplitude) }}
              </span>
            </template>
            <template v-else-if="column.key === 'day3_change_pct'">
              <span :class="getDayPctClass((record as any).day3_change_pct)">
                {{ formatDayPct((record as any).day3_change_pct) }}
              </span>
            </template>
            <template v-else-if="isDense && column.key === 'match_date'">
              <span class="text-xs">{{ formatDate((record as any).match_date) }}</span>
            </template>
            <template v-else-if="isDense && column.key === 'match_price'">
              <span class="font-mono text-xs">{{ formatPrice((record as any).match_price) }}</span>
            </template>
            <template v-else-if="isDense && column.key === 'current_price'">
              <span class="font-mono text-xs">{{ formatPrice((record as any).current_price) }}</span>
            </template>
            <template v-else-if="isDense && column.key === 'day2_amplitude'">
              <span :class="getAmplitudeClass((record as any).day2_amplitude)" class="text-xs">
                {{ formatAmplitude((record as any).day2_amplitude) }}
              </span>
            </template>
            <template v-else-if="isDense && column.key === 'day2_change_pct'">
              <span :class="getDayPctClass((record as any).day2_change_pct)" class="text-xs">
                {{ formatDayPct((record as any).day2_change_pct) }}
              </span>
            </template>
            <template v-else-if="isDense && column.key === 'day3_amplitude'">
              <span :class="getAmplitudeClass((record as any).day3_amplitude)" class="text-xs">
                {{ formatAmplitude((record as any).day3_amplitude) }}
              </span>
            </template>
            <template v-else-if="isDense && column.key === 'day3_change_pct'">
              <span :class="getDayPctClass((record as any).day3_change_pct)" class="text-xs">
                {{ formatDayPct((record as any).day3_change_pct) }}
              </span>
            </template>
            <template v-else-if="isDense && column.key === 'code'">
              <span class="font-mono text-xs font-semibold">{{ (record as any).code }}</span>
            </template>
          </template>
        </Table>
      </div>
      </div>
    </div>
  </Card>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, watch } from 'vue'
import { Card, Table, Spin, Alert, Button, Space, Input, Menu, MenuItem } from 'ant-design-vue'
import { SearchOutlined } from '@ant-design/icons-vue'
import { getResultsList, getResultsFile } from '@/api'
import type { ColumnsType } from 'ant-design-vue/es/table'
import type { StockResult, ResultFile } from '@/types'

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

// 监听 activeFile 变化，同步 selectedKeys
watch(activeFile, (newVal) => {
  if (newVal) {
    selectedKeys.value = [newVal]
  }
}, { immediate: true })

const hasResults = computed(() => results.value.length > 0)

// 筛选后的结果
const filteredResults = computed(() => {
  if (!isDense.value || !searchText.value) {
    return results.value
  }
  const search = searchText.value.toLowerCase()
  return results.value.filter(item => 
    item.code.toLowerCase().includes(search) || 
    item.name.toLowerCase().includes(search)
  )
})

function formatFileName(filename: string): string {
  // 移除 .jsonl 后缀，并截取合适的长度
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
    
    if (diffDays === 0) {
      return '今天'
    } else if (diffDays === 1) {
      return '昨天'
    } else if (diffDays < 7) {
      return `${diffDays}天前`
    } else {
      return date.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' })
    }
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
  if (!stock.current_price || !stock.match_price) {
    return '-'
  }
  const pct = ((stock.current_price - stock.match_price) / stock.match_price * 100).toFixed(2)
  return `${pct}%`
}

function getPctChangeValue(stock: StockResult): number {
  if (!stock.current_price || !stock.match_price) {
    return 0
  }
  return (stock.current_price - stock.match_price) / stock.match_price * 100
}

const columns: ColumnsType<StockResult> = [
  {
    title: '代码',
    dataIndex: 'code',
    key: 'code',
    width: 100,
    fixed: 'left'
  },
  {
    title: '名称',
    dataIndex: 'name',
    key: 'name',
    width: 120
  },
  {
    title: '匹配日期',
    dataIndex: 'match_date',
    key: 'match_date',
    width: 120
  },
  {
    title: '匹配价格',
    dataIndex: 'match_price',
    key: 'match_price',
    width: 120,
    customRender: ({ text }) => text ? text.toFixed(2) : '-'
  },
  {
    title: '当前价格',
    dataIndex: 'current_price',
    key: 'current_price',
    width: 120,
    customRender: ({ text }) => text ? text.toFixed(2) : '-'
  },
  {
    title: '涨跌幅',
    key: 'pctChange',
    width: 120
  },
  {
    title: '次日振幅',
    dataIndex: 'day2_amplitude',
    key: 'day2_amplitude',
    width: 100
  },
  {
    title: '次日涨跌幅',
    dataIndex: 'day2_change_pct',
    key: 'day2_change_pct',
    width: 100
  },
  {
    title: '第三日振幅',
    dataIndex: 'day3_amplitude',
    key: 'day3_amplitude',
    width: 100
  },
  {
    title: '第三日涨跌幅',
    dataIndex: 'day3_change_pct',
    key: 'day3_change_pct',
    width: 100,
    fixed: 'right'
  }
]

const denseColumns: ColumnsType<StockResult> = [
  {
    title: '代码',
    dataIndex: 'code',
    key: 'code',
    width: 80,
    fixed: 'left',
    sorter: (a, b) => a.code.localeCompare(b.code)
  },
  {
    title: '名称',
    dataIndex: 'name',
    key: 'name',
    width: 100,
    sorter: (a, b) => a.name.localeCompare(b.name)
  },
  {
    title: '匹配日期',
    dataIndex: 'match_date',
    key: 'match_date',
    width: 100,
    sortDirections: ['ascend', 'descend'],
    sorter: (a, b) => {
      const dateA = a.match_date || ''
      const dateB = b.match_date || ''
      // 处理空值：空值排在最后
      if (!dateA && !dateB) return 0
      if (!dateA) return 1
      if (!dateB) return -1
      // 使用日期字符串比较（格式：YYYY-MM-DD，可以直接用字符串比较）
      return dateA.localeCompare(dateB)
    }
  },
  {
    title: '匹配价',
    dataIndex: 'match_price',
    key: 'match_price',
    width: 80,
    align: 'right',
    sorter: (a, b) => (a.match_price || 0) - (b.match_price || 0)
  },
  {
    title: '当前价',
    dataIndex: 'current_price',
    key: 'current_price',
    width: 80,
    align: 'right',
    sorter: (a, b) => (a.current_price || 0) - (b.current_price || 0)
  },
  {
    title: '涨跌幅',
    key: 'pctChange',
    width: 90,
    align: 'right',
    sorter: (a, b) => {
      const pctA = getPctChangeValue(a)
      const pctB = getPctChangeValue(b)
      return pctA - pctB
    }
  },
  {
    title: '次日振幅',
    dataIndex: 'day2_amplitude',
    key: 'day2_amplitude',
    width: 85,
    align: 'right',
    sorter: (a, b) => (a.day2_amplitude || 0) - (b.day2_amplitude || 0)
  },
  {
    title: '次日涨跌幅',
    dataIndex: 'day2_change_pct',
    key: 'day2_change_pct',
    width: 90,
    align: 'right',
    sorter: (a, b) => (a.day2_change_pct || 0) - (b.day2_change_pct || 0)
  },
  {
    title: '第三日振幅',
    dataIndex: 'day3_amplitude',
    key: 'day3_amplitude',
    width: 85,
    align: 'right',
    sorter: (a, b) => (a.day3_amplitude || 0) - (b.day3_amplitude || 0)
  },
  {
    title: '第三日涨跌幅',
    dataIndex: 'day3_change_pct',
    key: 'day3_change_pct',
    width: 90,
    align: 'right',
    fixed: 'right',
    sorter: (a, b) => (a.day3_change_pct || 0) - (b.day3_change_pct || 0)
  }
]

function getPctChangeClass(stock: StockResult): string {
  if (!stock.current_price || !stock.match_price) {
    return ''
  }
  const pct = (stock.current_price - stock.match_price) / stock.match_price
  return pct >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'
}

function getPctChangeStyle(stock: StockResult): Record<string, string> {
  if (!stock.current_price || !stock.match_price) {
    return {}
  }
  const pct = (stock.current_price - stock.match_price) / stock.match_price
  return {
    color: pct >= 0 ? '#dc2626' : '#16a34a',
    fontWeight: '600'
  }
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
  if (!record.current_price || !record.match_price) {
    return ''
  }
  const pct = (record.current_price - record.match_price) / record.match_price
  return pct >= 0 ? 'bg-red-50' : 'bg-green-50'
}

// 战法名称列表（用于置顶）
const strategyNames = ['龙头战法', '断板反包', '均线上穿', '情绪周期', '三连板']

// 判断文件是否属于战法策略（精确匹配，避免匹配到组合名称）
function isStrategyFile(filename: string): boolean {
  // 移除.jsonl后缀和"结果"等后缀，只保留核心名称
  const baseName = filename.replace('_结果.jsonl', '').replace('.jsonl', '')
  // 检查是否完全匹配某个战法名称，或者以战法名称开头
  return strategyNames.some(name => 
    baseName === name || 
    baseName.startsWith(name + '_') || 
    baseName.startsWith(name + '-')
  )
}

// 文件排序函数：战法文件置顶，其他按修改时间倒序
function sortFileList(files: ResultFile[]): ResultFile[] {
  const strategyFiles: ResultFile[] = []
  const otherFiles: ResultFile[] = []
  
  files.forEach(file => {
    if (isStrategyFile(file.filename)) {
      strategyFiles.push(file)
    } else {
      otherFiles.push(file)
    }
  })
  
  // 战法文件内部按固定顺序排序（优先匹配纯战法名称）
  strategyFiles.sort((a, b) => {
    const baseNameA = a.filename.replace('_结果.jsonl', '').replace('.jsonl', '')
    const baseNameB = b.filename.replace('_结果.jsonl', '').replace('.jsonl', '')
    
    // 获取策略索引（完全匹配优先级更高）
    const getIndex = (baseName: string) => {
      // 优先匹配完全匹配的战法名称
      for (let idx = 0; idx < strategyNames.length; idx++) {
        if (baseName === strategyNames[idx]) {
          return idx * 100
        }
      }
      // 其次匹配以战法名称开头的
      for (let idx = 0; idx < strategyNames.length; idx++) {
        if (baseName.startsWith(strategyNames[idx] + '_') || baseName.startsWith(strategyNames[idx] + '-')) {
          return idx * 100 + 50
        }
      }
      return strategyNames.length * 100
    }
    
    const indexA = getIndex(baseNameA)
    const indexB = getIndex(baseNameB)
    
    return indexA - indexB
  })
  
  // 其他文件按修改时间倒序（最新的在前）
  otherFiles.sort((a, b) => {
    const dateA = new Date(a.modified).getTime()
    const dateB = new Date(b.modified).getTime()
    return dateB - dateA
  })
  
  return [...strategyFiles, ...otherFiles]
}

async function loadFileList() {
  loadingFiles.value = true
  error.value = ''
  try {
    const response = await getResultsList()
    if (response.success) {
      // 对文件列表进行排序：战法文件置顶
      fileList.value = sortFileList(response.data)
      // 如果有文件，默认选中第一个
      if (fileList.value.length > 0) {
        const firstFile = fileList.value[0].filename
        // 如果当前没有选中文件，或者当前选中的文件不在列表中，则选择第一个
        if (!activeFile.value || !fileList.value.find(f => f.filename === activeFile.value)) {
          // 强制加载第一个文件的数据
          await handleFileChange(firstFile, true)
        }
      } else {
        // 如果没有文件，清空选中状态
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
  
  // 如果文件名相同且不是强制加载，则跳过
  if (!force && filename === activeFile.value) return
  
  activeFile.value = filename
  loading.value = true
  error.value = ''
  results.value = []
  metaInfo.value = null
  
  try {
    const response = await getResultsFile(filename)
    if (response.success) {
      results.value = response.data.results
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
  if (data.length === 0) {
    return
  }
  
  // 按日期排序（日期早的在前，同日期按代码排序）
  const sortedData = [...data].sort((a, b) => {
    const dateA = a.match_date || ''
    const dateB = b.match_date || ''
    const dateCompare = dateA.localeCompare(dateB)
    if (dateCompare !== 0) {
      return dateCompare
    }
    // 日期相同，按代码排序
    return (a.code || '').localeCompare(b.code || '')
  })
  
  // 构建 CSV 内容
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
  
  const csvContent = [
    headers.join(','),
    ...rows.map(row => row.join(','))
  ].join('\n')
  
  // 添加 BOM 以支持中文
  const BOM = '\uFEFF'
  const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  const url = URL.createObjectURL(blob)
  link.setAttribute('href', url)
  link.setAttribute('download', `回测结果_${activeFile.value.replace('.jsonl', '')}_${new Date().toISOString().slice(0, 10)}.csv`)
  link.style.visibility = 'hidden'
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}

onMounted(() => {
  loadFileList()
})
</script>

<style scoped>
:deep(.ant-table-small) {
  font-size: 12px;
}

:deep(.ant-table-small .ant-table-thead > tr > th) {
  padding: 8px 4px;
  font-size: 12px;
  font-weight: 600;
}

:deep(.ant-table-small .ant-table-tbody > tr > td) {
  padding: 4px;
  font-size: 12px;
}

:deep(.bg-green-50) {
  background-color: #f0fdf4;
}

:deep(.bg-red-50) {
  background-color: #fef2f2;
}

/* 涨跌幅颜色样式 */
:deep(.text-red-600) {
  color: #dc2626 !important;
}

:deep(.text-green-600) {
  color: #16a34a !important;
}

/* 左侧导航栏样式 */
:deep(.ant-menu) {
  background: transparent;
  border-right: none;
}

:deep(.ant-menu-item) {
  margin: 2px 4px;
  padding: 6px 10px;
  height: auto;
  min-height: 48px;
  line-height: 1.4;
  border-radius: 4px;
  transition: all 0.2s;
  border: 1px solid transparent;
}

:deep(.ant-menu-item:hover) {
  background-color: #f0f0f0;
  border-color: #d9d9d9;
}

:deep(.ant-menu-item-selected) {
  background-color: #e6f7ff;
  color: #1890ff;
  font-weight: 600;
  border-color: #1890ff;
  box-shadow: 0 2px 4px rgba(24, 144, 255, 0.1);
}

:deep(.ant-menu-item-selected::after) {
  display: none;
}

/* 战法文件背景颜色 */
:deep(.strategy-file) {
  background-color: #fff7e6 !important;
  border-left: 3px solid #faad14 !important;
}

:deep(.strategy-file:hover) {
  background-color: #ffecc7 !important;
}

:deep(.strategy-file.ant-menu-item-selected) {
  background-color: #fff1b8 !important;
  border-color: #faad14 !important;
  border-left-width: 3px !important;
}

/* 文件菜单项样式 */
.file-menu-item {
  width: 100%;
}

.file-menu-item :deep(.ant-menu-title-content) {
  width: 100%;
}
</style>