<template>
  <div class="sector-ranking">
    <div class="flex items-center gap-4 mb-4">
      <h2 class="text-lg font-bold">板块/概念排行</h2>
      <Select
        v-model:value="selectedDate"
        :options="dateOptions"
        placeholder="选择日期"
        style="width: 160px"
        @change="onDateChange"
      />
      <Radio.Group v-model:value="viewMode" button-style="solid" size="small">
        <Radio.Button value="concept">概念</Radio.Button>
        <Radio.Button value="industry">行业</Radio.Button>
      </Radio.Group>
    </div>

    <Table
      :columns="columns"
      :data-source="tableData"
      :loading="loading"
      :pagination="{ pageSize: 20, showSizeChanger: true, pageSizeOptions: ['10', '20', '50'] }"
      size="small"
      :scroll="{ x: 600 }"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'pct'">
          <span :class="record.pct >= 0 ? 'text-red-500' : 'text-green-500'">
            {{ record.pct >= 0 ? '+' : '' }}{{ record.pct.toFixed(2) }}%
          </span>
        </template>
      </template>
    </Table>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { Table, Select, Radio, message } from 'ant-design-vue'

const loading = ref(false)
const selectedDate = ref('')
const viewMode = ref('concept')
const dates = ref<string[]>([])
const currentData = ref<any>(null)

const dateOptions = computed(() =>
  dates.value.map(d => ({ label: d, value: d }))
)

const columns = [
  { title: '排名', dataIndex: 'rank', width: 80, sorter: (a: any, b: any) => a.rank - b.rank },
  { title: '板块名称', dataIndex: 'name', width: 150 },
  { title: '涨跌幅', dataIndex: 'pct', width: 120, sorter: (a: any, b: any) => a.pct - b.pct, defaultSortOrder: 'ascend' },
]

const tableData = computed(() => {
  if (!currentData.value) return []
  const key = viewMode.value === 'concept' ? 'concepts' : 'industries'
  return (currentData.value[key] || []).map((item: any) => ({
    ...item,
    key: item.board_id,
  }))
})

async function fetchData(date?: string) {
  loading.value = true
  try {
    const url = date ? `/api/sector-ranking?date=${date}` : '/api/sector-ranking'
    const res = await fetch(url)
    const data = await res.json()
    if (data.success) {
      dates.value = data.dates || []
      currentData.value = data.data
      if (!selectedDate.value && dates.value.length > 0) {
        selectedDate.value = dates.value[0]
      }
    } else {
      message.error('获取数据失败: ' + data.error)
    }
  } catch (e: any) {
    message.error('请求失败: ' + e.message)
  } finally {
    loading.value = false
  }
}

function onDateChange(date: string) {
  fetchData(date)
}

onMounted(() => fetchData())
</script>

<style scoped>
.sector-ranking {
  padding: 16px;
}
</style>
