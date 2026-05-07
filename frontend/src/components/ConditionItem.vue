<template>
  <div class="flex items-center gap-2 p-3 bg-gray-50 rounded-lg">
    <Tooltip title="拖拽调整顺序">
      <span class="drag-handle cursor-move text-gray-400 hover:text-gray-600 flex-shrink-0">
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 8h16M4 16h16" />
        </svg>
      </span>
    </Tooltip>
    
    <Tooltip title="选择条件类型：涨停、涨幅大于/小于、成交量比例">
      <Select
        v-model:value="localCondition.type"
        class="flex-1 min-w-[140px]"
        @change="handleTypeChange"
      >
        <SelectOption value="limit_up">涨停</SelectOption>
        <SelectOption value="pct_change_gt">涨幅大于</SelectOption>
        <SelectOption value="pct_change_lt">涨幅小于</SelectOption>
        <SelectOption value="pct_change_between">涨幅大于且小于</SelectOption>
        <SelectOption value="three_limit_up">三连板</SelectOption>
        <SelectOption value="recent_limit_up">近N日有涨停</SelectOption>
        <SelectOption value="ma_cross_up">均线上穿</SelectOption>
        <SelectOption value="volume_ratio">成交量比例</SelectOption>
        <SelectOption value="consecutive_up_days_gte">连阳天数>=N</SelectOption>
        <SelectOption value="upper_shadow_pct_gt">上影线幅度&gt;N%</SelectOption>
      </Select>
    </Tooltip>
    
    <Tooltip :title="date1Tooltip">
      <InputNumber
        v-model:value="localCondition.date1"
        :placeholder="date1Placeholder"
        class="w-32"
        :min="-365"
        :max="365"
      />
    </Tooltip>
    
    <template v-if="localCondition.type === 'pct_change_gt' || localCondition.type === 'pct_change_lt'">
      <Tooltip title="涨幅百分比，例如：5 表示涨幅大于/小于 5%">
        <InputNumber
          v-model:value="localCondition.value"
          placeholder="涨幅(%)"
          class="w-24"
          :precision="2"
          :step="0.1"
        />
      </Tooltip>
    </template>
    
    <template v-if="localCondition.type === 'pct_change_between'">
      <Tooltip title="最小涨幅百分比，例如：5 表示涨幅大于等于 5%">
        <InputNumber
          v-model:value="localCondition.minValue"
          placeholder="最小涨幅(%)"
          class="w-24"
          :precision="2"
          :step="0.1"
        />
      </Tooltip>
      <span class="text-gray-500">且</span>
      <Tooltip title="最大涨幅百分比，例如：10 表示涨幅小于等于 10%">
        <InputNumber
          v-model:value="localCondition.maxValue"
          placeholder="最大涨幅(%)"
          class="w-24"
          :precision="2"
          :step="0.1"
        />
      </Tooltip>
    </template>
    
    <template v-if="localCondition.type === 'three_limit_up' || localCondition.type === 'recent_limit_up'">
      <Tooltip title="检查天数范围：从指定日期往前检查多少个交易日（三连板/近期有涨停）">
        <InputNumber
          v-model:value="localCondition.days"
          placeholder="检查天数"
          class="w-32"
          :min="3"
          :max="365"
        />
      </Tooltip>
    </template>

    <template v-if="localCondition.type === 'consecutive_up_days_gte'">
      <Tooltip title="最小连阳天数（连续涨跌幅>0的交易日，包含检查日）">
        <InputNumber
          v-model:value="localCondition.consecutiveDays"
          placeholder="连阳天数"
          class="w-32"
          :min="1"
          :max="30"
        />
      </Tooltip>
    </template>

    <template v-if="localCondition.type === 'upper_shadow_pct_gt'">
      <Tooltip title="上影线幅度阈值（%）：最高涨幅-收盘涨幅">
        <InputNumber
          v-model:value="localCondition.value"
          placeholder="上影幅度(%)"
          class="w-32"
          :precision="2"
          :step="0.1"
          :min="0"
        />
      </Tooltip>
    </template>
    
    <template v-if="localCondition.type === 'ma_cross_up'">
      <Tooltip title="短期均线周期（默认5日）">
        <InputNumber
          v-model:value="localCondition.shortPeriod"
          placeholder="短期"
          class="w-24"
          :min="2"
          :max="100"
        />
      </Tooltip>
      <span class="text-gray-500">上穿</span>
      <Tooltip title="长期均线周期（默认10日）">
        <InputNumber
          v-model:value="localCondition.longPeriod"
          placeholder="长期"
          class="w-24"
          :min="2"
          :max="100"
        />
      </Tooltip>
    </template>
    
    <template v-if="localCondition.type === 'volume_ratio'">
      <Tooltip title="第二个交易日偏移（负数表示往前推，0表示回测日期当天）">
        <InputNumber
          v-model:value="localCondition.date2"
          placeholder="交易日2"
          class="w-32"
          :min="-365"
          :max="365"
        />
      </Tooltip>
      <Tooltip title="成交量比例，例如：1.5 表示 date1 的成交量是 date2 的 1.5 倍">
        <InputNumber
          v-model:value="localCondition.ratio"
          placeholder="比例"
          class="w-24"
          :precision="2"
          :step="0.1"
          :min="0.01"
        />
      </Tooltip>
    </template>
    
    <Button
      type="text"
      danger
      @click="handleRemove"
      class="flex-shrink-0"
    >
      删除
    </Button>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { Select, SelectOption, InputNumber, Button, Tooltip } from 'ant-design-vue'
import type { StrategyCondition } from '@/types'

interface Props {
  condition: StrategyCondition
  index: number
}

interface Emits {
  (e: 'update', condition: StrategyCondition, index: number): void
  (e: 'remove', index: number): void
}

const props = defineProps<Props>()
const emit = defineEmits<Emits>()

const localCondition = ref<StrategyCondition>({ ...props.condition })

watch(() => props.condition, (newCondition) => {
  localCondition.value = { ...newCondition }
}, { deep: true })

watch(localCondition, (newCondition) => {
  emit('update', { ...newCondition }, props.index)
}, { deep: true })

function handleTypeChange() {
  // 根据类型重置相关字段
  if (localCondition.value.type === 'limit_up') {
    delete localCondition.value.value
    delete localCondition.value.minValue
    delete localCondition.value.maxValue
    delete localCondition.value.date2
    delete localCondition.value.ratio
    delete localCondition.value.days
    delete localCondition.value.shortPeriod
    delete localCondition.value.longPeriod
    delete localCondition.value.consecutiveDays
  } else if (localCondition.value.type === 'pct_change_gt' || localCondition.value.type === 'pct_change_lt') {
    localCondition.value.value = localCondition.value.value || 0
    delete localCondition.value.minValue
    delete localCondition.value.maxValue
    delete localCondition.value.date2
    delete localCondition.value.ratio
    delete localCondition.value.days
    delete localCondition.value.shortPeriod
    delete localCondition.value.longPeriod
    delete localCondition.value.consecutiveDays
  } else if (localCondition.value.type === 'pct_change_between') {
    delete localCondition.value.value
    localCondition.value.minValue = localCondition.value.minValue ?? 0
    localCondition.value.maxValue = localCondition.value.maxValue ?? 10
    delete localCondition.value.date2
    delete localCondition.value.ratio
    delete localCondition.value.days
    delete localCondition.value.shortPeriod
    delete localCondition.value.longPeriod
    delete localCondition.value.consecutiveDays
  } else if (localCondition.value.type === 'three_limit_up' || localCondition.value.type === 'recent_limit_up') {
    delete localCondition.value.value
    delete localCondition.value.minValue
    delete localCondition.value.maxValue
    delete localCondition.value.date2
    delete localCondition.value.ratio
    localCondition.value.days = localCondition.value.days || (localCondition.value.type === 'three_limit_up' ? 30 : 10)
    delete localCondition.value.shortPeriod
    delete localCondition.value.longPeriod
    delete localCondition.value.consecutiveDays
  } else if (localCondition.value.type === 'ma_cross_up') {
    delete localCondition.value.value
    delete localCondition.value.minValue
    delete localCondition.value.maxValue
    delete localCondition.value.date2
    delete localCondition.value.ratio
    delete localCondition.value.days
    localCondition.value.shortPeriod = localCondition.value.shortPeriod || 5
    localCondition.value.longPeriod = localCondition.value.longPeriod || 10
    delete localCondition.value.consecutiveDays
  } else if (localCondition.value.type === 'volume_ratio') {
    delete localCondition.value.value
    delete localCondition.value.minValue
    delete localCondition.value.maxValue
    localCondition.value.date2 = localCondition.value.date2 || 0
    localCondition.value.ratio = localCondition.value.ratio || 1
    delete localCondition.value.days
    delete localCondition.value.shortPeriod
    delete localCondition.value.longPeriod
    delete localCondition.value.consecutiveDays
  } else if (localCondition.value.type === 'consecutive_up_days_gte') {
    delete localCondition.value.value
    delete localCondition.value.minValue
    delete localCondition.value.maxValue
    delete localCondition.value.date2
    delete localCondition.value.ratio
    delete localCondition.value.days
    delete localCondition.value.shortPeriod
    delete localCondition.value.longPeriod
    localCondition.value.consecutiveDays = localCondition.value.consecutiveDays || 3
  } else if (localCondition.value.type === 'upper_shadow_pct_gt') {
    localCondition.value.value = localCondition.value.value ?? 2
    delete localCondition.value.minValue
    delete localCondition.value.maxValue
    delete localCondition.value.date2
    delete localCondition.value.ratio
    delete localCondition.value.days
    delete localCondition.value.shortPeriod
    delete localCondition.value.longPeriod
    delete localCondition.value.consecutiveDays
  }
}

function handleRemove() {
  emit('remove', props.index)
}

const date1Tooltip = ref('交易日偏移：负数表示往前推N个交易日，0表示回测日期当天')
const date1Placeholder = ref('交易日偏移')

watch(() => localCondition.value.type, (type) => {
  switch (type) {
    case 'limit_up':
      date1Tooltip.value = '交易日偏移：负数表示往前推N个交易日，0表示回测日期当天。例如：-3 表示往前推3个交易日'
      date1Placeholder.value = '交易日偏移'
      break
    case 'pct_change_gt':
    case 'pct_change_lt':
      date1Tooltip.value = '交易日偏移：负数表示往前推N个交易日，0表示回测日期当天。例如：-2 表示往前推2个交易日'
      date1Placeholder.value = '交易日偏移'
      break
    case 'pct_change_between':
      date1Tooltip.value = '交易日偏移：负数表示往前推N个交易日，0表示回测日期当天。例如：-2 表示往前推2个交易日'
      date1Placeholder.value = '交易日偏移'
      break
    case 'three_limit_up':
      date1Tooltip.value = '起始日期偏移：负数表示往前推N个交易日，0表示回测日期当天。从该日期往前检查是否出现三连板'
      date1Placeholder.value = '起始日期'
      break
    case 'recent_limit_up':
      date1Tooltip.value = '起始日期偏移：负数表示往前推N个交易日，0表示回测日期当天。从该日期往前的若干个交易日内是否至少有涨停'
      date1Placeholder.value = '起始日期'
      break
    case 'ma_cross_up':
      date1Tooltip.value = '检查日期偏移：负数表示往前推N个交易日，0表示回测日期当天。检查该日期是否出现均线上穿'
      date1Placeholder.value = '检查日期'
      break
    case 'volume_ratio':
      date1Tooltip.value = '第一个交易日偏移：负数表示往前推N个交易日，0表示回测日期当天。例如：-2 表示往前推2个交易日'
      date1Placeholder.value = '交易日1'
      break
    case 'consecutive_up_days_gte':
      date1Tooltip.value = '检查日期偏移：以该交易日为结束日，向前统计连续阳线（涨跌幅>0）天数'
      date1Placeholder.value = '检查日期'
      break
    case 'upper_shadow_pct_gt':
      date1Tooltip.value = '检查日期偏移：上影线幅度=最高涨幅-收盘涨幅（均相对前收盘）'
      date1Placeholder.value = '检查日期'
      break
    default:
      date1Tooltip.value = '交易日偏移：负数表示往前推N个交易日，0表示回测日期当天'
      date1Placeholder.value = '交易日偏移'
  }
}, { immediate: true })
</script>
