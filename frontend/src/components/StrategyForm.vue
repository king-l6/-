<template>
  <Card title="策略配置" class="h-full">
    <Form :model="formData" layout="vertical" @submit="handleSubmit">
      <FormItem label="策略名称">
        <Input
          v-model:value="formData.strategyName"
          placeholder="输入策略名称（可选）"
        />
      </FormItem>
      
      <FormItem label="回测时间范围（交易日，不含周末）">
        <Select v-model:value="formData.timeRange">
          <SelectOption :value="30">近30个交易日</SelectOption>
          <SelectOption :value="60">近60个交易日</SelectOption>
          <SelectOption :value="90">近90个交易日</SelectOption>
        </Select>
      </FormItem>
      
      <FormItem label="策略条件">
        <div class="space-y-1">
          <draggable
            v-model="conditions"
            handle=".drag-handle"
            :item-key="(_item: StrategyCondition, index: number) => index"
            @end="handleDragEnd"
          >
            <template #item="{ element, index }">
              <div class="mb-1">
                <ConditionItem
                  :key="`condition-${index}-${element.date1}-${element.date2}`"
                  :condition="element"
                  :index="index"
                  @update="handleConditionUpdate"
                  @remove="handleConditionRemove"
                />
              </div>
            </template>
          </draggable>
          <Button
            type="dashed"
            block
            size="small"
            @click="handleAddCondition"
            class="mt-1"
          >
            + 添加条件
          </Button>
        </div>
      </FormItem>
      
      <FormItem label="排除规则">
        <Space direction="vertical" size="small">
          <Checkbox v-model:checked="formData.exclude.kcb">排除科创板</Checkbox>
          <Checkbox v-model:checked="formData.exclude.cyb">排除创业板</Checkbox>
          <Checkbox v-model:checked="formData.exclude.bjs">排除北交所</Checkbox>
          <Checkbox v-model:checked="formData.exclude.st">排除ST股</Checkbox>
          <Checkbox v-model:checked="formData.exclude.delist">排除退市股</Checkbox>
        </Space>
      </FormItem>
      
      <FormItem>
        <Space size="small">
          <Button
            type="primary"
            html-type="submit"
            :loading="loading"
            size="small"
          >
            开始回测
          </Button>
          <Button @click="handleLoadExample" size="small">
            加载示例策略
          </Button>
          <Button @click="handleClear" size="small">
            清空
          </Button>
        </Space>
      </FormItem>
    </Form>
  </Card>
</template>

<script setup lang="ts">
import { ref, watch, onMounted } from 'vue'
import { Card, Form, FormItem, Input, Select, SelectOption, Button, Checkbox, Space } from 'ant-design-vue'
import draggable from 'vuedraggable'
import { useStrategyStore } from '@/store/modules/strategy'
import { useBacktest } from '@/hooks/strategy-backtest/useBacktest'
import ConditionItem from './ConditionItem.vue'
import type { StrategyCondition } from '@/types'

const strategyStore = useStrategyStore()
const { executeBacktest } = useBacktest()

const formData = ref({
  strategyName: strategyStore.strategyName,
  timeRange: strategyStore.timeRange,
  exclude: { ...strategyStore.exclude }
})

const conditions = ref<StrategyCondition[]>([...strategyStore.conditions])
const loading = ref(false)

watch(() => strategyStore.loading, (newLoading) => {
  loading.value = newLoading
})

watch(() => strategyStore.strategyName, (newName) => {
  formData.value.strategyName = newName
})

watch(() => strategyStore.timeRange, (newRange) => {
  formData.value.timeRange = newRange
})

watch(() => strategyStore.exclude, (newExclude) => {
  formData.value.exclude = { ...newExclude }
}, { deep: true })

let isUpdatingFromStore = false

watch(() => strategyStore.conditions, (newConditions) => {
  if (!isUpdatingFromStore) {
    conditions.value = [...newConditions]
  }
}, { deep: true })

onMounted(() => {
  if (conditions.value.length === 0) {
    handleAddCondition()
  }
})

function handleAddCondition() {
  // 将所有现有条件的 date1 和 date2 都减 1（多推移一天），创建新对象确保响应式更新
  const updatedConditions = conditions.value.map(condition => {
    const updated: StrategyCondition = { ...condition }
    if (updated.date1 !== undefined && updated.date1 !== null) {
      updated.date1 = updated.date1 - 1
    }
    if (updated.date2 !== undefined && updated.date2 !== null) {
      updated.date2 = updated.date2 - 1
    }
    return updated
  })
  
  // 添加新条件，date1 为 0（回测日期当天）
  const newCondition: StrategyCondition = {
    type: 'limit_up',
    date1: 0
  }
  updatedConditions.push(newCondition)
  
  // 先更新 store，再更新本地（避免 watch 覆盖）
  isUpdatingFromStore = true
  strategyStore.setConditions(updatedConditions)
  conditions.value = updatedConditions
  isUpdatingFromStore = false
}

function handleConditionUpdate(condition: StrategyCondition, index: number) {
  // 创建新数组确保响应式更新
  const updated = [...conditions.value]
  updated[index] = { ...condition }
  conditions.value = updated
  strategyStore.updateCondition(index, condition)
}

function handleConditionRemove(index: number) {
  // 删除指定索引的条件
  const updatedConditions = conditions.value.filter((_, i) => i !== index)
  
  // 重新调整所有条件的日期偏移，让最后一个条件的 date1 为 0，前面的依次递减
  const adjustedConditions = updatedConditions.map((condition, i) => {
    const adjusted: StrategyCondition = { ...condition }
    // 计算新的 date1：最后一个为 0，倒数第二个为 -1，以此类推
    const newDate1 = -(updatedConditions.length - 1 - i)
    adjusted.date1 = newDate1
    
    // 如果有 date2，也需要调整（保持相对关系）
    if (adjusted.date2 !== undefined && adjusted.date2 !== null) {
      // date2 相对于 date1 的偏移量
      const date2Offset = adjusted.date2 - adjusted.date1
      // 更新 date2 为新的 date1 + 偏移量
      adjusted.date2 = newDate1 + date2Offset
    }
    
    return adjusted
  })
  
  // 更新本地和 store
  isUpdatingFromStore = true
  conditions.value = adjustedConditions
  strategyStore.setConditions(adjustedConditions)
  isUpdatingFromStore = false
}

function handleDragEnd() {
  strategyStore.setConditions(conditions.value)
}

function handleSubmit(e: Event) {
  e.preventDefault()
  
  strategyStore.setStrategyName(formData.value.strategyName)
  strategyStore.setTimeRange(formData.value.timeRange)
  strategyStore.setExclude(formData.value.exclude)
  strategyStore.setConditions(conditions.value)
  
  executeBacktest()
}

function handleLoadExample() {
  strategyStore.resetStrategy()
  
  const exampleConditions: StrategyCondition[] = [
    { type: 'limit_up', date1: -3 },
    { type: 'pct_change_gt', date1: -2, value: 0 },
    { type: 'pct_change_lt', date1: -1, value: 0 },
    { type: 'volume_ratio', date1: -2, date2: -1, ratio: 1 },
    { type: 'volume_ratio', date1: 0, date2: -1, ratio: 1 },
    { type: 'pct_change_gt', date1: 0, value: 0 }
  ]
  
  strategyStore.setStrategyName('涨停回测策略')
  strategyStore.setTimeRange(30)
  strategyStore.setConditions(exampleConditions)
  
  formData.value.strategyName = '涨停回测策略'
  formData.value.timeRange = 30
  conditions.value = [...exampleConditions]
}

function handleClear() {
  strategyStore.resetStrategy()
  formData.value.strategyName = '涨停回测策略'
  formData.value.timeRange = 30
  formData.value.exclude = {
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true
  }
  conditions.value = []
  handleAddCondition()
}
</script>
