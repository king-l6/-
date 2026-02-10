import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { Strategy, StrategyCondition, ExcludeRules, StockResult } from '@/types'

export const useStrategyStore = defineStore('strategy', () => {
  // State
  const strategyName = ref<string>('涨停回测策略')
  const timeRange = ref<number>(30)
  const conditions = ref<StrategyCondition[]>([])
  const exclude = ref<ExcludeRules>({
    kcb: true,
    cyb: true,
    bjs: true,
    st: true,
    delist: true
  })
  
  // Results
  const results = ref<StockResult[]>([])
  const loading = ref<boolean>(false)
  const error = ref<string | null>(null)
  
  // Getters
  const hasResults = computed(() => results.value.length > 0)
  const resultsCount = computed(() => results.value.length)
  
  // Actions
  function setStrategyName(name: string) {
    strategyName.value = name
  }
  
  function setTimeRange(range: number) {
    timeRange.value = range
  }
  
  function addCondition(condition: StrategyCondition) {
    conditions.value.push(condition)
  }
  
  function removeCondition(index: number) {
    conditions.value.splice(index, 1)
  }
  
  function updateCondition(index: number, condition: StrategyCondition) {
    conditions.value[index] = condition
  }
  
  function setConditions(newConditions: StrategyCondition[]) {
    conditions.value = newConditions
  }
  
  function setExclude(newExclude: ExcludeRules) {
    exclude.value = { ...newExclude }
  }
  
  function setLoading(value: boolean) {
    loading.value = value
  }
  
  function setResults(newResults: StockResult[]) {
    results.value = newResults
    error.value = null
  }
  
  function setError(err: string | null) {
    error.value = err
    results.value = []
  }
  
  function clearResults() {
    results.value = []
    error.value = null
  }
  
  function resetStrategy() {
    strategyName.value = '涨停回测策略'
    timeRange.value = 30
    conditions.value = []
    exclude.value = {
      kcb: true,
      cyb: true,
      bjs: true,
      st: true,
      delist: true
    }
    clearResults()
  }
  
  function getStrategy(): Strategy {
    return {
      name: strategyName.value,
      conditions: conditions.value,
      exclude: exclude.value,
      timeRange: timeRange.value
    }
  }
  
  return {
    // State
    strategyName,
    timeRange,
    conditions,
    exclude,
    results,
    loading,
    error,
    // Getters
    hasResults,
    resultsCount,
    // Actions
    setStrategyName,
    setTimeRange,
    addCondition,
    removeCondition,
    updateCondition,
    setConditions,
    setExclude,
    setLoading,
    setResults,
    setError,
    clearResults,
    resetStrategy,
    getStrategy
  }
})
