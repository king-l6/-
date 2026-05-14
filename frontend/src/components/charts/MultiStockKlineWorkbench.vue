<template>
  <div ref="hostRef" class="w-full rounded border border-slate-200 bg-white dark:border-neutral-700 dark:bg-neutral-950" :style="{ height: heightPx + 'px' }" />
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import type { EChartsOption } from 'echarts'

const props = withDefaults(
  defineProps<{
    option: EChartsOption | null
    heightPx?: number
  }>(),
  { heightPx: 520 }
)

const hostRef = ref<HTMLDivElement | null>(null)
let chart: echarts.ECharts | null = null

function dispose() {
  chart?.dispose()
  chart = null
}

function render() {
  const el = hostRef.value
  if (!el) return
  if (!chart) chart = echarts.init(el)
  if (props.option) chart.setOption(props.option, { notMerge: true })
  else chart.clear()
}

onMounted(() => {
  render()
  window.addEventListener('resize', resize)
})

onUnmounted(() => {
  window.removeEventListener('resize', resize)
  dispose()
})

function resize() {
  chart?.resize()
}

watch(
  () => props.option,
  () => render(),
  { deep: true }
)

watch(
  () => props.heightPx,
  () => resize()
)
</script>
