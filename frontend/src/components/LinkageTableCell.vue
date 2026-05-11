<template>
  <span :class="textClass">
    <template v-if="conceptBlock">
      <span>概念:</span>
      <template v-for="(ch, i) in conceptBlock.chunks" :key="i">
        <span v-if="i > 0">、</span>
        <span
          :class="
            ch.highlight
              ? 'rounded px-0.5 font-semibold bg-amber-200 text-amber-950 shadow-sm ring-1 ring-amber-400/60 dark:bg-amber-500/30 dark:text-amber-50 dark:ring-amber-400/40'
              : ''
          "
        >{{ ch.text }}</span>
      </template>
      <span>{{ conceptBlock.meanSuffix }}</span>
    </template>
    <template v-if="industryLine">
      <span v-if="conceptBlock">；</span>
      <span>{{ industryLine }}</span>
    </template>
    <template v-if="!conceptBlock && !industryLine">
      <span>{{ plainFallback }}</span>
    </template>
  </span>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { StockResult } from '@/types'
import {
  formatLinkageConceptMeanSuffix,
  formatLinkageIndustryLine,
  linkageConceptChunks
} from '@/utils/linkageDisplay'

const props = withDefaults(
  defineProps<{
    record: StockResult
    /** 外层容器 Tailwind class（截断、字号等） */
    textClass?: string
  }>(),
  {
    textClass: 'text-xs text-gray-800 inline-block align-top max-w-full truncate'
  }
)

const conceptBlock = computed(() => {
  const lc = props.record.linkage_concepts
  if (!lc?.length) return null
  return {
    chunks: linkageConceptChunks(lc),
    meanSuffix: formatLinkageConceptMeanSuffix(props.record, lc)
  }
})

const industryLine = computed(() => formatLinkageIndustryLine(props.record))

const plainFallback = computed(() => props.record.linkage_text?.trim() || '-')
</script>
