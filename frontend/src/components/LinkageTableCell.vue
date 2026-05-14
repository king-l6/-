<template>
  <span :class="textClass">
    <span
      v-if="tDayBadge"
      class="mr-1 shrink-0 rounded bg-sky-100 text-sky-950 px-1 py-0.5 text-[10px] font-semibold ring-1 ring-sky-300/70 dark:bg-sky-900/40 dark:text-sky-100 dark:ring-sky-600/50"
      :title="'概念/行业涨幅与名次均按该交易日东财板块指数日 K 对齐'"
    >{{ tDayBadge }}</span>
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

const tDayBadge = computed(() => {
  const r = props.record
  if (r.linkage_date_aligned !== true) return ''
  const txt = (r.linkage_text || '').trim()
  if (txt.startsWith('[T日')) return ''
  const d = (r.linkage_board_trade_date || '').trim().slice(0, 10)
  if (!d || d.length < 10) return ''
  return `T日${d}`
})

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
