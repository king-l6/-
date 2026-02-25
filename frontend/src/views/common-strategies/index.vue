<template>
  <div>
    <CommonStrategies @strategy-loaded="handleStrategyLoaded" />
  </div>
</template>

<script setup lang="ts">
import { useRouter } from 'vue-router'
import CommonStrategies from '@/components/CommonStrategies.vue'

const router = useRouter()

function handleStrategyLoaded() {
  router.push({ name: 'StrategyBacktest' })
}
</script>
