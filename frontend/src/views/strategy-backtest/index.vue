<template>
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-2">
    <div class="lg:col-span-1">
      <StrategyForm />
    </div>
    <div class="lg:col-span-1">
      <ResultsTable />
    </div>
  </div>
</template>

<script setup lang="ts">
import StrategyForm from '@/components/StrategyForm.vue'
import ResultsTable from '@/components/ResultsTable.vue'
</script>
