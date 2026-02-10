<template>
  <div class="min-h-screen bg-gray-50">
    <div class="container mx-auto px-4 py-6">
      <header class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900 mb-1">股票策略回测系统</h1>
        <p class="text-sm text-gray-600">自定义策略条件，回测A股市场</p>
      </header>
      
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="lg:col-span-1">
          <StrategyForm />
        </div>
        <div class="lg:col-span-1">
          <ResultsTable />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import StrategyForm from '@/components/StrategyForm.vue'
import ResultsTable from '@/components/ResultsTable.vue'
</script>
