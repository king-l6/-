<template>
  <a-config-provider :theme="antdTheme">
    <router-view />
  </a-config-provider>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { theme } from 'ant-design-vue'
import { useThemeStore } from '@/store/modules/theme'

const themeStore = useThemeStore()

const antdTheme = computed(() =>
  themeStore.scheme === 'dark'
    ? { algorithm: theme.darkAlgorithm }
    : { algorithm: theme.defaultAlgorithm }
)
</script>

<style scoped>
</style>
