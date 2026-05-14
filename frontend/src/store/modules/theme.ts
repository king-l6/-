import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

const STORAGE_KEY = 'app-color-scheme'

export type ColorScheme = 'light' | 'dark'

export const useThemeStore = defineStore('theme', () => {
  const scheme = ref<ColorScheme>('light')
  const isDark = computed(() => scheme.value === 'dark')

  function applyDom() {
    const el = document.documentElement
    if (scheme.value === 'dark') el.classList.add('dark')
    else el.classList.remove('dark')
  }

  function setScheme(next: ColorScheme) {
    scheme.value = next
    try {
      localStorage.setItem(STORAGE_KEY, next)
    } catch {
      /* ignore */
    }
    applyDom()
  }

  function toggle() {
    setScheme(scheme.value === 'dark' ? 'light' : 'dark')
  }

  function init() {
    try {
      const saved = localStorage.getItem(STORAGE_KEY)
      if (saved === 'dark' || saved === 'light') {
        scheme.value = saved
      } else if (window.matchMedia?.('(prefers-color-scheme: dark)')?.matches) {
        scheme.value = 'dark'
      }
    } catch {
      /* ignore */
    }
    applyDom()
  }

  return { scheme, isDark, setScheme, toggle, init }
})
