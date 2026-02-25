import { createRouter, createWebHistory } from 'vue-router'
import Layout from '@/views/Layout.vue'

const routes = [
  {
    path: '/',
    component: Layout,
    redirect: '/strategy-backtest',
    children: [
      {
        path: 'strategy-backtest',
        name: 'StrategyBacktest',
        component: () => import('@/views/strategy-backtest/index.vue')
      },
      {
        path: 'common-strategies',
        name: 'CommonStrategies',
        component: () => import('@/views/common-strategies/index.vue')
      },
      {
        path: 'history-results',
        name: 'HistoryResults',
        component: () => import('@/views/history-results/index.vue')
      }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
