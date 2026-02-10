import { defineConfig, loadEnv } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

export default defineConfig(({ mode }) => {
  // 加载环境变量
  const env = loadEnv(mode, process.cwd(), '')
  
  return {
    plugins: [vue()],
    resolve: {
      alias: {
        '@': resolve(__dirname, 'src')
      }
    },
    build: {
      // 构建输出到项目根目录的 static 目录
      outDir: resolve(__dirname, '../static'),
      emptyOutDir: true
    },
    server: {
      // 支持通过环境变量配置端口，默认 5173
      port: parseInt(env.VITE_PORT || process.env.VITE_PORT || '5173'),
      proxy: {
        '/api': {
          // 支持通过环境变量配置后端地址，默认 localhost:8086
          target: env.VITE_API_TARGET || process.env.VITE_API_TARGET || 'http://localhost:8086',
          changeOrigin: true
        }
      }
    }
  }
})
