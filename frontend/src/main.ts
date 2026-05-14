import { createApp } from 'vue'
import { pinia } from './store'
import router from './router'
import App from './App.vue'
import Antd from 'ant-design-vue'
import 'ant-design-vue/dist/reset.css'
import './style.css'
import { useThemeStore } from './store/modules/theme'

const app = createApp(App)

app.use(pinia)
useThemeStore().init()
app.use(router)
app.use(Antd)

app.mount('#app')
