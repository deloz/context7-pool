import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  base: '/admin/',
  plugins: [vue()],
  server: {
    port: 42422,
    proxy: {
      '/api': 'http://127.0.0.1:42421',
    },
  },
})
