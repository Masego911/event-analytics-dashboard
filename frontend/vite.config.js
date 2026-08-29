import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8080',
      '/download': 'http://localhost:8080',
      '/logo': 'http://localhost:8080',
    },
  },
})
