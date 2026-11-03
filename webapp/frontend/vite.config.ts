import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Vite config:
// - dev: serves the SPA on :5173 and proxies /api/* to the FastAPI backend on :8000
// - build: emits to ./dist, which the FastAPI backend static-mounts in prod
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: false,
  },
})
