import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

export default defineConfig({
  plugins: [svelte()],
  server: {
    host: true, // listen on 0.0.0.0 so LAN devices can reach the Vite dev server
    proxy: {
      '/ws': {
        // Use IPv4 explicitly. On some systems Node resolves localhost to ::1,
        // while the Rust backend is bound on IPv4 (0.0.0.0:3000 by default).
        target: 'ws://127.0.0.1:3000',
        ws: true,
      },
    },
  },
})
