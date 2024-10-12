import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [sveltekit()],
	server: {
		port: process.env.VITE_PORT ? parseInt(process.env.VITE_PORT) : 3000, // Default to 3000 if not set
	  },
});
