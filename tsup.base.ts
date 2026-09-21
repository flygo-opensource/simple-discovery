import { defineConfig } from 'tsup'

// Every published package bundles the private @simple-discovery/core (code and types) into its own
// build, so users install a single package. Other dependencies stay external.
export default defineConfig({
    entry: ['src/index.ts'],
    format: 'esm',
    target: 'node18',
    outDir: 'build',
    clean: true,
    sourcemap: true,
    noExternal: ['@simple-discovery/core'],
    dts: { resolve: ['@simple-discovery/core'] },
})
