import react from '@vitejs/plugin-react'
import type { PluginOption } from 'vite'

/**
 * React with the compiler on, shared by the app and the unit tests so the tests
 * run the code that ships. `REACT_COMPILER=off` builds without it, which is how
 * to tell a compiler regression from an app one.
 */
export function reactPlugins(): PluginOption[] {
  if (process.env.REACT_COMPILER === 'off') return [react()]
  return [react({ compiler: true })]
}
