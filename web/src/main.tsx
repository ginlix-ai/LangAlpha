import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider } from './contexts/ThemeContext'
import { AuthProvider } from './contexts/AuthContext'
import App from './App'
import { initI18n } from './i18n'
import './index.css'
// Side-effect import: the modality listeners have to be running before the
// first click, not from whichever overlay happens to load first.
import './lib/inputModality'
import { applyCardStyle, getCardStyle } from './lib/cardStyle'
import { Toaster } from './components/ui/toaster'
import { StaleBuildBoundary } from './components/StaleBuildBoundary'

// Initialize a global QueryClient for data fetching and caching
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: true, // Auto-refetch when user comes back to the tab
      retry: 1,                   // Retry failed requests once before showing error
      staleTime: 1000 * 60 * 2,   // Data is considered fresh for 2 minutes by default
    },
  },
})

applyCardStyle(getCardStyle())

// Nothing renders until the active locale's catalog is in, or the first paint
// shows raw keys. A catalog that fails leaves #root to index.html's pre-boot
// recovery (reload, then its dead-end notice), handed over explicitly since
// its listeners miss some failures, such as a catalog on another origin.
// Rendering anyway would run markBooted(), which resets that recovery's
// attempt count and turns its bounded reload into a loop.
void initI18n().then(() => ReactDOM.createRoot(document.getElementById('root')!).render(
  <QueryClientProvider client={queryClient}>
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {/* Wraps App only, deliberately not the providers: Toaster has to stay
              OUTSIDE, or a caught error unmounts the very thing that renders the
              recovery toast and the notice is a silent no-op. */}
          <StaleBuildBoundary>
            <App />
          </StaleBuildBoundary>
          <Toaster />
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  </QueryClientProvider>,
), (error: unknown) => {
  console.error(error)
  window.__LA_RECOVER__?.('resource')
})
