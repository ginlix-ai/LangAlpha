import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider } from './contexts/ThemeContext'
import { AuthProvider } from './contexts/AuthContext'
import App from './App'
import './i18n'
import './index.css'
// Side-effect import: the modality listeners have to be running before the
// first click, not from whichever overlay happens to load first.
import './lib/inputModality'
// Side-effect import: animations created in a hidden tab must be instant from
// the first render, so the flag has to be set before anything mounts.
import './lib/hiddenTabMotion'
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

ReactDOM.createRoot(document.getElementById('root')!).render(
  <QueryClientProvider client={queryClient}>
    <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
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
)
