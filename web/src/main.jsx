import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ConfigProvider, theme as antdTheme } from 'antd'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider, useTheme } from './contexts/ThemeContext'
import { AuthProvider } from './contexts/AuthContext'
import App from './App.jsx'
import './i18n'
import './index.css'
import { Toaster } from './components/ui/toaster'

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

function AntdThemeProvider({ children }) {
  const { theme } = useTheme()
  return (
    <ConfigProvider
      theme={{
        algorithm: theme === 'light' ? antdTheme.defaultAlgorithm : antdTheme.darkAlgorithm,
      }}
    >
      {children}
    </ConfigProvider>
  )
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <QueryClientProvider client={queryClient}>
    <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <ThemeProvider>
        <AntdThemeProvider>
          <AuthProvider>
            <App />
            <Toaster />
          </AuthProvider>
        </AntdThemeProvider>
      </ThemeProvider>
    </BrowserRouter>
  </QueryClientProvider>,
)
