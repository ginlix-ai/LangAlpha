import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { AuthProvider } from './contexts/AuthContext'
import App from './App.jsx'
import './index.css'
import { shouldUseMSW } from './config/msw'
import { Toaster } from './components/ui/toaster'

// Conditionally enable MSW
async function enableMocking() {
  if (shouldUseMSW()) {
    const { worker } = await import('./mocks/browser')
    return worker.start({
      onUnhandledRequest: 'bypass', // Don't warn about unhandled requests
    })
  }
}

// Start MSW before rendering the app
enableMocking().then(() => {
  ReactDOM.createRoot(document.getElementById('root')).render(
    <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <AuthProvider>
        <App />
        <Toaster />
      </AuthProvider>
    </BrowserRouter>,
  )
})
