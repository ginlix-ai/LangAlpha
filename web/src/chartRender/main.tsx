/**
 * Entry of the chart render page (`chart-render.html`, built apart from the app
 * by `vite.chart-render.config.js` and shipped with chartkit into the sandbox).
 * Theme and locale go where the app reads them (localStorage, the `locale`
 * cookie) before anything mounts, so the chart starts as the user's would.
 */
import ReactDOM from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { ThemeProvider } from '@/contexts/ThemeContext';
import { initI18n } from '@/i18n';
import { applyCardStyle } from '@/lib/cardStyle';
import '@/index.css';

import { ChartRenderApp } from './ChartRenderApp';
import { loadFonts } from './fonts';
import { readPayload } from './payload';

const status = { mounted: false, error: null as string | null };
window.__LA_CHART_RENDER_STATUS__ = status;

async function boot(): Promise<void> {
  const payload = readPayload(window.__LA_CHART_RENDER__);
  localStorage.setItem('theme', payload.theme);
  document.documentElement.setAttribute('data-theme', payload.theme);
  // The app's default card style; with no attribute the page would draw Terminal.
  applyCardStyle('balanced');
  document.cookie = `locale=${payload.locale}; path=/`;
  document.body.style.margin = '0';
  await Promise.all([initI18n(), loadFonts()]);
  // No refetching: a snapshot is one frame of one answer.
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false, staleTime: Infinity } },
  });
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <ChartRenderApp payload={payload} />
      </ThemeProvider>
    </QueryClientProvider>,
  );
  status.mounted = true;
}

boot().catch((err: unknown) => {
  status.error = err instanceof Error ? err.message : String(err);
  console.error(err);
});
