import { describe, expect, it } from 'vitest';
import { render as rtlRender, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactElement } from 'react';
import '@/i18n';
import PreviewViewer from '../PreviewViewer';
import { previewFailure } from '../../../utils/api';

const render = (ui: ReactElement) =>
  rtlRender(<QueryClientProvider client={new QueryClient()}>{ui}</QueryClientProvider>);

describe('a preview whose port another program holds', () => {
  it('says the port is in use, not that the app stopped', () => {
    render(<PreviewViewer url="" port={8050} title="Dashboard" error="portInUse" chrome={false} />);
    expect(screen.getByText('Port in use')).toBeTruthy();
    expect(screen.getByText(/holds port 8050/)).toBeTruthy();
    expect(screen.queryByText('App not running')).toBeNull();
  });

  it('keeps the offline message for any other failure', () => {
    render(<PreviewViewer url="" port={8050} title="Dashboard" error="offline" chrome={false} />);
    expect(screen.getByText('App not running')).toBeTruthy();
  });

  it('reads a busy port by the code the preview routes give it, not by the status', () => {
    const busy = { response: { status: 409, data: { detail: { code: 'port_in_use', message: 'Port 8050 is already in use' } } } };
    expect(previewFailure(busy)).toBe('portInUse');
    expect(previewFailure({ response: { status: 409, data: { detail: 'Conflict' } } })).toBe('offline');
    expect(previewFailure({ response: { status: 503 } })).toBe('offline');
    expect(previewFailure(new Error('network'))).toBe('offline');
  });
});
