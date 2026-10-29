import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { WidgetDefinition, WidgetInstance } from '../../types';

const reportStaleBuild = vi.fn();
vi.mock('@/lib/staleBuild', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/staleBuild')>()),
  reportStaleBuild: (...args: unknown[]) => reportStaleBuild(...args),
}));

import { WidgetBody } from '../WidgetBody';

function widgetThrowing(error: Error): WidgetDefinition<unknown> {
  return {
    type: 'test.broken',
    component: () => {
      throw error;
    },
  } as unknown as WidgetDefinition<unknown>;
}

const instance = { id: 'w1', type: 'test.broken', config: {} } as WidgetInstance<unknown>;

describe('WidgetBody', () => {
  beforeEach(() => {
    reportStaleBuild.mockReset();
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  it('offers the reload when a widget chunk is gone after a deploy, and keeps the rest of the page', () => {
    const error = new Error(
      `Failed to fetch dynamically imported module: ${window.location.origin}/assets/ChartWidget-abc123.js`,
    );
    render(<WidgetBody definition={widgetThrowing(error)} instance={instance} updateConfig={vi.fn()} />);
    expect(reportStaleBuild).toHaveBeenCalledWith('widget');
    expect(screen.getByText('test.broken')).toBeInTheDocument();
  });

  it('keeps an ordinary render error to the widget', () => {
    render(<WidgetBody definition={widgetThrowing(new Error('boom'))} instance={instance} updateConfig={vi.fn()} />);
    expect(reportStaleBuild).not.toHaveBeenCalled();
    expect(screen.getByText('test.broken')).toBeInTheDocument();
  });
});
