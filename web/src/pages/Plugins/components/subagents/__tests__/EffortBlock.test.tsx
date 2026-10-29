import { describe, it, expect, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { SubagentView } from '@/pages/ChatAgent/utils/api';
import { EffortBlock, type SubagentModels } from '../SubagentAside';

/**
 * The levels the effort control offers are the ones a save can hold. The
 * server holds a set level to the saved model's ladder even while that model
 * is unavailable, so a pin that runs on the thread's model for now still
 * offers only its own levels.
 */

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: null, isLoading: false, isLoaded: true }),
}));

const MODELS = {
  models: { openai: { models: ['wide-model'], display_name: 'OpenAI' } },
  metadata: {
    'wide-model': { provider: 'openai', reasoning_efforts: ['minimal', 'low', 'medium', 'high'] },
    'short-ladder': { provider: 'openai', is_custom_model: true, reasoning_efforts: ['low', 'high'] },
    'no-ladder': { provider: 'openai', is_custom_model: true },
  },
} as unknown as SubagentModels;

function view(model: string | null, unavailable: boolean): SubagentView {
  return {
    name: 'r2-effort-sa',
    kind: 'custom',
    source: 'custom',
    plugin: null,
    edited: false,
    user_fields: ['model'],
    description: 'd',
    instructions: '',
    enabled: true,
    model,
    effort: null,
    tools: [],
    search_provider: null,
    search_depth: null,
    connectors: true,
    brokerages: 'off',
    memory: true,
    preferred_connectors: [],
    preferred_skills: [],
    available_to: ['analyst'],
    base: null,
    problems: unavailable && model ? [{ code: 'model_unavailable', detail: model }] : [],
    updated_by: 'user',
    updated_thread_id: null,
    updated_thread_title: null,
    updated_at: null,
    file: 'r2-effort-sa.md',
    version: 'v1',
  };
}

function levels(): string[] {
  const select = screen.getByRole('combobox');
  return within(select)
    .getAllByRole('option')
    .map((o) => (o as HTMLOptionElement).value)
    .filter(Boolean);
}

function renderBlock(model: string | null, unavailable: boolean) {
  render(<EffortBlock view={view(model, unavailable)} data={{} as never} models={MODELS} write={() => {}} />);
}

describe('EffortBlock on an unavailable pin', () => {
  it('offers only the levels the saved model has', () => {
    renderBlock('short-ladder', true);
    expect(levels()).toEqual(['low', 'high']);
    expect(screen.getByRole('option', { name: "Thread's setting" })).toBeInTheDocument();
  });

  it('offers no level when the saved model has no effort setting', () => {
    renderBlock('no-ladder', true);
    expect(screen.queryByRole('combobox')).toBeNull();
    expect(screen.getByText('No Ladder has no effort setting.')).toBeInTheDocument();
  });

  it('offers every level for a name the catalog no longer has, as the server accepts any', () => {
    renderBlock('gone-model', true);
    expect(levels()).toEqual(['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max']);
  });

  it('offers every level when following the thread', () => {
    renderBlock(null, false);
    expect(levels()).toEqual(['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max']);
  });
});
