import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { SubagentsResponse, SubagentView } from '@/pages/ChatAgent/utils/api';
import { DetailPageProvider } from '../../DetailPage';
import type { SubagentModels } from '../SubagentAside';
import { SubagentNew } from '../SubagentNew';

/**
 * The new-subagent form says what Blank grants, and says why a create was
 * refused even when the refusal is about a field the form has no input for.
 */

let refusal: unknown = null;

vi.mock('@/hooks/useSubagents', () => ({
  useCreateSubagent: () => ({
    isPending: false,
    mutate: (_vars: unknown, { onError }: { onError: (err: unknown) => void }) => onError(refusal),
  }),
}));
vi.mock('@/pages/ChatAgent/components/mcp/BrandMark', () => ({ BrandMark: () => null }));

const MODELS = { models: {}, metadata: {} } as unknown as SubagentModels;

function base(name: string): SubagentView {
  const values = { model: null, effort: null, tools: ['web'], connectors: false, brokerages: 'off', memory: true };
  return {
    name,
    kind: 'base',
    source: 'builtin',
    plugin: null,
    edited: false,
    user_fields: [],
    description: `${name} work.`,
    instructions: '',
    enabled: true,
    model: null,
    effort: null,
    tools: ['web'],
    search_provider: null,
    search_depth: null,
    connectors: false,
    brokerages: 'off',
    memory: true,
    preferred_connectors: [],
    preferred_skills: [],
    available_to: ['analyst'],
    base: values,
    problems: [],
    updated_by: null,
    updated_thread_id: null,
    updated_thread_title: null,
    updated_at: null,
    file: `${name}.md`,
    version: 'v1',
  } as unknown as SubagentView;
}

const DATA = {
  subagents: [
    base('r3-source'),
    { ...base('r3-off'), enabled: false },
    { ...base('r3-home'), available_to: ['chief_of_staff'] },
  ],
  limits: { max_custom: 20, description_max: 400, instructions_max_bytes: 16384 },
} as unknown as SubagentsResponse;

function fact(label: string): string | null {
  const dt = screen.queryAllByText(label).find((el) => el.tagName === 'DT');
  return dt?.nextElementSibling?.textContent ?? null;
}

function renderForm(startFrom: string | null) {
  render(
    <DetailPageProvider value={{ section: 'Plugins' }}>
      <SubagentNew data={DATA} models={MODELS} startFrom={startFrom} onClose={() => {}} onCreated={() => {}} />
    </DetailPageProvider>,
  );
}

function create(errors: { field: string; message: string }[]) {
  refusal = { response: { data: { detail: { errors } } } };
  fireEvent.change(screen.getByLabelText('Name'), { target: { value: 'r3-new' } });
  fireEvent.change(screen.getByPlaceholderText(/^When the agent should hand work to it/), {
    target: { value: 'When to use it.' },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Create' }));
}

beforeEach(() => {
  refusal = null;
});

describe('SubagentNew', () => {
  it('shows the access Blank grants, as a copy shows its own', () => {
    renderForm(null);
    expect(fact('Connectors')).toBe('All');
    expect(fact('Brokerages')).toBe('Access');
  });

  it('shows a copy without connectors or brokerages as it is', () => {
    renderForm('r3-source');
    expect(fact('Connectors')).toBe('None');
    expect(fact('Brokerages')).toBeNull();
  });

  it('says the source is gone under the picker, and clears it on a new pick', () => {
    renderForm('r3-source');
    create([{ field: 'start_from', message: 'No subagent named r3-source to start from.' }]);
    expect(screen.getByText('No subagent named r3-source to start from.')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('radio', { name: /Blank/ }));
    expect(screen.queryByText('No subagent named r3-source to start from.')).not.toBeInTheDocument();
  });

  it('holds back only the names the server holds back', () => {
    renderForm(null);
    const name = screen.getByLabelText('Name');
    fireEvent.change(name, { target: { value: 'readme' } });
    expect(name).not.toHaveAttribute('aria-invalid');
    fireEvent.change(name, { target: { value: 'main' } });
    expect(screen.getByText('main is reserved.')).toBeInTheDocument();
  });

  it.each([
    ['r3-source', 'Available from the next turn.'],
    ['r3-home', 'Available from the next turn.'],
    ['r3-off', 'Starts switched off, like r3-off. Switch it on after creating.'],
  ])('says whether a copy of %s starts on', (source, note) => {
    renderForm(source);
    expect(screen.getByText(note)).toBeInTheDocument();
  });

  it('says a refusal about a field the form has no place for by the button', () => {
    renderForm(null);
    create([{ field: 'tools', message: 'Not a tool a subagent has.' }]);
    expect(screen.getByText('Not a tool a subagent has.')).toBeInTheDocument();
  });
});
