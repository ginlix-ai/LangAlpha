import { describe, it, expect, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import type { SubagentsResponse, SubagentView } from '@/pages/ChatAgent/utils/api';
import { DetailPageProvider } from '../../DetailPage';
import type { SubagentModels } from '../SubagentAside';
import { SubagentDetail } from '../SubagentDetail';

/**
 * A save answers after the user may have typed on: what they typed while it
 * was in flight stays, and a field they left alone takes the saved text.
 */

type Saved = { description: string; instructions: string };
let answer: ((saved: Saved) => void) | null = null;

const idle = { isPending: false, mutate: () => {} };
vi.mock('@/hooks/useSubagents', () => ({
  usePatchSubagent: () => idle,
  useDeleteSubagent: () => idle,
  useSaveSubagentText: () => ({
    isPending: false,
    mutate: (_vars: unknown, { onSuccess }: { onSuccess: (saved: Saved) => void }) => {
      answer = onSuccess;
    },
  }),
}));
vi.mock('@/pages/ChatAgent/components/mcp/BrandMark', () => ({ BrandMark: () => null }));
vi.mock('../SubagentAside', () => {
  const none = () => null;
  return {
    AvailableBlock: none,
    BrokeragesBlock: none,
    EffortBlock: none,
    MemoryBlock: none,
    ModelBlock: none,
    PreferredBlock: none,
    ToolsBlock: none,
  };
});

const VIEW = {
  name: 'screener',
  kind: 'custom',
  source: 'user',
  plugin: null,
  edited: false,
  user_fields: [],
  description: 'Screens stocks.',
  instructions: 'Screen them.',
  enabled: true,
  tools: [],
  connectors: false,
  brokerages: 'off',
  memory: true,
  preferred_connectors: [],
  preferred_skills: [],
  available_to: ['analyst'],
  problems: [],
  updated_by: null,
  updated_at: null,
  file: 'screener.md',
  version: 'v1',
} as unknown as SubagentView;

const DATA = {
  subagents: [VIEW],
  brokerages: { connected: false },
  limits: { max_custom: 20, description_max: 400, instructions_max_bytes: 16384 },
} as unknown as SubagentsResponse;

describe('SubagentDetail save', () => {
  it('keeps what the user typed while the save was in flight', () => {
    const page = (view: SubagentView) => (
      <MemoryRouter>
        <DetailPageProvider value={{ section: 'Plugins' }}>
          <SubagentDetail
            view={view}
            data={DATA}
            models={{ models: {}, metadata: {} } as unknown as SubagentModels}
            onClose={() => {}}
            onDuplicate={() => {}}
            onOpenPlugin={() => {}}
          />
        </DetailPageProvider>
      </MemoryRouter>
    );
    const { rerender } = render(page(VIEW));
    const description = screen.getByLabelText('When to use');
    const instructions = screen.getByLabelText('Instructions');

    fireEvent.change(description, { target: { value: 'Screens stocks fast.' } });
    fireEvent.change(instructions, { target: { value: 'Screen them. ' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    fireEvent.change(description, { target: { value: 'Screens stocks fast and well.' } });
    // The server trims the instructions; the list then reads back the saved row.
    const saved = { description: 'Screens stocks fast.', instructions: 'Screen them.' };
    act(() => answer!(saved));
    rerender(page({ ...VIEW, ...saved, version: 'v2' }));

    expect(description).toHaveValue('Screens stocks fast and well.');
    expect(instructions).toHaveValue('Screen them.');
    expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled();
  });
});
