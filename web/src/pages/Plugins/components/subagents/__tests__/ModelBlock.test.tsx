import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { SubagentView } from '@/pages/ChatAgent/utils/api';
import type { SubagentEdit } from '@/hooks/useSubagents';
import { ModelBlock, type SubagentModels } from '../SubagentAside';

/**
 * A model change keeps a set effort the new model offers and drops one it
 * doesn't, as a provider change does the depth: the server holds a saved
 * effort to the ladder of the model it runs on.
 */

vi.mock('@/components/model/ModelSelector', () => ({
  ModelSelector: ({ value, onChange }: { value: string; onChange: (v: string) => void }) => (
    <select aria-label="model" value={value} onChange={(e) => onChange(e.target.value)}>
      <option value="">thread</option>
      <option value="wide-model">wide</option>
      <option value="short-ladder">short</option>
      <option value="no-ladder">none</option>
    </select>
  ),
}));

const MODELS = {
  models: {},
  metadata: {
    'wide-model': { provider: 'openai', reasoning_efforts: ['minimal', 'low', 'medium', 'high'] },
    'short-ladder': { provider: 'openai', is_custom_model: true, reasoning_efforts: ['low', 'high'] },
    'no-ladder': { provider: 'openai', is_custom_model: true },
  },
} as unknown as SubagentModels;

function pick(effort: string | null, next: string): SubagentEdit[] {
  const writes: SubagentEdit[] = [];
  const view = { name: 'r3-model-sa', model: 'wide-model', effort, problems: [], base: null } as unknown as SubagentView;
  const { unmount } = render(
    <ModelBlock view={view} data={{} as never} models={MODELS} write={(edit) => writes.push(edit)} />,
  );
  fireEvent.change(screen.getByLabelText('model'), { target: { value: next } });
  unmount();
  return writes;
}

describe('ModelBlock', () => {
  it('keeps a set effort the new model offers', () => {
    expect(pick('high', 'short-ladder')).toEqual([{ model: 'short-ladder' }]);
  });

  it("drops a set effort the new model doesn't offer", () => {
    expect(pick('medium', 'short-ladder')).toEqual([{ model: 'short-ladder', effort: null }]);
    expect(pick('low', 'no-ladder')).toEqual([{ model: 'no-ladder', effort: null }]);
  });

  it('keeps any effort when following the thread, and leaves an unset one alone', () => {
    expect(pick('medium', '')).toEqual([{ model: null }]);
    expect(pick(null, 'no-ladder')).toEqual([{ model: 'no-ladder' }]);
  });
});
