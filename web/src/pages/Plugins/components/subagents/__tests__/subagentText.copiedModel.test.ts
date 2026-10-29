import { describe, expect, it } from 'vitest';
import type { SubagentView } from '@/pages/ChatAgent/utils/api';
import { copiedModel } from '../subagentText';

/**
 * A base runs its own pin whatever the plan; a copy is held to the plan, so
 * the Copied from panel names the model the copy will actually run.
 */

const locked = { openai: { display_name: 'OpenAI', models: ['pinned-model'] } };
const view = (model: string | null, problems: SubagentView['problems'] = []) =>
  ({ model, problems }) as unknown as SubagentView;

describe('copiedModel', () => {
  it('leaves out a pin the source runs that the plan holds back', () => {
    expect(copiedModel(view('pinned-model'), locked)).toBeNull();
  });

  it('keeps a pin the plan offers, and one the source already cannot run', () => {
    expect(copiedModel(view('basic-model'), locked)).toBe('basic-model');
    const flagged = view('pinned-model', [{ code: 'model_unavailable', detail: 'pinned-model' }]);
    expect(copiedModel(flagged, locked)).toBe('pinned-model');
  });

  it('follows the thread with no pin or no source', () => {
    expect(copiedModel(view(null), locked)).toBeNull();
    expect(copiedModel(null, locked)).toBeNull();
  });
});
