/**
 * The deployment's own key has no row of the user's, so removing it would
 * delete nothing and the row would come straight back: the sheet offers no
 * removal for it.
 */
import { describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';

vi.mock('@/components/modelAccess/useModelPicker', () => ({
  useModelPicker: () => ({ loading: false, saving: false }),
}));
vi.mock('@/components/modelAccess/ModelPicker', () => ({ ModelPickerList: () => null }));

import { ManageSheet } from '../ManageSheet';

const key = (masked_key: string): ConfiguredProvider => ({
  provider: 'deepseek', display_name: 'DeepSeek', access_type: 'api_key', masked_key,
});

function removeButton(p: ConfiguredProvider) {
  renderWithProviders(
    <ManageSheet p={p} option={null} configured={[p]} onClose={() => {}} onRemove={() => {}} />,
  );
  return screen.queryByRole('button', { name: 'Remove key' });
}

describe('ManageSheet', () => {
  it('offers to remove a key the user saved', () => {
    expect(removeButton(key('sk-...7e48'))).not.toBeNull();
  });

  it("offers no removal for the deployment's own key", () => {
    expect(removeButton(key('(server)'))).toBeNull();
  });
});
