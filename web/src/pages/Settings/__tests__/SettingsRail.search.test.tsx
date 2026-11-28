/**
 * The light and dark row is drawn as Mode under the Theme heading, and people
 * still look for it as the theme, in either language. A row drawn only under
 * a feature flag is found only while the flag is on.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import i18n from '@/i18n';
import { queryKeys } from '@/lib/queryKeys';
import { SettingsRail } from '../components/SettingsRail';

afterEach(async () => {
  await i18n.changeLanguage('en-US');
});

function search(query: string, flags: string[] = []) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(queryKeys.features.list(), flags.map((key) => ({ key, enabled: true })));
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <SettingsRail view={{ kind: 'section', section: 'general' }} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  fireEvent.change(screen.getByRole('combobox'), { target: { value: query } });
  return screen.queryByRole('listbox') ? within(screen.getByRole('listbox')) : null;
}

describe('searching Settings for the theme', () => {
  it('finds the Mode row by "theme"', () => {
    expect(search('theme')!.getByText('Mode')).toBeTruthy();
  });

  it('finds the Mode row by 主题', async () => {
    await i18n.changeLanguage('zh-CN');
    expect(search('主题')!.getByText('模式')).toBeTruthy();
  });
});

describe('a row drawn under a feature flag', () => {
  const label = 'Use my preferences and memory in shared workspaces';

  it('is found while its flag is on', () => {
    expect(search('shared workspaces', ['shared_workspaces_member_context'])!.getByText(label)).toBeTruthy();
  });

  it('is not found while its flag is off', () => {
    expect(search('shared workspaces')?.queryByText(label) ?? null).toBeNull();
  });
});
