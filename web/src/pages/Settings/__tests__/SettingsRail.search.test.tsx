/**
 * The light and dark row is drawn as Mode under the Theme heading, and people
 * still look for it as the theme, in either language.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import i18n from '@/i18n';
import { SettingsRail } from '../components/SettingsRail';

afterEach(async () => {
  await i18n.changeLanguage('en-US');
});

function search(query: string) {
  render(
    <MemoryRouter>
      <SettingsRail section="general" />
    </MemoryRouter>,
  );
  fireEvent.change(screen.getByRole('combobox'), { target: { value: query } });
  return within(screen.getByRole('listbox'));
}

describe('searching Settings for the theme', () => {
  it('finds the Mode row by "theme"', () => {
    expect(search('theme').getByText('Mode')).toBeTruthy();
  });

  it('finds the Mode row by 主题', async () => {
    await i18n.changeLanguage('zh-CN');
    expect(search('主题').getByText('模式')).toBeTruthy();
  });
});
