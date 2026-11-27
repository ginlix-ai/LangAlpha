import { act, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import type { DriveEntry } from '@/types/drive';
import { DriveList } from '../components/DriveList';
import { DriveContext, type DriveContextValue } from '../driveContext';
import type { DriveActions } from '../useDriveActions';

const file = (name: string): DriveEntry => ({
  id: name, path: name, name, kind: 'file', size: 1, sha256: 'a'.repeat(64), content_type: null, is_text: true, version: 1,
  created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-01T00:00:00Z', modified_by: null, deleted_at: null, memo: null, undo_until: null,
});

afterEach(() => act(() => void i18n.changeLanguage('en-US')));

describe('DriveList', () => {
  it('sorts names by the language the app is in, and again when it changes', async () => {
    await act(() => i18n.changeLanguage('en-US'));
    const ctx = {
      space: 'personal', actions: { moveInto: vi.fn(), pending: {} } as unknown as DriveActions, go: vi.fn(), open: vi.fn(),
      trash: vi.fn(), leaveEditor: vi.fn(), back: vi.fn(), backTo: 'folder' as const, ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn(), memo: vi.fn() },
    } as DriveContextValue;
    renderWithProviders(
      <DriveContext value={ctx}>
        <DriveList dir="" entries={[file('中.md'), file('啊.md')]} loading={false} error={null} openPath={null} compact={false} usage={null} upload={vi.fn()} onRetry={vi.fn()} />
      </DriveContext>,
    );
    const order = () => screen.getAllByText(/^(中|啊)\.md$/).map((el) => el.textContent);
    // In English, by code point; in Chinese, by pinyin: 啊 (a) before 中 (zhong).
    expect(order()).toEqual(['中.md', '啊.md']);
    await act(() => i18n.changeLanguage('zh-CN'));
    expect(order()).toEqual(['啊.md', '中.md']);
  });

  it('marks the open file when the URL spells it in another case', () => {
    const ctx = {
      space: 'personal', actions: { moveInto: vi.fn(), pending: {} } as unknown as DriveActions, go: vi.fn(), open: vi.fn(),
      trash: vi.fn(), leaveEditor: vi.fn(), back: vi.fn(), backTo: 'folder' as const, ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn(), memo: vi.fn() },
    } as DriveContextValue;
    const { container } = renderWithProviders(
      <DriveContext value={ctx}>
        <DriveList dir="" entries={[file('Report.md'), file('other.md')]} loading={false} error={null} openPath="REPORT.md" compact usage={null} upload={vi.fn()} onRetry={vi.fn()} />
      </DriveContext>,
    );
    expect(container.querySelectorAll('.drive-row.is-active')).toHaveLength(1);
  });
});
