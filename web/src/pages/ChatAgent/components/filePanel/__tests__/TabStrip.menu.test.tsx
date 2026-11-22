/**
 * A tab carries two overlays, its hover card and its right-click menu, and
 * both have to reach the pill itself: wrapped one in the other, the outer
 * trigger handed its handlers to a Root with no element and the card never
 * opened. The menu's closes count from the tab that was right-clicked.
 */
import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { TabStrip } from '../TabStrip';
import type { FileTab } from '../useFileTabs';

const file = (id: string, path: string): FileTab => ({ id, kind: 'file', path, preview: false, location: null, locationSeq: 0 });
const TABS: FileTab[] = [file('a', 'notes.md'), file('b', 'results/report.html'), file('c', 'data/prices.csv')];

function strip(onClose = vi.fn(), onActivate = vi.fn()) {
  renderWithProviders(
    <TabStrip
      tabs={TABS}
      activeId="a"
      onActivate={onActivate}
      onClose={onClose}
      onPin={() => {}}
      onNewTab={null}
      hasChanged={() => false}
      treeOpen={false}
      onToggleTree={null}
      onPanelClose={null}
    />,
  );
  return onClose;
}

const tab = (id: string) => document.querySelector(`[data-tab-id="${id}"]`) as HTMLElement;

async function pick(id: string, item: string) {
  fireEvent.contextMenu(tab(id));
  fireEvent.click(await screen.findByRole('menuitem', { name: item }));
}

describe('TabStrip tab overlays', () => {
  it('opens the hover card on a tab that also carries the menu', async () => {
    strip();
    fireEvent.focus(tab('b'));
    const hint = (await screen.findAllByText('report.html')).find((el) => el.closest('.file-panel-tab-hint'));
    expect(hint).toBeTruthy();
    expect(within(hint!.closest('.file-panel-tab-hint') as HTMLElement).getByText('results')).toBeTruthy();
  });

  it('closes, from the tab right-clicked, the tabs each item names', async () => {
    const onClose = strip();
    await pick('b', 'Close');
    expect(onClose).toHaveBeenLastCalledWith(['b']);
    await pick('b', 'Close others');
    expect(onClose).toHaveBeenLastCalledWith(['a', 'c']);
    await pick('b', 'Close to the right');
    expect(onClose).toHaveBeenLastCalledWith(['c']);
    await pick('b', 'Close all');
    expect(onClose).toHaveBeenLastCalledWith(['a', 'b', 'c']);
  });

  it('lets the release of a long press neither close nor bring forward the tab', async () => {
    const onClose = vi.fn();
    const onActivate = vi.fn();
    strip(onClose, onActivate);
    fireEvent.pointerDown(tab('b'), { pointerType: 'touch', clientX: 40, clientY: 10 });
    await screen.findByRole('menuitem', { name: 'Close' }, { timeout: 2000 });
    fireEvent.pointerUp(tab('b'), { pointerType: 'touch' });
    fireEvent.click(tab('b').querySelector('.file-panel-tab-close')!);
    fireEvent.click(tab('b'));
    expect(onClose).not.toHaveBeenCalled();
    expect(onActivate).not.toHaveBeenCalled();
  });

  it('offers nothing to the right of the last tab', async () => {
    strip();
    fireEvent.contextMenu(tab('c'));
    expect((await screen.findByRole('menuitem', { name: 'Close to the right' })).getAttribute('data-disabled')).not.toBeNull();
  });
});
