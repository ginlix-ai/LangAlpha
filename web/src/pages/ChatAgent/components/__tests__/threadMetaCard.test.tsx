/**
 * A thread row is the trigger of its metadata hover card and holds the row's
 * own buttons. Radix opens the card on the trigger's focus as well as on
 * pointerenter, and an open does not clear an open timer already pending, so
 * focus bubbling up from a button inside the row used to arm a second timer
 * and leak the first: leaving the row cleared only the second, and the leaked
 * one opened the card after the pointer had gone.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { ThreadTreeRow } from '../NavigationRows';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const OPEN_DELAY = 500;

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

function renderRow() {
  render(
    <ThreadTreeRow
      wsId="ws-1"
      thread={{ thread_id: 'thread-1', title: 'Test thread' }}
      isCurrentThread={false}
      isExpanded={false}
      isMobile={false}
      onToggleThread={vi.fn()}
      onNavigateThread={vi.fn()}
      onSelectAgent={vi.fn()}
      onPinThread={vi.fn()}
      onArchiveThread={vi.fn()}
    />,
  );
  const row = screen.getByText('Test thread').closest<HTMLElement>('[data-state]')!;
  const archive = screen.getByRole('button', { name: 'nav.archiveThread' });
  return { row, archive };
}

function wait(ms: number) {
  act(() => {
    vi.advanceTimersByTime(ms);
  });
}

describe('thread row meta card', () => {
  it('opens once the pointer rests on the row', () => {
    const { row } = renderRow();

    fireEvent.pointerEnter(row);
    wait(OPEN_DELAY + 100);

    expect(row).toHaveAttribute('data-state', 'open');
  });

  it('stays closed when the pointer leaves after focusing a button inside the row', () => {
    const { row, archive } = renderRow();

    fireEvent.pointerEnter(row);
    act(() => archive.focus());
    fireEvent.pointerLeave(row);
    wait(OPEN_DELAY + 100);

    expect(row).toHaveAttribute('data-state', 'closed');
  });

  it('stays open while focus moves between buttons inside the hovered row', () => {
    const { row, archive } = renderRow();
    const pin = screen.getByRole('button', { name: 'nav.pinThread' });

    fireEvent.pointerEnter(row);
    wait(OPEN_DELAY + 100);
    act(() => pin.focus());
    act(() => archive.focus());
    wait(OPEN_DELAY);

    expect(row).toHaveAttribute('data-state', 'open');
  });
});
