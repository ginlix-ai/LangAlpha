/**
 * Where a composer send goes: what a pick does, and what the picker keeps
 * between openings.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { useRef, useState } from 'react';
import { ALL_WORKSPACES_KEY, WorkspacePicker } from '../chat-input.workspacePicker';
import type { Workspace } from '../chat-input.types';

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

const onPick = vi.fn();

const workspace = (n: number) => ({ workspace_id: `ws-${n}`, name: `Desk ${n}` }) as Workspace;

function Harness({ workspaces }: { workspaces: Workspace[] }) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  return (
    <>
      <button ref={triggerRef} type="button" onClick={() => setOpen((v) => !v)}>Where</button>
      <button type="button">Elsewhere</button>
      <WorkspacePicker
        triggerRef={triggerRef}
        isOpen={open}
        onOpenChange={setOpen}
        workspaces={workspaces}
        selectedKey={ALL_WORKSPACES_KEY}
        onPick={onPick}
        direction="up"
      />
    </>
  );
}

function open() {
  fireEvent.click(screen.getByRole('button', { name: 'Where' }));
  return screen.getByRole('dialog');
}

beforeEach(() => onPick.mockClear());

describe('the workspace picker', () => {
  it('picks a row and closes', () => {
    render(<Harness workspaces={[workspace(1), workspace(2)]} />);
    const dialog = open();
    fireEvent.click(within(dialog).getByRole('menuitemradio', { name: 'Desk 2' }));
    expect(onPick).toHaveBeenCalledWith('ws-2');
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('closes on a press outside it', () => {
    render(<Harness workspaces={[workspace(1)]} />);
    open();
    fireEvent.pointerDown(screen.getByRole('button', { name: 'Elsewhere' }));
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('searches a long list, and starts the next opening unfiltered', () => {
    const many = Array.from({ length: 8 }, (_, i) => workspace(i + 1));
    render(<Harness workspaces={many} />);
    const dialog = open();
    fireEvent.change(within(dialog).getByRole('searchbox'), { target: { value: 'Desk 3' } });
    expect(within(dialog).queryByRole('menuitemradio', { name: 'Desk 4' })).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Where' }));
    const reopened = open();
    expect(within(reopened).getByRole('searchbox')).toHaveValue('');
    expect(within(reopened).getByRole('menuitemradio', { name: 'Desk 4' })).toBeInTheDocument();
  });
});
