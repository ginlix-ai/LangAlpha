/**
 * A send refused because the user has no computer and none will be built
 * reads as a card offering to add one: this Mac in the desktop app on a Mac
 * that can pair itself, any computer elsewhere.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';

import { ErrorBanner } from '@/components/ui/error-banner';
import { renderWithProviders } from '@/test/utils';

const desktop = vi.hoisted(() => ({ mac: false, remote: false }));
vi.mock('@/lib/desktop', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  canAddThisMac: () => desktop.mac && desktop.remote,
  canRemoteControl: () => desktop.remote,
}));

import { closeComputersPanel, useComputersHost, useComputersPanel } from '../../hooks/computerPanelStore';
import { isNeedsComputer, needsBackupError, needsBackupOf, needsComputerError } from '../needsComputer';

/** A page that mounts the dialogs' host, and shows what the store opened. */
function Host() {
  useComputersHost(false);
  const { panel } = useComputersPanel();
  return <output data-testid="panel">{JSON.stringify(panel)}</output>;
}

function draw() {
  const t = (key: string) => (key === 'homePlacement.needsComputer' ? 'No computer yet.' : key);
  renderWithProviders(
    <>
      <Host />
      <ErrorBanner error={needsComputerError(t as Parameters<typeof needsComputerError>[0])} />
    </>,
  );
}

afterEach(() => {
  closeComputersPanel();
  desktop.mac = false;
  desktop.remote = false;
});

describe('isNeedsComputer', () => {
  it('reads the code off a stream refusal and off an API client one', () => {
    expect(isNeedsComputer({ status: 409, errorInfo: { code: 'needs_computer', message: 'x' } })).toBe(true);
    expect(isNeedsComputer({ response: { status: 409, data: { detail: { code: 'needs_computer' } } } })).toBe(true);
    expect(isNeedsComputer({ status: 409, errorInfo: { code: 'duplicate_request' } })).toBe(false);
    expect(isNeedsComputer(new Error('Network Error'))).toBe(false);
  });
});

describe('the needs_computer card', () => {
  it('offers to add a computer in a browser', () => {
    draw();
    expect(screen.getByText(/No computer yet\./)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Add a computer' }));
    expect(JSON.parse(screen.getByTestId('panel').textContent!)).toEqual({ view: 'create' });
  });

  it('offers to set up this Mac in the desktop app on a Mac that can pair itself', () => {
    desktop.mac = true;
    desktop.remote = true;
    draw();
    fireEvent.click(screen.getByRole('button', { name: 'Set up this Mac' }));
    expect(JSON.parse(screen.getByTestId('panel').textContent!)).toEqual({ view: 'create', pick: 'thisMac' });
  });
});

describe('the needs_backup card', () => {
  const refusal = { code: 'needs_backup', message: 'x', computer_id: 'c-mac', computer_name: 'Office Mac' };

  it('reads the computer off a stream refusal and off an API client one', () => {
    expect(needsBackupOf({ status: 409, errorInfo: refusal })).toEqual({ computerId: 'c-mac', name: 'Office Mac' });
    expect(needsBackupOf({ response: { status: 409, data: { detail: refusal } } })).toEqual({ computerId: 'c-mac', name: 'Office Mac' });
    expect(needsBackupOf({ status: 409, errorInfo: { code: 'needs_computer' } })).toBeNull();
  });

  it("opens that computer's details, where backup is turned on", () => {
    const t = ((key: string, values?: { name?: string }) =>
      key === 'homePlacement.openComputer' ? `Open ${values?.name}` : `Backup is off on ${values?.name}.`) as Parameters<typeof needsBackupError>[0];
    renderWithProviders(
      <>
        <Host />
        <ErrorBanner error={needsBackupError(t, { computerId: 'c-mac', name: 'Office Mac' })} />
      </>,
    );
    expect(screen.getByText(/Backup is off on Office Mac\./)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Open Office Mac' }));
    expect(JSON.parse(screen.getByTestId('panel').textContent!)).toEqual({ view: 'details', computerId: 'c-mac' });
  });
});
