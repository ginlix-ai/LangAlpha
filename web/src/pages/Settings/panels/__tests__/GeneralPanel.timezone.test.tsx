/**
 * A timezone pick shows at once and goes back to the account's zone if the
 * save fails, rather than looking set while nothing was saved.
 */
import { describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  saves: [] as Array<{ resolve: () => void; reject: (e: Error) => void }>,
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  updateCurrentUser: () => new Promise<void>((resolve, reject) => { h.saves.push({ resolve, reject }); }),
}));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { timezone: 'America/New_York' } }) }));
vi.mock('@/pages/Onboarding', () => ({ useOnboarding: () => ({ replayGuides: vi.fn(), resetOnboarding: vi.fn() }) }));
vi.mock('@/components/TimezonePicker', () => ({
  default: ({ value, onChange }: { value: string; onChange: (tz: string) => void }) => (
    <input aria-label="timezone" value={value} onChange={(e) => onChange(e.target.value)} />
  ),
}));

import { GeneralPanel } from '../GeneralPanel';

describe('GeneralPanel timezone', () => {
  it("goes back to the account's zone when the save fails, unless a later pick replaced it", async () => {
    renderWithProviders(<GeneralPanel />);
    const zone = screen.getByLabelText('timezone');

    fireEvent.change(zone, { target: { value: 'Asia/Shanghai' } });
    expect(zone).toHaveValue('Asia/Shanghai');
    await waitFor(() => expect(h.saves).toHaveLength(1));
    await act(async () => { h.saves[0].reject(new Error('502')); });
    expect(zone).toHaveValue('America/New_York');

    fireEvent.change(zone, { target: { value: 'Europe/London' } });
    await waitFor(() => expect(h.saves).toHaveLength(2));
    fireEvent.change(zone, { target: { value: 'Asia/Tokyo' } });
    await act(async () => { h.saves[1].reject(new Error('502')); });
    expect(zone).toHaveValue('Asia/Tokyo');
  });

  it('keeps a later pick of the same zone when an earlier save of it fails', async () => {
    h.saves.length = 0;
    renderWithProviders(<GeneralPanel />);
    const zone = screen.getByLabelText('timezone');

    fireEvent.change(zone, { target: { value: 'Europe/London' } });
    await waitFor(() => expect(h.saves).toHaveLength(1));
    fireEvent.change(zone, { target: { value: 'Asia/Tokyo' } });
    fireEvent.change(zone, { target: { value: 'Europe/London' } });
    await act(async () => { h.saves[0].reject(new Error('502')); });
    expect(zone).toHaveValue('Europe/London');
  });
});
