/**
 * The first answer step a surface opens claims setup for it while nobody
 * has, so the Mac app's shell resumes setup the app started.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import { runAuthResets } from '@/lib/authResets';
import type { ReactNode } from 'react';

const h = vi.hoisted(() => ({
  desktop: false,
  setupOn: null as null | string,
  completed: null as null | string,
  update: vi.fn(() => Promise.resolve({})),
}));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: true, HOST_MODE: 'platform', APP_ENTRY_PATH: '/app' }));
vi.mock('@/lib/desktop', () => ({ isDesktopShell: () => h.desktop, openConsolePage: vi.fn() }));
vi.mock('@/hooks/useOnboardingProgress', () => ({
  useOnboardingProgress: () => ({ update: h.update, isUpdating: false }),
}));
vi.mock('../useSetupFacts', () => ({
  useSetupFacts: () => ({
    progress: { setup_on: h.setupOn, setup_completed_at: h.completed },
    answers: { models: 'claude_code', runsOn: 'cloud' },
    rail: ['model', 'default'],
  }),
}));

import { useAnswerFlow } from '../flow';

const wrapper = ({ children }: { children: ReactNode }) => <MemoryRouter>{children}</MemoryRouter>;

describe('useAnswerFlow', () => {
  beforeEach(() => {
    h.desktop = false;
    h.setupOn = null;
    h.completed = null;
    h.update.mockClear();
  });

  it('claims unclaimed setup for the Mac app', () => {
    h.desktop = true;
    renderHook(() => useAnswerFlow('model'), { wrapper });
    expect(h.update).toHaveBeenCalledWith({ setup_on: 'app' });
  });

  it('claims unclaimed setup for the browser', () => {
    renderHook(() => useAnswerFlow('model'), { wrapper });
    expect(h.update).toHaveBeenCalledWith({ setup_on: 'browser' });
  });

  it('tries a refused claim again on focus, one write at a time', async () => {
    h.desktop = true;
    let refuse!: (err: Error) => void;
    h.update.mockImplementationOnce(() => new Promise((_resolve, reject) => { refuse = reject; }));
    renderHook(() => useAnswerFlow('model'), { wrapper });
    act(() => { window.dispatchEvent(new Event('focus')); });
    expect(h.update).toHaveBeenCalledTimes(1);
    await act(async () => { refuse(new Error('refused')); });
    act(() => { window.dispatchEvent(new Event('focus')); });
    expect(h.update).toHaveBeenCalledTimes(2);
    expect(h.update).toHaveBeenLastCalledWith({ setup_on: 'app' });
  });

  it('claims again for an account that signs in in place of the one it claimed for', () => {
    h.desktop = true;
    const { rerender } = renderHook(() => useAnswerFlow('model'), { wrapper });
    expect(h.update).toHaveBeenCalledTimes(1);
    runAuthResets();
    h.setupOn = 'app';
    rerender();
    h.setupOn = null;
    rerender();
    expect(h.update).toHaveBeenCalledTimes(2);
    expect(h.update).toHaveBeenLastCalledWith({ setup_on: 'app' });
  });

  it('leaves setup another surface has, or one that is finished', () => {
    h.desktop = true;
    h.setupOn = 'browser';
    renderHook(() => useAnswerFlow('model'), { wrapper });
    h.setupOn = null;
    h.completed = '2026-11-15T00:00:00Z';
    renderHook(() => useAnswerFlow('model'), { wrapper });
    expect(h.update).not.toHaveBeenCalled();
  });

  it('finishing the last step records it and leaves the hand-over to the step', async () => {
    h.setupOn = 'browser';
    const { result } = renderHook(() => ({ flow: useAnswerFlow('default'), location: useLocation() }), { wrapper });
    await act(async () => {
      expect(await result.current.flow.finish()).toBe(true);
    });
    expect(h.update).toHaveBeenCalledWith({ setup_step: 'default', setup_completed: true });
    expect(result.current.location.pathname).toBe('/');
    act(() => result.current.flow.toWelcome());
    expect(result.current.location.pathname).toBe('/welcome');
    expect(result.current.location.state).toEqual({ fromSetup: true });
  });
});
