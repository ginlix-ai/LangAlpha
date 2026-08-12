import { describe, it, expect, vi, afterEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useOnboardingOrchestrator } from '../engine/useOnboardingOrchestrator';

type Params = Parameters<typeof useOnboardingOrchestrator>[0];

function run(overrides: Partial<Params> = {}) {
  const showPageIntro = vi.fn();
  const showWhatsNew = vi.fn();
  const params: Params = {
    isLoading: false,
    phase: 'idle',
    pathname: '/chat',
    eligibleIntroId: 'chat',
    hasUnseenWhatsNew: false,
    suppress: false,
    showPageIntro,
    showWhatsNew,
    ...overrides,
  };
  renderHook(() => useOnboardingOrchestrator(params));
  return { showPageIntro, showWhatsNew };
}

describe('useOnboardingOrchestrator', () => {
  afterEach(() => {
    document.body.innerHTML = '';
  });

  it('opens the intro for the current page when one is eligible', () => {
    const { showPageIntro, showWhatsNew } = run();
    expect(showPageIntro).toHaveBeenCalledTimes(1);
    expect(showWhatsNew).not.toHaveBeenCalled();
  });

  it('shows What\'s-New (not an intro) when no intro matches', () => {
    const { showPageIntro, showWhatsNew } = run({
      eligibleIntroId: null,
      hasUnseenWhatsNew: true,
    });
    expect(showPageIntro).not.toHaveBeenCalled();
    expect(showWhatsNew).toHaveBeenCalledTimes(1);
  });

  it('the intro outranks What\'s-New when both are pending', () => {
    const { showPageIntro, showWhatsNew } = run({ hasUnseenWhatsNew: true });
    expect(showPageIntro).toHaveBeenCalledTimes(1);
    expect(showWhatsNew).not.toHaveBeenCalled();
  });

  it('suppresses everything while another flow owns the screen', () => {
    const { showPageIntro, showWhatsNew } = run({ suppress: true, hasUnseenWhatsNew: true });
    expect(showPageIntro).not.toHaveBeenCalled();
    expect(showWhatsNew).not.toHaveBeenCalled();
  });

  it('does nothing while a popup is already open (phase not idle)', () => {
    const { showPageIntro } = run({ phase: 'pageIntro' });
    expect(showPageIntro).not.toHaveBeenCalled();
  });

  it('defers to an open app dialog', () => {
    const dialog = document.createElement('div');
    dialog.setAttribute('role', 'dialog');
    dialog.setAttribute('data-state', 'open');
    document.body.appendChild(dialog);
    const { showPageIntro, showWhatsNew } = run({ hasUnseenWhatsNew: true });
    expect(showPageIntro).not.toHaveBeenCalled();
    expect(showWhatsNew).not.toHaveBeenCalled();
  });

  it('does nothing while prefs are still loading', () => {
    const { showPageIntro, showWhatsNew } = run({ isLoading: true });
    expect(showPageIntro).not.toHaveBeenCalled();
    expect(showWhatsNew).not.toHaveBeenCalled();
  });

  it("shows What's-New only once across re-renders (session guard)", () => {
    const showWhatsNew = vi.fn();
    const params: Params = {
      isLoading: false,
      phase: 'idle',
      pathname: '/chat',
      eligibleIntroId: null,
      hasUnseenWhatsNew: true,
      suppress: false,
      showPageIntro: vi.fn(),
      showWhatsNew,
    };
    const { rerender } = renderHook((p: Params) => useOnboardingOrchestrator(p), {
      initialProps: params,
    });
    // new params identity + changed dep so the effect actually re-runs
    rerender({ ...params, pathname: '/dashboard' });
    rerender({ ...params, pathname: '/settings' });
    expect(showWhatsNew).toHaveBeenCalledTimes(1);
  });

  it('re-evaluates on navigation after being deferred by an open dialog', () => {
    const dialog = document.createElement('div');
    dialog.setAttribute('role', 'dialog');
    dialog.setAttribute('data-state', 'open');
    document.body.appendChild(dialog);

    const showPageIntro = vi.fn();
    const params: Params = {
      isLoading: false,
      phase: 'idle',
      pathname: '/chat',
      eligibleIntroId: 'chat',
      hasUnseenWhatsNew: false,
      suppress: false,
      showPageIntro,
      showWhatsNew: vi.fn(),
    };
    const { rerender } = renderHook((p: Params) => useOnboardingOrchestrator(p), {
      initialProps: params,
    });
    expect(showPageIntro).not.toHaveBeenCalled(); // deferred by the open dialog

    dialog.remove();
    // closing the dialog changes no dep — only the next navigation re-runs the effect
    rerender({ ...params, pathname: '/dashboard', eligibleIntroId: 'dashboard' });
    expect(showPageIntro).toHaveBeenCalledTimes(1);
  });
});
