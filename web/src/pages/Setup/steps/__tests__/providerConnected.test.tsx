/**
 * A connection the answers' model step sent the user to make goes back
 * there, which makes it the default, unless no catalog model runs on it as it
 * is: a coding plan whose models are its brand's needs its models picked
 * first, or none of them would run. Where the visit came from decides, not
 * whether the account's onboarding is unfinished: Settings' connections go
 * on to the model step either way.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ReactNode } from 'react';
import { act, renderHook } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';

vi.mock('@/hooks/useOnboardingRoute', () => ({ useOnboardingRoute: () => ({ kind: 'answers' }) }));
const ret = vi.hoisted(() => ({ returnFromSetup: vi.fn(() => false) }));
vi.mock('@/lib/setupReturn', () => ({ returnFromSetup: ret.returnFromSetup }));
vi.mock('@/hooks/useModels', () => ({
  useModels: () => ({
    models: {
      model_metadata: {
        'kimi-a': { provider: 'moonshot-coding' },
        'glm-a': { provider: 'z-ai' },
      },
      key_candidates: {},
    },
  }),
}));

import { useProviderConnected } from '../connectStep/shared';

function connectAs(provider: string, answering: boolean) {
  const entry = { pathname: '/setup/connect', state: answering ? { answering: true } : null };
  const wrapper = ({ children }: { children: ReactNode }) => <MemoryRouter initialEntries={[entry]}>{children}</MemoryRouter>;
  const { result } = renderHook(() => ({ connect: useProviderConnected(answering), location: useLocation() }), { wrapper });
  act(() => result.current.connect({ provider }));
  return result.current.location;
}

describe('useProviderConnected', () => {
  it('goes back to the answers model step for a connection its catalog models run on', () => {
    const location = connectAs('moonshot-coding', true);
    expect(location.pathname).toBe('/setup/model');
    expect(location.state).toEqual({ connected: 'moonshot-coding' });
  });

  it('picks models first for a coding plan whose models are its brand\'s, still on the answers', () => {
    const location = connectAs('z-ai-coding', true);
    expect(location.pathname).toBe('/setup/models');
    expect(location.state).toEqual({ provider: 'z-ai-coding', answering: true });
  });

  it('finishes the answers before a return to where setup was opened from', () => {
    ret.returnFromSetup.mockReturnValue(true);
    const location = connectAs('moonshot-coding', true);
    expect(location.pathname).toBe('/setup/model');
    expect(ret.returnFromSetup).not.toHaveBeenCalled();
    ret.returnFromSetup.mockReturnValue(false);
  });

  it('goes on to the model step for a visit the answers did not send, onboarding unfinished or not', () => {
    const location = connectAs('moonshot-coding', false);
    expect(location.pathname).toBe('/setup/models');
    expect(location.state).toEqual({ provider: 'moonshot-coding' });
  });
});
