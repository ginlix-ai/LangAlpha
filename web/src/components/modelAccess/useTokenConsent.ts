import { useEffect, useRef, useState } from 'react';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { useUser } from '@/hooks/useUser';
import { CLAUDE_CODE_DISCLOSURE } from '@/lib/claudeCode';
import type { TokenRoutes } from './tokenTarget';

/**
 * The user's agreement to the setup token being stored, for a sheet that
 * asks for it (`enabled`): the box they tick (`form`, for `ConsentForm`)
 * and the token routes held on it. Every way in waits on the box (`waiting`)
 * and records the agreement before it asks for a token, all of them through
 * one recording at a time, which the box stays locked for. Once recorded the
 * box is ticked for good; withdrawn elsewhere, it is cleared to be ticked
 * again. A token asked for once the agreement lands goes out only while the
 * sheet is open; one answered for another account `choose` already refuses.
 */
export function useTokenConsent(routes: TokenRoutes, enabled: boolean) {
  const { custody, choose } = useClaudeCodeCustody({ enabled });
  const given = custody?.mode === 'stored';
  // The box ticked is the account's that ticked it: another signing in here
  // finds it clear.
  const { user } = useUser();
  const me = user?.user_id ?? null;
  const [agreement, setAgreement] = useState<{ by: string | null } | null>(null);
  const agreed = agreement?.by === me;
  const setAgreed = (on: boolean) => setAgreement(on ? { by: me } : null);
  const [wasGiven, setWasGiven] = useState(given);
  if (given !== wasGiven) {
    setWasGiven(given);
    if (!given) setAgreement(null);
  }
  const recording = useRef<Promise<unknown> | null>(null);
  const [inFlight, setInFlight] = useState(false);
  const open = useRef(true);
  useEffect(() => {
    open.current = true;
    return () => { open.current = false; };
  }, []);

  if (!enabled) return { routes, form: null, waiting: false };
  const afterConsent = async <T,>(send: () => Promise<T>): Promise<T> => {
    recording.current ??= (given ? Promise.resolve() : choose({ mode: 'stored', disclosure: CLAUDE_CODE_DISCLOSURE })).finally(() => {
      recording.current = null;
      if (open.current) setInFlight(false);
    });
    setInFlight(true);
    await recording.current;
    if (!open.current) throw new Error('The sheet closed before the agreement was recorded');
    return send();
  };
  return {
    routes: {
      ...routes,
      start: () => afterConsent(routes.start),
      submit: (code: string) => afterConsent(() => routes.submit(code)),
      save: (token: string) => afterConsent(() => routes.save(token)),
    },
    form: { agreed, locked: given || inFlight, onAgree: setAgreed },
    waiting: !given && !agreed,
  };
}
