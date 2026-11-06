import type { TFunction } from 'i18next';
import type { ClaudeCodeComputer, ClaudeCodeSetup } from '@/api/model';

const S = 'modelAccess.claudeCode.setup';

/** Whether a computer's token works by its last check and has not passed
 *  its expiry, as its own line reads it. */
export function computerTokenWorks(c: ClaudeCodeComputer, now: number): boolean {
  return (c.state === 'signed_in' || c.state === 'unverified') && !(c.expires_at && Date.parse(c.expires_at) <= now);
}

/** What the view leads with in each state, a title over a sentence; its
 *  title is what Model access's row says while no token works or is checked. */
export function claudeCodeHeadline(t: TFunction, setup: ClaudeCodeSetup, now: number): { title: string; body: string } {
  const own = setup.computers;
  const only = own.length === 1 ? own[0].name : null;
  switch (setup.state) {
    case 'no_computer':
      return { title: t(S + '.noComputer.title'), body: t(setup.cloud ? S + '.noComputer.bodyCloud' : S + '.noComputer.body') };
    case 'unsupported':
      return { title: t(S + '.unsupported.title'), body: t(S + '.unsupported.body') };
    case 'offline':
      return { title: only ? t(S + '.offline.title', { name: only }) : t(S + '.offline.titleMany'), body: t(S + '.offline.body') };
    case 'not_connected': {
      // Where no own computer can take a token, the saved token or the choice
      // for cloud computers is what is left to set.
      const ready = own.filter((c) => c.state !== 'offline');
      const kept = setup.on ? [] : ready.filter((c) => c.source === 'machine');
      if (kept.length) {
        return {
          title: t(S + '.ready.titleKept'),
          body: kept.length === 1 ? t(S + '.ready.bodyKept', { name: kept[0].name }) : t(S + '.ready.bodyKeptMany'),
        };
      }
      if (!ready.length && !setup.saved_token_runs) {
        return { title: t(S + '.ready.titleCloud'), body: t(setup.on ? S + '.ready.bodyCloud' : S + '.ready.bodyTurnOn') };
      }
      const body = ready.length === 1 ? t(S + '.ready.body', { name: ready[0].name })
        : ready.length > 1 ? t(S + '.ready.bodyMany') : t(S + '.ready.bodySaved');
      return { title: t(S + '.ready.title'), body };
    }
    case 'refused': {
      const refused = own.filter((c) => c.state === 'rejected');
      return {
        title: t(S + '.refused.title'),
        body: refused.length === 1 ? t(S + '.refused.body', { name: refused[0].name }) : t(S + '.refused.bodyAny'),
      };
    }
    case 'checking':
      return { title: t(S + '.checking.title'), body: t(S + '.checking.body') };
    case 'connected': {
      const ownWorks = own.some((c) => computerTokenWorks(c, now));
      return { title: t(S + '.connected.title'), body: t(ownWorks ? S + '.connected.body' : S + '.connected.bodyAny') };
    }
  }
}
