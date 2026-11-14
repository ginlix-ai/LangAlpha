import type { TFunction } from 'i18next';
import type { ClaudeCodeCliPlacement, ClaudeCodeSetup } from '@/api/model';
import { computerTokenWorks } from '@/lib/claudeCode';

const S = 'modelAccess.claudeCode.setup';

/** What the view leads with in each state, a title over a sentence, and once
 *  ready, where the Chief of Staff's CLI runs (`runsOn`), which a refusal
 *  leaves unsaid: on the own computer the server picked for the cloud
 *  workspaces too (`cliComputer`, `ClaudeCodeComputers.cli_computer`), said
 *  of both only where both run there, or on Home's own or cloud computer.
 *  Home's own computer is said only while its token works at `now`: its
 *  placement reads no token there, and admission refuses a turn on one that
 *  does not. Its title is what Model access's row says while no token works
 *  or is checked. */
export function claudeCodeHeadline(
  t: TFunction, setup: ClaudeCodeSetup, now: number, cliComputer: ClaudeCodeCliPlacement | null = null,
): { title: string; body: string; runsOn?: string; ready?: boolean } {
  const own = setup.computers;
  const only = own.length === 1 ? own[0].name : null;
  switch (setup.state) {
    case 'no_computer':
      return { title: t(S + '.noComputer.title'), body: t(S + '.noComputer.body') };
    case 'unsupported':
      return { title: t(S + '.unsupported.title'), body: t(S + '.unsupported.body') };
    case 'offline':
      return only
        ? { title: t(S + '.offline.title', { name: only }), body: t(S + '.offline.body') }
        : { title: t(S + '.offline.titleMany'), body: t(S + '.offline.bodyMany') };
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
      // A token works where nothing runs it yet: Home's cloud computer is too
      // small and no own computer's token works. Almost ready, never ready.
      const small = setup.chief_of_staff?.refusal === 'needs_bigger_computer' ? setup.chief_of_staff : null;
      if (small && !own.some((c) => computerTokenWorks(c, now))) {
        const body = small.name ? t(S + '.connected.almostBody', { name: small.name }) : t('modelAccess.claudeCode.medium.row');
        return { title: t(S + '.connected.almostTitle'), body };
      }
      const cos = setup.chief_of_staff?.refusal ? null : setup.chief_of_staff;
      const name = cos?.name;
      const bothPicked = cos?.kind === 'picked' && cliComputer?.kind === 'picked' && !cliComputer.refusal
        && !!cos.computer_id && cliComputer.computer_id === cos.computer_id;
      const homeOwn = cos?.kind === 'own' && own.some((c) => c.computer_id === cos.computer_id && computerTokenWorks(c, now));
      const runsOn = !name ? undefined
        : bothPicked ? t(S + '.connected.cliOwn', { name })
          : homeOwn || cos?.kind === 'home_cloud' ? t(S + '.connected.cliHome', { name }) : undefined;
      // A chat to start is a Chief of Staff turn, so only where Home runs it.
      return { title: t(S + '.connected.title'), body: t(S + '.connected.body'), runsOn, ready: !setup.chief_of_staff?.refusal };
    }
  }
}
