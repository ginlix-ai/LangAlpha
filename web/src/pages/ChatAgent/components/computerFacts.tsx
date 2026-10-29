/**
 * What a computer's card and its details say under its name, each fact set
 * off by a dot.
 */
import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';

import type { Computer, ComputerSsh } from '@/types/api';

import { computerSourceLabel } from './providerUi';
import { formatSshReached } from './sshTarget';

type Translate = ReturnType<typeof useTranslation>['t'];

interface ComputerFact {
  key: string;
  node: ReactNode;
}

/**
 * The facts under a name. Each keeps its dot, so a wrap never leaves one at
 * a line's end.
 */
export function ComputerFacts({ computer, showProvider }: { computer: Computer; showProvider: boolean }) {
  const { t } = useTranslation();
  const facts = computerFacts(t, computer, showProvider);
  if (facts.length === 0) return null;
  return (
    <div className="computer-card-kicker">
      {facts.map(({ key, node }, i) => (
        <span key={key} className="inline-flex min-w-0 gap-1.5">
          {i > 0 && <span aria-hidden="true">·</span>}
          {node}
        </span>
      ))}
    </div>
  );
}

/**
 * For a machine the user brings, how it is reached (when there are kinds to
 * tell apart) and where. A hosted one's corner already names its provider,
 * and the tier and the workspace count have rows of their own.
 */
function computerFacts(t: Translate, computer: Computer, showProvider: boolean): ComputerFact[] {
  const facts: ComputerFact[] = [];
  if (showProvider && computer.kind === 'ssh') {
    // This Mac has its own pill, so the fact says how it is reached.
    facts.push({ key: 'provider', node: <span>{computerSourceLabel(t, computer, false)}</span> });
  }
  if (computer.ssh) facts.push(...(computer.ssh.helper ? helperFacts(t, computer.ssh) : sshFacts(t, computer.ssh)));
  return facts;
}

function sshFacts(t: Translate, ssh: ComputerSsh): ComputerFact[] {
  const address = formatSshReached(ssh, ssh.tunnel);
  const facts: ComputerFact[] = [
    { key: 'address', node: <span className="min-w-0 truncate font-mono" title={address}>{address}</span> },
  ];
  if (ssh.tunnel) {
    facts.push({ key: 'tunnel', node: <span>{t('computer.ssh.tunnelBadge', 'tunnel')}</span> });
    if (ssh.tunnel_connected === false) {
      facts.push({ key: 'offline', node: <span>{t('computer.ssh.tunnelOffline', 'tunnel offline')}</span> });
    }
  }
  return facts;
}

/**
 * A Mac under remote control is an SSH computer only underneath: the login,
 * its host name and the tunnel are how the helper is reached, and none of them
 * is anything the user set up. What they chose is the account the agent works
 * as.
 */
function helperFacts(t: Translate, ssh: ComputerSsh): ComputerFact[] {
  const facts: ComputerFact[] = [
    {
      key: 'account',
      node: <span className="min-w-0 truncate">{t('computer.remoteControl.asAccount', 'as {{account}}', { account: ssh.username })}</span>,
    },
  ];
  if (ssh.tunnel_connected === false) {
    facts.push({ key: 'offline', node: <span>{t('computer.remoteControl.notConnected', 'Not connected')}</span> });
  }
  return facts;
}
