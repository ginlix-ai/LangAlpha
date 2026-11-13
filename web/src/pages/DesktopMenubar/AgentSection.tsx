import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { openInMain } from '@/lib/desktop';
import { threadName } from '@/pages/ChatAgent/components/messageList/sentMessage';
import { threadDetailQuery } from '@/pages/ChatAgent/utils/threadQueries';
import type { Thread } from '@/types/api';
import { Section } from './primitives';
import type { SectionProps } from './panelContext';
import { useAgentAttention } from './useGlanceState';
import { useRecentThreadsGlance } from './useGlanceData';

const AGENT_ROWS = 6;

type AgentState = 'needs_input' | 'running' | 'unseen';

const STATE_KEY: Record<AgentState, string> = {
  needs_input: 'menubar.agent.needsInput',
  running: 'menubar.agent.running',
  unseen: 'menubar.agent.finished',
};

const threadPath = (threadId: string) => `/chat/t/${encodeURIComponent(threadId)}`;

function AgentRow({ id, state, listed, active }: { id: string; state: AgentState; listed: Thread | undefined; active: boolean }) {
  const { t } = useTranslation();
  // A thread past the recent list is read on its own, once, while the panel shows.
  const { data: detail } = useQuery({ ...threadDetailQuery(id), enabled: active && !listed, staleTime: 5 * 60_000 });
  const name = threadName(listed) || threadName(detail as Thread | undefined) || t('menubar.agent.untitled');
  return (
    <li>
      <button type="button" className="mb-row mb-row-btn mb-agent-row" onClick={() => openInMain(threadPath(id))}>
        <span className="mb-glyph" aria-hidden="true">
          {state === 'running' ? (
            // The live glyph ticks on a timer, so it runs only while the panel shows.
            active ? <Loader size={12} className="text-(--color-accent-primary)" /> : <span className="mb-glyph-still">⠋</span>
          ) : state === 'needs_input' ? (
            <span className="mb-ring" />
          ) : (
            <span className="mb-dot" />
          )}
        </span>
        <span className="mb-agent-title">{name}</span>
        <span className="mb-agent-state">{t(STATE_KEY[state])}</span>
      </button>
    </li>
  );
}

export function AgentSection({ active }: SectionProps) {
  const { t } = useTranslation();
  const { needsInput, running, unseen } = useAgentAttention();
  const { threads } = useRecentThreadsGlance(active);
  const byId = useMemo(() => new Map(threads.map((th) => [th.thread_id, th])), [threads]);
  const items: { id: string; state: AgentState }[] = [
    ...needsInput.map((id) => ({ id, state: 'needs_input' as const })),
    ...running.map((id) => ({ id, state: 'running' as const })),
    ...unseen.map((id) => ({ id, state: 'unseen' as const })),
  ];
  const shown = items.slice(0, AGENT_ROWS);
  const more = items.length - shown.length;

  return (
    <Section title={t('menubar.agent.title')}>
      {items.length === 0 ? (
        <p className="mb-empty">{t('menubar.agent.empty')}</p>
      ) : (
        <ul className="mb-rows">
          {shown.map(({ id, state }) => (
            <AgentRow key={id} id={id} state={state} listed={byId.get(id)} active={active} />
          ))}
        </ul>
      )}
      {more > 0 && (
        <button type="button" className="mb-link" onClick={() => openInMain('/chat')}>
          {t('menubar.agent.more', { count: more })}
        </button>
      )}
    </Section>
  );
}
