import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { MessageSquareText, MessagesSquare } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { motion } from 'framer-motion';
import ChatInput from '@/components/ui/chat-input';
import { useChatInput } from '../../hooks/useChatInput';
import { useUser } from '@/hooks/useUser';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { getWorkspaceThreads } from '@/pages/ChatAgent/utils/api';
import { queryKeys } from '@/lib/queryKeys';
import type { Thread, ThreadsResponse } from '@/types/api';
import { registerWidget } from '../framework/WidgetRegistry';
import { ConversationConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';
import './ConversationWidget.css';

type ConversationConfig = Record<string, never>;

function greetingKey(): string {
  const hour = new Date().getHours();
  if (hour < 5) return 'dashboard.widgets.conversation.greetingLate';
  if (hour < 12) return 'dashboard.widgets.conversation.greetingMorning';
  if (hour < 17) return 'dashboard.widgets.conversation.greetingAfternoon';
  if (hour < 22) return 'dashboard.widgets.conversation.greetingEvening';
  return 'dashboard.widgets.conversation.greetingStillAtIt';
}

function formatDateStrip(): string {
  const now = new Date();
  const date = now.toLocaleDateString(i18n.language, {
    weekday: 'long',
    month: 'short',
    day: 'numeric',
  });
  const time = now.toLocaleTimeString(i18n.language, {
    hour: 'numeric',
    minute: '2-digit',
  });
  return `${date} · ${time}`;
}

function formatRelative(ts?: string): string {
  if (!ts) return '';
  const diffMs = Date.now() - new Date(ts).getTime();
  const m = Math.floor(diffMs / 60000);
  if (m < 1) return i18n.t('dashboard.widgets.common.relativeNow');
  let when: string;
  if (m < 60) when = `${m}m`;
  else {
    const h = Math.floor(m / 60);
    if (h < 24) when = `${h}h`;
    else {
      const d = Math.floor(h / 24);
      if (d < 7) when = `${d}d`;
      else return new Date(ts).toLocaleDateString(i18n.language, {
        month: 'short',
        day: 'numeric',
      });
    }
  }
  return i18n.t('dashboard.widgets.common.relativePast', { when });
}

function ConversationWidget(_props: WidgetRenderProps<ConversationConfig>) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  const {
    mode,
    setMode,
    isLoading,
    handleSend,
    workspaces,
    selectedWorkspaceId,
    setSelectedWorkspaceId,
  } = useChatInput();

  const { user } = useUser();
  const greeting = useMemo(() => {
    const tod = t(greetingKey());
    const first = (user?.name || '').trim().split(/\s+/)[0];
    return first ? `${tod}, ${first}.` : `${tod}.`;
  }, [user?.name, t]);
  // Computed per render so the strip updates if the dashboard sits open
  // across midnight. Empty-deps memo would freeze it at mount time.
  const dateStrip = formatDateStrip();

  // Recent threads for the Resume strip — peek at the first workspace.
  // Fetch 100 to match the shared React Query cache key used by useChatInput,
  // RecentThreadsWidget, and WorkspacePickerWidget (single cache entry serves all).
  const { data: wsData } = useWorkspaces({ limit: 100 });
  const firstWsId = wsData?.workspaces?.[0]?.workspace_id as string | undefined;

  const { data: threadsData } = useQuery<ThreadsResponse>({
    queryKey: firstWsId
      ? [...queryKeys.threads.byWorkspace(firstWsId), 4, 0]
      : ['threads', 'noop'],
    queryFn: () =>
      getWorkspaceThreads(firstWsId!, 4, 0) as Promise<ThreadsResponse>,
    enabled: !!firstWsId,
    staleTime: 30_000,
  });

  const recentThreads = useMemo<Thread[]>(
    () => (threadsData?.threads ?? []).slice(0, 4),
    [threadsData],
  );

  return (
    <div className="conversation-widget">
      <div className="conversation-widget__watermark" aria-hidden="true">
        <MessageSquareText size={280} strokeWidth={0.9} />
      </div>
      <div className="conversation-widget__wash" aria-hidden="true" />
      <div className="conversation-widget__grid" aria-hidden="true" />

      <motion.div
        className="conversation-widget__inner"
        initial="hidden"
        animate="show"
        variants={{
          hidden: {},
          show: { transition: { staggerChildren: 0.07, delayChildren: 0.04 } },
        }}
      >
        <motion.div
          className="conversation-widget__heading"
          variants={{ hidden: { opacity: 0, y: 8 }, show: { opacity: 1, y: 0 } }}
        >
          <h2 className="conversation-widget__greeting">{greeting}</h2>
          <span className="conversation-widget__date">{dateStrip}</span>
        </motion.div>

        <motion.div
          className="conversation-widget__stage"
          variants={{ hidden: { opacity: 0, y: 12 }, show: { opacity: 1, y: 0 } }}
        >
          <ChatInput
            onSend={handleSend}
            disabled={isLoading}
            mode={mode}
            onModeChange={setMode}
            workspaces={workspaces}
            selectedWorkspaceId={selectedWorkspaceId}
            onWorkspaceChange={setSelectedWorkspaceId}
            placeholder={t('dashboard.widgets.conversation.placeholder')}
            minRows={4}
          />
        </motion.div>

        {recentThreads.length > 0 && (
          <motion.div
            className="conversation-widget__resume"
            variants={{ hidden: { opacity: 0 }, show: { opacity: 1 } }}
          >
            <span className="conversation-widget__resume-label">
              <MessagesSquare size={11} />
              {t('dashboard.widgets.conversation.resume')}
            </span>
            <div className="conversation-widget__resume-chips">
              {recentThreads.map((thread) => (
                <button
                  key={thread.thread_id}
                  type="button"
                  className="conversation-widget__chip"
                  onClick={() => navigate(`/chat/t/${thread.thread_id}`)}
                  title={thread.title || t('dashboard.widgets.conversation.untitledThread')}
                >
                  <span className="conversation-widget__chip-title">
                    {thread.title || t('dashboard.widgets.conversation.untitledThread')}
                  </span>
                  <span className="conversation-widget__chip-age">
                    {formatRelative(thread.updated_at)}
                  </span>
                </button>
              ))}
            </div>
          </motion.div>
        )}
      </motion.div>
    </div>
  );
}

registerWidget<ConversationConfig>({
  type: 'agent.conversation',
  titleKey: 'dashboard.widgets.conversation.title',
  descriptionKey: 'dashboard.widgets.conversation.description',
  category: 'agent',
  icon: MessageSquareText,
  component: ConversationWidget,
  defaultConfig: {},
  configSchema: ConversationConfigSchema,
  defaultSize: { w: 12, h: 18 },
  minSize: { w: 8, h: 12 },
  maxSize: { w: 12, h: 44 },
  singleton: true,
  fitToContent: true,
});

export default ConversationWidget;
