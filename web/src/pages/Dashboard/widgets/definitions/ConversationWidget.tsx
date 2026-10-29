import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { clockTime, relativeTime, weekdayMonthDay } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { MessageSquareText, MessagesSquare } from 'lucide-react';
import { useNavigate } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { motion } from '@/lib/framer';
import ChatInput from '@/components/ui/chat-input';
import { useChatInput } from '../../hooks/useChatInput';
import { useUser } from '@/hooks/useUser';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { workspaceThreadsQuery } from '@/pages/ChatAgent/utils/threadQueries';
import type { Thread } from '@/types/api';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import { wrapWidgetContext } from '../framework/snapshotSerializers';
import { ConversationConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';
import './ConversationWidget.css';
import { STAGGER } from '@/lib/motion';

type ConversationConfig = Record<string, never>;

function greetingKey(now: number): string {
  const hour = new Date(now).getHours();
  if (hour < 5) return 'dashboard.widgets.conversation.greetingLate';
  if (hour < 12) return 'dashboard.widgets.conversation.greetingMorning';
  if (hour < 17) return 'dashboard.widgets.conversation.greetingAfternoon';
  if (hour < 22) return 'dashboard.widgets.conversation.greetingEvening';
  return 'dashboard.widgets.conversation.greetingStillAtIt';
}


function formatDateStrip(now: number, locale: string): string {
  return `${weekdayMonthDay(now, locale)} · ${clockTime(now, locale)}`;
}

function ConversationWidget({ instance }: WidgetRenderProps<ConversationConfig>) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const navigate = useNavigate();
  // Focus inside the composer means the user is writing, which warms the
  // target computer.
  const [focused, setFocused] = useState(false);

  const {
    scope,
    setScope,
    isLoading,
    handleSend,
    workspaces,
    selectedWorkspaceId,
    setSelectedWorkspaceId,
  } = useChatInput({ composing: focused });

  const { user } = useUser();
  const greeting = useMemo(() => {
    const tod = t(greetingKey(now));
    const first = (user?.name || '').trim().split(/\s+/)[0];
    return first ? `${tod}, ${first}.` : `${tod}.`;
  }, [user?.name, t, now]);
  const dateStrip = formatDateStrip(now, locale);

  // Recent threads for the Resume strip — peek at the first workspace.
  // Fetch 100 to match the shared React Query cache key used by useChatInput,
  // RecentThreadsWidget, and WorkspacePickerWidget (single cache entry serves all).
  const { data: wsData } = useWorkspaces({ limit: 100 });
  const firstWsId = wsData?.workspaces?.[0]?.workspace_id;

  const { data: threadsData } = useQuery({
    ...workspaceThreadsQuery(firstWsId ?? '', 4),
    enabled: !!firstWsId,
  });

  const recentThreads = useMemo<Thread[]>(
    () => (threadsData?.threads ?? []).slice(0, 4),
    [threadsData],
  );

  useWidgetContextExport(instance.id, {
    full: () => {
      const selectedWs = workspaces.find((w) => w.workspace_id === selectedWorkspaceId);
      // The Chief of Staff scope ignores the workspace it last selected.
      const inAllWorkspaces = scope === 'all';
      const workspaceId = inAllWorkspaces ? null : selectedWorkspaceId;
      const workspaceName = inAllWorkspaces ? t('agents.chiefOfStaff') : selectedWs?.name;
      const workspaceLine = inAllWorkspaces
        ? 'Workspace: Chief of Staff (Home)'
        : `Workspace: ${selectedWs?.name ?? '(none)'} (${selectedWorkspaceId ?? 'unset'})`;
      const lines: string[] = [workspaceLine];
      if (recentThreads.length) {
        lines.push('', '**Resume threads:**');
        recentThreads.forEach((th) => {
          const title = th.title || t('dashboard.widgets.conversation.untitledThread');
          lines.push(`- ${title} — ${th.updated_at ?? ''} (${th.thread_id})`);
        });
      }
      const text = wrapWidgetContext(
        'agent.conversation',
        { workspace_id: workspaceId },
        lines.join('\n'),
      );
      return {
        widget_type: 'agent.conversation',
        widget_id: instance.id,
        label: t('dashboard.widgets.conversation.title'),
        description: workspaceName ?? 'no workspace',
        captured_at: new Date().toISOString(),
        text,
        data: {
          workspace_id: workspaceId,
          workspace_name: workspaceName,
          recent_threads: recentThreads.map((th) => ({
            thread_id: th.thread_id,
            title: th.title,
            updated_at: th.updated_at,
          })),
        },
      };
    },
  });

  return (
    <div className="conversation-widget">
      <div className="conversation-widget__watermark" aria-hidden="true">
        <MessageSquareText size={280} strokeWidth={0.9} />
      </div>
      <div className="conversation-widget__grid" aria-hidden="true" />

      <motion.div
        className="conversation-widget__inner"
        initial="hidden"
        animate="show"
        variants={{
          hidden: {},
          show: { transition: { staggerChildren: STAGGER, delayChildren: STAGGER } },
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
          onFocus={() => setFocused(true)}
          onBlur={(e) => {
            if (!e.currentTarget.contains(e.relatedTarget)) setFocused(false);
          }}
        >
          <ChatInput
            onSend={handleSend}
            disabled={isLoading}
            scope={scope}
            onScopeChange={setScope}
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
                    {relativeTime(thread.updated_at, locale, now)}
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
