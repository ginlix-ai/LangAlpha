import { useMemo } from 'react';
import {
  MessageSquareText,
  Sparkles,
  MessagesSquare,
  Command,
} from 'lucide-react';
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
import type { WidgetRenderProps } from '../types';
import './ConversationWidget.css';

type ConversationConfig = Record<string, never>;

function formatGreeting(name?: string | null): string {
  const hour = new Date().getHours();
  const tod =
    hour < 5
      ? 'Up late'
      : hour < 12
        ? 'Good morning'
        : hour < 17
          ? 'Good afternoon'
          : hour < 22
            ? 'Good evening'
            : 'Still at it';
  const first = (name || '').trim().split(/\s+/)[0];
  return first ? `${tod}, ${first}.` : `${tod}.`;
}

function formatDateStrip(): string {
  const now = new Date();
  const date = now.toLocaleDateString(undefined, {
    weekday: 'long',
    month: 'short',
    day: 'numeric',
  });
  const time = now.toLocaleTimeString(undefined, {
    hour: 'numeric',
    minute: '2-digit',
  });
  return `${date} · ${time}`;
}

function formatRelative(ts?: string): string {
  if (!ts) return '';
  const diffMs = Date.now() - new Date(ts).getTime();
  const m = Math.floor(diffMs / 60000);
  if (m < 1) return 'now';
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h`;
  const d = Math.floor(h / 24);
  if (d < 7) return `${d}d`;
  return new Date(ts).toLocaleDateString(undefined, {
    month: 'short',
    day: 'numeric',
  });
}

function ConversationWidget(_props: WidgetRenderProps<ConversationConfig>) {
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
  const greeting = useMemo(() => formatGreeting(user?.name), [user?.name]);
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
        <MessageSquareText size={180} strokeWidth={1} />
      </div>
      <div className="conversation-widget__wash" aria-hidden="true" />

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
          className="conversation-widget__eyebrow"
          variants={{ hidden: { opacity: 0, y: -4 }, show: { opacity: 1, y: 0 } }}
        >
          <span className="conversation-widget__badge">
            <Sparkles size={12} />
            Work with LangAlpha
          </span>
          <span className="conversation-widget__date">{dateStrip}</span>
        </motion.div>

        <motion.div
          className="conversation-widget__heading"
          variants={{ hidden: { opacity: 0, y: 8 }, show: { opacity: 1, y: 0 } }}
        >
          <h2 className="conversation-widget__greeting">{greeting}</h2>
          <p className="conversation-widget__subline">
            What are we working on today?
          </p>
        </motion.div>

        <motion.div
          className="conversation-widget__stage"
          variants={{ hidden: { opacity: 0, y: 12 }, show: { opacity: 1, y: 0 } }}
        >
          <div className="conversation-widget__stage-caption">
            <Command size={11} />
            <span>
              Press <kbd>↵</kbd> to send · <kbd>⌘</kbd>+<kbd>↵</kbd> for newline
              · type <kbd>/</kbd> for skills &amp; subagents
            </span>
          </div>
          <ChatInput
            onSend={handleSend}
            disabled={isLoading}
            mode={mode}
            onModeChange={setMode}
            workspaces={workspaces}
            selectedWorkspaceId={selectedWorkspaceId}
            onWorkspaceChange={setSelectedWorkspaceId}
            placeholder="Put LangAlpha to work — a ticker to unpack, a filing to parse, a thesis to stress-test…"
            minRows={2}
          />
        </motion.div>

        {recentThreads.length > 0 && (
          <motion.div
            className="conversation-widget__resume"
            variants={{ hidden: { opacity: 0 }, show: { opacity: 1 } }}
          >
            <span className="conversation-widget__resume-label">
              <MessagesSquare size={11} />
              Resume
            </span>
            <div className="conversation-widget__resume-chips">
              {recentThreads.map((t) => (
                <button
                  key={t.thread_id}
                  type="button"
                  className="conversation-widget__chip"
                  onClick={() => navigate(`/chat/t/${t.thread_id}`)}
                  title={t.title || 'Untitled thread'}
                >
                  <span className="conversation-widget__chip-title">
                    {t.title || 'Untitled thread'}
                  </span>
                  <span className="conversation-widget__chip-age">
                    {formatRelative(t.updated_at)}
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
  title: 'Workbench',
  description:
    'A chat-first console: greeting, live chat input, and one-click resume.',
  category: 'agent',
  icon: MessageSquareText,
  component: ConversationWidget,
  defaultConfig: {},
  defaultSize: { w: 8, h: 12 },
  minSize: { w: 6, h: 8 },
  maxSize: { w: 12, h: 44 },
  singleton: true,
  fitToContent: true,
});

export default ConversationWidget;
