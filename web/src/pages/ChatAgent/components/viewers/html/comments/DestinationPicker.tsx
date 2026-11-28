import React, { useState } from 'react';
import { House, MessageSquare } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Menu, MenuItem, type Key } from 'react-aria-components';
import { Autocomplete, SearchField } from '@/components/ui/aria-search-field';
import { MENU_ROW } from '@/components/ui/chat-input.composerPopover';
import { useDestinationThreads } from '@/hooks/useArtifact';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import type { DestinationThread } from '@/types/api';

/** How long the search rests before it asks the server, so typing a title sends one read. */
const SEARCH_REST_MS = 250;

/** Where a chat is, as the picker says it under its title: Home's are the Chief of Staff's. */
function threadPlace(t: (key: string) => string, thread: DestinationThread): string {
  if (thread.home) return t('agents.chiefOfStaff');
  return thread.workspace_name?.trim() || t('pageViewer:comments.otherChatNoWorkspace');
}

/**
 * "Another chat…": a search over every chat a comment on the record could go
 * to, across the owner's workspaces and Home, newest activity first. Mounted
 * while open, so it starts empty each time; the server matches the title, and
 * the last answer stays up while the next is read.
 */
export function DestinationPicker({ artifactId, onPick }: {
  artifactId: string;
  onPick: (thread: DestinationThread) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const [query, setQuery] = useState('');
  const asked = useDebouncedValue(query.trim(), SEARCH_REST_MS);
  const { data, isPending, isError } = useDestinationThreads(artifactId, asked);
  const threads = data ?? [];
  const pick = (key: Key) => {
    const thread = threads.find((th) => th.thread_id === key);
    if (thread) onPick(thread);
  };
  const empty = isError
    ? t('pageViewer:comments.otherChatFailed')
    : isPending
      ? t('common.loading')
      : asked
        ? t('pageViewer:comments.otherChatNoMatch', { query: asked })
        : t('pageViewer:comments.otherChatNone');

  return (
    <Autocomplete inputValue={query} onInputChange={setQuery}>
      <SearchField
        aria-label={t('pageViewer:comments.otherChatSearch')}
        placeholder={t('pageViewer:comments.otherChatSearch')}
        autoFocus
      />
      <Menu
        aria-label={t('pageViewer:comments.otherChatList')}
        items={threads}
        onAction={pick}
        className="max-h-64 min-h-0 overflow-y-auto p-1 outline-hidden"
        renderEmptyState={() => (
          <p className="px-3 py-4 text-center text-[0.8125rem] text-(--color-text-tertiary)" role="status">{empty}</p>
        )}
      >
        {(thread) => {
          const title = thread.title?.trim() || t('pageViewer:comments.untitledThread');
          const place = threadPlace(t, thread);
          const Icon = thread.home ? House : MessageSquare;
          return (
            <MenuItem id={thread.thread_id} textValue={`${title}, ${place}`} data-slot="menu-item" className={MENU_ROW}>
              <Icon aria-hidden className="h-4 w-4 flex-none text-(--color-text-tertiary)" />
              <span className="flex min-w-0 flex-1 flex-col" title={title}>
                <span className="truncate">{title}</span>
                <span className="truncate text-[11px] text-(--color-text-tertiary)">{place}</span>
              </span>
            </MenuItem>
          );
        }}
      </Menu>
    </Autocomplete>
  );
}
