import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import type { StateFilter } from './ListControls';
import { EmptyState } from './EmptyState';

/**
 * What a narrowed list says when nothing survives the filter. "No matches"
 * leaves the user to work out which of the two controls emptied the page and
 * whether the tab is empty or merely filtered, so the sentence names what was
 * asked for, and the one control that undoes it sits under the sentence.
 */

/** Both controls are narrowing: the sentence has to carry both. */
const WITH_QUERY: Record<StateFilter, string> = {
  all: 'plugins.filter.empty.query',
  on: 'plugins.filter.empty.queryOn',
  off: 'plugins.filter.empty.queryOff',
  attention: 'plugins.filter.empty.queryAttention',
};

/** Only a state pill is narrowing ('all' cannot narrow, so it never lands). */
const STATE_ONLY: Record<StateFilter, string> = {
  all: 'plugins.filter.noMatches',
  on: 'plugins.filter.empty.on',
  off: 'plugins.filter.empty.off',
  attention: 'plugins.filter.empty.attention',
};

export function FilterEmpty({
  noun,
  filter,
  stateFilter,
  onReset,
}: {
  /** What this tab lists, already translated ("plugins", "servers", "skills"). */
  noun: string;
  filter: string;
  stateFilter: StateFilter;
  onReset: () => void;
}) {
  const { t } = useTranslation();
  const query = filter.trim();
  return (
    <EmptyState
      message={
        query
          ? t(WITH_QUERY[stateFilter], { noun, query })
          : t(STATE_ONLY[stateFilter], { noun })
      }
      action={
        <HeaderButton variant="secondary" icon={X} onClick={onReset}>
          {t('plugins.filter.reset')}
        </HeaderButton>
      }
    />
  );
}
