import { useTranslation } from 'react-i18next';
import type { LinkNote } from '@/lib/linkStanding';
import { ShareOutNote } from '@/components/sharedWorkspaces/ShareOutNote';
import { RuleNote } from './RuleRow';

/** What governs a link, as `linkStanding` says it: the workspace's managers'
 *  rule in the workspace's glyph, the account's in the organization's. */
export function LinkRuleNote({ note }: { note: LinkNote | null }) {
  const { t } = useTranslation();
  if (!note) return null;
  if (note.kind === 'shareOut') return <ShareOutNote state={note.state} />;
  return <RuleNote>{t(note.key, note.values)}</RuleNote>;
}
