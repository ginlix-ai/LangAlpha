import { LibraryBig } from 'lucide-react';
import { Link } from 'react-router';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useAccount } from '@/hooks/useAccount';
import { useLibraryEnabled, usePublished, usePublishToLibrary, useRemoveFromLibrary } from '@/hooks/useOrgLibrary';
import type { LibraryTarget } from '@/types/orgLibrary';
import { cn } from '@/lib/utils';
import { RuleNote } from './RuleRow';
import { WorkspaceRuleNote } from '@/components/sharedWorkspaces/ShareOutNote';

const RULE_LIBRARY_PUBLISH = 'org_library.publish';

/**
 * A thread's or file's place in the organization library, under its link in
 * a share dialog: publish it, or see that it is there and take it out. Shown
 * only while the link is on, since the library opens an item through its
 * link, and only where the library is on for the account. Publishing keeps
 * the link as it is, so it never widens who can open the content.
 */
export function LibraryPublishRow({ target, linkOn, hint, framed = true }: {
  target: LibraryTarget;
  /** A thread or file needs its link on; a skill is always publishable. */
  linkOn: boolean;
  hint: string;
  /** Ruled off from what is above it, as in a share dialog; off where a
   *  titled block already sets it apart. */
  framed?: boolean;
}) {
  const { t } = useTranslation();
  const { orgT, setterName } = useAccount();
  const enabled = useLibraryEnabled();
  const { data } = usePublished(target, enabled && linkOn);
  const publish = usePublishToLibrary();
  const remove = useRemoveFromLibrary();
  if (!enabled || !linkOn || !data) return null;

  const item = data.item;
  const busy = publish.isPending || remove.isPending;
  const failed = (key: string) => () => toast({ variant: 'destructive', title: t(key) });

  return (
    <div
      className={framed ? 'pt-3 mt-1 border-t space-y-2' : 'space-y-2'}
      style={framed ? { borderColor: 'var(--color-border-muted)' } : undefined}
    >
      <div className="flex flex-wrap items-center justify-between gap-x-2 gap-y-1">
        <span
          className={cn('flex items-start gap-2 min-w-0 text-xs', !framed && !item && 'hidden')}
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <LibraryBig aria-hidden="true" className="h-3.5 w-3.5 shrink-0 mt-px" style={{ color: 'var(--color-icon-muted)' }} />
          <span className="min-w-0 wrap-break-word">
            {item
              ? orgT('organization.library.inLibrary')
              : framed && t('organization.library.sectionLabel')}
          </span>
        </span>
        {item ? (
          <span className="flex items-center gap-2 shrink-0">
            <Link to="/library" className="text-xs hover:underline" style={{ color: 'var(--color-text-secondary)' }}>
              {t('organization.library.view')}
            </Link>
            <button
              type="button"
              disabled={busy}
              onClick={() => remove.mutate(item.item_id, { onError: failed('organization.library.removeFailed') })}
              className="text-xs px-1.5 py-0.5 rounded-md disabled:opacity-50 hover:underline"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {t('organization.library.removeShort')}
            </button>
          </span>
        ) : data.can_publish ? (
          <button
            type="button"
            disabled={busy}
            onClick={() => publish.mutate({ target }, { onError: failed('organization.library.publishFailed') })}
            className="text-xs px-2 py-1 rounded-md shrink-0 transition-colors hover:bg-foreground/10 disabled:opacity-50"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
          >
            {publish.isPending ? t('organization.library.publishing') : t('organization.library.publish')}
          </button>
        ) : null}
      </div>
      {!item && data.can_publish && (
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{hint}</p>
      )}
      {!item && !data.can_publish && (data.workspace_rule ? (
        <WorkspaceRuleNote>
          {data.workspace_rule === 'none'
            ? t('sharedWorkspace.shareOut.libraryNone')
            : t('sharedWorkspace.shareOut.libraryManagers')}
        </WorkspaceRuleNote>
      ) : (
        <RuleNote>
          {t('organization.library.publishNotAllowed', { who: setterName(RULE_LIBRARY_PUBLISH).start })}
        </RuleNote>
      ))}
    </div>
  );
}
