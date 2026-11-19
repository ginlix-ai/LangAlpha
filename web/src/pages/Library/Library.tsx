import { useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Download, ExternalLink, FileText, MessagesSquare, RefreshCw, Sparkles, Trash2 } from 'lucide-react';
import {
  ConfirmStrip,
  HeaderButton,
  KebabTrigger,
  ListError,
  ListSkeleton,
  MetaText,
  ServerNameLine,
  ServerRowShell,
  TagBadge,
} from '@/components/mcp/McpPrimitives';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { toast } from '@/components/ui/use-toast';
import { EmptyState } from '@/pages/Plugins/components/EmptyState';
import { useAccount } from '@/hooks/useAccount';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useInstallFromLibrary, useLibrary, useRemoveFromLibrary } from '@/hooks/useOrgLibrary';
import { apiErrorObject, apiErrorStatus } from '@/api/errors';
import { relativeTime } from '@/lib/format';
import { useScrollMemory } from '@/lib/scrollMemory';
import type { LibraryItem, LibraryKind } from '@/types/orgLibrary';
import './Library.css';

type Filter = 'all' | LibraryKind;

function filterParam(raw: string | null): Filter {
  return raw === 'thread' || raw === 'file' || raw === 'skill' ? raw : 'all';
}

const KIND_ICON = { thread: MessagesSquare, file: FileText, skill: Sparkles } as const;
const KIND_LABEL = {
  thread: 'organization.library.kindThread',
  file: 'organization.library.kindFile',
  skill: 'organization.library.kindSkill',
} as const;

function LibraryRow({ item }: { item: LibraryItem }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const [confirming, setConfirming] = useState(false);
  const remove = useRemoveFromLibrary();
  const install = useInstallFromLibrary();
  const Icon = KIND_ICON[item.kind];
  const onInstall = () =>
    install.mutate(item.item_id, {
      onSuccess: () =>
        toast({
          title: item.installed
            ? t('organization.library.updatedToast', { name: item.skill_name })
            : t('organization.library.installedToast', { name: item.skill_name }),
        }),
      onError: (err) => {
        const taken = apiErrorStatus(err) === 409 && apiErrorObject(err)?.type === 'name_taken';
        toast({
          variant: 'destructive',
          title: taken
            ? t('organization.library.nameTaken', { name: item.skill_name })
            : t('organization.library.installFailed'),
        });
      },
    });
  const date = relativeTime(item.published_at, locale, now);
  const by = item.mine
    ? t('organization.library.byYou', { date })
    : item.publisher_name
      ? t('organization.library.byName', { name: item.publisher_name, date })
      : t('organization.library.byUnnamed', { date });
  const open = () => {
    if (item.url) window.open(item.url, '_blank', 'noopener');
  };
  const onRemove = () =>
    remove.mutate(item.item_id, {
      onSuccess: () => toast({ title: t('organization.library.removed') }),
      onError: () => {
        setConfirming(false);
        toast({ variant: 'destructive', title: t('organization.library.removeFailed') });
      },
    });
  return (
    <div className="flex flex-col gap-1.5">
      <ServerRowShell
        testid={`library-item-${item.item_id}`}
        tile={
          <div
            className="h-8 w-8 rounded-md flex items-center justify-center"
            style={{ backgroundColor: 'var(--color-bg-tag)' }}
          >
            <Icon aria-hidden="true" className="h-4 w-4" style={{ color: 'var(--color-icon-muted)' }} />
          </div>
        }
        onOpen={item.url ? open : undefined}
        main={
          <>
            <ServerNameLine name={item.title} onOpen={item.url ? open : undefined}>
              <TagBadge soft>{t(KIND_LABEL[item.kind])}</TagBadge>
              {item.kind !== 'skill' && item.audience === 'organization' && (
                <TagBadge soft>{t('organization.library.membersOnly')}</TagBadge>
              )}
              {item.installed && !item.update_available && (
                <TagBadge soft>{t('organization.library.installed')}</TagBadge>
              )}
            </ServerNameLine>
            {item.summary && (
              <p className="text-xs line-clamp-2" style={{ color: 'var(--color-text-secondary)' }}>
                {item.summary}
              </p>
            )}
            <MetaText>{item.name && item.name !== item.title ? `${item.name} · ${by}` : by}</MetaText>
          </>
        }
        actions={
          <>
            {item.url && (
              <HeaderButton variant="secondary" icon={ExternalLink} onClick={open}>
                {t('organization.library.open')}
              </HeaderButton>
            )}
            {item.kind === 'skill' && (!item.installed || item.update_available) && (
              <HeaderButton
                variant={item.update_available ? 'primary' : 'secondary'}
                icon={item.update_available ? RefreshCw : Download}
                onClick={onInstall}
                disabled={install.isPending}
              >
                {install.isPending
                  ? t('organization.library.installing')
                  : item.update_available
                    ? t('organization.library.update')
                    : t('organization.library.install')}
              </HeaderButton>
            )}
            {item.mine && (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <KebabTrigger
                    busy={remove.isPending}
                    aria-label={t('organization.library.actions', { title: item.title })}
                  />
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onSelect={() => setConfirming(true)} variant="destructive">
                    <Trash2 className="h-3.5 w-3.5 mr-2" />
                    {t('organization.library.remove')}
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            )}
          </>
        }
      />
      {confirming && (
        <ConfirmStrip
          message={t('organization.library.removeConfirm', { title: item.title })}
          confirmLabel={t('organization.library.remove')}
          cancelLabel={t('common.cancel')}
          pending={remove.isPending}
          onConfirm={onRemove}
          onCancel={() => setConfirming(false)}
        />
      )}
    </div>
  );
}

/**
 * /library: what members of the viewer's organization published for each
 * other, newest first. Each item opens through its own link, so who can open
 * it is whatever that link says. The route opens it only where the library is
 * on for the account; should the server turn it off while it is open, its 404
 * reads here as a list that failed to load.
 */
function Library() {
  const { t } = useTranslation();
  const { orgT } = useAccount();
  const [searchParams, setSearchParams] = useSearchParams();
  const pageRef = useRef<HTMLDivElement>(null);
  useScrollMemory(pageRef, 'page:library');
  const filter = filterParam(searchParams.get('kind'));
  const setFilter = (next: Filter) => {
    const params = new URLSearchParams(searchParams);
    if (next === 'all') params.delete('kind');
    else params.set('kind', next);
    setSearchParams(params, { replace: true });
  };
  const { data, error, isLoading, hasNextPage, isFetchingNextPage, isFetchNextPageError, fetchNextPage } =
    useLibrary(filter === 'all' ? null : filter);
  const items = useMemo(() => (data?.pages ?? []).flatMap((page) => page.items), [data]);

  return (
    <div className="library-page">
      <div className="chrome-drag-strip" aria-hidden="true" />
      <div ref={pageRef} className="library-scroll">
        <div className="library-container">
          <div className="mb-6">
            <h1
              className="font-[family-name:var(--cs-face,var(--font-ui))] text-[length:var(--cs-text-title,1.25rem)] leading-7 font-semibold tracking-[var(--cs-title-track,normal)] mb-1"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {t('organization.library.title')}
            </h1>
            <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
              {orgT('organization.library.description')}
            </p>
          </div>

          <div className="flex flex-col gap-4">
            <div>
            <SegmentedControl<Filter>
              label={t('organization.library.filterLabel')}
              value={filter}
              onChange={setFilter}
              options={[
                { value: 'all', label: t('organization.library.filterAll') },
                { value: 'thread', label: t('organization.library.filterThreads') },
                { value: 'file', label: t('organization.library.filterFiles') },
                { value: 'skill', label: t('organization.library.filterSkills') },
              ]}
            />
            </div>

            {error && items.length === 0 ? (
              <ListError>{t('organization.library.loadFailed')}</ListError>
            ) : isLoading ? (
              <ListSkeleton rows={4} />
            ) : items.length === 0 ? (
              <EmptyState
                message={filter === 'all' ? t('organization.library.empty') : t('organization.library.emptyFiltered')}
              />
            ) : (
              <>
                <div className="flex flex-col gap-2">
                  {items.map((item) => (
                    <LibraryRow key={item.item_id} item={item} />
                  ))}
                </div>
                {hasNextPage && (
                  <div className="flex flex-col items-center gap-2">
                    {isFetchNextPageError && (
                      <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t('organization.library.loadFailed')}
                      </span>
                    )}
                    <HeaderButton
                      variant="secondary"
                      onClick={() => void fetchNextPage()}
                      disabled={isFetchingNextPage}
                    >
                      {isFetchingNextPage
                        ? t('organization.library.loading')
                        : isFetchNextPageError
                          ? t('common.retry')
                          : t('organization.library.loadMore')}
                    </HeaderButton>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default Library;
