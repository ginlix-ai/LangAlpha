import React, { useRef } from 'react';
import { Check, ChevronDown } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { SwapCell } from '@/components/ui/crossfade';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { useLocale } from '@/hooks/useLocale';
import { shortMonthDay } from '@/lib/format';
import type { ArtifactVersionView } from '../../filePanel/useArtifactVersionView';
import './ArtifactVersionPicker.css';

/** The radio value of the live file, which no version number can be. */
const LIVE = 'live';

interface ArtifactVersionPickerProps {
  view: ArtifactVersionView;
  /** `chip` sits in the file panel's crumb row, `bar` in the standalone view's top bar. */
  variant: 'chip' | 'bar';
}

/**
 * What is on screen, the live file or one published version, opening onto
 * the others: Live first, since that is where the owner works and comments,
 * then the versions newest first, each with the note it was published with.
 * A file never published has no versions, and no picker. While an Update has
 * just moved the view, the face says so ("Updated to v4"): the document
 * changed under the reader, and nothing else on screen would tell them.
 */
export function ArtifactVersionPicker({ view, variant }: ArtifactVersionPickerProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const { record, version, updatedTo } = view;
  const latestSeq = record?.latest_seq ?? 0;
  // Whether visitors see the latest now; an unshared file keeps its versions.
  const shared = !!record?.shared;
  const announcing = updatedTo !== null;

  // A pick closes the menu, which hands focus back to the trigger; an open
  // draft is what the reader was typing in, so it takes the focus instead.
  const picked = useRef(false);

  if (latestSeq < 1) return null;
  const label = version === null
    ? t(shared ? 'artifact.publishedVersion' : 'artifact.unsharedVersion', { seq: latestSeq })
    : announcing
      ? t('filePanel.pageUpdated', { seq: version })
      : t('filePanel.pageVersion', { seq: version });
  const name = version === null ? t('artifact.liveName') : t('filePanel.pageVersionName', { seq: version });
  const face = <SwapCell swapKey={label}>{label}</SwapCell>;
  const list = view.versions ?? [];

  return (
    <>
      {/* The chip's name is what is on screen; the notice is read out on its own. */}
      <span role="status" className="sr-only">{announcing ? label : ''}</span>
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          {variant === 'chip' ? (
            <button
              type="button"
              data-artifact-version-picker=""
              className="file-panel-port-chip artifact-version-chip"
              aria-label={name}
              title={t('filePanel.pageVersions')}
            >
              {face}
              <ChevronDown className="h-3 w-3" />
            </button>
          ) : (
            <Button
              type="button"
              data-artifact-version-picker=""
              variant="outline"
              size="sm"
              aria-label={name}
              title={t('filePanel.pageVersions')}
              className="h-[30px] gap-1 px-2 text-xs sm:px-2.5"
            >
              {face}
              <ChevronDown className="h-3.5 w-3.5" />
            </Button>
          )}
        </DropdownMenuTrigger>
        {/* Marked, trigger and menu both, so a comment's box open over the
            live file stays open through a pick. */}
        <DropdownMenuContent
          data-artifact-version-picker=""
          align={variant === 'chip' ? 'start' : 'end'}
          className="min-w-56 max-w-72"
          onCloseAutoFocus={(e) => {
            if (!picked.current) return;
            picked.current = false;
            const field = document.querySelector<HTMLTextAreaElement>('textarea[data-page-draft]');
            if (!field) return;
            e.preventDefault();
            field.focus({ preventScroll: true });
            field.setSelectionRange(field.value.length, field.value.length);
          }}
        >
          <DropdownMenuRadioGroup
            value={version === null ? LIVE : String(version)}
            onValueChange={(v) => {
              picked.current = true;
              view.pick(v === LIVE ? null : Number(v));
            }}
          >
            <DropdownMenuRadioItem value={LIVE} className="items-start">
              <Check className="mt-0.5 h-3.5 w-3.5 shrink-0" style={{ opacity: version === null ? 1 : 0 }} aria-hidden="true" />
              <span className="flex min-w-0 flex-col">
                <span>{t('artifact.live')}</span>
                <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('artifact.liveHint')}</span>
              </span>
            </DropdownMenuRadioItem>
            {list.length > 0 && <DropdownMenuSeparator />}
            {list.map((v) => (
              <DropdownMenuRadioItem key={v.seq} value={String(v.seq)} className="items-start">
                <Check className="mt-0.5 h-3.5 w-3.5 shrink-0" style={{ opacity: v.seq === version ? 1 : 0 }} aria-hidden="true" />
                <span className="flex min-w-0 flex-1 flex-col">
                  <span className="flex items-center gap-1.5">
                    <span>{t('filePanel.pageVersion', { seq: v.seq })}</span>
                    {v.seq === latestSeq && (
                      <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('filePanel.pageLatest')}</span>
                    )}
                    <span className="ml-auto pl-2 text-xs tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
                      {shortMonthDay(new Date(v.published_at), locale)}
                    </span>
                  </span>
                  {v.label && (
                    <span className="truncate text-xs" style={{ color: 'var(--color-text-tertiary)' }} title={v.label}>{v.label}</span>
                  )}
                </span>
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
        </DropdownMenuContent>
      </DropdownMenu>
    </>
  );
}
