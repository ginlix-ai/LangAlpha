import { useId, useLayoutEffect, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Check, ChevronDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { AUTO_APPROVE, useApproveAlways } from '@/hooks/useAutoApprove';
import Markdown from '../Markdown';
import { RunGlyph } from '../messageList/runCard';
import { AnalystMark } from './AnalystMark';
import { plainBrief, type ProposalData } from './handoffModel';

/** A card, since it asks the reader to decide rather than reports a run. */
const RUN_CARD =
  'overflow-hidden rounded-xl border border-(--color-border-default) bg-(--color-bg-card)';

/** The footer's answers: 28px pills in the app's button colors. */
const ANSWER = 'h-7 cursor-pointer rounded-full px-[13px] text-[0.78125rem] font-medium disabled:cursor-default';

interface HandoffApprovalProps {
  proposal: ProposalData;
  onApprove?: (overrides?: { report_back?: boolean }) => void;
  onReject?: () => void;
}

/**
 * A hand-off the Chief of Staff asks to make. The brief is written for an
 * analyst who sees nothing else, so it runs long: two lines say what it is,
 * and the rest opens in place for whoever wants to check it before handing
 * off. Once answered, the card gives way to the slip or the declined row.
 */
export function HandoffApproval({ proposal, onApprove, onReject }: HandoffApprovalProps) {
  const { t } = useTranslation();
  const [reportBack, setReportBack] = useState(proposal.report_back ?? true);
  const [expanded, setExpanded] = useState(false);
  const { approveAlways, saving } = useApproveAlways(AUTO_APPROVE.handoffs);
  const briefId = useId();

  const brief = plainBrief(proposal.question);
  const clampRef = useRef<HTMLParagraphElement>(null);
  const [clamped, setClamped] = useState(false);
  // Measured on the clamped line itself, as it rewraps with the column. While
  // the brief is open the clamp is not drawn, so the last reading holds and
  // the control that closes it stays.
  useLayoutEffect(() => {
    const el = clampRef.current;
    if (!el) return;
    const measure = () => setClamped(el.scrollHeight > el.clientHeight + 1);
    measure();
    if (typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [expanded, brief]);

  const name = proposal.workspace_name?.trim();
  const approve = () => onApprove?.({ report_back: reportBack });

  return (
    <div data-handoff-approval className={cn(RUN_CARD, '@container')}>
      <div className="grid grid-cols-[28px_minmax(0,1fr)_auto] items-start gap-x-3 pt-3.5 pr-3.5 pl-3">
        <AnalystMark />
        <div className="min-w-0 pb-3.5">
          {/* As tall as the mark, so one line sits level with it; narrow,
              the workspace wraps under the words rather than be cut. */}
          <p className="flex min-h-7 items-center text-[0.84375rem] leading-[1.35] font-medium text-(--color-text-primary)">
            <span className="min-w-0">
              {name ? (
                <Trans
                  i18nKey="chat.ptcCard.handOffTo"
                  values={{ name }}
                  components={{ ws: <span className="font-normal text-(--color-text-secondary)" /> }}
                />
              ) : (
                t('chat.ptcCard.handOffToAnalyst')
              )}
            </span>
          </p>
          {expanded ? (
            <div id={briefId} className="mt-0.5 text-(--color-text-secondary)">
              <Markdown variant="compact" content={proposal.question} className="text-[0.8125rem] leading-relaxed" />
            </div>
          ) : (
            <p id={briefId} ref={clampRef} className="mt-0.5 line-clamp-2 text-[0.8125rem] leading-[1.55] text-(--color-text-secondary)">
              {brief}
            </p>
          )}
          {(clamped || expanded) && (
            <button
              type="button"
              aria-expanded={expanded}
              aria-controls={briefId}
              onClick={() => setExpanded((open) => !open)}
              className="mt-1.5 inline-flex cursor-pointer items-center gap-[3px] rounded-sm text-xs font-medium text-(--color-text-primary) underline-offset-2 hover:underline"
            >
              {expanded ? t('chat.ptcCard.briefLess') : t('chat.ptcCard.briefMore')}
              <ChevronDown aria-hidden className={cn('h-3 w-3 transition-transform motion-reduce:transition-none', expanded && 'rotate-180')} />
            </button>
          )}
        </div>
        <span className="flex h-7 items-center gap-1.5 text-xs text-(--color-text-secondary)">
          <RunGlyph kind="pending" />
          <span className="@max-md:sr-only">{t('chat.ptcCard.waitingForYou')}</span>
        </span>
      </div>

      {/* Narrow, the answers wrap under the checkbox and keep to the right. */}
      <div className="flex flex-wrap items-center gap-x-1.5 gap-y-2 border-t border-(--color-border-subtle) bg-(--color-bg-subtle) py-[9px] pr-3 pl-3.5">
        {/* Locked while "Always approve" saves: its approval already holds this choice. */}
        <label className={cn('mr-auto flex items-center gap-2 text-[0.78125rem] text-(--color-text-primary)', saving ? 'cursor-default opacity-60' : 'cursor-pointer')}>
          <input type="checkbox" className="peer sr-only" checked={reportBack} disabled={saving} onChange={(e) => setReportBack(e.target.checked)} />
          <span
            aria-hidden
            className={cn(
              'flex h-[15px] w-[15px] shrink-0 items-center justify-center rounded-[4px] border transition-colors',
              'peer-focus-visible:outline-2 peer-focus-visible:outline-offset-2 peer-focus-visible:outline-(--color-focus-ring)',
              reportBack
                ? 'border-(--color-btn-primary-bg) bg-(--color-btn-primary-bg) text-(--color-btn-primary-text)'
                : 'border-(--color-border-strong) bg-(--color-bg-card)',
            )}
          >
            {reportBack && <Check className="h-[11px] w-[11px] stroke-[2.5]" />}
          </span>
          {t('chat.ptcCard.reportBack')}
        </label>
        <span className="ml-auto flex items-center gap-1.5">
          <Button
            type="button"
            variant="ghost"
            disabled={saving}
            onClick={() => onReject?.()}
            className={cn(ANSWER, 'px-2.5 text-(--color-text-secondary) hover:bg-transparent hover:text-(--color-text-primary)')}
          >
            {t('chat.ptcCard.decline')}
          </Button>
          <Button
            type="button"
            variant="outline"
            disabled={saving}
            title={t('chat.ptcCard.alwaysApproveHint')}
            onClick={() => approveAlways(approve)}
            className={cn(ANSWER, 'border-(--color-border-default) bg-(--color-bg-card) text-(--color-text-primary)')}
          >
            {t('chat.ptcCard.alwaysApprove')}
          </Button>
          <Button type="button" disabled={saving} onClick={approve} className={ANSWER}>
            {t('chat.ptcCard.handOff')}
          </Button>
        </span>
      </div>
    </div>
  );
}
