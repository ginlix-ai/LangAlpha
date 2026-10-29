import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { SwapCell } from '@/components/ui/crossfade';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { DURATION } from '@/lib/motion';
import { cn } from '@/lib/utils';
import { SceneStage } from './introScenes/SceneStage';
import { useIntro } from './loadIntro';
import type { PageIntroDef } from '../registry';
import './pageIntro.css';

// The intro is a story told a page at a time, so a page turns out and then in
// rather than crossfading: what arrives waits for what leaves to be gone, and
// takes the entrance beat. Still one grid cell, so nothing jumps between them.
const PAGE_TURN = { enterDelay: DURATION.exit, enterDuration: DURATION.enter };

/**
 * One-time contextual intro for the page the user just landed on. Two
 * panels: stepped copy + navigation in the narrower one, a blueprint-style
 * product mockup in the wider one. Seen-state is per-intro — any close path
 * (final CTA, X, Esc) at any step marks the intro seen. `onAbandon` reports
 * an intro that could not show, which stays unseen.
 */
export function PageIntroModal({
  intro,
  onClose,
  onAbandon,
}: {
  intro: PageIntroDef;
  onClose: () => void;
  onAbandon: () => void;
}) {
  const { t } = useTranslation();
  // Suspends until this intro's copy and scenes are in, so the modal opens
  // whole and the copy below is already there.
  const scenes = useIntro(intro);
  const { t: tCopy } = useTranslation('intros');
  const [stepIdx, setStepIdx] = useState(0);
  // +1 forward / -1 back — drives which side the copy slides from. Updated on
  // every navigation (dots can jump non-adjacent steps in either direction).
  const [dir, setDir] = useState(1);
  // Opening focus goes to the primary button, so Enter walks the tour.
  const primaryRef = useRef<HTMLButtonElement>(null);
  const goTo = (i: number) => {
    setDir(i > stepIdx ? 1 : -1);
    setStepIdx(Math.min(Math.max(i, 0), intro.steps.length - 1));
  };
  const shown = Math.min(stepIdx, intro.steps.length - 1);
  const step = intro.steps[shown];
  const isLast = stepIdx >= intro.steps.length - 1;

  // The copy did not load, as when a deploy retired its chunk. Show nothing
  // rather than raw keys, and hand the screen back: the intro stays unseen
  // and opens after the reload the stale-build notice offers.
  const abandoned = scenes === null;
  useEffect(() => {
    if (abandoned) onAbandon();
  }, [abandoned, onAbandon]);
  if (abandoned) return null;

  const scene = scenes[shown];
  // When every step's mockup anchors right (e.g. the thread intro), the
  // visual takes the LEFT half: the anchored hot region then sits beside the
  // copy and the wireframe bleeds off the modal's outer edge. Per-intro, not
  // per-step, so the panel never jumps sides mid-intro.
  const visualFirst = scenes.every((s) => s?.anchor === 'right');

  return (
    <Dialog
      open
      onOpenChange={(next) => {
        if (!next) onClose();
      }}
    >
      <DialogContent
        variant="centered"
        className="intro-dialog w-[min(94vw,980px)] max-w-none gap-0 overflow-hidden p-0"
        onOpenAutoFocus={(e) => {
          e.preventDefault();
          primaryRef.current?.focus();
        }}
      >
        <div
          className={cn(
            'grid sm:min-h-[min(500px,72vh)]',
            visualFirst ? 'sm:grid-cols-[7fr_5fr]' : 'sm:grid-cols-[5fr_7fr]'
          )}
          key={intro.id}
        >
          {/* Copy + step navigation */}
          <div className={cn('flex flex-col gap-4 p-6 sm:p-9', visualFirst && 'sm:order-2')}>
            <span
              className="w-fit rounded-full px-2.5 py-0.5 text-xs font-medium"
              style={{
                backgroundColor: 'color-mix(in srgb, var(--color-accent-primary) 12%, transparent)',
                color: 'var(--color-accent-primary)',
              }}
            >
              {t('onboarding.badge', 'Quick tour')}
            </span>

            {/* Directional slide-fade per stage: the old copy eases out the way
                travel is headed, then the new copy follows it in. */}
            <SwapCell as="div" className="items-start" swapKey={step.id} shift={{ x: 28 * dir }} {...PAGE_TURN}>
              <DialogHeader className="space-y-3 text-left">
                <DialogTitle
                  className="text-2xl font-semibold leading-tight sm:text-[1.75rem]"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {tCopy(`${intro.id}.${step.id}.title`)}
                </DialogTitle>
                <DialogDescription
                  className="text-sm leading-relaxed sm:text-[0.9375rem]"
                  style={{ color: 'var(--color-text-secondary)' }}
                >
                  {tCopy(`${intro.id}.${step.id}.body`)}
                </DialogDescription>
              </DialogHeader>
            </SwapCell>

            <div className="mt-auto flex items-center gap-3 pt-6">
              <div className="flex items-center gap-1.5">
                {intro.steps.map((s, i) => (
                  <button
                    key={s.id}
                    type="button"
                    aria-label={t('onboarding.step', 'Step {{n}}', { n: i + 1 })}
                    aria-current={i === stepIdx ? 'step' : undefined}
                    onClick={() => goTo(i)}
                    className={cn(
                      'h-1.5 rounded-full transition-all duration-fold hover:opacity-80 focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2',
                      i === stepIdx ? 'w-6' : 'w-1.5'
                    )}
                    style={{
                      background:
                        i === stepIdx
                          ? 'var(--color-accent-primary)'
                          : 'var(--color-border-default)',
                    }}
                  />
                ))}
              </div>

              <div className="ml-auto flex gap-2">
                {stepIdx > 0 && (
                  <button
                    type="button"
                    onClick={() => goTo(stepIdx - 1)}
                    className="rounded-lg px-3.5 py-2 text-sm font-medium transition-colors hover:bg-(--color-bg-subtle) focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    {t('onboarding.back', 'Back')}
                  </button>
                )}
                <button
                  ref={primaryRef}
                  type="button"
                  onClick={() => (isLast ? onClose() : goTo(stepIdx + 1))}
                  className="rounded-lg px-5 py-2 text-sm font-semibold transition-opacity hover:opacity-90 focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                  style={{
                    background: 'var(--color-accent-primary)',
                    color: 'var(--color-text-on-accent)',
                  }}
                >
                  {isLast
                    ? t('onboarding.done', 'Start exploring')
                    : t('onboarding.next', 'Continue')}
                </button>
              </div>
            </div>
          </div>

          {/* Blueprint mockup panel, hidden on small screens where the copy
              needs the room. Keyed by step: the outgoing scene fades out,
              then the incoming one fades in and its script starts from the
              top. The swap only animates opacity, so the stage's scale
              transform is left alone. */}
          <div
            className={cn('intro-visual hidden sm:block', visualFirst && 'sm:order-1')}
            aria-hidden
            data-testid="intro-illustration"
          >
            <SwapCell
              as="div"
              className="h-full items-center"
              swapKey={step.id}
              layerClassName="flex w-full items-center"
              {...PAGE_TURN}
            >
              {scene && <SceneStage scene={scene} />}
            </SwapCell>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
