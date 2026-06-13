import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion, useReducedMotion, type Variants } from 'framer-motion';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { cn } from '@/lib/utils';
import { INTRO_VISUALS, INTRO_VISUAL_ANCHOR } from './introVisuals';
import type { PageIntroDef } from '../registry';
import './pageIntro.css';

/**
 * One-time contextual intro for the page the user just landed on. Large
 * half-screen two-panel layout: stepped copy + navigation on the left, a
 * blueprint-style product mockup on the right. Seen-state is per-intro —
 * any close path (final CTA, X, Esc) at any step marks the intro seen.
 */
export function PageIntroModal({ intro, onClose }: { intro: PageIntroDef; onClose: () => void }) {
  const { t } = useTranslation();
  const reduceMotion = useReducedMotion();
  const [stepIdx, setStepIdx] = useState(0);
  // +1 forward / -1 back — drives which side the copy slides from. Updated on
  // every navigation (dots can jump non-adjacent steps in either direction).
  const [dir, setDir] = useState(1);
  const goTo = (i: number) => {
    setDir(i > stepIdx ? 1 : -1);
    setStepIdx(Math.min(Math.max(i, 0), intro.steps.length - 1));
  };
  // Slide distance collapses to a pure (instant) crossfade under reduced motion.
  const slide = reduceMotion ? 0 : 28;
  const copyVariants: Variants = {
    enter: (d: number) => ({ opacity: 0, x: slide * d }),
    center: {
      opacity: 1,
      x: 0,
      transition: { duration: reduceMotion ? 0 : 0.3, ease: [0.16, 1, 0.3, 1] },
    },
    exit: (d: number) => ({
      opacity: 0,
      x: -slide * d,
      transition: { duration: reduceMotion ? 0 : 0.15, ease: 'easeIn' },
    }),
  };
  const step = intro.steps[Math.min(stepIdx, intro.steps.length - 1)];
  const isLast = stepIdx >= intro.steps.length - 1;
  const Visual = INTRO_VISUALS[step.visual];
  const anchor = INTRO_VISUAL_ANCHOR[step.visual];
  // When every step's mockup anchors right (e.g. the thread intro), the
  // visual takes the LEFT half: the anchored hot region then sits beside the
  // copy and the wireframe bleeds off the modal's outer edge. Per-intro, not
  // per-step, so the panel never jumps sides mid-intro.
  const visualFirst = intro.steps.every((s) => INTRO_VISUAL_ANCHOR[s.visual] === 'right');

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
      >
        <div className="grid sm:min-h-[min(580px,76vh)] sm:grid-cols-2" key={intro.id}>
          {/* Copy + step navigation */}
          <div className={cn('flex flex-col gap-4 p-6 sm:p-10', visualFirst && 'sm:order-2')}>
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
                travel is headed, the new copy follows it in. */}
            <AnimatePresence mode="wait" initial={false} custom={dir}>
              <motion.div
                key={step.id}
                custom={dir}
                variants={copyVariants}
                initial="enter"
                animate="center"
                exit="exit"
              >
                <DialogHeader className="space-y-3 text-left">
                  <DialogTitle
                    className="text-2xl font-semibold leading-tight sm:text-3xl"
                    style={{ color: 'var(--color-text-primary)' }}
                  >
                    {t(step.titleKey)}
                  </DialogTitle>
                  <DialogDescription
                    className="text-sm leading-relaxed sm:text-[15px]"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    {t(step.bodyKey)}
                  </DialogDescription>
                </DialogHeader>
              </motion.div>
            </AnimatePresence>

            <div className="mt-auto flex flex-col gap-4 pt-6">
              <div className="flex items-center gap-1.5">
                {intro.steps.map((s, i) => (
                  <button
                    key={s.id}
                    type="button"
                    aria-label={t('onboarding.step', 'Step {{n}}', { n: i + 1 })}
                    aria-current={i === stepIdx ? 'step' : undefined}
                    onClick={() => goTo(i)}
                    className={cn(
                      'h-1.5 rounded-full transition-all duration-300 hover:opacity-80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)] focus-visible:ring-offset-2',
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
                <span
                  className="ml-auto font-mono text-xs tabular-nums"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {stepIdx + 1} / {intro.steps.length}
                </span>
              </div>

              <div className="flex gap-3">
                {stepIdx > 0 && (
                  <button
                    type="button"
                    onClick={() => goTo(stepIdx - 1)}
                    className="rounded-lg border px-4 py-2.5 text-sm font-medium transition-colors hover:bg-[var(--color-bg-subtle)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]"
                    style={{
                      borderColor: 'var(--color-border-default)',
                      color: 'var(--color-text-secondary)',
                    }}
                  >
                    {t('onboarding.back', 'Back')}
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => (isLast ? onClose() : goTo(stepIdx + 1))}
                  className="flex-1 rounded-lg px-4 py-2.5 text-sm font-semibold transition-opacity hover:opacity-90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)] focus-visible:ring-offset-2"
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

          {/* Blueprint mockup panel — hidden on small screens where the copy
              needs the room. The mockup keeps its 400px design size and is
              scaled up on a static wrapper (intro-rv animates `transform`,
              so the scale cannot live on an animated element), bleeding off
              the panel's clipped edge opposite its anchor. Keyed by step:
              the outgoing scene crossfades out, then the incoming one fades
              in and its CSS stagger replays. The motion wrapper only animates
              opacity, so the scale transform stays on the static inner div. */}
          <div
            className={cn('intro-visual hidden items-center sm:flex', visualFirst && 'sm:order-1')}
            aria-hidden
            data-testid="intro-illustration"
          >
            <AnimatePresence mode="wait" initial={false}>
              <motion.div
                key={step.id}
                className="flex w-full items-center"
                initial={{ opacity: 0 }}
                animate={{
                  opacity: 1,
                  transition: { duration: reduceMotion ? 0 : 0.25, ease: 'easeOut' },
                }}
                exit={{
                  opacity: 0,
                  transition: { duration: reduceMotion ? 0 : 0.15, ease: 'easeIn' },
                }}
              >
                <div
                  className={cn(
                    'w-[400px] shrink-0',
                    anchor === 'left' && 'ml-8',
                    anchor === 'right' && 'ml-auto mr-8',
                    anchor === 'center' && 'mx-auto'
                  )}
                  style={{
                    transform: 'scale(1.45)',
                    transformOrigin:
                      anchor === 'center' ? 'center center' : `${anchor} center`,
                  }}
                >
                  <Visual />
                </div>
              </motion.div>
            </AnimatePresence>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
