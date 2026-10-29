/**
 * The frame of a full-screen, step-by-step flow: a top bar (wordmark, progress,
 * the way out), one centred column that scrolls, and a footer that stays put.
 *
 * Nothing of the app's chrome renders around it, so the top bar is the window's
 * titlebar on the macOS desktop shell: it carries the drag and clears the
 * window buttons (see `.wizard-topbar` in wizard.css).
 */
import { useEffect, useId, useLayoutEffect, useRef, useState, type ReactNode, type Ref, type RefObject } from 'react';
import { Check } from 'lucide-react';
import { LanguageToggle } from '@/components/LanguageToggle';
import { Loader } from '@/components/ui/loader';
import { AnimatePresence, motion, useIsPresent, useReducedMotion, type Variants } from '@/lib/framer';
import { DURATION, EASE_REVEAL, SPRING_INDICATOR, TWEEN } from '@/lib/motion';
import { useTheme } from '@/contexts/ThemeContext';
import { cn } from '@/lib/utils';
import logoLight from '@/assets/img/logo.svg';
import logoDark from '@/assets/img/logo-dark.svg';
import './wizard.css';

interface WizardShellProps {
  /** Centre of the top bar. */
  progress: ReactNode;
  /** Right of the top bar: the quiet way out. */
  exit?: ReactNode;
  /** The footer's content: the bar is always drawn. */
  footer: ReactNode;
  /** Changes with each step, so the body enters afresh. */
  stepKey: string;
  /** Where the step sits in the flow: a step further on comes in from the
   *  right, one gone back to from the left. */
  stepIndex: number;
  children: ReactNode;
  testId?: string;
}

export function WizardShell({ progress, exit, footer, stepKey, stepIndex, children, testId }: WizardShellProps) {
  const scroller = useRef<HTMLDivElement>(null);
  const body = useRef<HTMLDivElement>(null);
  const lift = useOpticalLift(scroller, body, stepKey);
  const direction = useStepDirection(stepKey, stepIndex);
  const still = useReducedMotion() ?? false;
  // Handed to the step leaving as well as the one arriving, so both read the
  // move that is happening now rather than the one that brought them in.
  const custom: StepCustom = { direction, still };

  // A new step starts at its top, wherever the last one was scrolled to.
  useEffect(() => {
    if (scroller.current) scroller.current.scrollTop = 0;
  }, [stepKey]);

  return (
    <div data-testid={testId} className="wizard-shell fixed inset-0 flex flex-col bg-(--color-bg-page)">
      <WizardTopBar progress={progress} exit={exit} />
      {/* Positioned, so a step on its way out is pinned to the scrolling area
          rather than the column: the column moves when the next step lowers
          or raises it (useOpticalLift) and widens or narrows with it, and the
          step leaving stays exactly where it was read. Clipped across, since a
          step arriving from the side starts a little past the edge on a
          phone, and that must not flash a sideways scrollbar. */}
      <div ref={scroller} className="relative min-h-0 flex-1 overflow-y-auto overflow-x-hidden overscroll-contain">
        {/* As wide as the widest step; each step sets its own width inside. */}
        <div
          ref={body}
          style={{ marginTop: lift }}
          className="mx-auto w-full max-w-[1080px] px-4 pb-10 pt-6 sm:px-6 sm:pt-10"
        >
          {/* popLayout: the step leaving is lifted out of the flow where it
              stands, so the next one takes its place at once rather than
              after it, and never lands under it. */}
          <AnimatePresence mode="popLayout" custom={custom}>
            <StepFrame
              key={stepKey}
              custom={custom}
              // The footer's column, 600px less its padding, so a step's right
              // edge and the button under it line up.
              className="mx-auto w-full max-w-[568px] sm:max-w-[552px]"
            >
              {children}
            </StepFrame>
          </AnimatePresence>
        </div>
      </div>
      <footer className="wizard-footer shrink-0 border-t border-(--color-border-muted) bg-(--color-bg-page)">
        <div className="mx-auto w-full max-w-[600px] px-4 py-3 sm:px-6">{footer}</div>
      </footer>
    </div>
  );
}

/**
 * The top bar of a full-screen flow: the wordmark, what sits in the middle
 * (a step flow's progress), the language, and the way out. It is the window's
 * titlebar on the macOS desktop shell (see `.wizard-topbar` in wizard.css), so
 * it carries the drag.
 *
 * Every flow drawn in it is one a user meets first (setup, the welcome), so
 * the language is here on every screen of them: what the browser or system
 * asked for is the default, and the first screen is where a wrong guess is
 * put right.
 */
export function WizardTopBar({ progress, exit }: { progress?: ReactNode; exit?: ReactNode }) {
  const { theme } = useTheme();
  const logo = theme === 'light' ? logoDark : logoLight;
  return (
    <header
      data-chrome="drag"
      className="wizard-topbar grid h-14 shrink-0 grid-cols-[1fr_auto_1fr] items-center gap-3 px-4 sm:px-6"
    >
      <div className="flex min-w-0 items-center gap-2 justify-self-start">
        <img src={logo} alt="" className="h-6 w-6 shrink-0 object-contain" />
        <span className="title-font truncate text-[0.9375rem] font-semibold text-(--color-text-primary)">
          LangAlpha
        </span>
      </div>
      <div className="justify-self-center">{progress}</div>
      <div className="flex items-center gap-1 justify-self-end">
        <LanguageToggle />
        {exit}
      </div>
    </header>
  );
}

/** Forward, back, or neither: the step the flow opens on. */
type Direction = 1 | -1 | 0;

/** Far enough to read as a direction, short of a slide across the screen. */
const STEP_SHIFT = 24;

/**
 * Steps hand over along the line the progress rail draws: the one leaving
 * fades as it moves off the way the user is going, and the next comes in from
 * that side once it is nearly gone, so the two are never read over each other.
 * The step a flow opens on has no direction to tell and rises into place.
 * Exits are shorter than entrances, as everywhere (lib/motion.ts), and the
 * next step waits half the exit, by when the one leaving is nearly gone.
 */
const STEP_HANDOVER = DURATION.exit / 2;

const STEP_MOTION: Variants = {
  enter: ({ direction, still }: StepCustom) =>
    still ? { opacity: 0 } : direction === 0 ? { opacity: 0, y: 8 } : { opacity: 0, x: direction * STEP_SHIFT },
  shown: {
    opacity: 1,
    x: 0,
    y: 0,
    transition: {
      default: { ...TWEEN.enter, delay: STEP_HANDOVER },
      opacity: { delay: STEP_HANDOVER, duration: DURATION.fold, ease: EASE_REVEAL },
    },
  },
  leave: ({ direction, still }: StepCustom) => ({
    opacity: 0,
    x: still ? 0 : direction * -(STEP_SHIFT / 2),
    transition: TWEEN.exit,
  }),
};

interface StepCustom {
  direction: Direction;
  /** Reduced motion: the steps cross-fade in place. */
  still: boolean;
}

function StepFrame({
  ref,
  custom,
  className,
  children,
}: {
  /** From AnimatePresence, which measures the step to lift it out as it leaves. */
  ref?: Ref<HTMLDivElement>;
  custom: StepCustom;
  className: string;
  children: ReactNode;
}) {
  const present = useIsPresent();
  return (
    <motion.div
      ref={ref}
      custom={custom}
      variants={STEP_MOTION}
      initial="enter"
      animate="shown"
      exit="leave"
      className={className}
    >
      {/* A step on its way out is a picture of itself: nothing in it can be
          clicked, focused or read out while the next one takes its place. */}
      <div inert={!present} aria-hidden={present ? undefined : true} className="wizard-step">
        {children}
      </div>
    </motion.div>
  );
}

/** Which way the flow last moved, read off the step's place in it. */
function useStepDirection(stepKey: string, stepIndex: number): Direction {
  const [seen, setSeen] = useState<{ key: string; index: number; direction: Direction }>({
    key: stepKey,
    index: stepIndex,
    direction: 0,
  });
  if (seen.key !== stepKey) {
    setSeen({ key: stepKey, index: stepIndex, direction: stepIndex < seen.index ? -1 : 1 });
  }
  return seen.direction;
}

/** Of the room a short step leaves, the share that goes above it: a little
 *  under half, so the step sits at the optical centre rather than the true one,
 *  which reads as low. */
const LIFT_SHARE = 0.4;

/**
 * How far to lower a step that does not fill the window, so a short one is not
 * a band across the top of an empty screen. Measured as the step arrives, and
 * again while what it shows is still loading in; held from the first click or
 * key on, so a fold the user opens grows down from where they are reading
 * rather than pushing the whole step up under the pointer. Resizing the window
 * measures afresh, since the room itself has changed.
 */
function useOpticalLift(
  scroller: RefObject<HTMLDivElement | null>,
  body: RefObject<HTMLDivElement | null>,
  stepKey: string,
): number {
  const [lift, setLift] = useState(0);
  useLayoutEffect(() => {
    const outer = scroller.current;
    const inner = body.current;
    if (!outer || !inner) return;
    let held = false;
    const measure = () => {
      setLift(Math.max(0, Math.round((outer.clientHeight - inner.offsetHeight) * LIFT_SHARE)));
    };
    measure();
    const hold = () => {
      held = true;
    };
    outer.addEventListener('pointerdown', hold);
    outer.addEventListener('keydown', hold);
    // jsdom has no ResizeObserver; the first measure above is all it needs.
    const observer =
      typeof ResizeObserver === 'undefined'
        ? null
        : new ResizeObserver((entries) => {
            if (!held || entries.some((e) => e.target === outer)) measure();
          });
    observer?.observe(outer);
    observer?.observe(inner);
    return () => {
      outer.removeEventListener('pointerdown', hold);
      outer.removeEventListener('keydown', hold);
      observer?.disconnect();
    };
  }, [scroller, body, stepKey]);
  return lift;
}

interface WizardProgressProps {
  /** Every step's short name, in order. */
  steps: readonly string[];
  /** Index into `steps`. */
  current: number;
  /** Names the list for a screen reader ("Progress"). */
  label: string;
}

/**
 * Where the user is: every step by name from 640px up, a bare "2 / 5" on a
 * phone. Steps behind are marked done, the current one carries the accent
 * dot. The phone's count stays in the accessible tree: the list is not
 * rendered there, and no step's header says where it sits, so the count is
 * the only place a screen reader learns the position.
 *
 * The dot travels to the step the user moves to, and a step left behind
 * answered takes its check as the dot leaves it.
 */
export function WizardProgress({ steps, current, label }: WizardProgressProps) {
  const dot = useId();
  return (
    <>
      <ol aria-label={label} className="hidden items-center gap-2 sm:flex">
        {steps.map((name, i) => {
          const state = i === current ? 'current' : i > current ? 'upcoming' : 'done';
          return (
            <li key={name} aria-current={state === 'current' ? 'step' : undefined} className="flex items-center gap-2">
              {i > 0 && <span aria-hidden="true" className="h-px w-4 bg-(--color-border-default)" />}
              <span
                className={cn(
                  'flex items-center gap-1.5 whitespace-nowrap text-xs transition-colors',
                  state === 'current' && 'font-medium text-(--color-text-primary)',
                  state === 'done' && 'text-(--color-text-secondary)',
                  state === 'upcoming' && 'text-(--color-text-tertiary)',
                )}
              >
                <StepMark state={state} dot={dot} />
                <span>{name}</span>
              </span>
            </li>
          );
        })}
      </ol>
      <span
        data-testid="wizard-progress-count"
        className="text-xs tabular-nums text-(--color-text-secondary) sm:hidden"
      >
        <span key={current} className="wizard-fade-in inline-block">
          {current + 1}
        </span>
        <span> / {steps.length}</span>
      </span>
    </>
  );
}

/** `dot` names the one accent dot, so it moves rather than blinks. */
function StepMark({ state, dot }: { state: 'done' | 'current' | 'upcoming'; dot: string }) {
  if (state === 'done') {
    return (
      <Check aria-hidden="true" className="wizard-mark-in h-3 w-3 text-(--color-text-tertiary)" strokeWidth={2.5} />
    );
  }
  if (state === 'current') {
    return (
      <motion.span
        layoutId={dot}
        transition={SPRING_INDICATOR}
        aria-hidden="true"
        className="h-1.5 w-1.5 rounded-full bg-(--color-accent-primary)"
      />
    );
  }
  return <span aria-hidden="true" className="h-1.5 w-1.5 rounded-full border border-(--color-border-default)" />;
}

/** The top bar's quiet way out: a text button, never a boxed one, since
 *  leaving is not what the screen is for. */
export function WizardExit({ label, onClick, testId }: { label: string; onClick: () => void; testId?: string }) {
  return (
    <button
      type="button"
      onClick={onClick}
      data-testid={testId}
      className="inline-flex h-8 items-center rounded-md px-2.5 text-sm text-(--color-text-secondary) press hover:bg-(--color-bg-hover) hover:text-(--color-text-primary)"
    >
      {label}
    </button>
  );
}

interface WizardStepHeaderProps {
  /** Text, or text with a mark beside it (a provider's step). */
  title: ReactNode;
  /** id for the title, so the step's control can be named by it. */
  titleId: string;
  /** One line under the question; inline content only, as it sits in a <p>. */
  helper?: ReactNode;
}

/**
 * The step's question and one line of help. The question takes focus
 * when the step changes, so a screen reader announces the new step and Tab
 * starts from its top rather than from the button that was just pressed.
 */
export function WizardStepHeader({ title, titleId, helper }: WizardStepHeaderProps) {
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => {
    heading.current?.focus({ preventScroll: true });
  }, []);
  return (
    <div className="mb-6 sm:mb-8">
      <h1
        ref={heading}
        id={titleId}
        tabIndex={-1}
        className="title-font text-balance text-2xl font-semibold leading-tight text-(--color-text-primary) outline-none sm:text-[1.875rem]"
      >
        {title}
      </h1>
      {helper && <p className="mt-2 text-sm text-(--color-text-secondary)">{helper}</p>}
    </div>
  );
}

/** A labelled block inside a step: a second question under the first. */
export function WizardSection({
  title,
  titleId,
  hint,
  children,
  className,
}: {
  title: string;
  titleId: string;
  /** Beside the title, quieter ("Optional"). */
  hint?: string;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={cn('mt-8', className)}>
      <h2 id={titleId} className="mb-3 text-sm font-medium text-(--color-text-primary)">
        {title}
        {hint && <span className="ml-2 font-normal text-(--color-text-tertiary)">{hint}</span>}
      </h2>
      {children}
    </section>
  );
}

interface FooterAction {
  label: string;
  onClick: () => void;
}

export interface WizardFooterProps {
  back?: FooterAction | null;
  /** For a step that can be passed over. */
  skip?: FooterAction | null;
  /** None for a step whose own content is the way forward. */
  primary?: (FooterAction & { disabled?: boolean; busy?: boolean }) | null;
}

// Every footer button gives a little under a press, so a tap that lands is
// felt before the next screen arrives; never when it is disabled, since that
// press does nothing.
const ghost =
  'inline-flex h-10 items-center justify-center rounded-md px-3 text-sm font-medium text-(--color-text-secondary) press hover:bg-(--color-bg-hover) hover:text-(--color-text-primary)';

/**
 * Back on the left, the way forward on the right, in the same place on every
 * step whatever the step shows. On a phone the primary
 * button takes the full width on its own row and the other two share the row
 * under it, so the button most often pressed sits nearest the thumb's reach
 * without stacking three rows of buttons.
 */
export function WizardFooter({ back, skip, primary }: WizardFooterProps) {
  const disabled = primary?.disabled || primary?.busy;
  return (
    <div className="flex flex-col-reverse gap-1 sm:flex-row sm:items-center sm:gap-3">
      {/* Held at a button's height on a phone when it is empty (the first
          step has no Back), so the primary button over it does not drop. */}
      <div className="flex min-h-10 items-center justify-between sm:contents">
        {back ? (
          <button type="button" onClick={back.onClick} className={cn(ghost, '-ml-3 sm:mr-auto')}>
            {back.label}
          </button>
        ) : (
          <span className="sm:mr-auto" />
        )}
        {skip && (
          <button type="button" onClick={skip.onClick} className={cn(ghost, '-mr-3 sm:mr-0')}>
            {skip.label}
          </button>
        )}
      </div>
      {primary && (
        <button
          type="button"
          onClick={primary.onClick}
          disabled={disabled}
          aria-busy={primary.busy || undefined}
          className="inline-flex h-11 w-full items-center justify-center rounded-md bg-(--color-btn-primary-bg) px-5 text-sm font-medium text-(--color-btn-primary-text) press hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40 sm:h-10 sm:w-auto sm:min-w-[7.5rem]"
        >
          {/* Live work: the shared glyph beside the label, never in its place. */}
          {primary.busy && (
            <span aria-hidden="true" className="wizard-fade-in mr-1.5 shrink-0">
              <Loader size={16} className="text-current" />
            </span>
          )}
          {primary.label}
        </button>
      )}
    </div>
  );
}
