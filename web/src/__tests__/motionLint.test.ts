import { resolve } from 'node:path';
import { Linter } from 'eslint';
import { beforeAll, describe, expect, it } from 'vitest';

/**
 * The motion rules are `no-restricted-syntax` selectors in eslint.config.js,
 * and a selector that matches nothing passes every file, so this runs the
 * shipped config over fixtures: what each selector must flag, and what it
 * must leave alone. Selectors were once written as "any literal under a
 * `duration` key", which flagged `DURATION.fold * 2` and a debounce's
 * `{ delay: 300 }` while a named curve, a stagger or a `${n}ms` slipped by.
 *
 * It runs under jsdom, not the node pragma: loading the config loads
 * @eslint-react, which reads `navigator` as it imports.
 */

const WEB = resolve(__dirname, '../..');

const KINDS: [RegExp, string][] = [
  [/^A literal duration/, 'duration'],
  [/^A literal spring/, 'spring'],
  [/^A literal curve/, 'curve'],
  [/^A literal timing in a style/, 'style'],
  [/^A literal timing class/, 'class'],
];

let linter: Linter;
let config: Linter.Config[];

beforeAll(async () => {
  // A computed specifier, so tsc does not look for a declaration of the .js.
  const configPath = resolve(WEB, 'eslint.config.js');
  config = (await import(/* @vite-ignore */ configPath)).default;
  linter = new Linter({ cwd: WEB });
});

/** The motion findings in `code`, as the kind of each, in source order. */
function flagged(code: string, filename = 'src/pages/Probe.tsx'): string[] {
  const messages = linter.verify(code, config, { filename });
  // A fixture that does not parse reports nothing else, so it would pass.
  const broken = messages.find((m) => m.fatal);
  if (broken) throw new Error(`fixture does not parse: ${broken.message}\n${code}`);
  return messages
    .filter((m) => m.ruleId === 'no-restricted-syntax')
    .map((m) => KINDS.find(([re]) => re.test(m.message))?.[1] ?? `unknown: ${m.message}`);
}

describe('motion lint: what fails', () => {
  describe('a named or literal curve', () => {
    it.each(['easeIn', 'easeOut', 'easeInOut', 'linear', 'anticipate', 'circOut', 'backInOut'])(
      'flags ease: %s',
      (name) => {
        expect(flagged(`const t = { duration: DURATION.fold, ease: '${name}' };`)).toEqual(['curve']);
      },
    );

    it.each([
      ['a bezier array', 'const t = { ease: [0.16, 1, 0.3, 1] };'],
      ['a bezier array on one branch', 'const t = { ease: open ? EASE_OUT : [0.4, 0, 0.2, 1] };'],
      ['a cubic-bezier string', "const t = { ease: 'cubic-bezier(0.4, 0, 0.2, 1)' };"],
      ['a WAAPI easing name', "el.animate(frames, { easing: 'ease-out' });"],
      ['a curve constant', 'const EASE = [0.4, 0, 0.2, 1] as const;'],
    ])('flags %s', (_, code) => {
      expect(flagged(code)).toEqual(['curve']);
    });
  });

  describe('a stagger or a repeat delay', () => {
    it.each([
      ['staggerChildren', 'const v = { show: { transition: { staggerChildren: 0.07 } } };'],
      ['delayChildren', 'const v = { show: { transition: { delayChildren: 0.04 } } };'],
      ['repeatDelay', 'const t = { repeat: Infinity, repeatDelay: 1 };'],
    ])('flags a literal %s', (_, code) => {
      expect(flagged(code)).toEqual(['duration']);
    });
  });

  describe('a duration or a delay in a framer context', () => {
    it.each([
      ['a transition prop', 'const a = <motion.div transition={{ duration: 0.3 }} />;'],
      ['a transition key', 'const a = <motion.div exit={{ opacity: 0, transition: { delay: 0.1 } }} />;'],
      ['an animate() call', 'animate(el, { x: 1 }, { duration: 0.3 });'],
      ['a WAAPI animate() call', 'el.animate(frames, { duration: 300 });'],
      ['an object typed as a Transition', 'const T: Transition = { duration: 0.2 };'],
      ['an object that satisfies a Transition', 'const T = { duration: 0.2 } satisfies Transition;'],
      ['a function returning a Transition', 'const fade = (): Transition => ({ delay: 0.2 });'],
      ['a constant named for a transition', 'const FADE_TRANSITION = { duration: 0.2 };'],
      ['a spread of a tween', 'const t = { ...TWEEN.quick, delay: 0.1 };'],
      ['a spread of a spring', 'const t = { ...SPRING_FOLD, delay: 0.1 };'],
      ['an object with a curve beside it', 'const t = { duration: 0.4, ease: EASE_OUT };'],
      ['an object that says it is a spring', "const t = { type: 'spring', duration: 0.4 };"],
      ['a stagger the literal builds', 'const a = <motion.li transition={{ ...TWEEN.quick, delay: i * 0.05 }} />;'],
      ['the literal arm of a conditional', 'const a = <motion.div transition={{ duration: reduced ? 0 : 0.3 }} />;'],
    ])('flags %s', (_, code) => {
      expect(flagged(code)).toEqual(['duration']);
    });
  });

  it.each([
    ['stiffness and damping', 'const t = { type: "spring", stiffness: 300, damping: 30 };', ['spring', 'spring']],
    ['bounce', 'const t = { type: "spring", bounce: 0.25 };', ['spring']],
    ['a spring beside a delay token', 'const t = { stiffness: 260, delay: STAGGER };', ['spring']],
  ])('flags a literal spring: %s', (_, code, kinds) => {
    expect(flagged(code)).toEqual(kinds);
  });

  describe('a timing in a style', () => {
    it.each([
      ['an interpolated millisecond value', 'const a = <i style={{ transitionDuration: `${x}ms` }} />;'],
      ['an interpolated shorthand', 'const a = <i style={{ transition: `all ${n}ms ease` }} />;'],
      ['an interpolated time beside a css token', 'const a = <i style={{ transition: `opacity ${n}ms var(--ease-out)` }} />;'],
      ['an interpolated delay', 'const a = <i style={{ animationDelay: `${i * 60}ms` }} />;'],
      ['a time written into a template', 'const a = <i style={{ transition: `opacity 150ms ${curve}` }} />;'],
      ['a string time and curve', "const a = <i style={{ transition: 'opacity 150ms ease' }} />;"],
      ['a fractional animation', "const a = <i style={{ animation: 'spin 1.2s linear infinite' }} />;"],
      ['a bare-dot time', "const a = <i style={{ transitionDelay: '.2s' }} />;"],
      ['a timing function', "const a = <i style={{ transitionTimingFunction: 'ease-in-out' }} />;"],
      ['a cubic-bezier', "const a = <i style={{ animationTimingFunction: 'cubic-bezier(0.4, 0, 0.2, 1)' }} />;"],
      ['a number as a duration', 'const a = <i style={{ transitionDuration: 300 }} />;'],
      ['a style assignment', "el.style.transition = 'opacity 150ms ease';"],
      ['a time on one branch', "const a = <i style={{ transition: open ? 'opacity 150ms' : 'none' }} />;"],
    ])('flags %s', (_, code) => {
      expect(flagged(code)).toEqual(['style']);
    });
  });

  it.each([
    ['a duration class', 'const a = <i className="transition-opacity duration-300" />;'],
    ['an arbitrary delay class', 'const a = <i className="delay-[120ms]" />;'],
    ['an arbitrary ease class', 'const a = <i className="ease-[cubic-bezier(.2,.8,.2,1)]" />;'],
    ['a class in a template', 'const a = <i className={`px-2 duration-150 ${x}`} />;'],
  ])('flags %s', (_, code) => {
    expect(flagged(code)).toEqual(['class']);
  });

  it('honors an exemption with its reason', () => {
    const code = [
      'const a = (',
      '  <motion.div',
      '    // eslint-disable-next-line no-restricted-syntax -- the loop period',
      '    transition={{ duration: 1.2, repeat: Infinity }}',
      '  />',
      ');',
    ].join('\n');
    expect(flagged(code)).toEqual([]);
    expect(flagged(code.replace(/^.*eslint-disable.*\n/m, ''))).toEqual(['duration']);
  });
});

describe('motion lint: what passes', () => {
  describe('tokens and what is built from them', () => {
    it.each([
      ['TWEEN', 'const a = <motion.div transition={TWEEN.fold} />;'],
      ['a spring token', 'const a = <motion.div transition={SPRING_SNAPPY} />;'],
      ['a tween with a delay', 'const a = <motion.div transition={{ ...TWEEN.quick, delay }} />;'],
      ['a tween with a stagger', 'const a = <motion.li transition={{ ...TWEEN.quick, delay: staggerDelay(i) }} />;'],
      ['DURATION and a curve', 'const a = <motion.div transition={{ duration: DURATION.fold, ease: EASE_OUT }} />;'],
      ['a token times a factor', 'const a = <motion.div transition={{ duration: DURATION.fold * 2 }} />;'],
      ['a token times a fraction', 'const a = <motion.div transition={{ duration: DURATION.fold * 0.5 }} />;'],
      ['a token plus a token', 'const a = <motion.div transition={{ duration: DURATION.exit + STAGGER }} />;'],
      ['an index offset in a stagger', 'const a = <motion.li transition={{ delay: (i + 1) * STAGGER }} />;'],
      ['a stagger token', 'const v = { show: { transition: { staggerChildren: STAGGER } } };'],
      ['a stagger token and a factor', 'const v = { show: { transition: { staggerChildren: STAGGER * 2 } } };'],
      ['a delay children token', 'const v = { show: { transition: { delayChildren: DURATION.exit } } };'],
      ['seconds as WAAPI milliseconds', 'el.animate(frames, { duration: DURATION.fold * 1000 });'],
      ['a unit conversion', 'animate(el, { x: 1 }, { duration: ms / 1000 });'],
      ['an imperative animate()', 'animate(height, next, TWEEN.fold);'],
      ['an infinite duration', 'const a = <motion.div transition={{ duration: Infinity }} />;'],
      ['a curve token', 'el.animate(frames, { easing: cssEase(EASE_REVEAL) });'],
      ['a css curve token as a WAAPI easing', "el.animate(frames, { easing: 'var(--ease-out)' });"],
      ['a typed constant of tokens', 'const T: Transition = { ...TWEEN.exit, delay: DURATION.exit };'],
    ])('passes %s', (_, code) => {
      expect(flagged(code)).toEqual([]);
    });

    it.each([
      ['a token in a duration', 'const a = <i style={{ transitionDuration: `${DURATION.exit}s` }} />;'],
      ['a token in milliseconds', 'const a = <i style={{ transitionDuration: `${DURATION.exit * 1000}ms` }} />;'],
      ['a token and a css curve', 'const a = <i style={{ transition: `opacity ${DURATION.fold}s var(--ease-out)` }} />;'],
      ['css tokens', "const a = <i style={{ transition: 'opacity var(--motion-quick) var(--ease-out)' }} />;"],
      ['css tokens in calc', 'const a = <i style={{ animationDelay: `calc(var(--motion-stagger) * ${Math.min(i, 5)})` }} />;'],
      ['a variable duration', 'const a = <i style={{ transitionDuration: ms }} />;'],
      ['no transition', "const a = <i style={{ transition: 'none' }} />;"],
      ['a zero time', "const a = <i style={{ transition: 'opacity 0s', transitionDelay: '0ms' }} />;"],
      ['a transition property list', "const a = <i style={{ transition: 'opacity, transform' }} />;"],
      ['a custom property named for a time', "const a = <i style={{ '--pill-transition': 'x' }} />;"],
      ['a plain class', 'const a = <i className="transition-opacity duration-quick ease-out" />;'],
    ])('passes %s in a style', (_, code) => {
      expect(flagged(code)).toEqual([]);
    });
  });

  describe('zero, which means no motion', () => {
    it.each([
      ['a reduced-motion transition', 'const a = <motion.div transition={reduced ? { duration: 0 } : TWEEN.fold} />;'],
      ['an imperative animate()', 'animate(height, next, reduced ? { duration: 0 } : TWEEN.fold);'],
      ['a typed instant transition', 'export const INSTANT: Transition = { duration: 0 };'],
      ['a mid-drag tween', 'const a = <motion.div transition={{ duration: dragging ? 0 : DURATION.fold, ease: EASE_OUT }} />;'],
    ])('passes %s', (_, code) => {
      expect(flagged(code)).toEqual([]);
    });
  });

  describe('a duration or a delay that is not motion', () => {
    it.each([
      ['a debounce option', 'debounce(fn, { delay: 300 });'],
      ['a setTimeout-like helper', 'schedule(fn, { delay: 300, retries: 3 });'],
      ['a toast duration', 'toast({ title, duration: 6000 });'],
      ['a toast method', 'toast.success(msg, { duration: 8000 });'],
      ['an i18n parameter', "t('toolArtifact.thoughtFor', { duration: '1s' });"],
      ['a retry record', 'const retry = { delay: 300, attempts: 5 };'],
      ['a hook option', 'const v = useDebouncedValue(value, { delay: 250 });'],
      [
        'a toast beside a transition prop',
        'const a = <motion.div transition={TWEEN.fold} onClick={() => toast({ duration: 6000 })} />;',
      ],
    ])('passes %s', (_, code) => {
      expect(flagged(code)).toEqual([]);
    });
  });

  it('leaves lib/motion.ts and test files alone', () => {
    const code = 'export const FOLD = { duration: 0.24, ease: [0.16, 1, 0.3, 1], stiffness: 260 };';
    expect(flagged(code, 'src/lib/motion.ts')).toEqual([]);
    expect(flagged(code, 'src/lib/__tests__/motion.test.ts')).toEqual([]);
    expect(flagged(code, 'src/pages/Probe.test.tsx')).toEqual([]);
    expect(flagged(code, 'src/pages/Probe.ts')).not.toEqual([]);
  });
});
