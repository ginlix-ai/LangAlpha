import { resolve } from 'node:path'
import { includeIgnoreFile } from 'eslint/config'
import js from '@eslint/js'
import globals from 'globals'
import eslintReact from '@eslint-react/eslint-plugin'
import reactHooks from 'eslint-plugin-react-hooks'
import { reactRefresh } from 'eslint-plugin-react-refresh'
import tseslint from '@typescript-eslint/eslint-plugin'
import tsParser from '@typescript-eslint/parser'

// The checks eslint-plugin-react's recommended preset ran, under their
// @eslint-react names. That preset also caught JSX undefined names, duplicate
// props, string refs, isMounted and a missing render return, which tsc already
// rejects, and unescaped entities, which has no counterpart. @eslint-react's
// own presets add ~290 findings of new policy (index keys, ref naming) plus
// copies of the react-hooks rules, so they stay off until chosen on purpose.
const reactRules = Object.fromEntries([
  'no-missing-key',
  'no-missing-component-display-name',
  'no-direct-mutation-state',
  'no-component-will-mount',
  'no-component-will-receive-props',
  'no-component-will-update',
  'jsx-no-children-prop',
  'jsx-no-comment-textnodes',
  'dom-no-dangerously-set-innerhtml-with-children',
  'dom-no-find-dom-node',
  'dom-no-hydrate',
  'dom-no-render',
  'dom-no-render-return-value',
  'dom-no-unknown-property',
  'dom-no-unsafe-target-blank',
].map((rule) => [`@eslint-react/${rule}`, 'error']))

// react-hooks 6+ puts the React Compiler's checks in its recommended preset
// at error. Existing code predates them, so they report as warnings until the
// code is brought in line; the two rules v5 shipped keep their levels.
const HOOKS_V5_RULES = new Set(['react-hooks/rules-of-hooks', 'react-hooks/exhaustive-deps'])
const hooksCompilerRulesAsWarnings = Object.fromEntries(
  Object.keys(reactHooks.configs.flat.recommended.rules)
    .filter((rule) => !HOOKS_V5_RULES.has(rule))
    .map((rule) => [rule, 'warn']),
)

// Export shapes @vitejs/plugin-react can hot-swap in place, the same pair
// react-refresh's own vite preset allows.
const refreshExportOptions = { allowConstantExport: true, allowCompoundComponents: true }

// The motion selectors (DESIGN.md § Motion) read "a motion key whose value
// holds a literal". The value position is what makes a number a timing, so an
// expression built from tokens (`DURATION.fold * 2`, `DURATION.exit + STAGGER`)
// passes however many literals it carries, and a `delay` that belongs to a
// debounce is not caught. Every motion check below is by AST shape, never by
// source text.

/** An esquery regex for one whole name out of `|`-joined alternatives. */
const nameIn = (/** @type {string} */ names) => `/^(${names})$/`;

/** A Property keyed by one of `names`, written bare or quoted. */
const keyed = (/** @type {string} */ names) =>
  `Property:matches([key.name=${nameIn(names)}], [key.value=${nameIn(names)}])`;

// Anything from lib/motion.ts. A value that names one is built from the
// vocabulary, so the literals beside it (a factor, an index offset) are not
// timings. Matched by name: a local constant that wraps a token does not count,
// the token has to appear where the value is written.
const TOKEN = 'Identifier[name=/^(DURATION|TWEEN|STAGGER|EASE_\\w+|SPRING_\\w+|staggerDelay|cssEase)$/]';
const NO_TOKEN = `:not(:has(${TOKEN}))`;

// A number above zero. Zero is "no motion" (a reduced-motion or mid-drag
// tween), which is not a second timing, and `* 1000` or `/ 1000` is only the
// unit conversion between framer's seconds and WAAPI's milliseconds.
const POSITIVE =
  'Literal[value>0]:not(:matches(BinaryExpression[operator="*"], BinaryExpression[operator="/"]) > Literal[value=1000])';

// What a CSS value encodes: a nonzero time (`150ms`, `.2s`, never `0s`) or a
// curve. `ease` only counts as a whole word, so `var(--ease-out)` is left alone.
const CSS_TIME = '(?<![\\w.])(?=\\d*\\.?\\d*[1-9])\\d*\\.?\\d+m?s\\b';
const CSS_CURVE = 'cubic-bezier|(?<![\\w-])(?:ease(?:-in-out|-in|-out)?|linear)(?![\\w-])';
const CSS_TIMING = `/${CSS_TIME}|${CSS_CURVE}/`;

// `duration` and `delay` also name a debounce, a retry and a toast, and no
// syntax tells those apart from a transition's, so they are only checked where
// framer reads them: a transition prop or key, an animate()/.animate() call, an
// object typed as a framer type or named for one, and an object shaped like a
// transition (a curve, a spring key or a spread token beside it). The limit: a
// bare `{ duration: 0.3 }` handed to framer through an untyped, unremarkably
// named constant is not seen. Type it (`: Transition`) and it is.
const FRAMER_PROPS =
  'transition|animate|exit|initial|variants|dragTransition|whileHover|whileTap|whileFocus|whileInView|whileDrag';
const FRAMER_TYPES = nameIn(
  'Transition|Variants|Variant|Tween|Spring|Inertia|TargetAndTransition|AnimationOptions|ValueAnimationTransition',
);
const FRAMER_CONTEXT = `:matches(${[
  `JSXAttribute[name.name=${nameIn(FRAMER_PROPS)}]`,
  keyed('transition|transitionEnd|dragTransition'),
  'CallExpression[callee.name="animate"]',
  'CallExpression[callee.property.name="animate"]',
  `VariableDeclarator[id.typeAnnotation.typeAnnotation.typeName.name=${FRAMER_TYPES}]`,
  `:function[returnType.typeAnnotation.typeName.name=${FRAMER_TYPES}]`,
  `TSSatisfiesExpression[typeAnnotation.typeName.name=${FRAMER_TYPES}]`,
  `TSAsExpression[typeAnnotation.typeName.name=${FRAMER_TYPES}]`,
  'VariableDeclarator[id.name=/(transition|tween|spring|variants)/i]',
].join(', ')})`;
const TRANSITION_SHAPED = `ObjectExpression:has(${[
  `> ${keyed('ease|easing|repeatType|repeatDelay|stiffness|damping|mass|bounce|staggerChildren|delayChildren')}`,
  '> Property[key.name="type"][value.value=/^(spring|tween|inertia|keyframes)$/]',
  '> SpreadElement[argument.object.name="TWEEN"]',
  '> SpreadElement[argument.name=/^SPRING_/]',
].join(', ')})`;
const TIME_PROPERTY = `${keyed('duration|delay')}${NO_TOKEN} ${POSITIVE}`;

// A style's timing, in an object or an assignment (`el.style.transition = ...`).
const CSS_KEYS = '(transition|animation)(Duration|Delay|TimingFunction)?';
const CSS_SLOT = `:matches(${keyed(CSS_KEYS)}, AssignmentExpression[left.property.name=${nameIn(CSS_KEYS)}])`;

const motionSelectors = [
  {
    selector: [
      `${keyed('repeatDelay|staggerChildren|delayChildren')}${NO_TOKEN} ${POSITIVE}`,
      `${FRAMER_CONTEXT} ${TIME_PROPERTY}`,
      `${TRANSITION_SHAPED} > ${TIME_PROPERTY}`,
    ].join(', '),
    message: 'A literal duration, delay or stagger. Use DURATION, TWEEN, STAGGER or staggerDelay from @/lib/motion.',
  },
  {
    selector: `${keyed('stiffness|damping|mass|bounce|bounceStiffness|bounceDamping|visualDuration')}${NO_TOKEN} ${POSITIVE}`,
    message: 'A literal spring. Use a SPRING_* token from @/lib/motion.',
  },
  {
    selector: [
      `${keyed('ease|easing')} ArrayExpression`,
      // A name framer or CSS resolves (`'easeOut'`, `'linear'`, a cubic-bezier),
      // but not a `var(--ease-*)`, which is the CSS token.
      `${keyed('ease|easing')} Literal[value=/^(?!var\\()/]`,
      `${keyed('ease|easing')} TemplateElement[value.raw=/${CSS_CURVE}/]`,
      'VariableDeclarator[id.name=/^EAS(E|ING)/] ArrayExpression:has(> Literal)',
    ].join(', '),
    message: 'A literal curve. Use EASE_OUT, EASE_REVEAL or EASE_COLLAPSE from @/lib/motion.',
  },
  {
    selector: [
      `${CSS_SLOT} :matches(Literal[value=${CSS_TIMING}], TemplateElement[value.raw=${CSS_TIMING}])`,
      // A unit right after an interpolation (`${n}ms`) is a time the template
      // takes from a variable: fine only when a token supplies it.
      `${CSS_SLOT} TemplateLiteral${NO_TOKEN} > TemplateElement[value.raw=/^m?s(?![\\w-])/]`,
      // A bare number as the value; one inside a template is the template's.
      `${keyed('(transition|animation)(Duration|Delay)')} > ${POSITIVE}`,
      `${keyed('(transition|animation)(Duration|Delay)')} > ConditionalExpression > ${POSITIVE}`,
    ].join(', '),
    message: 'A literal timing in a style. Use a token from @/lib/motion (DURATION, cssEase) or var(--motion-*) and var(--ease-*) from styles/tokens.css.',
  },
  {
    selector: [
      'Literal[value=/(^|[\\s:])(duration|delay)-(\\d|\\[)|(^|[\\s:])ease-\\[/]',
      'TemplateElement[value.raw=/(^|[\\s:])(duration|delay)-(\\d|\\[)|(^|[\\s:])ease-\\[/]',
    ].join(', '),
    message: 'A literal timing class. Use duration-quick/fold/enter/exit/press and ease-out/reveal/collapse.',
  },
];

export default [
  // Skip what git ignores. Flat config never reads .gitignore itself, so build
  // output or local tooling in a checkout would fail lint where a fresh clone
  // passes.
  ...includeIgnoreFile(
    [resolve(import.meta.dirname, '../.gitignore'), resolve(import.meta.dirname, '.gitignore')],
    { gitignoreResolution: true },
  ),

  js.configs.recommended,
  {
    // ESLint 10 added these to recommended and existing code violates them.
    rules: {
      'no-useless-assignment': 'warn',
      'preserve-caught-error': 'warn',
    },
  },

  {
    files: ['**/*.{js,jsx,ts,tsx}'],
    plugins: { '@eslint-react': eslintReact },
    rules: reactRules,
  },

  reactHooks.configs.flat.recommended,
  { rules: hooksCompilerRulesAsWarnings },
  // Promoted once the code was clean: an impure call in render is cached by
  // the compiler and freezes at its first value.
  { rules: { 'react-hooks/purity': 'error' } },

  {
    files: ['**/*.{js,jsx}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: {
        ...globals.browser,
        process: 'readonly',
      },
    },
    plugins: { 'react-refresh': reactRefresh.plugin },
    rules: {
      'no-unused-vars': ['warn', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_|^React$',
        caughtErrorsIgnorePattern: '^_',
        destructuredArrayIgnorePattern: '^_',
      }],
      'react-refresh/only-export-components': ['warn', refreshExportOptions],
    },
  },

  {
    files: ['**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      parser: tsParser,
      parserOptions: {
        ecmaFeatures: { jsx: true },
      },
      globals: {
        ...globals.browser,
        process: 'readonly',
      },
    },
    plugins: {
      '@typescript-eslint': tseslint,
      'react-refresh': reactRefresh.plugin,
    },
    rules: {
      'no-undef': 'off',
      'no-redeclare': 'off',
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': ['warn', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_|^React$',
        caughtErrorsIgnorePattern: '^_',
        destructuredArrayIgnorePattern: '^_',
      }],
      // Under verbatimModuleSyntax `import { type A } from 'x'` still loads
      // 'x' for its side effects, and tsc accepts it. `import type` does not.
      '@typescript-eslint/no-import-type-side-effects': 'error',
      'react-refresh/only-export-components': ['warn', refreshExportOptions],
    },
  },

  // lib/framer is the only module that may import framer-motion: it arms the
  // hidden-tab rule, and ES module order only guarantees that runs first for
  // code that imports framer through it.
  //
  // zod core sits in the entry chunk, because the entry validates with
  // zod/mini, so the core code any lazy schema reaches is paid on first load.
  // Classic zod reaches nearly all of it (its methods do not tree-shake) and
  // adds its own API and English locale in a chunk of their own.
  {
    files: ['src/**/*.{js,jsx,ts,tsx}'],
    ignores: ['src/lib/framer.ts'],
    rules: {
      'no-restricted-imports': ['error', {
        paths: [{
          name: 'framer-motion',
          message: 'Import from @/lib/framer, which sets up framer before any animation exists.',
        }],
        // zod/v4 and zod/v3 are classic zod under another name.
        patterns: [{
          regex: '^zod(/v3|/v4)?$',
          message: 'Import zod/mini. Classic zod puts most of zod core on the entry chunk.',
        }],
      }],
    },
  },

  // The motion vocabulary (DESIGN.md § Motion) is lib/motion.ts: a duration, a
  // curve or a spring written anywhere else is a second vocabulary. CSS is
  // held by styles/__tests__/motionTokens.test.ts; this holds the scripts, and
  // __tests__/motionLint.test.ts pins what these selectors flag and pass.
  // A literal that no token names on purpose (a measured physics value, a
  // loop's period) takes an eslint-disable-next-line with its reason.
  {
    files: ['src/**/*.{ts,tsx}'],
    ignores: [
      'src/lib/motion.ts',
      '**/__tests__/**',
      '**/*.test.{ts,tsx}',
    ],
    rules: {
      'no-restricted-syntax': ['error', ...motionSelectors],
    },
  },

  {
    files: ['vite.config.js', 'scripts/**/*.mjs'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: { ...globals.node },
    },
  },
]
