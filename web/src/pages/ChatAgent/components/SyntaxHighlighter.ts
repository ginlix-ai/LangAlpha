import { createElement, useState, type ComponentProps, type CSSProperties, type Key, type ReactNode } from 'react';
import PrismAsyncLight from 'react-syntax-highlighter/dist/esm/prism-async-light';
import createNode, { createClassNameString, createStyleObject } from 'react-syntax-highlighter/dist/esm/create-element';
import { oneDark as prismOneDark, oneLight as prismOneLight } from 'react-syntax-highlighter/dist/esm/styles/prism';

import python from 'react-syntax-highlighter/dist/esm/languages/prism/python';
import javascript from 'react-syntax-highlighter/dist/esm/languages/prism/javascript';
import jsx from 'react-syntax-highlighter/dist/esm/languages/prism/jsx';
import typescript from 'react-syntax-highlighter/dist/esm/languages/prism/typescript';
import tsx from 'react-syntax-highlighter/dist/esm/languages/prism/tsx';
import json from 'react-syntax-highlighter/dist/esm/languages/prism/json';
import bash from 'react-syntax-highlighter/dist/esm/languages/prism/bash';
import css from 'react-syntax-highlighter/dist/esm/languages/prism/css';
import markup from 'react-syntax-highlighter/dist/esm/languages/prism/markup';
import markdown from 'react-syntax-highlighter/dist/esm/languages/prism/markdown';
import yaml from 'react-syntax-highlighter/dist/esm/languages/prism/yaml';
import sql from 'react-syntax-highlighter/dist/esm/languages/prism/sql';
import r from 'react-syntax-highlighter/dist/esm/languages/prism/r';
import go from 'react-syntax-highlighter/dist/esm/languages/prism/go';
import rust from 'react-syntax-highlighter/dist/esm/languages/prism/rust';
import java from 'react-syntax-highlighter/dist/esm/languages/prism/java';
import cpp from 'react-syntax-highlighter/dist/esm/languages/prism/cpp';
import c from 'react-syntax-highlighter/dist/esm/languages/prism/c';
import ruby from 'react-syntax-highlighter/dist/esm/languages/prism/ruby';

PrismAsyncLight.registerLanguage('python', python);
PrismAsyncLight.registerLanguage('javascript', javascript);
PrismAsyncLight.registerLanguage('jsx', jsx);
PrismAsyncLight.registerLanguage('typescript', typescript);
PrismAsyncLight.registerLanguage('tsx', tsx);
PrismAsyncLight.registerLanguage('json', json);
PrismAsyncLight.registerLanguage('bash', bash);
PrismAsyncLight.registerLanguage('css', css);
PrismAsyncLight.registerLanguage('markup', markup);
PrismAsyncLight.registerLanguage('html', markup);
PrismAsyncLight.registerLanguage('xml', markup);
PrismAsyncLight.registerLanguage('markdown', markdown);
PrismAsyncLight.registerLanguage('yaml', yaml);
PrismAsyncLight.registerLanguage('sql', sql);
PrismAsyncLight.registerLanguage('r', r);
PrismAsyncLight.registerLanguage('go', go);
PrismAsyncLight.registerLanguage('rust', rust);
PrismAsyncLight.registerLanguage('java', java);
PrismAsyncLight.registerLanguage('cpp', cpp);
PrismAsyncLight.registerLanguage('c', c);
PrismAsyncLight.registerLanguage('ruby', ruby);

// Prism's One themes were drawn for their own backgrounds, and several of their
// token colors land under 4.5:1 on our code surfaces. Each step here is the same
// hue taken just far enough to hold it on `--color-bg-code` in that theme.
const LEGIBLE_LIGHT: Record<string, string> = {
  'hsl(230, 4%, 64%)': '#6B6E78', // comment
  'hsl(230, 1%, 62%)': '#6B6E78',
  'hsl(5, 74%, 59%)': '#C5392C', // tag, deleted
  'hsl(119, 34%, 47%)': '#2E7D32', // string
  'hsl(35, 99%, 36%)': '#8A5A00', // number, class
  'hsl(221, 87%, 60%)': '#2563D8', // function
  'hsl(198, 99%, 37%)': '#0277A6', // builtin, regex
};
const LEGIBLE_DARK: Record<string, string> = {
  'hsl(220, 10%, 40%)': '#8B92A0', // comment
  'hsl(220, 9%, 55%)': '#8B92A0',
};

function withLegibleTokens(theme: Stylesheet, colors: Record<string, string>): Stylesheet {
  return Object.fromEntries(Object.entries(theme).map(([selector, style]) => {
    const color = style.color && colors[style.color];
    return [selector, color ? { ...style, color } : style];
  }));
}

const oneLight = withLegibleTokens(prismOneLight, LEGIBLE_LIGHT);
const oneDark = withLegibleTokens(prismOneDark, LEGIBLE_DARK);

declare module 'react-syntax-highlighter/dist/esm/create-element' {
  export function createStyleObject(classNames: string[], elementStyle?: CSSProperties, stylesheet?: Stylesheet): CSSProperties;
  export function createClassNameString(classNames: string[]): string;
}

type HighlighterProps = ComponentProps<typeof PrismAsyncLight>;
type Renderer = NonNullable<HighlighterProps['renderer']>;
type RendererProps = Parameters<Renderer>[0];
type RendererNode = RendererProps['rows'][number];
type Stylesheet = RendererProps['stylesheet'];

interface StylesheetIndex {
  classes: Set<string>;
  shapes: Map<string, { className: string | undefined; style: CSSProperties }>;
}

const indexes = new WeakMap<Stylesheet, StylesheetIndex>();

function indexFor(stylesheet: Stylesheet): StylesheetIndex {
  let index = indexes.get(stylesheet);
  if (!index) {
    index = { classes: new Set(Object.keys(stylesheet).flatMap((selector) => selector.split('.'))), shapes: new Map() };
    indexes.set(stylesheet, index);
  }
  return index;
}

const NUMBERED_LINE_CODE: CSSProperties = { flex: 1, minWidth: 0, overflowWrap: 'anywhere' };

// The library draws a wrapped, numbered line as a flex row, which makes every
// token its own column: a line too long for the row breaks up token by token,
// its words stacked in their columns and its end cut off. Beside the number the
// tokens have to be one run of text, so they share one flex item.
function joinNumberedLine(row: RendererNode): RendererNode {
  if (row.properties?.style?.display !== 'flex' || !row.children) return row;
  const [lineNumber, ...code] = row.children;
  return {
    ...row,
    children: [lineNumber, { type: 'element', tagName: 'span', properties: { className: [], style: NUMBERED_LINE_CODE }, children: code }],
  };
}

// The library's createElement with inline styles, output for output. It
// rebuilds the class list of every selector in the theme for each token it
// draws (O(selectors) with an Array.includes dedupe, per token), which made a
// streaming code block's redraws most of the highlighter's cost. Here that list
// is built once per theme, and a token's class string and style are shared by
// every token with the same classes, so React also skips an unchanged style.
function renderNode(node: RendererNode, stylesheet: Stylesheet, index: StylesheetIndex, key: Key): ReactNode {
  if (node.type === 'text') return node.value;
  if (!node.tagName) return undefined;
  const properties = node.properties ?? { className: [] };
  const classNames: string[] = properties.className;
  let shape = properties.style === undefined ? index.shapes.get(classNames.join(' ')) : undefined;
  if (!shape) {
    const kept = classNames.filter((name) => !index.classes.has(name));
    shape = {
      className: createClassNameString(classNames.includes('token') ? ['token', ...kept] : kept) || undefined,
      style: createStyleObject(classNames, { ...properties.style }, stylesheet),
    };
    if (properties.style === undefined) index.shapes.set(classNames.join(' '), shape);
  }
  const children = (node.children ?? []).map((child, i) => renderNode(child, stylesheet, index, `code-segment-1-${i}`));
  return createElement(node.tagName, { key, ...properties, ...shape }, children);
}

// One per highlighter: a row drawn the same as last time at its position keeps
// its element, so React skips it. A fence still streaming redraws every line it
// has on each newline, and React used to diff every token of them again.
function createHighlightRenderer(): Renderer {
  let sheet: Stylesheet | undefined;
  let drawn: Array<{ source: string; element: ReactNode }> = [];
  return ({ rows, stylesheet, useInlineStyles }: RendererProps): ReactNode => {
    if (!useInlineStyles) {
      return rows.map((node, i) => createNode({ node, stylesheet, useInlineStyles, key: `code-segment-${i}` }));
    }
    const index = indexFor(stylesheet);
    if (stylesheet !== sheet) {
      sheet = stylesheet;
      drawn = [];
    }
    drawn = rows.map((node, i) => {
      const source = JSON.stringify(node);
      return drawn[i]?.source === source ? drawn[i] : { source, element: renderNode(joinNumberedLine(node), stylesheet, index, `code-segment-${i}`) };
    });
    return drawn.map((row) => row.element);
  };
}

// Passing a renderer turns line wrapping on when wrapLines is unset, so it is
// pinned to what the library does without one.
function SyntaxHighlighter(props: HighlighterProps): ReactNode {
  const [renderer] = useState(createHighlightRenderer);
  return createElement(PrismAsyncLight, { ...props, renderer, wrapLines: props.wrapLines ?? false });
}

export default SyntaxHighlighter;
export { oneDark, oneLight, createHighlightRenderer };
