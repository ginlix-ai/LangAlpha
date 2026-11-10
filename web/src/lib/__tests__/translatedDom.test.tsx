import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { pageIsTranslated, tolerateTranslatedDom } from '../translatedDom';

// What Chrome's translator does to a text node: swaps it for <font> wrappers
// holding the translation, so the node React kept is out of the document.
function translate(text: Text): void {
  const outer = document.createElement('font');
  const inner = document.createElement('font');
  inner.textContent = '你好';
  outer.appendChild(inner);
  text.parentNode!.replaceChild(outer, text);
}

function Greeting({ hello, badge }: { hello: boolean; badge: boolean }) {
  return (
    <div>
      {badge && <b>new</b>}
      {hello ? 'Hello' : null}
      <span>end</span>
    </div>
  );
}

describe('tolerateTranslatedDom', () => {
  let container: HTMLDivElement;
  let root: Root;
  let restore: () => void = () => {};

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => root.unmount());
    container.remove();
    restore();
  });

  function renderTranslated() {
    act(() => root.render(<Greeting hello badge={false} />));
    const text = [...container.firstChild!.childNodes].find((n) => n.nodeType === Node.TEXT_NODE) as Text;
    translate(text);
  }

  it('without it, React throws once a translated text node goes', () => {
    renderTranslated();
    expect(() => act(() => root.render(<Greeting hello={false} badge={false} />))).toThrow(/not a child/);
  });

  it('lets React remove a translated text node', () => {
    restore = tolerateTranslatedDom();
    renderTranslated();
    act(() => root.render(<Greeting hello={false} badge={false} />));
    expect(container.querySelector('span')?.textContent).toBe('end');
  });

  it('lets React insert before a translated text node', () => {
    restore = tolerateTranslatedDom();
    renderTranslated();
    act(() => root.render(<Greeting hello badge />));
    expect(container.querySelector('b')?.textContent).toBe('new');
    expect(container.querySelector('span')?.textContent).toBe('end');
  });

  it('still removes a form whose controls shadow parentNode', () => {
    restore = tolerateTranslatedDom();
    const parent = document.createElement('div');
    const form = parent.appendChild(document.createElement('form'));
    form.innerHTML = '<input name="parentNode"><input name="removeChild">';
    parent.removeChild(form);
    expect(parent.children).toHaveLength(0);
  });

  it('still rejects an argument that is not a node', () => {
    restore = tolerateTranslatedDom();
    const parent = document.createElement('div');
    expect(() => parent.removeChild({} as Node)).toThrow(TypeError);
    expect(() => parent.insertBefore(document.createElement('i'), {} as Node)).toThrow(TypeError);
  });

  it('leaves ordinary DOM calls alone', () => {
    restore = tolerateTranslatedDom();
    const parent = document.createElement('div');
    const a = parent.appendChild(document.createElement('i'));
    const b = parent.insertBefore(document.createElement('u'), a);
    expect([...parent.children]).toEqual([b, a]);
    parent.removeChild(a);
    expect([...parent.children]).toEqual([b]);
  });

  it('hands each absorbed error to the reporter', () => {
    const onAbsorb = vi.fn();
    restore = tolerateTranslatedDom(onAbsorb);
    const parent = document.createElement('div');
    parent.removeChild(document.createElement('i'));
    parent.insertBefore(document.createElement('u'), document.createElement('i'));
    expect(onAbsorb).toHaveBeenCalledTimes(2);
    expect(onAbsorb.mock.calls[0][0]).toMatchObject({ name: 'NotFoundError' });
  });

  it('still absorbs when the reporter throws', () => {
    restore = tolerateTranslatedDom(() => {
      throw new Error('reporter down');
    });
    renderTranslated();
    act(() => root.render(<Greeting hello={false} badge={false} />));
    expect(container.querySelector('span')?.textContent).toBe('end');
  });
});

describe('pageIsTranslated', () => {
  afterEach(() => {
    document.documentElement.classList.remove('translated-ltr', 'translated-rtl');
    document.querySelectorAll('font').forEach((font) => font.remove());
  });

  it('reads an untouched page as not translated', () => {
    expect(pageIsTranslated()).toBe(false);
  });

  it("reads Chrome's mark on the root", () => {
    document.documentElement.classList.add('translated-rtl');
    expect(pageIsTranslated()).toBe(true);
  });

  it("reads the translator's <font> wrappers", () => {
    const font = document.body.appendChild(document.createElement('font'));
    font.setAttribute('style', 'vertical-align: inherit;');
    expect(pageIsTranslated()).toBe(true);
  });

  it('does not read a <font> the page drew itself, as in a Mermaid label', () => {
    document.body.appendChild(document.createElement('font')).setAttribute('color', 'red');
    expect(pageIsTranslated()).toBe(false);
  });
});
