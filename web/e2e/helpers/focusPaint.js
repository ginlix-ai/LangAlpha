/**
 * Reading the ring a control actually wears.
 *
 * jsdom paints no outlines, so a unit test cannot see any of this: whether a
 * ring is there, what colour it is, and which device earned it only exist in a
 * real browser under a real click. Shared by the specs that measure it.
 */

/**
 * The outline painted on one control, plus enough identity to say which control
 * answered. Defaults to whatever holds focus, since most questions here are
 * about the focused element rather than a named one.
 *
 * `shadow` is here because an outline is only half of how this app draws a
 * ring: a Tailwind `ring-*` utility is a box-shadow, which no outline property
 * reports. Asserting `outline: none` alone would call a field clean while a
 * ring was plainly painted on it, so the mouse assertions compare this too.
 */
export async function outlineOn(page, sel = ':focus') {
  return page.evaluate((s) => {
    const el = document.querySelector(s);
    if (!el) throw new Error(`${s} is not on the page`);
    const cs = getComputedStyle(el);
    return {
      tag: el.tagName,
      tabindex: el.getAttribute('tabindex'),
      focused: document.activeElement === el,
      style: cs.outlineStyle,
      width: cs.outlineWidth,
      color: cs.outlineColor,
      shadow: cs.boxShadow,
    };
  }, sel);
}

/**
 * Is this outline invisible on screen? Two shapes mean the same thing to a
 * viewer: no outline at all, and one painted in nothing. The second is what a
 * rule writes when it has to leave forced colors something to repaint, so a
 * check that only accepted `none` would read that as a ring coming back.
 */
export function unpainted(style, color) {
  return style === 'none' || /,\s*0\)\s*$/.test(color);
}

/**
 * Put focus on a control down the keyboard path, for the fields a Tab walk
 * cannot reach cheaply -- one inside a modal, one at the end of a long route.
 *
 * A keystroke first, then a programmatic move. What decides the ring is the
 * device recorded when focus last moved, and `focusin` fires for either kind of
 * move, so this reaches the same state a Tab does. It does not prove the
 * control is a tab stop; `tabTo` is what proves that, and one field exercises
 * it so the tab order itself stays covered. ArrowDown rather than Escape or
 * Tab: it clears the pointer flag without closing a modal or moving focus off
 * the element under test.
 */
export async function keyboardFocus(page, sel) {
  await page.keyboard.press('ArrowDown');
  await page.locator(sel).evaluate((el) => {
    // Blur first, or this is a no-op on a field something already autofocused
    // -- the Create Workspace modal does -- and no focusin fires to re-read the
    // device. A keyboard user arrives by a real focus move; so does this.
    document.activeElement?.blur();
    el.focus();
  });
}

/**
 * Walk the tab order until it lands on a control. A real Tab rather than a
 * programmatic focus, because the two arrive by different paths and only one of
 * them is what a keyboard user does.
 */
export async function tabTo(page, sel, max = 40) {
  for (let i = 0; i < max; i += 1) {
    await page.keyboard.press('Tab');
    const there = await page.evaluate(
      (s) => document.activeElement === document.querySelector(s),
      sel,
    );
    if (there) return;
  }
  throw new Error(`the tab order never reached ${sel} in ${max} presses`);
}
