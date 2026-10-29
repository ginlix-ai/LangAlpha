/**
 * When the static nav tree (../navTreeKit) swaps in the interactive kit: not
 * while the remount would show (a row focused from the keyboard, a row's menu
 * open or closing) or a press is held, and a press on a workspace header that
 * moves past dnd-kit's activation distance is handed to dnd-kit mid-gesture.
 *
 * Every case loads a fresh copy of the kit module, so each starts static.
 */
import React, { Activity } from 'react';
import { describe, it, expect, vi, afterEach } from 'vitest';
import { act, render, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';

const WS_IDS = ['ws-a', 'ws-b'];

async function mountTree() {
  vi.resetModules();
  const { NavTreeContext, loadNavTreeKit, useNavTreeKit } = await import('../navTreeKit');
  const onDragStart = vi.fn();
  const onDragEnd = vi.fn();
  const onHeaderClick = vi.fn();

  function Tree() {
    const rootRef = React.useRef<HTMLDivElement>(null);
    const navTree = useNavTreeKit(rootRef);
    const { TreeRoot, Section } = navTree.kit;
    return (
      <div ref={rootRef}>
        <NavTreeContext value={navTree}>
          <TreeRoot
            ids={WS_IDS}
            onDragStart={onDragStart}
            onDragEnd={onDragEnd}
            onDragCancel={() => {}}
            clampOverlay={({ transform }) => transform}
            overlay={null}
          >
            {WS_IDS.map((wsId) => (
              <Section key={wsId} wsId={wsId} disabled={false}>
                {({ dragHandleProps }) => (
                  <>
                    <div data-nav-key="header" onClick={onHeaderClick} {...dragHandleProps}>{wsId}</div>
                    <button type="button" data-nav-key="pin">pin</button>
                  </>
                )}
              </Section>
            ))}
          </TreeRoot>
        </NavTreeContext>
      </div>
    );
  }

  const view = render(<Activity mode="visible"><Tree /></Activity>);
  const setVisible = (visible: boolean) =>
    view.rerender(<Activity mode={visible ? 'visible' : 'hidden'}><Tree /></Activity>);
  const navElement = (wsId: string, key: string) =>
    view.container.querySelector<HTMLElement>(`[data-ws-id="${wsId}"] [data-nav-key="${key}"]`)!;
  const header = (wsId: string) => navElement(wsId, 'header');
  // dnd-kit's handle points into its DndContext; the static one does not.
  const isInteractive = () => header('ws-a').hasAttribute('aria-describedby');
  const load = () => act(() => loadNavTreeKit());
  return { ...view, setVisible, navElement, header, isInteractive, load, onDragStart, onDragEnd, onHeaderClick };
}

afterEach(() => {
  vi.restoreAllMocks();
});

type PointerInit = PointerEventInit & { pointerId: number };

const pointerEvent = (type: string, init: PointerInit) => new PointerEvent(type, {
  bubbles: true,
  cancelable: true,
  composed: true,
  pointerType: 'mouse',
  isPrimary: true,
  ...init,
});

function pointer(target: EventTarget, type: string, init: PointerInit) {
  act(() => {
    target.dispatchEvent(pointerEvent(type, init));
  });
}

/** Releases a drag dnd-kit owns; its overlay settles a microtask after the drop. */
async function drop(init: PointerInit) {
  await act(async () => {
    document.dispatchEvent(pointerEvent('pointerup', { ...init, buttons: 0 }));
  });
}

const MOUSE: PointerInit = { pointerId: 1, button: 0, buttons: 1, clientX: 20, clientY: 20 };

// The swap re-polls a held tree every 250ms: past one poll, a swap that has not
// landed is being held.
const pastAPoll = () => act(() => new Promise((resolve) => setTimeout(resolve, 400)));

describe('static nav tree swap', () => {
  it('wait for a row focused from the keyboard to lose focus', async () => {
    const tree = await mountTree();
    act(() => tree.navElement('ws-a', 'pin').focus());
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    act(() => tree.navElement('ws-a', 'pin').blur());
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
  });

  it('wait for a clicked row while the page is away, then carry its focus to the twin', async () => {
    const tree = await mountTree();
    const user = userEvent.setup();
    const pin = tree.navElement('ws-b', 'pin');
    await user.click(pin);
    await user.unhover(pin);
    const hasFocus = vi.spyOn(document, 'hasFocus').mockReturnValue(false);
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    hasFocus.mockReturnValue(true);
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
    expect(pin.isConnected).toBe(false);
    expect(document.activeElement).toBe(tree.navElement('ws-b', 'pin'));
  });

  it("wait for a row's menu to close and for the menu to leave", async () => {
    const tree = await mountTree();
    const trigger = tree.navElement('ws-a', 'pin');
    trigger.setAttribute('data-state', 'open');
    const menu = document.body.appendChild(document.createElement('div'));
    menu.setAttribute('role', 'menu');
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    // Closed, with the menu still animating out.
    trigger.setAttribute('data-state', 'closed');
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    menu.remove();
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
  });

  it('swap from a timer when the kit is already in as the tree takes its listeners', async () => {
    const tree = await mountTree();
    tree.setVisible(false);
    await tree.load();
    expect(tree.isInteractive()).toBe(false);

    // React refuses the swap's flushSync inside the effect that re-attaches.
    const consoleError = vi.spyOn(console, 'error');
    tree.setVisible(true);
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
    expect(consoleError).not.toHaveBeenCalled();
  });
});

describe('static nav tree presses', () => {
  it('hold the swap until the press is released', async () => {
    const tree = await mountTree();
    pointer(tree.header('ws-a'), 'pointerdown', MOUSE);
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    pointer(document, 'pointerup', { ...MOUSE, buttons: 0 });
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
  });

  it('let go of a press whose release was lost outside the window', async () => {
    const tree = await mountTree();
    pointer(tree.header('ws-a'), 'pointerdown', MOUSE);
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    // Back over the page with the button already up.
    pointer(document, 'pointermove', { ...MOUSE, buttons: 0, clientX: 200 });
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
    expect(tree.onDragStart).not.toHaveBeenCalled();
  });

  it('let go of a press when the window loses focus', async () => {
    const tree = await mountTree();
    pointer(tree.header('ws-a'), 'pointerdown', MOUSE);
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    act(() => {
      window.dispatchEvent(new Event('blur'));
    });
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
  });

  it('follow the pointer that pressed first: another pointer coming and going does not let it go', async () => {
    const tree = await mountTree();
    const pen: PointerInit = { ...MOUSE, pointerType: 'pen' };
    const touch: PointerInit = { pointerId: 7, pointerType: 'touch', button: 0, buttons: 1, clientX: 20, clientY: 60 };
    pointer(tree.header('ws-a'), 'pointerdown', pen);
    pointer(tree.header('ws-b'), 'pointerdown', touch);
    pointer(document, 'pointermove', { ...touch, clientY: 90 });
    pointer(document, 'pointerup', { ...touch, buttons: 0, clientY: 90 });
    pointer(document, 'pointermove', { pointerId: 9, buttons: 0, clientX: 300 });
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    pointer(document, 'pointerup', { ...pen, buttons: 0 });
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
    expect(tree.onDragStart).not.toHaveBeenCalled();
  });

  it('let go of a touch whose release was lost when the next touch goes down', async () => {
    const tree = await mountTree();
    const touch: PointerInit = { pointerId: 7, pointerType: 'touch', button: 0, buttons: 1 };
    pointer(tree.header('ws-a'), 'pointerdown', touch);
    await tree.load();
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    const next: PointerInit = { ...touch, pointerId: 8 };
    pointer(tree.header('ws-b'), 'pointerdown', next);
    pointer(document, 'pointerup', { ...next, buttons: 0 });
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
  });

  it('hand a header press that moved past the activation distance to dnd-kit once it loads', async () => {
    const tree = await mountTree();
    pointer(tree.header('ws-b'), 'pointerdown', MOUSE);
    pointer(document, 'pointermove', { ...MOUSE, clientY: 32 });
    expect(tree.onDragStart).not.toHaveBeenCalled();

    await tree.load();
    expect(tree.isInteractive()).toBe(true);
    expect(tree.onDragStart).toHaveBeenCalledTimes(1);
    expect(tree.onDragStart.mock.calls[0][0].active.id).toBe('ws-b');

    // Released where it was handed over, with no move after: the drag is already at the pointer.
    await drop({ ...MOUSE, clientY: 32 });
    expect(tree.onDragEnd).toHaveBeenCalledTimes(1);
    expect(tree.onDragEnd.mock.calls[0][0].delta).toMatchObject({ x: 0, y: 12 });
  });

  it('hand a press over as soon as it moves past the activation distance once the kit is in', async () => {
    const tree = await mountTree();
    pointer(tree.header('ws-a'), 'pointerdown', MOUSE);
    await tree.load();
    pointer(document, 'pointermove', { ...MOUSE, clientY: 26 });
    expect(tree.isInteractive()).toBe(false);

    pointer(document, 'pointermove', { ...MOUSE, clientY: 32 });
    expect(tree.isInteractive()).toBe(true);
    expect(tree.onDragStart).toHaveBeenCalledTimes(1);
    await drop({ ...MOUSE, clientY: 32 });
    expect(tree.onDragEnd.mock.calls[0][0].delta).toMatchObject({ x: 0, y: 12 });
  });

  it('never hand over a press made with another button', async () => {
    const tree = await mountTree();
    pointer(tree.header('ws-a'), 'pointerdown', { ...MOUSE, button: 2, buttons: 2 });
    await tree.load();
    pointer(document, 'pointermove', { ...MOUSE, buttons: 2, clientY: 40 });
    await pastAPoll();
    expect(tree.isInteractive()).toBe(false);

    pointer(document, 'pointerup', { ...MOUSE, button: 2, buttons: 0, clientY: 40 });
    await waitFor(() => expect(tree.isInteractive()).toBe(true));
    expect(tree.onDragStart).not.toHaveBeenCalled();
  });

  it('swallow the click a drag ends in when the kit arrived too late for it', async () => {
    const tree = await mountTree();
    pointer(tree.header('ws-a'), 'pointerdown', MOUSE);
    pointer(document, 'pointermove', { ...MOUSE, clientY: 40 });
    pointer(document, 'pointerup', { ...MOUSE, buttons: 0, clientY: 40 });
    tree.header('ws-a').click();
    expect(tree.onHeaderClick).not.toHaveBeenCalled();

    // A press that stayed a press clicks as usual.
    await new Promise((resolve) => setTimeout(resolve, 60));
    pointer(tree.header('ws-a'), 'pointerdown', MOUSE);
    pointer(document, 'pointerup', { ...MOUSE, buttons: 0 });
    tree.header('ws-a').click();
    expect(tree.onHeaderClick).toHaveBeenCalledTimes(1);
  });
});
