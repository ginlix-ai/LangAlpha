import { renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { PanelTarget } from '../types';
import type { FileTabsApi } from '../useFileTabs';
import type { PreviewsApi } from '../usePreviews';
import { usePanelTarget } from '../usePanelTarget';

function setup(drive: boolean | null) {
  const openDrive = vi.fn();
  const onTargetHandled = vi.fn();
  const target: PanelTarget = { kind: 'drive', seq: 1, loc: { dir: 'research' } };
  const view = renderHook((props: { drive: boolean | null }) => usePanelTarget({
    target,
    tabs: { openDrive } as unknown as FileTabsApi,
    previews: {} as PreviewsApi,
    openFileRef: vi.fn(),
    cancelPending: vi.fn(),
    clearSearch: vi.fn(),
    setTreeOpen: vi.fn(),
    setScopeDir: vi.fn(),
    onTargetHandled,
    drive: props.drive,
  }), { initialProps: { drive } });
  return { ...view, openDrive, onTargetHandled };
}

describe('usePanelTarget: a Drive ask', () => {
  it('waits for the flags while they are read, then opens', () => {
    const { rerender, openDrive, onTargetHandled } = setup(null);
    expect(openDrive).not.toHaveBeenCalled();
    expect(onTargetHandled).not.toHaveBeenCalled();

    rerender({ drive: true });
    expect(openDrive).toHaveBeenCalledWith(expect.objectContaining({ loc: { dir: 'research' } }));
    expect(onTargetHandled).toHaveBeenCalledWith(1);
  });

  it('opens nothing where the person has no Drive, and lets the ask go', () => {
    const { openDrive, onTargetHandled } = setup(false);
    expect(openDrive).not.toHaveBeenCalled();
    expect(onTargetHandled).toHaveBeenCalledWith(1);
  });
});
