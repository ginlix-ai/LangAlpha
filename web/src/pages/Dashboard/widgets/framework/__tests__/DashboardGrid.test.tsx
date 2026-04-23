import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';
import { DashboardGrid } from '../DashboardGrid';
import { registerWidget } from '../WidgetRegistry';
import type { DashboardPrefs } from '../../types';

let latestGridProps: {
  onLayoutChange?: (current: any, allLayouts: any) => void;
} | null = null;

const fitHeightById = new Map<string, (px: number) => void>();

vi.mock('react-grid-layout', () => ({
  ResponsiveGridLayout: (props: any) => {
    latestGridProps = props;
    return <div data-testid="mock-rgl">{props.children}</div>;
  },
  useContainerWidth: () => ({ width: 1280, mounted: true, containerRef: { current: null } }),
}));

vi.mock('../WidgetFrame', () => ({
  WidgetFrame: ({ instance, onFitHeight, children }: any) => {
    if (onFitHeight) fitHeightById.set(instance.id, onFitHeight);
    return <div data-testid={`frame-${instance.id}`}>{children}</div>;
  },
}));

function TestWidget() {
  return <div>test widget</div>;
}

registerWidget({
  type: 'test.fit-height',
  title: 'Test Fit Height',
  category: 'intel',
  icon: (() => null) as any,
  component: TestWidget,
  defaultConfig: {},
  defaultSize: { w: 8, h: 18 },
  minSize: { w: 4, h: 15 },
  maxSize: { w: 12, h: 44 },
  fitToContent: true,
});

function makePrefs(): DashboardPrefs {
  return {
    version: 1,
    mode: 'custom',
    widgets: [{ id: 'widget-1', type: 'test.fit-height', config: {} }],
    layouts: {
      lg: [{ i: 'widget-1', x: 0, y: 0, w: 8, h: 18, minW: 4, minH: 18, maxW: 12, maxH: 18 }],
      md: [{ i: 'widget-1', x: 0, y: 0, w: 8, h: 18, minW: 4, minH: 18, maxW: 12, maxH: 18 }],
    },
  };
}

describe('DashboardGrid fit-to-content height locking', () => {
  beforeEach(() => {
    latestGridProps = null;
    fitHeightById.clear();
  });

  it('preserves the fitted height when RGL emits a stale onLayoutChange height during a width drag', () => {
    const onChange = vi.fn();

    render(
      <DashboardGrid
        prefs={makePrefs()}
        editMode
        onChange={onChange}
        onOpenSettings={() => {}}
      />
    );

    fitHeightById.get('widget-1')?.(462);

    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange.mock.calls[0][0].layouts.lg[0]).toMatchObject({ w: 8, h: 20, minH: 20, maxH: 20 });

    latestGridProps?.onLayoutChange?.([], {
      lg: [{ i: 'widget-1', x: 0, y: 0, w: 5, h: 18, minW: 4, minH: 18, maxW: 12, maxH: 18 }],
      md: [{ i: 'widget-1', x: 0, y: 0, w: 5, h: 18, minW: 4, minH: 18, maxW: 12, maxH: 18 }],
    });

    expect(onChange).toHaveBeenCalledTimes(2);
    expect(onChange.mock.calls[1][0].layouts.lg[0]).toMatchObject({ w: 5, h: 20, minH: 20, maxH: 20 });
  });
});
