import { Component as ReactComponent, Suspense, type ErrorInfo, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { isStaleBuildError, reportStaleBuild } from '@/lib/staleBuild';
import type { WidgetDefinition, WidgetInstance } from '../types';

function WidgetFallback() {
  const { t } = useTranslation();
  return (
    <div
      className="h-full w-full flex items-center justify-center text-[0.6875rem]"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {t('dashboard.widgets.common.loading')}
    </div>
  );
}

// A function component so the message follows a language switch: the boundary
// is a class, and its parent's compiled JSX does not re-render it on one.
function WidgetErrorFallback({ widgetType }: { widgetType: string }) {
  const { t } = useTranslation();
  return (
    <div
      className="h-full w-full flex flex-col items-center justify-center px-3 py-2 text-center gap-1"
      style={{ color: 'var(--color-text-tertiary)', fontSize: '0.6875rem', lineHeight: 1.4 }}
    >
      <span style={{ fontWeight: 500 }}>{t('dashboard.widgets.frame.errorBoundary')}</span>
      <span style={{ opacity: 0.75 }}>{widgetType}</span>
    </div>
  );
}

/** Isolates one widget's render failure so a bug in, say, ChartWidget can't
 *  blank out every other widget on the dashboard. */
class WidgetErrorBoundary extends ReactComponent<
  { widgetType: string; children: ReactNode },
  { error: Error | null }
> {
  override state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  override componentDidCatch(error: Error, info: ErrorInfo) {
    // A widget's chunk gone after a deploy is caught here, short of the
    // route's stale-build boundary, so the reload is offered from here: the
    // rest of the page stays up, and the toast carries the Reload button.
    if (isStaleBuildError(error)) {
      reportStaleBuild('widget');
      return;
    }
    console.error(`[widget:${this.props.widgetType}] render error`, error, info.componentStack);
  }
  override render() {
    if (this.state.error) {
      return <WidgetErrorFallback widgetType={this.props.widgetType} />;
    }
    return this.props.children;
  }
}

/** A widget as the dashboard draws it, in the grid or the phone's column:
 *  isolated from its siblings' failures, with a placeholder while its chunk loads. */
export function WidgetBody({
  definition,
  instance,
  updateConfig,
}: {
  definition: WidgetDefinition<unknown>;
  instance: WidgetInstance<unknown>;
  updateConfig: (patch: Partial<unknown>) => void;
}) {
  const Component = definition.component;
  return (
    <WidgetErrorBoundary widgetType={definition.type}>
      <Suspense fallback={<WidgetFallback />}>
        <Component instance={instance} updateConfig={updateConfig} />
      </Suspense>
    </WidgetErrorBoundary>
  );
}
