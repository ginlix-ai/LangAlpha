import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import {
  Workflow,
  ArrowUpRight,
  Pause,
  Play,
  AlertTriangle,
  Zap,
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useAutomations } from '@/pages/Automations/hooks/useAutomations';
import { useAutomationMutations } from '@/pages/Automations/hooks/useAutomationMutations';
import type { Automation } from '@/types/automation';
import { registerWidget } from '../framework/WidgetRegistry';
import { AutomationsConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';

type AutomationsConfig = { limit?: number };

type BucketKey = 'active' | 'paused' | 'error';

const BUCKET_KEY: Record<BucketKey, string> = {
  active: 'dashboard.widgets.automations.bucket_active',
  paused: 'dashboard.widgets.automations.bucket_paused',
  error: 'dashboard.widgets.automations.bucket_attention',
};

function formatRelativeRun(ts?: string | null): string {
  if (!ts) return '';
  const then = new Date(ts).getTime();
  if (Number.isNaN(then)) return '';
  const diff = then - Date.now();
  const abs = Math.abs(diff);
  const mins = Math.round(abs / 60000);
  if (mins < 1) return i18n.t('dashboard.widgets.common.relativeNow');
  let when: string;
  if (mins < 60) when = `${mins}m`;
  else {
    const hrs = Math.round(mins / 60);
    if (hrs < 24) when = `${hrs}h`;
    else {
      const days = Math.round(hrs / 24);
      if (days < 7) when = `${days}d`;
      else when = `${Math.round(days / 7)}w`;
    }
  }
  const key = diff >= 0
    ? 'dashboard.widgets.common.relativeFuture'
    : 'dashboard.widgets.common.relativePast';
  return i18n.t(key, { when });
}

function triggerLabel(a: Automation): string {
  if (a.trigger_type === 'price') {
    const sym = a.trigger_config?.symbol;
    return sym
      ? i18n.t('dashboard.widgets.automations.trigger.priceSymbol', { symbol: sym })
      : i18n.t('dashboard.widgets.automations.trigger.price');
  }
  if (a.trigger_type === 'once') return i18n.t('dashboard.widgets.automations.trigger.once');
  if (a.schedule) return i18n.t('dashboard.widgets.automations.trigger.cron');
  return i18n.t('dashboard.widgets.automations.trigger.auto');
}

function AutomationRow({
  automation,
  onOpen,
  onToggle,
  onRun,
  busy,
}: {
  automation: Automation;
  onOpen: () => void;
  onToggle: () => void;
  onRun: () => void;
  busy: boolean;
}) {
  const { t } = useTranslation();
  const isActive = automation.status === 'active';
  const isPaused = automation.status === 'paused';
  const isError = automation.status === 'error';

  const dotColor = isError
    ? 'var(--color-loss)'
    : isActive
      ? 'var(--color-accent-primary)'
      : 'var(--color-text-tertiary)';

  const nextRun = isActive ? formatRelativeRun(automation.next_run_at) : '';
  const lastRun = !isActive && automation.last_run_at ? formatRelativeRun(automation.last_run_at) : '';
  const rightText = nextRun
    || (lastRun
      ? t('dashboard.widgets.automations.lastRun', { when: lastRun })
      : isPaused
        ? t('dashboard.widgets.automations.statusPaused')
        : isError
          ? t('dashboard.widgets.automations.statusError')
          : '');

  return (
    <div
      className="group relative w-full flex items-center gap-3 py-2 pr-2 pl-3 rounded-md transition-colors duration-150"
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
      }}
    >
      <button
        type="button"
        onClick={onOpen}
        className="flex-1 min-w-0 flex items-center gap-3 text-left"
      >
        {isError ? (
          <AlertTriangle
            className="h-3 w-3 flex-shrink-0"
            style={{ color: 'var(--color-loss)' }}
          />
        ) : (
          <span
            aria-hidden
            className="flex-shrink-0 rounded-full"
            style={{
              width: 6,
              height: 6,
              backgroundColor: dotColor,
              opacity: isActive ? 1 : 0.55,
              boxShadow: isActive ? `0 0 0 3px color-mix(in srgb, ${dotColor} 18%, transparent)` : 'none',
            }}
          />
        )}
        <span className="flex-1 min-w-0 flex flex-col gap-0.5 overflow-hidden">
          <span
            className="text-[13px] truncate leading-tight font-medium"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {automation.name || t('dashboard.widgets.automations.untitled')}
          </span>
          <span
            className="text-[10px] uppercase tracking-wider truncate"
            style={{ color: 'var(--color-text-tertiary)', opacity: 0.85 }}
          >
            {triggerLabel(automation)}
          </span>
        </span>
      </button>

      {rightText ? (
        <span
          className="text-[10.5px] dashboard-mono uppercase tracking-wider tabular-nums flex-shrink-0 group-hover:opacity-0 transition-opacity"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {rightText}
        </span>
      ) : null}

      <div className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onRun();
          }}
          disabled={busy}
          title={t('dashboard.widgets.automations.runNow')}
          aria-label={t('dashboard.widgets.automations.runNow')}
          className="p-1 rounded-md transition-colors disabled:opacity-50"
          style={{ color: 'var(--color-text-secondary)' }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = 'var(--color-bg-subtle)';
            e.currentTarget.style.color = 'var(--color-text-primary)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = 'transparent';
            e.currentTarget.style.color = 'var(--color-text-secondary)';
          }}
        >
          <Zap className="h-3.5 w-3.5" fill="currentColor" />
        </button>
        {!isError && (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onToggle();
            }}
            disabled={busy}
            title={isActive ? t('dashboard.widgets.automations.pause') : t('dashboard.widgets.automations.resume')}
            aria-label={isActive ? t('dashboard.widgets.automations.pauseAria') : t('dashboard.widgets.automations.resumeAria')}
            className="p-1 rounded-md transition-colors disabled:opacity-50"
            style={{ color: 'var(--color-text-secondary)' }}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = 'var(--color-bg-subtle)';
              e.currentTarget.style.color = 'var(--color-text-primary)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.backgroundColor = 'transparent';
              e.currentTarget.style.color = 'var(--color-text-secondary)';
            }}
          >
            {isActive ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
          </button>
        )}
      </div>
    </div>
  );
}

function BucketSection({
  label,
  items,
  onOpen,
  onToggle,
  onRun,
  busy,
}: {
  label: string;
  items: Automation[];
  onOpen: (a: Automation) => void;
  onToggle: (a: Automation) => void;
  onRun: (a: Automation) => void;
  busy: boolean;
}) {
  if (items.length === 0) return null;
  return (
    <div className="flex flex-col">
      <div
        className="flex items-baseline gap-2 px-3 mb-0.5 sticky top-0 z-10 py-1 -mx-1"
        style={{ backgroundColor: 'var(--color-bg-card)' }}
      >
        <span
          className="text-[9.5px] font-semibold uppercase tracking-[0.16em]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {label}
        </span>
        <span
          className="flex-1 h-px"
          style={{ backgroundColor: 'var(--color-border-muted)' }}
        />
        <span
          className="text-[10px] dashboard-mono tabular-nums"
          style={{ color: 'var(--color-text-tertiary)', opacity: 0.7 }}
        >
          {String(items.length).padStart(2, '0')}
        </span>
      </div>
      <div className="flex flex-col">
        {items.map((a) => (
          <AutomationRow
            key={a.id}
            automation={a}
            busy={busy}
            onOpen={() => onOpen(a)}
            onToggle={() => onToggle(a)}
            onRun={() => onRun(a)}
          />
        ))}
      </div>
    </div>
  );
}

function AutomationsWidget({ instance }: WidgetRenderProps<AutomationsConfig>) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const limit = instance.config.limit ?? 8;

  const { automations, loading, refetch } = useAutomations();
  const { pause, resume, trigger, loading: mutating } = useAutomationMutations(refetch);

  const grouped = useMemo(() => {
    const buckets: Record<BucketKey, Automation[]> = { active: [], paused: [], error: [] };
    for (const a of automations) {
      if (a.status === 'error') buckets.error.push(a);
      else if (a.status === 'paused') buckets.paused.push(a);
      else buckets.active.push(a);
    }
    buckets.active.sort((a, b) => {
      const at = a.next_run_at ? new Date(a.next_run_at).getTime() : Infinity;
      const bt = b.next_run_at ? new Date(b.next_run_at).getTime() : Infinity;
      return at - bt;
    });
    const total = buckets.error.length + buckets.active.length + buckets.paused.length;
    if (total > limit) {
      const keep = (arr: Automation[], n: number) => arr.slice(0, n);
      const errorSlice = keep(buckets.error, limit);
      const remaining = Math.max(0, limit - errorSlice.length);
      const activeSlice = keep(buckets.active, remaining);
      const pausedSlice = keep(buckets.paused, Math.max(0, remaining - activeSlice.length));
      return { error: errorSlice, active: activeSlice, paused: pausedSlice };
    }
    return buckets;
  }, [automations, limit]);

  const total = automations.length;

  const handleOpen = (a: Automation) => {
    navigate(`/automations?id=${encodeURIComponent(a.id)}`);
  };

  const handleToggle = async (a: Automation) => {
    try {
      if (a.status === 'active') await pause(a.id);
      else await resume(a.id);
    } catch {
      // toast already shown by mutation hook
    }
  };

  const handleRun = async (a: Automation) => {
    try {
      await trigger(a.id);
    } catch {
      // toast already shown
    }
  };

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <Workflow
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.automations.header')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {total}
          </span>
        </div>
        <button
          type="button"
          onClick={() => navigate('/automations')}
          className="group flex items-center gap-1 text-[11px] uppercase tracking-wider transition-colors"
          style={{ color: 'var(--color-text-tertiary)' }}
          onMouseEnter={(e) => {
            e.currentTarget.style.color = 'var(--color-text-primary)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.color = 'var(--color-text-tertiary)';
          }}
        >
          <span>{t('dashboard.widgets.automations.viewAll')}</span>
          <ArrowUpRight className="h-3 w-3 transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
        </button>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto -mx-1 px-1">
        {loading && total === 0 ? (
          <div className="space-y-1.5 py-1">
            {Array.from({ length: 5 }).map((_, i) => (
              <div
                key={i}
                className="h-8 rounded animate-pulse"
                style={{
                  backgroundColor: 'var(--color-bg-subtle)',
                  opacity: 1 - i * 0.1,
                }}
              />
            ))}
          </div>
        ) : total === 0 ? (
          <div className="h-full flex flex-col items-center justify-center gap-3 py-6">
            <div
              className="h-9 w-9 rounded-full flex items-center justify-center"
              style={{ backgroundColor: 'var(--color-bg-subtle)' }}
            >
              <Workflow className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
            <div className="text-center">
              <div
                className="dashboard-mono text-sm mb-0.5"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('dashboard.widgets.automations.empty')}
              </div>
              <button
                type="button"
                onClick={() => navigate('/automations')}
                className="text-[11px] uppercase tracking-wider underline-offset-4 hover:underline"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {t('dashboard.widgets.automations.emptyCta')}
              </button>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            <BucketSection
              label={t(BUCKET_KEY.error)}
              items={grouped.error}
              busy={mutating}
              onOpen={handleOpen}
              onToggle={handleToggle}
              onRun={handleRun}
            />
            <BucketSection
              label={t(BUCKET_KEY.active)}
              items={grouped.active}
              busy={mutating}
              onOpen={handleOpen}
              onToggle={handleToggle}
              onRun={handleRun}
            />
            <BucketSection
              label={t(BUCKET_KEY.paused)}
              items={grouped.paused}
              busy={mutating}
              onOpen={handleOpen}
              onToggle={handleToggle}
              onRun={handleRun}
            />
          </div>
        )}
      </div>
    </div>
  );
}

registerWidget<AutomationsConfig>({
  type: 'automations.list',
  titleKey: 'dashboard.widgets.automations.title',
  descriptionKey: 'dashboard.widgets.automations.description',
  category: 'personal',
  icon: Workflow,
  component: AutomationsWidget,
  defaultConfig: { limit: 8 },
  configSchema: AutomationsConfigSchema,
  defaultSize: { w: 4, h: 22 },
  minSize: { w: 3, h: 14 },
});

export default AutomationsWidget;
