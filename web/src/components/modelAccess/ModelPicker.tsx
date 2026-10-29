import { useId, useState } from 'react';
import { ChevronDown, Plus, RefreshCw, Search, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { CheckboxChoice } from '@/components/ui/choice';
import { Input } from '@/components/ui/input';
import { Disclosure } from '@/components/ui/Disclosure';
import { useLocale } from '@/hooks/useLocale';
import { createFormatter } from '@/lib/format';
import { cn } from '@/lib/utils';
import { modelLabel } from '@/lib/modelLabel';
import { isSubscriptionProvider } from '@/api/oauth';
import type { CustomModelEntry } from '@/components/model/types';
import type { ListedModel } from '@/api/model';
import { CustomModelFields, useCustomModelDraft } from './CustomModelFields';
import { QuietNote, StatusLabel } from './parts';
import type { ModelPickerState } from './useModelPicker';

const compact = createFormatter({ notation: 'compact', maximumFractionDigits: 1 });
const usd = createFormatter({ style: 'currency', currency: 'USD', maximumFractionDigits: 2 });

/** Search results shown at once; a narrower search finds the rest. */
const RESULT_CAP = 50;

function PickRow({
  checked,
  onToggle,
  label,
  meta,
  onRemove,
  removeLabel,
}: {
  checked: boolean;
  onToggle: () => void;
  label: string;
  meta?: React.ReactNode;
  onRemove?: () => void;
  removeLabel?: string;
}) {
  return (
    <div className="flex items-center">
      <CheckboxChoice
        size="md"
        checked={checked}
        onCheckedChange={onToggle}
        className="min-w-0 flex-1 items-center gap-3 rounded-md px-3 py-2 transition-colors hover:bg-(--color-bg-hover)"
      >
        <span className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-0.5">
          <span className="truncate text-sm" style={{ color: 'var(--color-text-primary)' }}>{label}</span>
          {meta}
        </span>
      </CheckboxChoice>
      {onRemove && (
        <button
          type="button"
          onClick={onRemove}
          aria-label={removeLabel}
          className="shrink-0 rounded p-1.5 transition-colors hover:bg-(--color-bg-hover)"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <X aria-hidden="true" className="h-3.5 w-3.5" />
        </button>
      )}
    </div>
  );
}

function Tag({ children }: { children: React.ReactNode }) {
  return (
    <span className="rounded px-1.5 py-0.5 text-[0.6875rem]" style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-tertiary)' }}>
      {children}
    </span>
  );
}

/** An untuned model from the provider's list, with its context and price in
 *  the quiet register of a caption. */
function ListedRow({ picker, model }: { picker: ModelPickerState; model: ListedModel }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const parts: string[] = [];
  if (model.context_window) parts.push(t('modelAccess.models.context', { size: compact(model.context_window, locale) }));
  if (model.pricing?.input != null && model.pricing?.output != null) {
    parts.push(t('modelAccess.models.price', { input: usd(model.pricing.input, locale), output: usd(model.pricing.output, locale) }));
  }
  const name = picker.nameOf(model);
  return (
    <PickRow
      checked={picker.selected.has(name)}
      onToggle={() => picker.toggle(name)}
      label={model.display_name || model.id}
      meta={(
        <>
          {/* A model the gateway's catalog describes runs tuned by it, known or not. */}
          {(model.known || !model.described) && (
            <Tag>{t(model.known ? 'modelAccess.models.known' : 'modelAccess.models.notTuned')}</Tag>
          )}
          {parts.length > 0 && (
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)', fontVariantNumeric: 'tabular-nums' }}>
              {parts.join(' · ')}
            </span>
          )}
        </>
      )}
    />
  );
}

function CustomModelForm({ provider, onAdd, onClose }: { provider: string; onAdd: (entry: CustomModelEntry) => void; onClose: () => void }) {
  const { t } = useTranslation();
  const draft = useCustomModelDraft();
  const submit = () => {
    const entry = draft.toEntry(provider);
    if (!entry) return;
    onAdd(entry);
    onClose();
  };
  return (
    <div className="flex flex-col gap-3 rounded-lg p-3" style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
      <CustomModelFields state={draft} onSubmit={submit} autoFocus />
      <div className="flex items-center justify-end gap-2">
        <Button type="button" variant="ghost" size="sm" onClick={onClose}>{t('common.cancel')}</Button>
        <Button type="button" size="sm" disabled={!draft.ready} onClick={submit}>{t('setup.add')}</Button>
      </div>
    </div>
  );
}

const FALLBACK_NOTE: Record<string, string> = {
  needs_reauth: 'modelAccess.models.fallbackReauth',
  refresh_unavailable: 'modelAccess.models.fallbackRetry',
  unauthorized: 'modelAccess.models.fallbackRefused',
  no_api_access: 'modelAccess.models.fallbackNoApiAccess',
};

/** The picker's view: what is ready, what is not on this connection, the
 *  untuned rest behind Show all (searched, for OpenRouter), and the user's own. */
export function ModelPickerList({ picker, labelledBy }: { picker: ModelPickerState; labelledBy?: string }) {
  const { t } = useTranslation();
  const searchId = useId();
  const [showAdd, setShowAdd] = useState(false);
  const [filter, setFilter] = useState('');
  const [showAll, setShowAll] = useState(false);
  const othersId = useId();
  const label = (m: string) => picker.manifestMeta[m]?.display_name || modelLabel(m, picker.metadata);

  if (picker.loading) return <StatusLabel status="working" className="py-6" />;

  const filterWords = filter.toLowerCase().split(/\s+/).filter(Boolean);
  const filteredOthers = filterWords.length === 0
    ? picker.others
    : picker.others.filter((m) => filterWords.every((w) => `${m.id} ${m.display_name ?? ''}`.toLowerCase().includes(w)));
  // A model already listed above, as ready or as the user's own, shows there only.
  const shown = new Set([...picker.ready.map((r) => r.name), ...picker.yours]);
  const visibleOthers = filteredOthers.filter((m) => !shown.has(picker.nameOf(m)));
  const knownSuggestions = picker.others.filter((m) => m.known).slice(0, 8);
  // Search shows the known suggestions until there is a search, and again
  // under a search that found nothing.
  const noResults = !!picker.searchedFor && picker.results.length === 0 && !picker.searching;
  const searchRows = picker.searchedFor && !noResults ? picker.results.slice(0, RESULT_CAP) : knownSuggestions;
  const searchNote = noResults
    ? t('modelAccess.models.noResults', { query: picker.searchedFor })
    : !picker.searchedFor && knownSuggestions.length > 0 ? t('modelAccess.models.suggested') : null;

  return (
    <div className="flex flex-col gap-4" role="group" aria-labelledby={labelledBy}>
      {/* What the connection's own list says, and how fresh it is. */}
      {picker.listable && (
        <div className="flex flex-wrap items-center justify-between gap-2">
          {picker.listing ? (
            <StatusLabel status="working" />
          ) : picker.listFallback ? (
            <QuietNote>{t(FALLBACK_NOTE[picker.fallbackReason ?? ''] ?? 'modelAccess.models.fallback')}</QuietNote>
          ) : (
            <QuietNote>{t('modelAccess.models.fromList')}</QuietNote>
          )}
          <Button type="button" variant="ghost" size="sm" onClick={picker.refresh} disabled={picker.refreshing} className="h-7 gap-1.5 px-2 text-xs">
            <RefreshCw aria-hidden="true" className="h-3 w-3" />
            {t('modelAccess.models.refresh')}
          </Button>
        </div>
      )}

      {picker.ready.length > 0 && (
        <section className="flex flex-col gap-1">
          <div className="flex items-center justify-between gap-2">
            <h4 className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>{t('modelAccess.models.ready')}</h4>
            {/* Once the list has settled, so it picks what the provider confirmed. */}
            {picker.ready.length > 1 && !picker.listing && (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={picker.selectAllReady}
                disabled={picker.ready.every((r) => picker.selected.has(r.name))}
                className="h-7 px-2 text-xs"
              >
                {t('modelAccess.models.selectAll')}
              </Button>
            )}
          </div>
          {picker.ready.map((r) => r.listed
            ? <ListedRow key={r.name} picker={picker} model={r.listed} />
            : <PickRow key={r.name} checked={picker.selected.has(r.name)} onToggle={() => picker.toggle(r.name)} label={label(r.name)} />)}
        </section>
      )}

      {picker.missing.length > 0 && (
        <QuietNote>
          {t(isSubscriptionProvider(picker.provider) ? 'modelAccess.models.missingPlan' : 'modelAccess.models.missingKey', {
            models: picker.missing.map(label).join(', '),
          })}
        </QuietNote>
      )}

      {picker.yours.length > 0 && (
        <section className="flex flex-col gap-1">
          <h4 className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>{t('modelAccess.models.yours')}</h4>
          {picker.yours.map((m) => (
            <PickRow
              key={m}
              checked={picker.selected.has(m)}
              onToggle={() => picker.toggle(m)}
              label={m}
              meta={<Tag>{t('modelAccess.models.notTuned')}</Tag>}
              onRemove={() => picker.removeYours(m)}
              removeLabel={t('setup.removeModel', { model: m })}
            />
          ))}
        </section>
      )}

      {picker.searchFirst ? (
        <section className="flex flex-col gap-2">
          <label htmlFor={searchId} className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('modelAccess.models.searchLabel')}
          </label>
          <div className="relative">
            <Search aria-hidden="true" className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2" style={{ color: 'var(--color-text-tertiary)' }} />
            <Input
              id={searchId}
              type="search"
              value={picker.query}
              onChange={(e) => picker.setQuery(e.target.value)}
              placeholder={t('modelAccess.models.searchPlaceholder')}
              className="pl-8"
              autoComplete="off"
              spellCheck={false}
            />
          </div>
          {picker.searching && <StatusLabel status="working" />}
          {(searchNote || searchRows.length > 0) && (
            <div className="flex flex-col gap-1">
              {searchNote && <QuietNote>{searchNote}</QuietNote>}
              {searchRows.map((m) => <ListedRow key={m.id} picker={picker} model={m} />)}
              {picker.searchedFor && picker.results.length > RESULT_CAP && (
                <QuietNote>{t('modelAccess.models.moreResults', { count: picker.results.length - RESULT_CAP })}</QuietNote>
              )}
            </div>
          )}
        </section>
      ) : (
        picker.others.length > 0 && (
          <section className="flex flex-col">
            <button
              type="button"
              aria-expanded={showAll}
              aria-controls={othersId}
              onClick={() => setShowAll((v) => !v)}
              className="inline-flex items-center gap-1 self-start text-xs font-medium hover:opacity-80"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              <ChevronDown aria-hidden="true" className={cn('h-3.5 w-3.5 transition-transform', showAll && 'rotate-180')} />
              {t(showAll ? 'modelAccess.models.hideAll' : 'modelAccess.models.showAll', { count: visibleOthers.length })}
            </button>
          <Disclosure open={showAll} id={othersId}>
            <div className="flex flex-col gap-2 pt-2">
              <QuietNote>{t('modelAccess.models.notTunedNote')}</QuietNote>
              {picker.others.length > 12 && (
                <Input
                  type="search"
                  value={filter}
                  onChange={(e) => setFilter(e.target.value)}
                  placeholder={t('modelAccess.models.filterPlaceholder')}
                  aria-label={t('modelAccess.models.filterPlaceholder')}
                  autoComplete="off"
                  spellCheck={false}
                />
              )}
              <div className="flex max-h-72 flex-col gap-1 overflow-y-auto">
                {visibleOthers.map((m) => <ListedRow key={m.id} picker={picker} model={m} />)}
              </div>
            </div>
          </Disclosure>
          </section>
        )
      )}

      {picker.localError && <QuietNote>{t('setup.errorFetchModels')}</QuietNote>}

      {showAdd ? (
        <CustomModelForm provider={picker.provider} onAdd={picker.addManual} onClose={() => setShowAdd(false)} />
      ) : (
        <button
          type="button"
          onClick={() => setShowAdd(true)}
          className="inline-flex items-center gap-1 self-start text-xs font-medium underline underline-offset-2 hover:opacity-80"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          <Plus aria-hidden="true" className="h-3 w-3" />
          {t('setup.addModel')}
        </button>
      )}

      {picker.error && (
        <p role="alert" className="text-sm" style={{ color: 'var(--color-loss)' }}>{picker.error}</p>
      )}
    </div>
  );
}
