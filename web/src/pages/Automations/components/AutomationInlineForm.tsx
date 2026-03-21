import React, { useState, useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronDown } from 'lucide-react';
import { AnimatedTabs } from '@/components/ui/animated-tabs';
import {
  Select,
  SelectItem,
  SelectListBox,
  SelectPopover,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/aria-select';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { searchStocks } from '@/lib/marketUtils';
import { toast } from '@/components/ui/use-toast';
import CronScheduleBuilder from './CronScheduleBuilder';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import {
  type FormState,
  COMMON_TIMEZONES,
  PRICE_CONDITION_TYPES,
  PRICE_REFERENCE_OPTIONS,
  RETRIGGER_MODES,
  isPctCondition,
  formStateToPayload,
} from '../utils/templates';

// ── Shared styles ──────────────────────────────────────────

const inputStyle = {
  backgroundColor: 'var(--color-bg-card)',
  borderColor: 'var(--color-border-default)',
};

const labelClass = 'form-label';

// ── TickerAutocomplete ─────────────────────────────────────

const INDEX_SYMBOLS: Array<{ symbol: string; name: string }> = [
  { symbol: 'SPX', name: 'S&P 500' },
  { symbol: 'DJI', name: 'Dow Jones Industrial Average' },
  { symbol: 'COMP', name: 'Nasdaq Composite' },
  { symbol: 'NDX', name: 'Nasdaq 100' },
  { symbol: 'RUT', name: 'Russell 2000' },
  { symbol: 'VIX', name: 'CBOE Volatility Index' },
];

const INDEX_SYMBOL_SET = new Set(INDEX_SYMBOLS.map((i) => i.symbol));

export function isIndexSymbol(symbol: string): boolean {
  return INDEX_SYMBOL_SET.has(symbol.toUpperCase());
}

interface SearchResult { symbol: string; name?: string; exchangeShortName?: string; isIndex?: boolean }

interface TickerAutocompleteProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
}

function TickerAutocomplete({ value, onChange, placeholder = 'AAPL, SPX...' }: TickerAutocompleteProps) {
  const { t } = useTranslation();
  const [query, setQuery] = useState(value);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Sync external value changes (e.g., template switch)
  useEffect(() => {
    setQuery(value);
  }, [value]);

  // Debounced search — merge index matches + stock API results
  useEffect(() => {
    if (!query || query.length < 1) {
      setResults([]);
      return;
    }
    const q = query.toUpperCase();

    // Instant: filter static index list
    const indexMatches: SearchResult[] = INDEX_SYMBOLS
      .filter((idx) => idx.symbol.includes(q) || idx.name.toUpperCase().includes(q))
      .map((idx) => ({ symbol: idx.symbol, name: idx.name, exchangeShortName: 'INDEX', isIndex: true }));

    // Show index matches immediately
    if (indexMatches.length > 0) {
      setResults(indexMatches);
      setShowDropdown(true);
    }

    // Debounced: fetch stock results and merge
    const timer = setTimeout(async () => {
      const data = await searchStocks(query, 20);
      const stockResults: SearchResult[] = (data.results as SearchResult[]).map((r) => ({ ...r, isIndex: false }));
      // Deduplicate: index symbols already shown won't appear in stock results (different symbols)
      setResults([...indexMatches, ...stockResults]);
      if (indexMatches.length > 0 || stockResults.length > 0) setShowDropdown(true);
    }, 300);
    return () => clearTimeout(timer);
  }, [query]);

  // Click outside to close
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div ref={containerRef} className="relative">
      <Input
        value={query}
        onChange={(e) => {
          const val = e.target.value.toUpperCase();
          setQuery(val);
          onChange(val);
          if (val.length >= 1) setShowDropdown(true);
        }}
        onFocus={() => { if (results.length > 0) setShowDropdown(true); }}
        placeholder={placeholder}
        required
        className="placeholder:text-gray-500 border font-mono uppercase"
        style={inputStyle}
      />
      {showDropdown && results.length > 0 && (
        <div
          className="absolute z-50 mt-1 w-full max-h-48 overflow-y-auto rounded-md border shadow-lg"
          style={{
            backgroundColor: 'var(--color-bg-elevated)',
            borderColor: 'var(--color-border-default)',
          }}
        >
          {results.map((item) => (
            <button
              key={item.symbol}
              type="button"
              className="w-full text-left px-3 py-2 text-sm hover:bg-foreground/5 flex items-center gap-2"
              onClick={() => {
                setQuery(item.symbol);
                onChange(item.symbol);
                setShowDropdown(false);
              }}
            >
              <span className="font-mono font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {item.symbol}
              </span>
              {item.name && (
                <span className="truncate" style={{ color: 'var(--color-text-tertiary)' }}>
                  {item.name}
                  {item.exchangeShortName ? ` (${item.exchangeShortName})` : ''}
                </span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Workspace type ─────────────────────────────────────────

interface WorkspaceOption {
  workspace_id: string;
  name: string;
}

// ── AutomationInlineForm ───────────────────────────────────

interface AutomationInlineFormProps {
  initialValues: FormState;
  isEdit: boolean;
  onSubmit: (payload: Record<string, unknown>) => void;
  onCancel: () => void;
  loading: boolean;
}

export default function AutomationInlineForm({
  initialValues,
  isEdit,
  onSubmit,
  onCancel,
  loading,
}: AutomationInlineFormProps) {
  const { t } = useTranslation();
  const [form, setForm] = useState<FormState>(initialValues);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const outerRef = useRef<HTMLDivElement>(null);
  const advancedRef = useRef<HTMLDivElement>(null);

  // Release overflow:hidden after expand animation so box-shadows (card shadow, focus rings) aren't clipped.
  // Re-apply overflow:hidden when any animation starts (collapse needs clipping).
  const releaseOverflow = (ref: React.RefObject<HTMLDivElement | null>) => ({
    onAnimationStart: () => { if (ref.current) ref.current.style.overflow = 'hidden'; },
    onAnimationComplete: () => { if (ref.current && ref.current.offsetHeight > 0) ref.current.style.overflow = 'visible'; },
  });

  const { data: wsData } = useWorkspaces({ limit: 100 });
  const workspaces = (wsData as { workspaces?: WorkspaceOption[] })?.workspaces ?? [];

  const set = (key: keyof FormState) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement> | string) =>
    setForm((f) => ({ ...f, [key]: typeof e === 'string' ? e : e.target.value }));

  const setSelection = (key: keyof FormState) => (k: React.Key | null) =>
    setForm((f) => ({ ...f, [key]: k == null ? '' : String(k) }));

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (form.agent_mode === 'ptc' && !form.workspace_id) {
      setAdvancedOpen(true);
      toast({ variant: 'destructive', description: t('automation.workspaceRequired') });
      return;
    }
    onSubmit(formStateToPayload(form));
  };

  return (
    <motion.div
      ref={outerRef}
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
      exit={{ opacity: 0, height: 0 }}
      transition={{ duration: 0.25, ease: 'easeInOut' }}
      style={{ overflow: 'hidden' }}
      {...releaseOverflow(outerRef)}
    >
      <form
        onSubmit={handleSubmit}
        className="automation-inline-form rounded-xl p-5 flex flex-col gap-5"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-default)',
          boxShadow: 'var(--shadow-card)',
        }}
      >
        {/* ── Basic Section ─────────────────────────────── */}

        {/* Name */}
        <div className="flex flex-col gap-1.5">
          <label className={labelClass}>{t('common.name')}</label>
          <Input
            value={form.name}
            onChange={set('name')}
            placeholder={t('automation.namePlaceholder')}
            required
            className="placeholder:text-gray-500 border"
            style={inputStyle}
          />
        </div>

        {/* Trigger Type */}
        <div className="flex flex-col gap-1.5">
          <label className={labelClass}>{t('automation.triggerType')}</label>
          <AnimatedTabs
            layoutId="trigger_type"
            tabs={[
              { id: 'cron', label: t('automation.cronRecurring') },
              { id: 'once', label: t('automation.once') },
              { id: 'price', label: t('automation.priceAlert') },
            ]}
            value={form.trigger_type}
            onChange={set('trigger_type')}
          />
        </div>

        {/* Trigger-specific basic fields */}
        {form.trigger_type === 'price' && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            {/* Symbol with autocomplete (stocks + indices) */}
            <div className="flex flex-col gap-1.5">
              <label className={labelClass}>{t('automation.priceSymbol')}</label>
              <TickerAutocomplete
                value={form.price_symbol}
                onChange={(val) => setForm((f) => ({ ...f, price_symbol: val }))}
              />
            </div>

            {/* Condition Type */}
            <div className="flex flex-col gap-1.5">
              <label className={labelClass}>{t('automation.priceCondition')}</label>
              <Select aria-label={t('automation.priceCondition')} selectedKey={form.price_condition_type} onSelectionChange={setSelection('price_condition_type')}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectPopover>
                  <SelectListBox>
                    {PRICE_CONDITION_TYPES.map((opt) => (
                      <SelectItem key={opt.value} id={opt.value}>{t(opt.labelKey)}</SelectItem>
                    ))}
                  </SelectListBox>
                </SelectPopover>
              </Select>
            </div>

            {/* Price Value */}
            <div className="flex flex-col gap-1.5">
              <label className={labelClass}>
                {t('automation.priceValue')} ({isPctCondition(form.price_condition_type) ? '%' : '$'})
              </label>
              <Input
                type="number"
                step="any"
                min={0}
                value={form.price_value}
                onChange={set('price_value')}
                placeholder={isPctCondition(form.price_condition_type) ? '5' : '150.00'}
                required
                className="placeholder:text-gray-500 border w-full"
                style={inputStyle}
              />
            </div>

            {/* Reference (only for pct conditions) */}
            {isPctCondition(form.price_condition_type) && (
              <div className="flex flex-col gap-1.5">
                <label className={labelClass}>{t('automation.priceReference')}</label>
                <Select aria-label={t('automation.priceReference')} selectedKey={form.price_reference} onSelectionChange={setSelection('price_reference')}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectPopover>
                    <SelectListBox>
                      {PRICE_REFERENCE_OPTIONS.map((opt) => (
                        <SelectItem key={opt.value} id={opt.value}>{t(opt.labelKey)}</SelectItem>
                      ))}
                    </SelectListBox>
                  </SelectPopover>
                </Select>
              </div>
            )}
          </div>
        )}

        {form.trigger_type === 'cron' && (
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.schedule')}</label>
            <CronScheduleBuilder
              value={form.cron_expression}
              onChange={set('cron_expression')}
            />
          </div>
        )}

        {form.trigger_type === 'once' && (
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.runAt')}</label>
            <Input
              type="datetime-local"
              value={form.next_run_at}
              onChange={set('next_run_at')}
              required
              className="border"
              style={inputStyle}
            />
          </div>
        )}

        {/* Instruction */}
        <div className="flex flex-col gap-1.5">
          <label className={labelClass}>{t('automation.instruction')}</label>
          <Textarea
            value={form.instruction}
            onChange={set('instruction')}
            placeholder={t('automation.instructionPlaceholder')}
            required
            rows={4}
            className="placeholder:text-gray-500 border"
            style={inputStyle}
          />
        </div>

        {/* ── Advanced Section ──────────────────────────── */}

        <div>
          <button
            type="button"
            onClick={() => setAdvancedOpen(!advancedOpen)}
            className="w-full flex items-center justify-between text-sm font-medium cursor-pointer"
            style={{ color: 'var(--color-text-primary)' }}
          >
            <span>{t('automation.advancedOptions')}</span>
            <ChevronDown
              className="h-4 w-4 transition-transform"
              style={{
                color: 'var(--color-text-tertiary)',
                transform: advancedOpen ? 'rotate(180deg)' : 'rotate(0deg)',
              }}
            />
          </button>

          <AnimatePresence>
            {advancedOpen && (
            <motion.div
              ref={advancedRef}
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              transition={{ duration: 0.2 }}
              style={{ overflow: 'hidden' }}
              {...releaseOverflow(advancedRef)}
              className="flex flex-col gap-4 mt-3"
            >
              {/* Agent Mode */}
              <div className="flex flex-col gap-1.5">
                <label className={labelClass}>{t('automation.agentMode')}</label>
                <AnimatedTabs
                  layoutId="agent_mode"
                  tabs={[
                    { id: 'flash', label: t('automation.flash') },
                    { id: 'ptc', label: t('automation.ptcSandbox') },
                  ]}
                  value={form.agent_mode}
                  onChange={set('agent_mode')}
                />
              </div>

              {/* Workspace */}
              {form.agent_mode === 'ptc' && (
                <div className="flex flex-col gap-1.5">
                  <label className={labelClass}>{t('thread.workspace')}</label>
                  <Select
                    aria-label={t('thread.workspace')}
                    selectedKey={form.workspace_id || null}
                    onSelectionChange={setSelection('workspace_id')}
                    placeholder={t('automation.selectWorkspace')}
                  >
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectPopover>
                      <SelectListBox>
                        {workspaces.map((ws) => (
                          <SelectItem key={ws.workspace_id} id={ws.workspace_id}>
                            {ws.name}
                          </SelectItem>
                        ))}
                      </SelectListBox>
                    </SelectPopover>
                  </Select>
                </div>
              )}

              {/* Timezone (hidden for price triggers) */}
              {form.trigger_type !== 'price' && (
                <div className="flex flex-col gap-1.5">
                  <label className={labelClass}>{t('settings.timezone')}</label>
                  <Select aria-label={t('settings.timezone')} selectedKey={form.timezone} onSelectionChange={setSelection('timezone')}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectPopover>
                      <SelectListBox>
                        {COMMON_TIMEZONES.map((tz) => (
                          <SelectItem key={tz} id={tz}>{tz}</SelectItem>
                        ))}
                      </SelectListBox>
                    </SelectPopover>
                  </Select>
                </div>
              )}

              {/* Thread Strategy */}
              <div className="flex flex-col gap-1.5">
                <label className={labelClass}>{t('automation.threadStrategy')}</label>
                <AnimatedTabs
                  layoutId="thread_strategy"
                  tabs={[
                    { id: 'new', label: t('automation.newThreadEachRun') },
                    { id: 'continue', label: t('automation.continueExisting') },
                  ]}
                  value={form.thread_strategy}
                  onChange={set('thread_strategy')}
                />
              </div>

              {/* Delivery */}
              <div className="flex flex-col gap-1.5">
                <label className={labelClass}>{t('automation.delivery')}</label>
                <AnimatedTabs
                  layoutId="delivery_method"
                  tabs={[
                    { id: '', label: t('automation.deliverNone') },
                    { id: 'slack', label: t('automation.deliverToSlack') },
                    { id: 'discord', label: t('automation.deliverToDiscord') },
                  ]}
                  value={form.delivery_method}
                  onChange={set('delivery_method')}
                />
              </div>

              {/* Retrigger Mode (price only) */}
              {form.trigger_type === 'price' && (
                <>
                  <div className="flex flex-col gap-1.5">
                    <label className={labelClass}>{t('automation.priceRetrigger')}</label>
                    <AnimatedTabs
                      layoutId="price_retrigger"
                      tabs={RETRIGGER_MODES.map((opt) => ({ id: opt.value, label: t(opt.labelKey) }))}
                      value={form.price_retrigger_mode}
                      onChange={set('price_retrigger_mode')}
                    />
                  </div>

                  {form.price_retrigger_mode === 'recurring' && (
                    <div className="flex flex-col gap-1.5">
                      <label className={labelClass}>{t('automation.priceCooldown')}</label>
                      <div className="flex items-center gap-2">
                        <Input
                          type="number"
                          min={240}
                          placeholder="Leave empty for daily"
                          value={form.price_cooldown_minutes}
                          onChange={set('price_cooldown_minutes')}
                          className="border w-32"
                          style={inputStyle}
                        />
                        <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                          {t('automation.minutes')}
                        </span>
                      </div>
                      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t('automation.cooldownHint')}
                      </p>
                    </div>
                  )}
                </>
              )}

              {/* Max Failures */}
              <div className="flex flex-col gap-1.5">
                <label className={labelClass}>{t('automation.maxFailures')}</label>
                <Input
                  type="number"
                  min={1}
                  max={100}
                  value={form.max_failures}
                  onChange={set('max_failures')}
                  className="border w-24"
                  style={inputStyle}
                />
              </div>

              {/* Description */}
              <div className="flex flex-col gap-1.5">
                <label className={labelClass}>{t('common.description')}</label>
                <Input
                  value={form.description}
                  onChange={set('description')}
                  placeholder={t('automation.descPlaceholder')}
                  className="placeholder:text-gray-500 border"
                  style={inputStyle}
                />
              </div>
            </motion.div>
          )}
          </AnimatePresence>
        </div>

        {/* ── Actions ───────────────────────────────────── */}

        <div
          className="form-actions flex flex-col-reverse sm:flex-row sm:justify-end gap-2 pt-4 mt-1"
          style={{ borderTop: '1px solid var(--color-border-default)' }}
        >
          <Button type="button" variant="ghost" onClick={onCancel}>
            {t('common.cancel')}
          </Button>
          <Button
            type="submit"
            disabled={loading}
            style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
          >
            {loading ? t('common.saving') : isEdit ? t('automation.saveChanges') : t('common.create')}
          </Button>
        </div>
      </form>
    </motion.div>
  );
}
