/* eslint-disable react-refresh/only-export-components -- the draft's hook and its fields belong together */
import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Input } from '@/components/ui/input';
import type { CustomModelEntry } from '@/components/model/types';
import type { ListedModel } from '@/api/model';
import { slugifyModelName } from './slugifyModelName';
import { ModalityChips, QuietNote } from './parts';

/** The window a custom model may declare (`_MIN_CONTEXT` / `_MAX_CONTEXT` on the server). */
const MIN_CONTEXT = 1_000;
const MAX_CONTEXT = 100_000_000;
const SUFFIX: Record<string, number> = { k: 1_000, m: 1_000_000 };
const CONTEXT_PRESETS = [
  ['32K', 32_000],
  ['128K', 128_000],
  ['200K', 200_000],
  ['256K', 256_000],
  ['1M', 1_000_000],
] as const;

/** `128K`, `1M`, `1.5m`, `131,072` as a token count; null when blank or unreadable. */
export function parseTokenCount(raw: string): number | null {
  const m = raw.trim().replace(/[,_\s]/g, '').match(/^(\d+(?:\.\d+)?)([km])?$/i);
  if (!m) return null;
  const n = Math.round(Number(m[1]) * (m[2] ? SUFFIX[m[2].toLowerCase()] : 1));
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}

export interface CustomModelDraft {
  id: string;
  name: string;
  context: string;
  maxOutput: string;
  modalities: ReadonlySet<string>;
  /** The window came from the endpoint's list, so picking another id replaces it. */
  contextFromList: boolean;
}

const EMPTY: CustomModelDraft = { id: '', name: '', context: '', maxOutput: '', modalities: new Set(), contextFromList: false };

/**
 * A model the manifest does not know, described by the user. The window is
 * asked for because nothing else can tell compaction when to summarize: a
 * model with none declared gets the default band, which on a short-window
 * model overflows before it ever summarizes.
 */
export function useCustomModelDraft() {
  const [draft, setDraft] = useState<CustomModelDraft>(EMPTY);
  const slug = slugifyModelName(draft.name.trim() || draft.id.trim());
  const context = parseTokenCount(draft.context);
  const contextOk = context !== null && context >= MIN_CONTEXT && context <= MAX_CONTEXT;
  const maxOutput = parseTokenCount(draft.maxOutput);
  const maxOutputOk = !draft.maxOutput.trim() || maxOutput !== null;
  const started = !!(draft.id.trim() || draft.name.trim());
  const ready = !!draft.id.trim() && !!slug && contextOk && maxOutputOk;

  const toEntry = (provider: string): CustomModelEntry | null => {
    if (!ready || context === null) return null;
    const entry: CustomModelEntry = { name: slug, model_id: draft.id.trim(), provider, context };
    if (draft.modalities.size > 0) entry.input_modalities = ['text', ...[...draft.modalities].filter((m) => m !== 'text')];
    if (maxOutput !== null) entry.parameters = { max_tokens: maxOutput };
    return entry;
  };

  return { draft, setDraft, slug, started, ready, toEntry, reset: () => setDraft(EMPTY) };
}

export type CustomModelDraftState = ReturnType<typeof useCustomModelDraft>;

/**
 * The model's id, its name in menus, its context window and output cap, and
 * what it reads. `suggestions` are the endpoint's own unrecognized models:
 * offered for the id, and their listed window fills the context.
 */
export function CustomModelFields({
  state,
  suggestions = [],
  onSubmit,
  autoFocus = false,
}: {
  state: CustomModelDraftState;
  suggestions?: readonly ListedModel[];
  onSubmit?: () => void;
  autoFocus?: boolean;
}) {
  const { t } = useTranslation();
  const idId = useId();
  const listId = useId();
  const nameId = useId();
  const contextId = useId();
  const contextHintId = useId();
  const maxId = useId();
  const { draft, setDraft, slug } = state;

  const setId = (id: string) => setDraft((prev) => {
    const listed = suggestions.find((m) => m.id === id.trim())?.context_window;
    if (typeof listed === 'number' && listed > 0 && (!prev.context.trim() || prev.contextFromList)) {
      return { ...prev, id, context: String(listed), contextFromList: true };
    }
    return prev.contextFromList ? { ...prev, id, context: '', contextFromList: false } : { ...prev, id };
  });
  const setContext = (context: string) => setDraft((prev) => ({ ...prev, context, contextFromList: false }));
  const toggleModality = (m: string) => setDraft((prev) => {
    const modalities = new Set(prev.modalities);
    if (modalities.has(m)) modalities.delete(m); else modalities.add(m);
    return { ...prev, modalities };
  });

  const typedName = draft.name.trim() || draft.id.trim();
  const contextValue = parseTokenCount(draft.context);
  const contextBad = !!draft.context.trim() && (contextValue === null || contextValue < MIN_CONTEXT || contextValue > MAX_CONTEXT);
  const maxBad = !!draft.maxOutput.trim() && parseTokenCount(draft.maxOutput) === null;
  const enter = onSubmit ? (e: React.KeyboardEvent) => { if (e.key === 'Enter') onSubmit(); } : undefined;
  const labelClass = 'text-xs font-medium';
  const labelStyle = { color: 'var(--color-text-secondary)' };

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1">
        <label htmlFor={idId} className={labelClass} style={labelStyle}>{t('modelAccess.customModel.idLabel')}</label>
        <Input
          id={idId}
          value={draft.id}
          onChange={(e) => setId(e.target.value)}
          placeholder={t('modelAccess.customModel.idPlaceholder')}
          list={suggestions.length > 0 ? listId : undefined}
          className="font-mono text-xs"
          autoComplete="off"
          spellCheck={false}
          autoFocus={autoFocus}
          onKeyDown={enter}
        />
        {suggestions.length > 0 && (
          <datalist id={listId}>
            {suggestions.map((m) => <option key={m.id} value={m.id}>{m.display_name !== m.id ? m.display_name : undefined}</option>)}
          </datalist>
        )}
      </div>

      <div className="flex flex-col gap-1">
        <label htmlFor={nameId} className={labelClass} style={labelStyle}>{t('modelAccess.customModel.nameLabel')}</label>
        <Input
          id={nameId}
          value={draft.name}
          onChange={(e) => setDraft((prev) => ({ ...prev, name: e.target.value }))}
          placeholder={t('modelAccess.customModel.namePlaceholder')}
          autoComplete="off"
          spellCheck={false}
          onKeyDown={enter}
        />
        {typedName && slug !== typedName && (
          <QuietNote>{slug ? t('setup.customModelNameSlugPreview', { slug }) : t('setup.customModelNameSlugEmpty')}</QuietNote>
        )}
      </div>

      <div className="flex flex-col gap-1">
        <label htmlFor={contextId} className={labelClass} style={labelStyle}>{t('modelAccess.customModel.contextLabel')}</label>
        <div className="flex flex-wrap items-center gap-2">
          <Input
            id={contextId}
            value={draft.context}
            onChange={(e) => setContext(e.target.value)}
            placeholder={t('modelAccess.customModel.contextPlaceholder')}
            aria-invalid={contextBad || undefined}
            aria-describedby={contextHintId}
            className="w-36 font-mono text-xs"
            autoComplete="off"
            spellCheck={false}
            onKeyDown={enter}
          />
          <div role="group" aria-label={t('modelAccess.customModel.contextPresets')} className="flex flex-wrap gap-1">
            {CONTEXT_PRESETS.map(([label, value]) => {
              const on = contextValue === value;
              return (
                <button
                  key={label}
                  type="button"
                  aria-pressed={on}
                  onClick={() => setContext(label)}
                  className="rounded-full px-2 py-0.5 text-xs transition-colors"
                  style={{
                    background: on ? 'var(--color-bg-tag)' : 'transparent',
                    color: on ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                    border: `1px solid ${on ? 'var(--color-text-primary)' : 'var(--color-border-default)'}`,
                  }}
                >
                  {label}
                </button>
              );
            })}
          </div>
        </div>
        <p id={contextHintId} className="text-xs" style={{ color: contextBad ? 'var(--color-loss)' : 'var(--color-text-tertiary)' }}>
          {contextBad
            ? t('modelAccess.customModel.contextInvalid')
            : draft.contextFromList ? t('modelAccess.customModel.contextFromList') : t('modelAccess.customModel.contextHint')}
        </p>
      </div>

      <div className="flex flex-col gap-1">
        <label htmlFor={maxId} className={labelClass} style={labelStyle}>{t('modelAccess.customModel.maxOutputLabel')}</label>
        <Input
          id={maxId}
          value={draft.maxOutput}
          onChange={(e) => setDraft((prev) => ({ ...prev, maxOutput: e.target.value }))}
          placeholder={t('modelAccess.customModel.maxOutputPlaceholder')}
          aria-invalid={maxBad || undefined}
          className="w-36 font-mono text-xs"
          autoComplete="off"
          spellCheck={false}
          onKeyDown={enter}
        />
        <p className="text-xs" style={{ color: maxBad ? 'var(--color-loss)' : 'var(--color-text-tertiary)' }}>
          {maxBad ? t('modelAccess.customModel.maxOutputInvalid') : t('modelAccess.customModel.maxOutputHint')}
        </p>
      </div>

      <ModalityChips active={draft.modalities} onToggle={toggleModality} />
    </div>
  );
}
