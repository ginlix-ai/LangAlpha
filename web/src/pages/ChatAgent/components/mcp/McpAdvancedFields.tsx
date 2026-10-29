import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { Field, FieldError } from '@/components/mcp/McpPrimitives';
import { DESCRIPTION_MAX, EXPOSURE_MODES, INSTRUCTION_MAX } from './mcpSchemas';
import { transportPinned, type FormAction, type FormState } from './mcpServerDraft';
import type { McpTransport } from '../../utils/api';

interface McpAdvancedFieldsProps {
  /** What the form holds, null until the field says what the server is. */
  transport: McpTransport | null;
  /** The edited row's transport as saved. */
  savedTransport?: McpTransport;
  meta: FormState['meta'];
  dispatch: (action: FormAction) => void;
  /** An authenticated remote server lists its tools only with its key. */
  discoveryForced: boolean;
  errorFor: (path: string) => { message: string } | undefined;
}

const fieldStyle = {
  color: 'var(--color-text-primary)',
  border: '1px solid var(--color-border-muted)',
} as const;

/**
 * What only tunes how the server is reached and how the agent sees it, behind
 * a fold. A problem behind the fold opens it, or Add would sit disabled with
 * its reason hidden.
 */
export function McpAdvancedFields({
  transport,
  savedTransport,
  meta,
  dispatch,
  discoveryForced,
  errorFor,
}: McpAdvancedFieldsProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const shown = open || !!(errorFor('description') || errorFor('instruction') || errorFor('tool_exposure_mode'));
  // SSE is never a choice: no check lets it through, so nothing can be made
  // SSE. A row that is one, or an entry read as one, still shows it, held and
  // marked unsupported, so the control says what the form holds and HTTP is a
  // real change from it.
  const transports: McpTransport[] =
    savedTransport === 'sse' || transport === 'sse' ? ['http', 'sse', 'stdio'] : ['http', 'stdio'];

  const textArea = (
    key: 'description' | 'instruction',
    max: number,
    words: { label: string; hint: string; placeholder: string },
  ) => (
    <Field label={words.label} hint={words.hint}>
      <textarea
        value={meta[key]}
        onChange={(e) => dispatch({ type: 'meta', meta: { [key]: e.target.value } })}
        placeholder={words.placeholder}
        rows={2}
        className="w-full px-3 py-2 text-sm rounded-md bg-transparent resize-none"
        style={fieldStyle}
        maxLength={max}
      />
      <FieldError error={errorFor(key)} />
    </Field>
  );

  return (
    <div className="flex flex-col gap-3">
      <button
        type="button"
        onClick={() => setOpen(!shown)}
        aria-expanded={shown}
        className="inline-flex items-center gap-1 text-xs self-start"
        style={{ color: 'var(--color-text-secondary)' }}
        data-testid="mcp-advanced-toggle"
      >
        <ChevronRight
          className="h-3.5 w-3.5 transition-transform"
          style={{ transform: shown ? 'rotate(90deg)' : undefined }}
        />
        {t('mcp.modal.advanced')}
      </button>

      <Disclosure open={shown} className="flex flex-col gap-4 pl-1">
        <Field label={t('mcp.modal.transportLabel')} hint={t('mcp.modal.transportHint')}>
          <SegmentedControl
            value={transport}
            onChange={(next) => dispatch({ type: 'draft', change: (d) => transportPinned(d, next) })}
            label={t('mcp.modal.transportLabel')}
            options={transports.map((tr) => ({
              value: tr,
              label: tr === 'sse' ? t('mcp.modal.transportSseRefused') : <span className="uppercase">{tr}</span>,
              disabled: tr === 'sse',
              testId: `mcp-transport-${tr}`,
            }))}
          />
        </Field>

        {textArea('description', DESCRIPTION_MAX, {
          label: t('mcp.modal.descriptionLabel'),
          hint: t('mcp.modal.descriptionHint'),
          placeholder: t('mcp.modal.descriptionPlaceholder'),
        })}
        {textArea('instruction', INSTRUCTION_MAX, {
          label: t('mcp.modal.instructionLabel'),
          hint: t('mcp.modal.instructionHint'),
          placeholder: t('mcp.modal.instructionPlaceholder'),
        })}

        <Field label={t('mcp.modal.exposureLabel')} hint={t('mcp.modal.exposureHint')}>
          <SegmentedControl
            value={meta.exposure}
            onChange={(exposure) => dispatch({ type: 'meta', meta: { exposure } })}
            label={t('mcp.modal.exposureLabel')}
            options={EXPOSURE_MODES.map((m) => ({
              value: m,
              label: m === 'summary' ? t('mcp.modal.exposureSummary') : t('mcp.modal.exposureDetailed'),
            }))}
          />
        </Field>

        <Field
          label={t('mcp.modal.discoveryLabel')}
          hint={discoveryForced ? t('mcp.modal.discoveryForced') : t('mcp.modal.discoveryHint')}
        >
          <label
            className="flex items-center gap-2 text-sm"
            style={{
              color: 'var(--color-text-primary)',
              cursor: discoveryForced ? 'not-allowed' : 'pointer',
              opacity: discoveryForced ? 0.7 : 1,
            }}
          >
            <input
              type="checkbox"
              checked={meta.discoveryUsesSecrets || discoveryForced}
              disabled={discoveryForced}
              onChange={(e) => dispatch({ type: 'meta', meta: { discoveryUsesSecrets: e.target.checked } })}
              className="h-4 w-4 rounded"
              style={{ accentColor: 'var(--color-accent-primary)' }}
            />
            {t('mcp.modal.discoveryToggle')}
          </label>
        </Field>
      </Disclosure>
    </div>
  );
}
