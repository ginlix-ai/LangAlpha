import { useId, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';

/** When to use it: the agent routes on this text. `sameAs` names the
 *  subagent it now reads the same as, '' for one the server leaves unnamed. */
export function DescriptionField({
  value,
  onChange,
  used,
  max,
  over,
  sameAs,
  error,
  placeholder,
}: {
  value: string;
  onChange: (value: string) => void;
  used: number;
  max: number;
  over: boolean;
  sameAs: string | null;
  error: string | null;
  placeholder?: string;
}) {
  const { t } = useTranslation();
  const id = useId();
  return (
    <div>
      <div className="sa-field-head">
        <label htmlFor={id}>{t('plugins.subagents.whenToUse')}</label>
        <span className={over ? 'sa-field-aside is-over' : sameAs !== null ? 'sa-field-aside is-warn' : 'sa-field-aside'}>
          {sameAs !== null && !over
            ? t('plugins.subagents.sayWhen', { name: sameAs })
            : t('plugins.subagents.whenToUseHint')}
          {t('plugins.subagents.sep')}
          {/* One unit: on a phone the hint wraps, and "40 /" over "400" reads as two numbers. */}
          <span className="whitespace-nowrap">{t('plugins.subagents.charCount', { used, max })}</span>
        </span>
      </div>
      <textarea
        id={id}
        className="sa-input"
        rows={3}
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
      />
      {error && <p className="sa-field-error">{error}</p>}
    </div>
  );
}

/** The instructions, Markdown the subagent reads as its prompt. */
export function InstructionsField({
  value,
  onChange,
  over,
  aside,
  error,
  placeholder,
}: {
  value: string;
  onChange: (value: string) => void;
  over: boolean;
  aside: ReactNode;
  error: string | null;
  placeholder?: string;
}) {
  const { t } = useTranslation();
  const id = useId();
  return (
    <div>
      <div className="sa-field-head">
        <label htmlFor={id}>{t('plugins.subagents.instructions')}</label>
        <span className={over ? 'sa-field-aside is-over' : 'sa-field-aside'}>{aside}</span>
      </div>
      <textarea
        id={id}
        className="sa-input is-code"
        value={value}
        placeholder={placeholder}
        spellCheck={false}
        onChange={(e) => onChange(e.target.value)}
      />
      {error && <p className="sa-field-error">{error}</p>}
    </div>
  );
}
