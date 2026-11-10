import { useTranslation } from 'react-i18next';
import { PREFIXES } from './prefixes';
import type { Kind } from './types';

/**
 * The prefixes that narrow the search, said under an empty field. Text, not
 * a row: it takes no selection, so Enter never lands on it.
 */
export function PrefixHint() {
  const { t } = useTranslation();
  return (
    <p className="command-palette-prefixes">
      <span className="sr-only">{t('palette.prefixes.label')}</span>
      {(Object.keys(PREFIXES) as Kind[]).map((kind) => (
        <span key={kind}>
          <kbd>{PREFIXES[kind]}</kbd>
          {t(`palette.prefixes.${kind}`)}
        </span>
      ))}
    </p>
  );
}
