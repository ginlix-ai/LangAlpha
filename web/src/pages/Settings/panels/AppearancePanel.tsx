import { useState } from 'react';
import { Sun, Moon, Monitor } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useTheme } from '@/contexts/ThemeContext';
import { FONT_SCALES, getFontScale, setFontScale, type FontScale } from '@/lib/fontScale';
import { CARD_STYLES, getCardStyle, setCardStyle, type CardStyle } from '@/lib/cardStyle';
import { SegmentedRow, SettingsGroup } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';

/** Appearance: theme, font size and how data cards are drawn. Font size and
 *  card style are kept per device. */
export function AppearancePanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const { preference, setTheme } = useTheme();
  const [fontScale, setFontScaleState] = useState(getFontScale);
  const [cardStyle, setCardStyleState] = useState(getCardStyle);
  const cardStyleLabels: Record<CardStyle, string> = {
    quiet: t('settings.cardStyleQuiet'),
    balanced: t('settings.cardStyleBalanced'),
    terminal: t('settings.cardStyleTerminal'),
  };

  return (
    <>
      <SettingsGroup title={t('settings.displayGroup')}>
        <SegmentedRow
          {...copy(SETTINGS.theme)}
          value={preference}
          onChange={setTheme}
          options={[
            { value: 'dark', label: <><Moon className="h-3.5 w-3.5" />{t('settings.dark')}</> },
            { value: 'light', label: <><Sun className="h-3.5 w-3.5" />{t('settings.light')}</> },
            { value: 'auto', label: <><Monitor className="h-3.5 w-3.5" />{t('settings.auto')}</> },
          ]}
        />
        <SegmentedRow
          {...copy(SETTINGS.fontSize)}
          value={String(fontScale)}
          onChange={(v) => { const next = Number(v) as FontScale; setFontScale(next); setFontScaleState(next); }}
          options={FONT_SCALES.map((scale) => ({ value: String(scale), label: `${Math.round(scale * 100)}%` }))}
        />
      </SettingsGroup>

      <SettingsGroup title={t('settings.chartsGroup')}>
        <SegmentedRow
          {...copy(SETTINGS.cardStyle)}
          value={cardStyle}
          onChange={(v) => { setCardStyle(v); setCardStyleState(v); }}
          options={CARD_STYLES.map((style) => ({ value: style, label: cardStyleLabels[style] }))}
        />
      </SettingsGroup>
    </>
  );
}
