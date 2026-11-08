import { useId, useState } from 'react';
import { Sun, Moon, Monitor } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { ThemePreference } from '@/contexts/ThemeContext';
import { useAppearance } from '@/hooks/useAppearance';
import { FONT_SCALES, getFontScale, setFontScale, type FontScale } from '@/lib/fontScale';
import { CARD_STYLES, type CardStyle } from '@/lib/cardStyle';
import { OptionCard, OptionGroup } from '@/components/ui/option-card';
import { SegmentedRow, SettingsCardBody, SettingsGroup } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { useSaveFailureToast } from './usePreferenceWrite';
import { SETTINGS } from '../sections';

/** A card as each style draws it, small enough for a choice: Soft leaves it open
 *  with its title cut into the edge, Balanced fills it, Terminal dashes
 *  the edge and brackets the title. Drawn in the card's ink, so the picked one
 *  darkens with it. */
const STYLE_PREVIEW: Record<CardStyle, React.ReactNode> = {
  soft: (
    <>
      <path d="M12 9.5H11.5a3 3 0 0 0-3 3v16a3 3 0 0 0 3 3h33a3 3 0 0 0 3-3v-16a3 3 0 0 0-3-3H30" fill="none" stroke="currentColor" strokeOpacity="0.45" />
      <rect x="14" y="8.25" width="14" height="2.5" rx="1.25" fill="currentColor" />
      <rect x="13" y="20" width="24" height="2" rx="1" fill="currentColor" fillOpacity="0.45" />
    </>
  ),
  balanced: (
    <>
      <rect x="8.5" y="8.5" width="39" height="23" rx="3" fill="currentColor" fillOpacity="0.08" stroke="currentColor" strokeOpacity="0.35" />
      <rect x="13" y="14" width="14" height="2.5" rx="1.25" fill="currentColor" />
      <rect x="13" y="21" width="24" height="2" rx="1" fill="currentColor" fillOpacity="0.45" />
    </>
  ),
  terminal: (
    <>
      <path d="M12 9.5H8.5v22h39v-22H31" fill="none" stroke="currentColor" strokeOpacity="0.45" strokeDasharray="2 1.5" />
      <path d="M15 7.5h-1.5v4H15M27 7.5h1.5v4H27" fill="none" stroke="currentColor" strokeOpacity="0.6" />
      <rect x="16" y="8.25" width="10" height="2.5" fill="currentColor" />
      <rect x="13" y="20" width="24" height="2" fill="currentColor" fillOpacity="0.45" />
    </>
  ),
};

const STYLE_COPY: Record<CardStyle, { title: string; desc: string }> = {
  soft: { title: 'settings.cardStyleSoft', desc: 'settings.cardStyleSoftDesc' },
  balanced: { title: 'settings.cardStyleBalanced', desc: 'settings.cardStyleBalancedDesc' },
  terminal: { title: 'settings.cardStyleTerminal', desc: 'settings.cardStyleTerminalDesc' },
};

/** Appearance: the theme (a style and light or dark, both saved to the
 *  account) and the font size, which is kept per device. */
export function AppearancePanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const reportFailure = useSaveFailureToast();
  const { cardStyle, theme, pickStyle, pickTheme } = useAppearance();
  const [fontScale, setFontScaleState] = useState(getFontScale);
  const styleLabelId = useId();
  const styleCopy = copy(SETTINGS.cardStyle);

  return (
    <>
      <SettingsGroup title={t('settings.theme')}>
        <SettingsCardBody id={styleCopy.id}>
          <span id={styleLabelId} className="settings-row-label">{styleCopy.label}</span>
          <p className="settings-row-desc">{styleCopy.description}</p>
          <OptionGroup value={cardStyle} onChange={(style: CardStyle) => void reportFailure(pickStyle(style))} labelledBy={styleLabelId} className="settings-style-tiles mt-3 sm:grid-cols-3">
            {CARD_STYLES.map((style) => (
              <OptionCard
                key={style}
                value={style}
                title={t(STYLE_COPY[style].title)}
                description={t(STYLE_COPY[style].desc)}
                media={STYLE_PREVIEW[style]}
              />
            ))}
          </OptionGroup>
        </SettingsCardBody>
        <SegmentedRow
          {...copy(SETTINGS.theme)}
          value={theme}
          onChange={(mode: ThemePreference) => void reportFailure(pickTheme(mode))}
          options={[
            { value: 'dark', label: <><Moon className="h-3.5 w-3.5" />{t('settings.dark')}</> },
            { value: 'light', label: <><Sun className="h-3.5 w-3.5" />{t('settings.light')}</> },
            { value: 'auto', label: <><Monitor className="h-3.5 w-3.5" />{t('settings.auto')}</> },
          ]}
        />
      </SettingsGroup>

      <SettingsGroup title={t('settings.displayGroup')}>
        <SegmentedRow
          {...copy(SETTINGS.fontSize)}
          value={String(fontScale)}
          onChange={(v) => { const next = Number(v) as FontScale; setFontScale(next); setFontScaleState(next); }}
          options={FONT_SCALES.map((scale) => ({ value: String(scale), label: `${Math.round(scale * 100)}%` }))}
        />
      </SettingsGroup>
    </>
  );
}
