import { useState } from 'react';
import { SlidersHorizontal, Trash2 } from 'lucide-react';
import { useNavigate } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { usePreferences } from '@/hooks/usePreferences';
import { useClearPreferences } from '@/hooks/useUpdatePreferences';
import { queryKeys } from '@/lib/queryKeys';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { welcomeUrl } from '@/pages/Onboarding/welcome/paths';
import { useWelcomeOffer } from '@/pages/Onboarding/welcome/useWelcomeOffer';
import { SettingRow, SettingsCardBody, SettingsGroup } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';
import type { Preferences } from './types';

// The visibility test and the renderer used to disagree about what "empty"
// means: the test counted keys, the renderer dropped nulls and empty strings.
// A group holding only those passed the test and drew a labelled empty box.
// Derive the rows once and let their count decide.
//
// A string of spaces is the same empty box with a different value in it.
// These come from a free-text onboarding answer, so whitespace is what a user
// submits by holding the spacebar, not a shape only a fuzzer produces.
function renderableEntries(data?: Record<string, unknown> | null) {
  return Object.entries(data ?? {}).filter(
    ([, value]) => value != null && (typeof value !== 'string' || value.trim() !== ''),
  );
}

function formatValue(value: unknown): string {
  if (Array.isArray(value)) return value.join(', ');
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

/** Investing profile: the onboarding answers the agent's research is fitted
 *  to, the way back into the questions, and resetting them. */
export function InvestingProfilePanel() {
  const navigate = useNavigate();
  const welcomeOffer = useWelcomeOffer();
  const { preferences: prefsData } = usePreferences();
  const queryClient = useQueryClient();
  const { mutateAsync: clearPreferences } = useClearPreferences();
  const { t } = useTranslation();
  const copy = useSettingCopy();
  // The answers a reset cleared, hidden until the refetch replaces them.
  const [clearedFrom, setClearedFrom] = useState<unknown>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [isResetting, setIsResetting] = useState(false);

  const preferences = clearedFrom === prefsData ? null : (prefsData as Preferences | null);
  const prefSections = [
    { label: t('settings.riskTolerance'), entries: renderableEntries(preferences?.risk_preference) },
    { label: t('settings.investmentStyle'), entries: renderableEntries(preferences?.investment_preference) },
  ].filter((section) => section.entries.length > 0);

  const handleResetConfirm = async () => {
    setIsResetting(true);
    setError(null);
    try {
      await clearPreferences();
      setClearedFrom(prefsData);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
      // The reset also marks onboarding incomplete, and the welcome offer, the
      // dashboard banner and the checklist read that off the user row.
      queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
      // Feature overrides live in preferences too: refresh effective flags
      // so gated surfaces (Watch pill, Experiments toggles) update at once.
      queryClient.invalidateQueries({ queryKey: queryKeys.features.all });
    } catch {
      setError(t('settings.failedToResetPreferences'));
    }
    setShowResetConfirm(false);
    setIsResetting(false);
  };

  const hasAnswers = prefSections.length > 0;
  const editProfile = () => navigate(welcomeUrl({ edit: true }));

  return (
    <>
      {/* With answers on file the offer is to finish them, above them; with
          none it is the empty state itself, below. */}
      {welcomeOffer && hasAnswers && (
        <SettingsGroup>
          <SettingRow label={t('settings.completeProfile')} description={t('settings.completeProfileDesc')}>
            <Button type="button" size="sm" className="h-8" onClick={() => welcomeOffer.open()}>
              {welcomeOffer.label}
            </Button>
          </SettingRow>
        </SettingsGroup>
      )}

      <SettingsGroup
        title={t(SETTINGS.investingAnswers.labelKey)}
        description={t(SETTINGS.investingAnswers.descKey)}
        action={hasAnswers && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 gap-1.5 shrink-0"
            onClick={editProfile}
            data-testid="settings-edit-profile"
          >
            <SlidersHorizontal className="h-3.5 w-3.5" /> {t('onboarding.welcome.edit')}
          </Button>
        )}
      >
        {hasAnswers ? (
          prefSections.map(({ label, entries }, index) => (
            <SettingsCardBody key={label} id={index === 0 ? SETTINGS.investingAnswers.anchor : undefined}>
              <h3 className="settings-row-label mb-2">{label}</h3>
              <dl className="grid grid-cols-[minmax(0,38%)_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-[0.8125rem] sm:grid-cols-[minmax(0,180px)_minmax(0,1fr)] sm:gap-x-4">
                {entries.map(([key, value]) => (
                  <div key={key} className="contents">
                    <dt style={{ color: 'var(--color-text-tertiary)' }}>
                      {key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}
                    </dt>
                    <dd style={{ color: 'var(--color-text-primary)', wordBreak: 'break-word' }}>
                      {formatValue(value)}
                    </dd>
                  </div>
                ))}
              </dl>
            </SettingsCardBody>
          ))
        ) : (
          <SettingsCardBody id={SETTINGS.investingAnswers.anchor} className="flex flex-col items-center gap-3 py-8 text-center">
            <div>
              <p className="settings-row-label">{t('settings.completeProfile')}</p>
              <p className="settings-row-desc">{t('settings.completeProfileDesc')}</p>
            </div>
            <Button
              type="button"
              size="sm"
              className="h-8 gap-1.5"
              onClick={welcomeOffer ? () => welcomeOffer.open() : editProfile}
              data-testid="settings-edit-profile"
            >
              <SlidersHorizontal className="h-3.5 w-3.5" /> {welcomeOffer ? welcomeOffer.label : t('onboarding.welcome.banner.start')}
            </Button>
          </SettingsCardBody>
        )}
      </SettingsGroup>

      <SettingsGroup>
        <SettingRow
          {...copy(SETTINGS.resetPreferences)}
          description={error
            ? <span style={{ color: 'var(--color-loss)' }}>{error}</span>
            : t(SETTINGS.resetPreferences.descKey)}
        >
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 gap-1.5"
            style={{ color: 'var(--color-loss)' }}
            onClick={() => setShowResetConfirm(true)}
          >
            <Trash2 className="h-3.5 w-3.5" /> {t('settings.resetPreferences')}
          </Button>
        </SettingRow>
      </SettingsGroup>

      <ConfirmDialog
        open={showResetConfirm}
        title={t('settings.resetPreferences')}
        message={t('settings.resetConfirmMsg')}
        confirmLabel={isResetting ? t('settings.resetting') : t('settings.resetPreferences')}
        onConfirm={handleResetConfirm}
        onOpenChange={setShowResetConfirm}
      />
    </>
  );
}
