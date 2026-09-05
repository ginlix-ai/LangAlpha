import { useEffect, useState } from 'react';
import { Trash2, MessageSquareText, FileText, Code2 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { clearPreferences } from '@/pages/Dashboard/utils/api';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import { getFlashWorkspace } from '@/pages/ChatAgent/utils/api';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { useOnboarding } from '@/pages/Onboarding';
import type { Preferences } from './types';

/** Preferences tab: investment-preference summary, output format, onboarding
 * replay/reset entry points, and the reset-preferences flow. */
export function PreferencesTab() {
  const navigate = useNavigate();
  const { toast } = useToast();
  const { user: authUser } = useUser();
  const { preferences: prefsData } = usePreferences();
  const updatePrefsMutation = useUpdatePreferences();
  const queryClient = useQueryClient();
  const { t } = useTranslation();
  const { replayGuides, resetOnboarding } = useOnboarding();

  const [preferences, setPreferences] = useState<Preferences | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [isResetting, setIsResetting] = useState(false);

  // Sync local preferences state from usePreferences hook
  useEffect(() => {
    if (prefsData) {
      setPreferences(prefsData);
    }
  }, [prefsData]);

  const handleOutputFormatChange = async (format: 'markdown' | 'html') => {
    const currentAgentPref = (prefsData as any)?.agent_preference || {};
    // null deletes the key (default behavior); 'html' opts into HTML reports.
    const nextOutputFormat = format === 'html' ? 'html' : null;
    try {
      await updatePrefsMutation.mutateAsync({
        agent_preference: {
          ...currentAgentPref,
          output_format: nextOutputFormat,
        },
      });
    } catch {
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('settings.failedToSaveSettings'),
      });
    }
  };

  const handleModifyPreferences = async () => {
    try {
      const flashWs = await getFlashWorkspace();
      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId: flashWs.workspace_id,
          isModifyingPreferences: true,
          agentMode: 'flash',
          workspaceStatus: 'flash',
        },
      });
    } catch (err) {
      console.error('Error navigating to modify preferences:', err);
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('dashboard.failedPrefUpdate'),
      });
    }
  };

  const handleStartOnboarding = async () => {
    try {
      const flashWs = await getFlashWorkspace();
      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId: flashWs.workspace_id,
          isOnboarding: true,
          agentMode: 'flash',
          workspaceStatus: 'flash',
        },
      });
    } catch (err) {
      console.error('Error setting up onboarding:', err);
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('dashboard.failedOnboarding'),
      });
    }
  };

  const handleResetConfirm = async () => {
    setIsResetting(true);
    try {
      await clearPreferences();
      setPreferences(null);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
      // Feature overrides live in preferences too — refresh effective flags
      // so gated surfaces (Watch pill, Experiments toggles) update at once.
      queryClient.invalidateQueries({ queryKey: queryKeys.features.all });
      setShowResetConfirm(false);
    } catch {
      setError(t('settings.failedToResetPreferences'));
      setShowResetConfirm(false);
    } finally {
      setIsResetting(false);
    }
  };

  // The visibility test and the renderer used to disagree about what "empty"
  // means: the test counted keys, the renderer dropped nulls, empty strings and
  // output_format. A group holding only those passed the test and drew a
  // labelled empty box. Derive the rows once and let their count decide.
  //
  // A string of spaces is the same empty box with a different value in it: the
  // row renders, the label renders, and the cell beside it is blank. These come
  // from a free-text onboarding answer, so whitespace is what a user submits by
  // holding the spacebar, not a shape only a fuzzer produces.
  const renderableEntries = (data?: Record<string, unknown> | null) =>
    Object.entries(data ?? {}).filter(
      ([key, value]) =>
        key !== 'output_format'
        && value != null
        && (typeof value !== 'string' || value.trim() !== ''),
    );

  const prefSections = [
    { label: t('settings.riskTolerance'), entries: renderableEntries(preferences?.risk_preference) },
    { label: t('settings.investmentStyle'), entries: renderableEntries(preferences?.investment_preference) },
    { label: t('settings.agentSettings'), entries: renderableEntries(preferences?.agent_preference) },
  ].filter((section) => section.entries.length > 0);

  return (
    <>
    <div className="space-y-4">
      {authUser?.onboarding_completed !== true && (
        <div
          className="rounded-lg p-3 flex items-center justify-between gap-3"
          style={{
            backgroundColor: 'hsl(var(--primary) / 0.08)',
            border: '1px solid hsl(var(--primary) / 0.2)',
          }}
        >
          <div>
            <p className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('settings.completeProfile')}
            </p>
            <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('settings.completeProfileDesc')}
            </p>
          </div>
          <button
            type="button"
            onClick={handleStartOnboarding}
            className="shrink-0 flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-opacity hover:opacity-90"
            style={{
              backgroundColor: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            {t('settings.startOnboarding')}
          </button>
        </div>
      )}

      {prefSections.length > 0 ? (
        <div className="space-y-4">
          {prefSections.map(({ label, entries }) => (
            <div key={label}>
              <label className="block text-[0.8125rem] font-medium mb-1.5" style={{ color: 'var(--color-text-primary)' }}>{label}</label>
              <div
                className="rounded-md px-3 py-2.5 text-sm space-y-1"
                style={{
                  backgroundColor: 'var(--color-bg-card)',
                  border: '1px solid var(--color-border-muted)',
                }}
              >
                {entries.map(([key, value]) => (
                  <div key={key} className="flex gap-2">
                    <span className="shrink-0 font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                      {key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}:
                    </span>
                    <span style={{ color: 'var(--color-text-primary)', wordBreak: 'break-word' }}>
                      {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div
          className="rounded-md px-3 py-5 text-center"
          style={{
            backgroundColor: 'var(--color-bg-card)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('settings.noPreferencesYet')}
          </p>
        </div>
      )}

      {/* Output Format */}
      {(() => {
        const outputFormat = ((prefsData as any)?.agent_preference?.output_format) === 'html' ? 'html' : 'markdown';
        return (
          <div className="p-3 rounded-lg" style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
            <div className="flex items-center justify-between gap-3">
              <label className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('settings.outputFormat')}
              </label>
              <div className="inline-flex rounded-lg overflow-hidden clips-focus-ring" style={{ border: '1px solid var(--color-border-muted)' }}>
                <button
                  type="button"
                  onClick={() => handleOutputFormatChange('markdown')}
                  className="flex items-center gap-1.5 px-2.5 py-1 text-[0.8125rem] font-medium transition-colors"
                  style={{
                    backgroundColor: outputFormat === 'markdown' ? 'var(--color-accent-soft)' : 'transparent',
                    color: outputFormat === 'markdown' ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
                  }}
                >
                  <FileText className="h-3.5 w-3.5" />
                  {t('settings.outputFormatDefault')}
                </button>
                <button
                  type="button"
                  onClick={() => handleOutputFormatChange('html')}
                  className="flex items-center gap-1.5 px-2.5 py-1 text-[0.8125rem] font-medium transition-colors"
                  style={{
                    backgroundColor: outputFormat === 'html' ? 'var(--color-accent-soft)' : 'transparent',
                    color: outputFormat === 'html' ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
                  }}
                >
                  <Code2 className="h-3.5 w-3.5" />
                  {t('settings.outputFormatHtml')}
                </button>
              </div>
            </div>
            <p className="text-xs mt-2" style={{ color: 'var(--color-text-tertiary)' }}>
              {outputFormat === 'html'
                ? t('settings.outputFormatDescriptionHtml')
                : t('settings.outputFormatDescriptionDefault')}
            </p>
          </div>
        );
      })()}

      {/* Column until there is room for a row. The two actions are a fixed
          ~259px whatever the viewport, so side-by-side on a phone leaves the
          description about 46px to wrap into and it shreds into a 150px-tall
          ribbon two characters wide. */}
      <div
        className="rounded-md p-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"
        style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
      >
        <div className="min-w-0">
          <p className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('onboarding.settings.sectionTitle')}
          </p>
          <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('onboarding.settings.description')}
          </p>
        </div>
        <div className="shrink-0 flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => {
              if (replayGuides()) toast({ description: t('onboarding.settings.replayDone') });
            }}
            className="px-2.5 py-1 rounded-md text-xs font-medium transition-opacity hover:opacity-80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)]"
            style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
          >
            {t('onboarding.settings.replayGuides')}
          </button>
          <button
            type="button"
            onClick={() => {
              if (resetOnboarding()) toast({ description: t('onboarding.settings.resetDone') });
            }}
            className="px-2.5 py-1 rounded-md text-xs font-medium transition-opacity hover:opacity-80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus-ring)]"
            style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
          >
            {t('onboarding.settings.reset')}
          </button>
        </div>
      </div>

      {error && (
        <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
        </div>
      )}

      <p className="text-xs pt-1" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('settings.preferencesDesc')}
      </p>

      <div className="flex gap-3 justify-between pt-4" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
        <button
          type="button"
          onClick={() => setShowResetConfirm(true)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium transition-colors"
          style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
        >
          <Trash2 className="h-4 w-4" /> {t('settings.resetPreferences')}
        </button>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={handleModifyPreferences}
            className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium transition-opacity hover:opacity-90"
            style={{
              backgroundColor: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            <MessageSquareText className="h-4 w-4" /> {t('settings.modifyWithAgent')}
          </button>
        </div>
      </div>
    </div>

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
