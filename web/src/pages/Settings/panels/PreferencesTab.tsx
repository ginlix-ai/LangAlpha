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

  return (
    <>
    <div className="space-y-5">
      {authUser?.onboarding_completed !== true && (
        <div
          className="rounded-lg px-4 py-4 flex items-center justify-between gap-3"
          style={{
            backgroundColor: 'hsl(var(--primary) / 0.08)',
            border: '1px solid hsl(var(--primary) / 0.2)',
          }}
        >
          <div>
            <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('settings.completeProfile')}
            </p>
            <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('settings.completeProfileDesc')}
            </p>
          </div>
          <button
            type="button"
            onClick={handleStartOnboarding}
            className="shrink-0 flex items-center gap-1.5 px-4 py-2 rounded-md text-sm font-medium"
            style={{
              backgroundColor: 'var(--color-accent-primary)',
              color: 'var(--color-text-on-accent)',
            }}
          >
            {t('settings.startOnboarding')}
          </button>
        </div>
      )}

      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('settings.preferencesDesc')}
      </p>

      <div
        className="rounded-md px-4 py-4"
        style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
      >
        <div className="flex items-center justify-between gap-3">
          <div>
            <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('onboarding.settings.sectionTitle')}
            </p>
            <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('onboarding.settings.description')}
            </p>
          </div>
          <button
            type="button"
            onClick={() => {
              if (resetOnboarding()) toast({ description: t('onboarding.settings.resetDone') });
            }}
            className="shrink-0 rounded text-xs font-medium transition-opacity hover:opacity-80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('onboarding.settings.reset')}
          </button>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => {
              if (replayGuides()) toast({ description: t('onboarding.settings.replayDone') });
            }}
            className="px-3 py-1.5 rounded-md text-xs font-medium transition-opacity hover:opacity-80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]"
            style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
          >
            {t('onboarding.settings.replayGuides')}
          </button>
        </div>
      </div>

      {preferences && (preferences.risk_preference || preferences.investment_preference || preferences.agent_preference) ? (
        <div className="space-y-4">
          {[
            { label: t('settings.riskTolerance'), data: preferences.risk_preference },
            { label: t('settings.investmentStyle'), data: preferences.investment_preference },
            { label: t('settings.agentSettings'), data: preferences.agent_preference },
          ].filter((item): item is { label: string; data: Record<string, unknown> } => !!item.data && Object.keys(item.data).length > 0).map(({ label, data }) => (
            <div key={label}>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{label}</label>
              <div
                className="rounded-md px-3 py-2.5 text-sm space-y-1"
                style={{
                  backgroundColor: 'var(--color-bg-card)',
                  border: '1px solid var(--color-border-muted)',
                }}
              >
                {Object.entries(data).filter(([key]) => key !== 'output_format').map(([key, value]) => (
                  value != null && value !== '' && (
                    <div key={key} className="flex gap-2">
                      <span className="shrink-0 font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                        {key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}:
                      </span>
                      <span style={{ color: 'var(--color-text-primary)', wordBreak: 'break-word' }}>
                        {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                      </span>
                    </div>
                  )
                ))}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div
          className="rounded-md px-4 py-6 text-center"
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
              <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('settings.outputFormat')}
              </label>
              <div className="inline-flex rounded-lg overflow-hidden" style={{ border: '1px solid var(--color-border-muted)' }}>
                <button
                  type="button"
                  onClick={() => handleOutputFormatChange('markdown')}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium transition-colors"
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
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium transition-colors"
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

      {error && (
        <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
        </div>
      )}

      <div className="flex gap-3 justify-between pt-4" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
        <button
          type="button"
          onClick={() => setShowResetConfirm(true)}
          className="flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors"
          style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
        >
          <Trash2 className="h-4 w-4" /> {t('settings.resetPreferences')}
        </button>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={handleModifyPreferences}
            className="flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium"
            style={{
              backgroundColor: 'var(--color-accent-primary)',
              color: 'var(--color-text-on-accent)',
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
