import { useId } from 'react';
import { Link } from 'react-router';
import { ExternalLink, Sparkles, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { ClaudeCodeSetupState } from '@/api/model';
import { Button } from '@/components/ui/button';
import { buttonClass } from '@/components/ui/button-class';
import { useClaudeCodeSetup } from '@/hooks/useClaudeCode';
import { readSignInNoticeDismissed, signInNoticeDismissedPatch, useClaudeSignInEnd } from '@/hooks/useClaudeSignInEnd';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { CLAUDE_CODE_CHOICE_URL, CLAUDE_CODE_DOCS } from '@/lib/claudeCode';

/** Where a user's Claude Code stands once they have added a token: what is
 *  left is in its own view, not this announcement. */
const SET_UP = new Set<ClaudeCodeSetupState>(['connected', 'checking', 'refused', 'offline']);

/**
 * App-wide announcement of Claude Code mode to the users signed in with the
 * Claude sign-in, once the deployment set the day it stops working. Its
 * action opens Claude Code's view in Model access, where it is set up, so
 * it shows only where the server offers Claude Code and until the user has
 * added a token for it. Dismissed per user for that day, so it follows them
 * across devices and asks again if the day moves.
 *
 * Mounted beside `NetworkBanner`, in flow for the same reason: the route
 * below shrinks by its height.
 */
export default function ClaudeCodeAnnouncement() {
  const { t } = useTranslation();
  const titleId = useId();
  const end = useClaudeSignInEnd();
  const { preferences, isLoaded } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const due = !!end && isLoaded && readSignInNoticeDismissed(preferences) !== end.endsOn;
  const { setup } = useClaudeCodeSetup({ enabled: due });
  if (!due || !setup || (setup.on && SET_UP.has(setup.state))) return null;

  return (
    <section
      aria-labelledby={titleId}
      className="flex shrink-0 items-start gap-3 border-b py-3 pl-4 pr-2"
      style={{ backgroundColor: 'hsl(var(--card))', borderColor: 'hsl(var(--border))' }}
    >
      <Sparkles aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
      <div className="flex min-w-0 flex-1 flex-wrap items-center gap-x-6 gap-y-2.5">
        <div className="flex min-w-0 flex-[1_1_22rem] flex-col gap-0.5">
          <h2 id={titleId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('claudeCodeAnnouncement.title')}
          </h2>
          <p className="text-xs text-pretty" style={{ color: 'var(--color-text-secondary)' }}>
            {t('claudeCodeAnnouncement.body')} {t('claudeCodeAnnouncement.endsOn', { date: end.date })}
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Link to={CLAUDE_CODE_CHOICE_URL} className={buttonClass({ size: 'sm', className: 'tap-target' })}>
            {t('claudeCodeAnnouncement.switch')}
          </Link>
          <a
            href={CLAUDE_CODE_DOCS.fromClaudeSignIn}
            target="_blank"
            rel="noopener noreferrer"
            className={buttonClass({ variant: 'ghost', size: 'sm', className: 'tap-target gap-1.5' })}
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('claudeCodeAnnouncement.learnMore')}
            <ExternalLink aria-hidden="true" className="h-3.5 w-3.5" />
          </a>
        </div>
      </div>
      <Button
        type="button"
        variant="ghost"
        size="icon"
        className="tap-target h-8 w-8 shrink-0"
        aria-label={t('claudeCodeAnnouncement.dismiss')}
        onClick={() => updatePreferences.mutate(signInNoticeDismissedPatch(end.endsOn))}
      >
        <X aria-hidden="true" className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
      </Button>
    </section>
  );
}
