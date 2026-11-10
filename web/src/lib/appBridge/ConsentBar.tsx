import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ShieldCheck } from 'lucide-react';
import { Button } from '@/components/ui/button';
import type { Scope } from './protocol';

/**
 * How long Allow stays off once the bar, or what it asks for, appears: the
 * frame picks that moment, so it could slide the bar under a click the user
 * already meant for the page.
 */
const ARM_MS = 600;

interface ConsentBarProps {
  /** How the bar names the frame: a running app, or a page. */
  subject: 'app' | 'page';
  /** The ask-first scopes the frame declared and the user has not allowed. */
  pending: Scope[];
  /** On screen; a bar in a layer out of sight never arms. */
  visible: boolean;
  onAllow: () => void;
  onDismiss: () => void;
}

/** The one line above a frame that asks the user for what the frame may not read unasked. */
export function ConsentBar({ subject, pending, visible, onAllow, onDismiss }: ConsentBarProps): React.ReactElement {
  const { t } = useTranslation();
  // Armed per showing: the bar on screen asking for this set.
  const showing = visible ? pending.join(',') : '';
  const [arm, setArm] = useState({ showing: '', ready: false });
  if (arm.showing !== showing) setArm({ showing, ready: false });
  useEffect(() => {
    if (!showing) return;
    const timer = setTimeout(() => setArm({ showing, ready: true }), ARM_MS);
    return () => clearTimeout(timer);
  }, [showing]);
  const armed = arm.showing === showing && arm.ready;
  const scopes = pending.map((s) => t(`appBridge.scope.${s}`)).join(t('appBridge.and'));

  return (
    <div
      className="flex shrink-0 items-center gap-2 border-b px-3 py-1.5 text-xs"
      style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-surface)', color: 'var(--color-text-secondary)' }}
      role="region"
      aria-label={t('appBridge.accessLabel')}
    >
      <ShieldCheck className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
      <span className="min-w-0 flex-1 truncate">
        {t(subject === 'page' ? 'appBridge.asksPage' : 'appBridge.asksApp', { scopes })}
      </span>
      <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={onDismiss}>
        {t('appBridge.notNow')}
      </Button>
      <Button variant="outline" size="sm" className="h-6 px-2 text-xs" disabled={!armed} onClick={onAllow}>
        {t('appBridge.allow')}
      </Button>
    </div>
  );
}
