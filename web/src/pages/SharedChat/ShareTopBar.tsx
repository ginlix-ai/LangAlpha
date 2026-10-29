import React from 'react';
import { Link } from 'react-router';
import type { LucideIcon } from 'lucide-react';
import { Button } from '@/components/ui/button';

interface ShareTopBarProps {
  name: string;
  actions?: React.ReactNode;
}

/** The one-line header a shared file or app page carries above its content. */
export function ShareTopBar({ name, actions }: ShareTopBarProps): React.ReactElement {
  return (
    // The drag band inside the desktop shell; the page has no other chrome.
    <header className="share-topbar" data-chrome="drag">
      <Link to="/" className="share-topbar-brand title-font">LangAlpha</Link>
      <span className="share-topbar-sep" aria-hidden="true">/</span>
      <h1 className="share-topbar-name" title={name}>{name}</h1>
      {actions && <div className="share-topbar-actions">{actions}</div>}
    </header>
  );
}

interface ShareTopBarActionProps {
  icon: LucideIcon;
  label: string;
  onClick: () => void;
  disabled?: boolean;
}

/** An action in the bar. On a phone the label folds away, so the button carries it as its name. */
export function ShareTopBarAction({ icon: Icon, label, onClick, disabled }: ShareTopBarActionProps): React.ReactElement {
  return (
    <Button
      type="button"
      variant="outline"
      size="sm"
      onClick={onClick}
      disabled={disabled}
      aria-label={label}
      className="h-[30px] gap-1.5 px-2 text-xs sm:px-2.5"
    >
      <Icon className="h-3.5 w-3.5" />
      <span className="hidden sm:inline">{label}</span>
    </Button>
  );
}
