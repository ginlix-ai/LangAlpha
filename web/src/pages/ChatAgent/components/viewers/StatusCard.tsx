import React from 'react';
import type { LucideIcon } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import './StatusCard.css';

/** 'working' is a spinner: something is under way, and the card offers nothing to press. */
export type StatusIcon = LucideIcon | 'working';

interface StatusCardProps {
  icon: StatusIcon;
  title: string;
  /** A monospaced line naming what the card is about. */
  detail?: string;
  body?: string;
  action?: { label: string; run: () => void };
}

/** A card over a view that has nothing to show yet, saying why. */
export function StatusCard({ icon: Icon, title, detail, body, action }: StatusCardProps): React.ReactElement {
  return (
    <div className="status-card-overlay">
      <div className="status-card">
        {Icon === 'working'
          ? <Loader size={20} label={title} style={{ color: 'var(--color-accent-primary)' }} />
          : <Icon size={28} className="status-card-icon" />}
        <div className="status-card-info">
          <span className="status-card-title">{title}</span>
          {detail && <span className="status-card-detail">{detail}</span>}
          {body && <span className="status-card-body">{body}</span>}
        </div>
        {action && (
          <Button type="button" variant="outline" size="sm" className="status-card-action" onClick={action.run}>
            {action.label}
          </Button>
        )}
      </div>
    </div>
  );
}
