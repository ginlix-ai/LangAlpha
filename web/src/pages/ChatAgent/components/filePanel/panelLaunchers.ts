import type { LucideIcon } from 'lucide-react';

/** One thing a new tab can open on, built by the panel, which owns the openers. */
export interface PanelLauncher {
  key: string;
  label: string;
  icon: LucideIcon;
  open: () => void;
}

/** The new tab button's menu: the kinds of tab, then the running apps. */
export interface NewTabMenu {
  kinds: PanelLauncher[];
  apps: PanelLauncher[];
}
