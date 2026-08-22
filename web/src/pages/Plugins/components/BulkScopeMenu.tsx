import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowRightLeft, Check, ChevronDown, FolderOpen, Globe } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
} from '@/components/ui/dropdown-menu';
import type { ScopeWorkspace } from './ScopeControl';

/**
 * The bulk counterpart of the row ScopeControl, rendered in the select-mode
 * action bar. Same three scope states, applied to the whole selection: all
 * workspaces, only a chosen set of workspaces (deny-list), or moved into one
 * workspace (tier change). Each entry carries the count of selected rows it
 * can actually reach; ineligible rows are simply left out of the run.
 */

export interface BulkScopeSpec {
  workspaces: ScopeWorkspace[];
  everywhereCount: number;
  onEverywhere: () => void;
  onlyInCount: number;
  onOnlyIn: (workspaceIds: string[]) => void;
  moveCount: number;
  onMoveTo: (workspaceId: string) => void;
}

export function BulkScopeMenu({
  workspaces,
  everywhereCount,
  onEverywhere,
  onlyInCount,
  onOnlyIn,
  moveCount,
  onMoveTo,
}: BulkScopeSpec) {
  const { t } = useTranslation();
  // The checklist stages locally and commits on Apply: a bulk deny-list write
  // is a fan-out, not something to fire on every checkbox flip.
  const [staged, setStaged] = useState<ReadonlySet<string>>(new Set());

  const anyEligible = everywhereCount > 0 || onlyInCount > 0 || moveCount > 0;

  return (
    <DropdownMenu onOpenChange={(open) => !open && setStaged(new Set())}>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          disabled={!anyEligible}
          aria-label={t('plugins.bulk.scopeAria')}
          className="inline-flex items-center gap-1 px-2 py-1 text-xs rounded transition-colors hover:bg-foreground/10 disabled:opacity-40 disabled:hover:bg-transparent"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('plugins.bulk.scope')}
          <ChevronDown className="h-3 w-3" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" data-testid="bulk-scope-menu">
        <DropdownMenuItem
          disabled={everywhereCount === 0}
          onSelect={onEverywhere}
        >
          <Globe className="h-3.5 w-3.5 mr-2" />
          {t('plugins.bulk.scopeEverywhere', { count: everywhereCount })}
        </DropdownMenuItem>

        <DropdownMenuSub>
          <DropdownMenuSubTrigger disabled={onlyInCount === 0 || workspaces.length === 0}>
            <Check className="h-3.5 w-3.5 mr-2" />
            {t('plugins.bulk.scopeOnlyIn', { count: onlyInCount })}
          </DropdownMenuSubTrigger>
          <DropdownMenuSubContent>
            <DropdownMenuLabel>{t('plugins.scope.activeIn')}</DropdownMenuLabel>
            {workspaces.map((ws) => {
              const checked = staged.has(ws.id);
              return (
                <DropdownMenuItem
                  key={ws.id}
                  onSelect={(e) => {
                    // Keep the menu open: the checklist is a multi-pick.
                    e.preventDefault();
                    setStaged((prev) => {
                      const next = new Set(prev);
                      if (checked) next.delete(ws.id);
                      else next.add(ws.id);
                      return next;
                    });
                  }}
                >
                  <Check className="h-3.5 w-3.5 mr-2" style={{ opacity: checked ? 1 : 0 }} />
                  <span className="truncate">{ws.name}</span>
                </DropdownMenuItem>
              );
            })}
            <DropdownMenuSeparator />
            <DropdownMenuItem
              // Zero chosen workspaces would mean "active nowhere" — that
              // intent already has a name (Disable), so Apply requires one.
              disabled={staged.size === 0}
              onSelect={() => onOnlyIn([...staged])}
            >
              <span className="font-medium">
                {t('plugins.bulk.scopeApply', { count: onlyInCount })}
              </span>
            </DropdownMenuItem>
          </DropdownMenuSubContent>
        </DropdownMenuSub>

        <DropdownMenuSub>
          <DropdownMenuSubTrigger disabled={moveCount === 0 || workspaces.length === 0}>
            <ArrowRightLeft className="h-3.5 w-3.5 mr-2" />
            {t('plugins.bulk.scopeMoveTo', { count: moveCount })}
          </DropdownMenuSubTrigger>
          <DropdownMenuSubContent>
            {workspaces.map((ws) => (
              <DropdownMenuItem key={ws.id} onSelect={() => onMoveTo(ws.id)}>
                <FolderOpen className="h-3.5 w-3.5 mr-2" />
                <span className="truncate">{ws.name}</span>
              </DropdownMenuItem>
            ))}
          </DropdownMenuSubContent>
        </DropdownMenuSub>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
