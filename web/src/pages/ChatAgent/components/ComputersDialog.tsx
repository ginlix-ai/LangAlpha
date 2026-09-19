/**
 * The machines behind the workspace gallery.
 *
 * Deliberately a dialog rather than a page: a computer is infrastructure a
 * user visits when something needs starting or a new one is wanted, not a
 * place they work. Everything about running the machine that already had a
 * home stays in the sandbox settings panel.
 */
import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Play, Plus, Square, Star } from 'lucide-react';

import { useMutation, useQueryClient } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { Disclosure } from '@/components/ui/Disclosure';
import { ModalShell } from '@/components/ui/ModalShell';
import { Input } from '@/components/ui/input';
import { Loader } from '@/components/ui/loader';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ResourceTier } from '@/types/api';

import { createComputer, startComputer, stopComputer } from '../utils/api';
import { patchComputerStatusInCaches, useComputers } from '../hooks/useComputers';
import { useTierQuota } from '../hooks/useTierQuota';
import { ComputerStatusIndicator, isComputerStatusTransitional } from './computerStatusUi';
import { TierRadioGroup, normalizeTier, tierLabel } from './tierUi';
import { denialMessage } from '../utils/denialMessage';

interface ComputersDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

function ComputersDialog({ open, onOpenChange }: ComputersDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();
  const queryClient = useQueryClient();
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [tier, setTier] = useState<ResourceTier>('standard');
  const [error, setError] = useState<string | null>(null);

  const { data, isLoading } = useComputers({ enabled: open });
  const computers = data?.computers ?? [];
  // The server names a machine after where it runs; the listed ones share
  // the stack's provider, so the placeholder can promise the same name.
  const namePlaceholder = computers[0]?.kind === 'docker'
    ? t('computer.namePlaceholderLocal', 'Local computer')
    : t('computer.namePlaceholderCloud', 'Cloud computer');

  const { data: workspaceQuota } = useTierQuota({ enabled: open && creating });

  const createMutation = useMutation({
    mutationFn: () => createComputer({ name: name.trim() || undefined, resource_tier: tier }),
    onSuccess: () => {
      setCreating(false);
      setName('');
      setTier('standard');
      setError(null);
      void queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() });
      void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() });
    },
    // The platform wrote the denial; relaying its sentence is the whole job.
    onError: (err) => setError(denialMessage(err, t)),
  });

  const actionMutation = useMutation({
    mutationFn: async ({ computerId, action }: { computerId: string; action: 'start' | 'stop' }) =>
      action === 'start'
        ? startComputer(computerId, { lazy: true })
        : stopComputer(computerId),
    onSuccess: (res) => {
      // Reflecting the response status is what brings a transitional machine
      // into the fan-out's watch set, so the stream reports the rest.
      patchComputerStatusInCaches(queryClient, res.computer_id, res.status);
      void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    },
    onError: (err) => setError(denialMessage(err, t)),
  });

  const closeAndReset = () => {
    setCreating(false);
    setError(null);
    onOpenChange(false);
  };

  const footer = (
    <div className="flex justify-end gap-2">
      {creating ? (
        <>
          <Button variant="ghost" onClick={() => { setCreating(false); setError(null); }} disabled={createMutation.isPending}>
            {t('common.cancel')}
          </Button>
          <Button onClick={() => createMutation.mutate()} disabled={createMutation.isPending}>
            {createMutation.isPending
              ? t('computer.creating', 'Creating...')
              : t('computer.create', 'Create computer')}
          </Button>
        </>
      ) : (
        <Button variant="outline" onClick={() => { setCreating(true); setError(null); }}>
          <Plus className="h-4 w-4" />
          {t('computer.newComputer', 'New computer')}
        </Button>
      )}
    </div>
  );

  return (
    <AnimatePresence>
      {open && (
        <ModalShell
          labelId={titleId}
          title={t('computer.computers', 'Computers')}
          subtitle={t('computer.computersDesc', 'Your workspaces are folders on these machines. Starting or stopping one affects every workspace it holds.')}
          onClose={closeAndReset}
          width="standard"
          density="form"
          footer={footer}
        >
          {isLoading ? (
            <div className="flex items-center gap-2 py-6 justify-center" style={{ color: 'var(--color-text-tertiary)' }}>
              <Loader size={14} style={{ color: 'var(--color-accent-primary)' }} />
              <span className="text-sm">{t('computer.loading', 'Loading computers')}</span>
            </div>
          ) : (
            <div className="flex flex-col gap-2">
              {computers.length === 0 && (
                <p className="text-sm py-4" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('computer.empty', 'No computers yet. Your first workspace creates one.')}
                </p>
              )}
              {computers.map((computer) => (
                <ComputerRow
                  key={computer.computer_id}
                  computer={computer}
                  busy={
                    actionMutation.isPending &&
                    actionMutation.variables?.computerId === computer.computer_id
                  }
                  onAction={(action) =>
                    actionMutation.mutate({ computerId: computer.computer_id, action })
                  }
                />
              ))}
            </div>
          )}

          <Disclosure open={creating} className="flex flex-col gap-3 pt-2">
            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }} htmlFor="new-computer-name">
                {t('computer.nameLabel', 'Name')} <span style={{ color: 'var(--color-text-tertiary)' }}>{t('common.optional')}</span>
              </label>
              <Input
                id="new-computer-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder={namePlaceholder}
                maxLength={255}
              />
            </div>
            <TierRadioGroup
              value={tier}
              onChange={setTier}
              label={t('computer.tierLabel', 'Computer spec')}
              quota={workspaceQuota}
            />
          </Disclosure>

          {error && (
            <p className="text-sm" role="alert" style={{ color: 'var(--color-icon-danger)' }}>
              {error}
            </p>
          )}
        </ModalShell>
      )}
    </AnimatePresence>
  );
}

interface ComputerRowProps {
  computer: Computer;
  busy: boolean;
  onAction: (action: 'start' | 'stop') => void;
}

function ComputerRow({ computer, busy, onAction }: ComputerRowProps) {
  const { t } = useTranslation();
  const isRunning = computer.status === 'running';
  const isTransitional = isComputerStatusTransitional(computer.status);
  const tier = normalizeTier(computer.resource_tier);

  return (
    <div
      className="flex items-center gap-3 rounded-lg border p-3"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 min-w-0">
          <span className="font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {computer.name}
          </span>
          {computer.is_primary && (
            <span
              className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[0.625rem] font-medium flex-shrink-0"
              style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
              title={t('computer.primaryBadgeTitle', 'New workspaces are created on this computer')}
            >
              <Star className="h-3 w-3" />
              {t('computer.primaryBadge', 'Primary')}
            </span>
          )}
        </div>
        <div className="flex items-center gap-2 text-xs mt-1" style={{ color: 'var(--color-text-tertiary)' }}>
          <ComputerStatusIndicator status={computer.status} />
          <span aria-hidden="true">·</span>
          <span>{tierLabel(t, tier)}</span>
          {typeof computer.workspace_count === 'number' && (
            <>
              <span aria-hidden="true">·</span>
              <span>{t('computer.workspaceCount', { count: computer.workspace_count })}</span>
            </>
          )}
        </div>
      </div>
      {isRunning ? (
        <Button variant="ghost" size="sm" disabled={busy} onClick={() => onAction('stop')}>
          <Square className="h-3.5 w-3.5" />
          {t('computer.stop', 'Stop')}
        </Button>
      ) : (
        <Button
          variant="ghost"
          size="sm"
          disabled={busy || isTransitional || computer.status === 'deleted'}
          onClick={() => onAction('start')}
        >
          <Play className="h-3.5 w-3.5" />
          {t('computer.start', 'Start')}
        </Button>
      )}
    </div>
  );
}

export default ComputersDialog;
