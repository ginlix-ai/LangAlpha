import React, { useState } from 'react';
import { Server, Plus, Trash2, Pencil, PackagePlus } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import {
  useMcpCatalog,
  useCreateMcpCatalogServer,
  useUpdateMcpCatalogServer,
  useDeleteMcpCatalogServer,
} from '@/hooks/useMcpServers';
import { toast } from '@/components/ui/use-toast';
import { McpServerModal } from './McpServerModal';
import { formatApiErrorDetail, type CatalogServer, type McpServerInput, type EffectiveServer } from '../../utils/api';

/**
 * The Templates view — the user's reusable MCP catalog. Each template can be
 * copied into the current workspace ("Add to this workspace" → POST
 * `{ from_template }`). Templates are CRUD-able here; nothing runs from them.
 */

interface TemplatesViewProps {
  workspaceId: string;
  secretNames: string[];
  /** Adds a template to the current workspace by name. */
  onAddToWorkspace: (templateName: string) => Promise<void>;
  /** Names already present in the workspace (to disable duplicate "add"). */
  workspaceServerNames: Set<string>;
}

/** Adapt a catalog row to the modal's `EffectiveServer`-shaped initial value. */
function catalogToInitial(c: CatalogServer): EffectiveServer {
  return {
    name: c.name,
    origin: 'workspace',
    transport: c.transport,
    enabled: true,
    editable: true,
    deletable: true,
    status: 'unknown',
    error: '',
    tool_count: 0,
    tools: [],
    missing_secrets: [],
    env_refs: c.env_refs,
    header_refs: c.header_refs,
    description: c.description,
    instruction: c.instruction,
    tool_exposure_mode: c.tool_exposure_mode,
    command: c.command,
    args: c.args,
    url: c.url,
    config_version: 0,
  };
}

export function TemplatesView({
  workspaceId,
  secretNames,
  onAddToWorkspace,
  workspaceServerNames,
}: TemplatesViewProps) {
  const { data: catalog, isLoading, error } = useMcpCatalog();
  const createMutation = useCreateMcpCatalogServer();
  const updateMutation = useUpdateMcpCatalogServer();
  const deleteMutation = useDeleteMcpCatalogServer();

  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<CatalogServer | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [addingName, setAddingName] = useState<string | null>(null);
  const [deletingName, setDeletingName] = useState<string | null>(null);

  async function handleSubmit(body: McpServerInput) {
    setSubmitError(null);
    try {
      if (editing) {
        await updateMutation.mutateAsync({ name: editing.name, body });
      } else {
        await createMutation.mutateAsync(body);
      }
      setModalOpen(false);
      setEditing(null);
    } catch (err) {
      setSubmitError(formatApiErrorDetail(err));
    }
  }

  async function handleAdd(name: string) {
    setAddingName(name);
    try {
      await onAddToWorkspace(name);
    } catch (err) {
      toast({
        variant: 'destructive',
        title: 'Could not add to workspace',
        description: formatApiErrorDetail(err),
      });
    } finally {
      setAddingName(null);
    }
  }

  async function handleDelete(name: string) {
    try {
      await deleteMutation.mutateAsync(name);
      setDeletingName(null);
    } catch (err) {
      toast({
        variant: 'destructive',
        title: 'Could not delete template',
        description: formatApiErrorDetail(err),
      });
    }
  }

  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        {[1, 2].map((i) => (
          <div key={i} className="h-14 rounded-lg animate-pulse" style={{ backgroundColor: 'var(--color-bg-card)' }} />
        ))}
      </div>
    );
  }

  const templates = catalog?.servers ?? [];
  const maxServers = catalog?.max_servers ?? 0;
  const atCap = maxServers > 0 && templates.length >= maxServers;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          Templates
        </span>
        <button
          type="button"
          onClick={() => { setEditing(null); setSubmitError(null); setModalOpen(true); }}
          disabled={atCap}
          title={atCap ? `At ${maxServers}/${maxServers} — delete one first` : undefined}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
          style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
        >
          <Plus className="h-3 w-3" />
          New template
        </button>
      </div>

      {error ? (
        <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
          {(error as { message?: string })?.message || 'Failed to load templates'}
        </div>
      ) : templates.length === 0 ? (
        <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          No templates yet. Create one to reuse across workspaces.
        </div>
      ) : (
        <div className="flex flex-col gap-1.5">
          {templates.map((t) => {
            const alreadyAdded = workspaceServerNames.has(t.name);
            return (
              <div
                key={t.name}
                className="flex items-start justify-between gap-3 py-2.5 px-3 rounded-lg"
                style={{ backgroundColor: 'var(--color-bg-card)' }}
                data-testid={`mcp-template-${t.name}`}
              >
                <div className="min-w-0 flex flex-col gap-0.5">
                  <div className="flex items-center gap-2">
                    <Server className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
                      {t.name}
                    </span>
                    <span className="text-[0.625rem] px-1.5 py-0.5 rounded uppercase" style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-tag)' }}>
                      {t.transport}
                    </span>
                  </div>
                  {t.description && (
                    <p className="text-[0.6875rem] line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
                      {t.description}
                    </p>
                  )}
                </div>

                <div className="flex items-center gap-1 flex-shrink-0">
                  <button
                    type="button"
                    onClick={() => handleAdd(t.name)}
                    disabled={alreadyAdded || addingName === t.name}
                    title={alreadyAdded ? 'Already in this workspace' : 'Add to this workspace'}
                    className="inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md transition-colors disabled:opacity-50"
                    style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                  >
                    {addingName === t.name ? <Loader size={12} className="text-current" /> : <PackagePlus className="h-3 w-3" />}
                    {alreadyAdded ? 'Added' : 'Add to workspace'}
                  </button>
                  <button
                    type="button"
                    onClick={() => { setEditing(t); setSubmitError(null); setModalOpen(true); }}
                    className="p-1.5 rounded hover:bg-foreground/10"
                    style={{ color: 'var(--color-text-tertiary)' }}
                    aria-label={`Edit ${t.name}`}
                  >
                    <Pencil className="h-3.5 w-3.5" />
                  </button>
                  {deletingName === t.name ? (
                    <div className="flex items-center gap-1">
                      <button
                        type="button"
                        onClick={() => handleDelete(t.name)}
                        disabled={deleteMutation.isPending}
                        className="px-2 py-1 text-[0.6875rem] rounded disabled:opacity-50"
                        style={{ color: 'var(--color-loss)' }}
                      >
                        {deleteMutation.isPending ? 'Deleting…' : 'Confirm'}
                      </button>
                      <button
                        type="button"
                        onClick={() => setDeletingName(null)}
                        className="px-2 py-1 text-[0.6875rem] rounded hover:bg-foreground/10"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        Cancel
                      </button>
                    </div>
                  ) : (
                    <button
                      type="button"
                      onClick={() => setDeletingName(t.name)}
                      className="p-1.5 rounded hover:bg-foreground/10"
                      style={{ color: 'var(--color-text-tertiary)' }}
                      aria-label={`Delete ${t.name}`}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {modalOpen && (
        <McpServerModal
          workspaceId={workspaceId}
          secretNames={secretNames}
          initial={editing ? catalogToInitial(editing) : null}
          allowDiscover={false}
          onClose={() => { setModalOpen(false); setEditing(null); }}
          onSubmit={handleSubmit}
          saving={createMutation.isPending || updateMutation.isPending}
          submitError={submitError}
        />
      )}
    </div>
  );
}
