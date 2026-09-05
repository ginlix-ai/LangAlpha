import { BookOpen, RefreshCw, Server } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { RefreshResult, SandboxStats } from './sandboxTypes';

interface ToolsTabProps {
  stats: SandboxStats;
  refreshing: boolean;
  refreshResult: RefreshResult | null;
  onRefresh: () => void;
}

export function ToolsTab({ stats, refreshing, refreshResult, onRefresh }: ToolsTabProps) {
  return (
    <div className="flex flex-col gap-5">
      {/* MCP Servers list */}
      <div>
        <h3 className="text-sm font-medium mb-3" style={{ color: 'var(--color-text-primary)' }}>
          Connected MCP Servers
        </h3>
        {stats.mcp_servers && stats.mcp_servers.length > 0 ? (
          <div className="flex flex-col gap-1">
            {stats.mcp_servers.map(name => (
              <div
                key={name}
                className="flex items-center gap-2.5 py-2 px-3 rounded text-sm"
                style={{ backgroundColor: 'var(--color-bg-card)' }}
              >
                <Server className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                <span style={{ color: 'var(--color-text-primary)' }}>{name}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="py-4 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            No MCP servers connected
          </div>
        )}
      </div>

      {/* Skills list */}
      <div>
        <h3 className="text-sm font-medium mb-3" style={{ color: 'var(--color-text-primary)' }}>
          Available Skills
        </h3>
        {stats.skills && stats.skills.length > 0 ? (
          <div className="flex flex-col gap-1">
            {stats.skills.map(skill => (
              <div
                key={skill.name}
                className="flex items-start gap-2.5 py-2 px-3 rounded text-sm"
                style={{ backgroundColor: 'var(--color-bg-card)' }}
              >
                <BookOpen className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-accent-primary)' }} />
                <div className="min-w-0">
                  <span style={{ color: 'var(--color-text-primary)' }}>{skill.name}</span>
                  {skill.description && (
                    <p className="text-xs mt-0.5 line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
                      {skill.description}
                    </p>
                  )}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="py-4 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            No skills installed
          </div>
        )}
      </div>

      {/* Sync button */}
      <div
        className="flex flex-col gap-3 pt-3 border-t"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <button
          onClick={onRefresh}
          disabled={refreshing}
          className="flex items-center justify-center gap-2 w-full px-4 py-2.5 text-sm rounded-md transition-colors disabled:opacity-50"
          style={{
            color: 'var(--color-text-primary)',
            border: '1px solid var(--color-border-muted)',
            backgroundColor: 'var(--color-bg-card)',
          }}
        >
          {refreshing ? <Loader size={16} className="text-current" /> : <RefreshCw className="h-4 w-4" />}
          Sync Tools & Skills
        </button>

        {refreshResult && (
          <div
            className="text-xs p-3 rounded"
            style={{
              backgroundColor: 'var(--color-bg-card)',
              color: refreshResult.status === 'error' ? 'var(--color-loss)' : 'var(--color-text-secondary)',
            }}
          >
            {refreshResult.status === 'error' ? (
              refreshResult.message
            ) : (
              <div className="flex flex-col gap-1">
                <span>Tools refreshed: {refreshResult.refreshed_tools ? 'Yes' : 'No'}</span>
                <span>Skills uploaded: {refreshResult.skills_uploaded ? 'Yes' : 'No'}</span>
                {refreshResult.servers && refreshResult.servers.length > 0 && (
                  <span>Servers: {refreshResult.servers.length} connected</span>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
