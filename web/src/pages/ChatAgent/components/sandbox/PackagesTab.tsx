import { useMemo, useState } from 'react';
import { Package, Search } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { formatApiErrorDetail, installSandboxPackages } from '../../utils/api';
import type { InstallResult, SandboxPackage } from './sandboxTypes';

interface PackagesTabProps {
  workspaceId: string;
  packages: SandboxPackage[];
  /** Requirement specifiers as shipped in the image, e.g. `pandas>=2.2`. */
  defaultPackages: string[];
  /** Re-read the sandbox stats so a successful install shows up in the list. */
  onInstalled: () => void;
}

export function PackagesTab({ workspaceId, packages, defaultPackages, onInstalled }: PackagesTabProps) {
  const [pkgSearch, setPkgSearch] = useState('');
  const [installInput, setInstallInput] = useState('');
  const [installing, setInstalling] = useState(false);
  const [installResult, setInstallResult] = useState<InstallResult | null>(null);

  const filteredPackages = useMemo(() => {
    const q = pkgSearch.trim().toLowerCase();
    if (!q) return packages;
    return packages.filter(p => p.name.toLowerCase().includes(q));
  }, [packages, pkgSearch]);

  const defaultPkgSet = useMemo(
    () => new Set(defaultPackages.map(p => p.split(/[<>=!~]/)[0].toLowerCase())),
    [defaultPackages],
  );

  async function handleInstall() {
    const names = installInput.split(/[\s,]+/).filter(Boolean);
    if (!names.length) return;
    setInstalling(true);
    setInstallResult(null);
    try {
      const result = await installSandboxPackages(workspaceId, names);
      setInstallResult(result);
      if (result.success) {
        setInstallInput('');
        onInstalled();
      }
    } catch (err) {
      setInstallResult({
        success: false,
        output: '',
        error: formatApiErrorDetail(err),
        installed: [],
      });
    } finally {
      setInstalling(false);
    }
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
        <input
          type="text"
          value={pkgSearch}
          onChange={e => setPkgSearch(e.target.value)}
          placeholder="Filter packages..."
          className="w-full pl-9 pr-3 py-2 text-sm rounded-md bg-transparent outline-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-ring"
          style={{
            color: 'var(--color-text-primary)',
            border: '1px solid var(--color-border-muted)',
          }}
        />
      </div>

      {/* Package list */}
      <div
        className="flex flex-col gap-0.5 overflow-y-auto"
        style={{ maxHeight: '320px' }}
      >
        {filteredPackages.length === 0 ? (
          <div className="py-6 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            {pkgSearch ? 'No matching packages' : 'No packages installed'}
          </div>
        ) : (
          filteredPackages.map(p => {
            const isDefault = defaultPkgSet.has(p.name.toLowerCase());
            return (
              <div
                key={p.name}
                className="flex justify-between items-center py-1.5 px-3 rounded text-sm"
                style={{ backgroundColor: 'var(--color-bg-card)' }}
              >
                <div className="flex items-center gap-2">
                  <span style={{ color: isDefault ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)' }}>
                    {p.name}
                  </span>
                  {isDefault && (
                    <span
                      className="text-[0.625rem] px-1.5 py-0.5 rounded"
                      style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}
                    >
                      default
                    </span>
                  )}
                </div>
                <span className="font-mono text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                  {p.version}
                </span>
              </div>
            );
          })
        )}
      </div>

      {/* Install section */}
      <div
        className="flex flex-col gap-2 pt-3 border-t"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex gap-2">
          <input
            type="text"
            value={installInput}
            onChange={e => setInstallInput(e.target.value)}
            placeholder="Package names (e.g. torch transformers>=4.0)"
            onKeyDown={e => e.key === 'Enter' && !installing && handleInstall()}
            className="flex-1 px-3 py-2 text-sm rounded-md bg-transparent outline-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-ring"
            style={{
              color: 'var(--color-text-primary)',
              border: '1px solid var(--color-border-muted)',
            }}
          />
          <button
            onClick={handleInstall}
            disabled={installing || !installInput.trim()}
            className="flex items-center gap-1.5 px-4 py-2 text-sm rounded-md transition-colors disabled:opacity-50"
            style={{
              color: 'var(--color-btn-primary-text)',
              backgroundColor: 'var(--color-btn-primary-bg)',
            }}
          >
            {installing ? <Loader size={14} className="text-current" /> : <Package className="h-3.5 w-3.5" />}
            Install
          </button>
        </div>

        {/* Install result */}
        {installResult && (
          <div
            className="text-xs p-2 rounded font-mono whitespace-pre-wrap max-h-32 overflow-y-auto"
            style={{
              backgroundColor: 'var(--color-bg-card)',
              color: installResult.success ? 'var(--color-text-secondary)' : 'var(--color-loss)',
            }}
          >
            {installResult.error || installResult.output || 'Done'}
          </div>
        )}
      </div>
    </div>
  );
}
