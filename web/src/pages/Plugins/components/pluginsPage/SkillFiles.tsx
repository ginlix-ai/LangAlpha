import { FileCode, FileText, Folder } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { SkillFile } from '@/pages/ChatAgent/utils/api';
import { isMarkdownPath } from './skillPaths';

/**
 * The skill's folder as the agent finds it in the sandbox: SKILL.md, the files
 * beside it, then each folder (`references/`, `scripts/`) with what it holds.
 * One level of folders is enough for a skill; anything deeper keeps its
 * remaining path on the file's line.
 */
export function SkillFiles({
  files,
  selected,
  onSelect,
}: {
  files: SkillFile[];
  selected: string;
  onSelect: (path: string) => void;
}) {
  const top = files
    .filter((f) => !f.path.includes('/'))
    .sort((a, b) => (a.path === 'SKILL.md' ? -1 : b.path === 'SKILL.md' ? 1 : a.path.localeCompare(b.path)));
  const folders = new Map<string, SkillFile[]>();
  for (const f of files) {
    const slash = f.path.indexOf('/');
    if (slash < 0) continue;
    const dir = f.path.slice(0, slash);
    folders.set(dir, [...(folders.get(dir) ?? []), f]);
  }

  const file = (f: SkillFile, label: string, nested: boolean) => (
    <button
      key={f.path}
      type="button"
      className={cn('pp-file', nested && 'is-nested', f.path === selected && 'is-selected')}
      aria-current={f.path === selected ? 'true' : undefined}
      onClick={() => onSelect(f.path)}
      title={f.path}
    >
      {isMarkdownPath(f.path) ? <FileText aria-hidden /> : <FileCode aria-hidden />}
      <span>{label}</span>
    </button>
  );

  return (
    <div className="pp-files">
      {top.map((f) => file(f, f.path, false))}
      {[...folders.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([dir, items]) => (
          <div key={dir}>
            <div className="pp-files-dir">
              <Folder aria-hidden />
              <span>{dir}</span>
            </div>
            {items.map((f) => file(f, f.path.slice(dir.length + 1), true))}
          </div>
        ))}
    </div>
  );
}
