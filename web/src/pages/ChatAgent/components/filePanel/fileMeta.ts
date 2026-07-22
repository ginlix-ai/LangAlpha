import { File, FileImage, FileText } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import type { SortOption } from './types';

// --- Constants ---

export const EXT_TO_LANG: Record<string, string> = {
  py: 'python', js: 'javascript', jsx: 'jsx', ts: 'typescript', tsx: 'tsx',
  json: 'json', html: 'html', css: 'css', sql: 'sql', sh: 'bash', bash: 'bash',
  yaml: 'yaml', yml: 'yaml', xml: 'xml', java: 'java', go: 'go', rs: 'rust', rb: 'ruby',
};

export const EDITABLE_EXTENSIONS = new Set([
  ...Object.keys(EXT_TO_LANG),
  'md', 'txt', 'csv', 'env', 'toml', 'cfg', 'ini', 'log',
]);

export function getFileIcon(fileName: string): LucideIcon {
  const ext = fileName.split('.').pop()?.toLowerCase();
  if (['md', 'txt', 'csv', 'json', 'py', 'js', 'html'].includes(ext!)) return FileText;
  if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'].includes(ext!)) return FileImage;
  return File;
}

export function getFileExtension(fileName: string): string {
  return fileName.split('.').pop()?.toLowerCase() || '';
}

// Map extensions to human-readable type categories
const EXT_TO_TYPE: Record<string, string> = {
  md: 'Docs', txt: 'Docs', pdf: 'Docs',
  py: 'Code', js: 'Code', jsx: 'Code', ts: 'Code', tsx: 'Code',
  html: 'Code', css: 'Code', sql: 'Code', sh: 'Code', bash: 'Code',
  java: 'Code', go: 'Code', rs: 'Code', rb: 'Code',
  json: 'Data', csv: 'Data', yaml: 'Data', yml: 'Data', xml: 'Data',
  xlsx: 'Data', xls: 'Data',
  png: 'Image', jpg: 'Image', jpeg: 'Image', gif: 'Image', svg: 'Image', webp: 'Image',
};

export function getFileType(filePath: string): string {
  const ext = getFileExtension(filePath.split('/').pop() || '');
  return EXT_TO_TYPE[ext] || 'Other';
}

/** Derive available type categories from current file list */
export function getAvailableTypes(filePaths: string[]): string[] {
  const types = new Set<string>();
  for (const fp of filePaths) types.add(getFileType(fp));
  // Fixed display order, filtered to only those present
  return ['Docs', 'Code', 'Data', 'Image', 'Other'].filter((t) => types.has(t));
}

export const SORT_OPTIONS: SortOption[] = [
  { value: 'name-asc', label: 'Name A-Z' },
  { value: 'name-desc', label: 'Name Z-A' },
  { value: 'type', label: 'Type' },
];

export function sortFiles(filePaths: string[], sortBy: string): string[] {
  const sorted = [...filePaths];
  switch (sortBy) {
    case 'name-asc':
      return sorted.sort((a, b) => {
        const na = a.split('/').pop()!.toLowerCase();
        const nb = b.split('/').pop()!.toLowerCase();
        return na.localeCompare(nb);
      });
    case 'name-desc':
      return sorted.sort((a, b) => {
        const na = a.split('/').pop()!.toLowerCase();
        const nb = b.split('/').pop()!.toLowerCase();
        return nb.localeCompare(na);
      });
    case 'type':
      return sorted.sort((a, b) => {
        const ea = getFileExtension(a.split('/').pop() || '');
        const eb = getFileExtension(b.split('/').pop() || '');
        if (ea !== eb) return ea.localeCompare(eb);
        return a.split('/').pop()!.toLowerCase().localeCompare(b.split('/').pop()!.toLowerCase());
      });
    default:
      return sorted;
  }
}

/** Directory display priority: root first, then results/, data/, rest alphabetical */
const DIR_PRIORITY: Record<string, number> = { '/': 0, 'results': 1, 'data': 2 };

/** System directory prefixes -- collapsed by default when visible.
 *  Source of truth: src/ptc_agent/core/paths.py -> AGENT_SYSTEM_DIRS */
export const SYSTEM_DIR_PREFIXES = ['.system', 'code', 'tools', 'mcp_servers', '.agents', '.self-improve'];

export function dirSortKey(dir: string): number {
  if (DIR_PRIORITY[dir] != null) return DIR_PRIORITY[dir];
  if (SYSTEM_DIR_PREFIXES.includes(dir)) return 99;
  return 3;
}
