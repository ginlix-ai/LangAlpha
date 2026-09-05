/** Wire shapes behind the sandbox settings tabs (`GET .../sandbox/stats`). */

export interface SandboxPackage {
  name: string;
  version: string;
}

export interface DirBreakdownEntry {
  path: string;
  size: string;
}

export interface DiskUsage {
  used: string;
  available: string;
  total: string;
  use_percent: string;
}

export interface SandboxSkill {
  name: string;
  description?: string;
}

export interface SandboxStats {
  /** Display vocabulary, not the backend RuntimeState enum: 'running' is canonical
   *  across providers; anything outside TERMINAL_STATES is treated as in-progress. */
  state: string | null;
  /** Null, not absent, for a workspace that has no sandbox yet. */
  sandbox_id?: string | null;
  /** Which provider answered, e.g. 'daytona' | 'docker'. Null, not absent, when unresolved. */
  provider?: string | null;
  created_at?: string;
  auto_stop_interval?: number;
  resources: {
    cpu?: number;
    memory?: number;
    disk?: number;
    gpu?: number;
  };
  disk_usage?: DiskUsage;
  directory_breakdown?: DirBreakdownEntry[];
  packages?: SandboxPackage[];
  default_packages?: string[];
  mcp_servers?: string[];
  skills?: SandboxSkill[];
}

export interface InstallResult {
  success: boolean;
  output: string;
  error?: string;
  installed: string[];
}

export interface RefreshResult {
  status: string;
  message?: string;
  refreshed_tools?: boolean;
  skills_uploaded?: boolean;
  servers?: string[];
}
