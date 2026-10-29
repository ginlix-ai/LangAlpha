import React from 'react';
import { getToolIcon } from './toolDisplayConfig';
import { directToolIdentity } from '../utils/directTools';
import { DirectToolRowMark } from './mcp/DirectToolMark';
import { useWorkspaceFolders } from '../contexts/WorkspaceContext';

/**
 * The icon for a tool call: a direct MCP tool wears its vendor's mark,
 * everything else keeps its lucide glyph. One component so the same call looks
 * the same in the timeline row, the message body and the detail header, and so
 * the vendor lookup hook only mounts on the rows that need it.
 *
 * The mark is looked up by server name, and an aliased tool name carries a
 * digest there rather than the server, so a call whose result is in hand
 * passes its artifact: without it the lookup misses and the vendor's logo
 * silently gives way to the generic connector mark.
 */
export function ToolIcon({ toolName, args, artifact, className, style }: {
  toolName: string;
  args?: Record<string, unknown>;
  artifact?: unknown;
  className?: string;
  style?: React.CSSProperties;
}): React.ReactElement {
  const root = useWorkspaceFolders()?.rootDir;
  const direct = directToolIdentity(toolName, artifact);
  if (direct) return <DirectToolRowMark server={direct.server} className={className} style={style} />;
  const IconComponent = getToolIcon(toolName, args, root);
  return <IconComponent className={className} style={style} />;
}
