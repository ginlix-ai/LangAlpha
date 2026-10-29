import { useBrokerages } from '@/hooks/useMcpServers';

export interface DirectToolVendor {
  name: string;
  label: string;
}

/**
 * The vendor behind a direct MCP tool's `<server>` segment, when it is one of
 * the brokerages this build ships. The server row is named after the vendor,
 * so the name is the lookup; anything else is a plain connector.
 */
export function useDirectToolVendor(server: string): DirectToolVendor | null {
  const { data } = useBrokerages();
  if (!server || !data) return null;
  const key = server.toLowerCase();
  const vendor = data.find((b) => b.name.toLowerCase() === key);
  return vendor ? { name: vendor.name, label: vendor.label } : null;
}

/** The vendor's label when known, else the server name as written. */
export function useDirectToolVendorLabel(server: string): string {
  const vendor = useDirectToolVendor(server);
  return vendor?.label || server;
}
