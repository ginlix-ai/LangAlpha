import { ListError } from '@/components/mcp/McpPrimitives';
import { apiErrorReason } from '@/pages/ChatAgent/utils/api';

/**
 * A list that could not load: which list, in the page's words, then the
 * server's reason if it gave one, and a retry. The client's own message was
 * all these lists used to show, and to someone who has keys stored
 * "Request failed with status code 500" says neither what failed nor what to do.
 */
export function LoadFailed({
  what,
  error,
  onRetry,
}: {
  what: string;
  error: unknown;
  onRetry: () => void;
}) {
  const reason = apiErrorReason(error);
  return <ListError onRetry={onRetry}>{reason ? `${what} ${reason}` : what}</ListError>;
}
