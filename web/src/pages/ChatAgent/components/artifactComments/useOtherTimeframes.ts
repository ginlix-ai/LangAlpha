import { useQueries } from '@tanstack/react-query';
import { artifactCommentsQuery, readFailed, useSymbolCharts } from '@/hooks/useArtifact';
import { INTERVAL_LABEL } from '@/lib/bars/chartConstants';
import { TIMEFRAMES, chartParts } from '@/lib/bars/chartId';
import type { Artifact, ArtifactComment, ArtifactTarget } from '@/types/api';

/** A comment on another timeframe of the chart on screen, with where it is. */
export interface OtherTimeframeComment {
  comment: ArtifactComment;
  /** Its chart's id, `SYMBOL:timeframe`. */
  path: string;
  /** Its timeframe as the chart's toolbar says it: "1H". */
  timeframe: string;
}

export interface OtherTimeframeRecords {
  records: Artifact[];
  /** The records could not be read: what counts them may be missing some. */
  failed: boolean;
  retry: () => void;
}

export interface OtherTimeframes {
  /** The timeframe on screen as the toolbar says it; null off a chart. */
  here: string | null;
  /** The other timeframes' records holding something unread. */
  unreadIds: string[];
  comments: OtherTimeframeComment[];
  /** A record or a list could not be read: what shows may be missing some. */
  failed: boolean;
  retry: () => void;
}

const NO_RECORDS: Artifact[] = [];
const NO_COMMENTS: OtherTimeframeComment[] = [];

/** A timeframe as the chart's toolbar says it. */
export function timeframeLabel(timeframe: string): string {
  return INTERVAL_LABEL[timeframe] ?? timeframe;
}

function timeframeOrder(path: string): number {
  const timeframe = chartParts(path)?.timeframe;
  return timeframe ? TIMEFRAMES.indexOf(timeframe) : TIMEFRAMES.length;
}

/**
 * The records of a chart's ticker at its other timeframes, each a record of
 * its own (`SYMBOL:timeframe`), shortest timeframe first, each with its own
 * counts. None off a chart, or with `enabled` false.
 */
export function useOtherTimeframeRecords(
  workspaceId: string,
  target: ArtifactTarget,
  enabled: boolean,
): OtherTimeframeRecords {
  const symbol = enabled && target.kind === 'chart' ? (chartParts(target.path)?.symbol ?? null) : null;
  const charts = useSymbolCharts(workspaceId, symbol, { enabled: symbol !== null });
  const records = symbol && charts.data
    ? charts.data.filter((c) => c.path !== target.path).sort((a, b) => timeframeOrder(a.path) - timeframeOrder(b.path))
    : NO_RECORDS;
  return {
    records,
    failed: symbol !== null && readFailed(charts),
    retry: () => void charts.refetch(),
  };
}

/**
 * The comments on a chart's ticker at its other timeframes, in the order of
 * their records, by pin within each. Their comments are read only while
 * `listed` (the list is open), since only the list shows them.
 */
export function useOtherTimeframes(
  workspaceId: string,
  target: ArtifactTarget,
  { enabled, listed }: { enabled: boolean; listed: boolean },
): OtherTimeframes {
  const { records, failed, retry } = useOtherTimeframeRecords(workspaceId, target, enabled);
  const reads = useQueries({
    queries: records.map((c) => ({ ...artifactCommentsQuery(c.artifact_id), enabled: listed })),
  });
  const comments = records.flatMap((record, i) => {
    const timeframe = timeframeLabel(chartParts(record.path)?.timeframe ?? record.path);
    return (reads[i]?.data ?? []).map((comment) => ({ comment, path: record.path, timeframe }));
  });
  const here = target.kind === 'chart' ? chartParts(target.path) : null;
  return {
    here: here ? timeframeLabel(here.timeframe) : null,
    unreadIds: records.filter((r) => r.unread > 0).map((r) => r.artifact_id),
    comments: comments.length > 0 ? comments : NO_COMMENTS,
    failed: failed || reads.some(readFailed),
    retry: () => {
      if (failed) retry();
      for (const read of reads) if (read.isError) void read.refetch();
    },
  };
}
