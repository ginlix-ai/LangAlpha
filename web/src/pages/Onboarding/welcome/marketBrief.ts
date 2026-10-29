import { queryKeys } from '@/lib/queryKeys';
import { getInsightDetail, getTodayInsights } from '@/pages/Dashboard/utils/api';
import { normalizeInsight } from '@/pages/Dashboard/utils/insightFetch';
import { serializeInsightToMarkdown } from '@/pages/Dashboard/widgets/framework/snapshotSerializers';
import { MAX_BRIEF_CHARS } from './kickoff';

/**
 * Today's market brief, as the dashboard's attach button hands one to the
 * agent: headline, summary, topics, the news items and their sources, in
 * markdown. The newest brief written for the whole market, not one written
 * for this user, since what the opening turn wants from it is what moved
 * today. Null when there is none yet (a fresh install, or none written today
 * or yesterday) and when either read fails: the turn goes without it.
 */
export async function fetchMarketBrief(): Promise<string | null> {
  try {
    const card = (await getTodayInsights()).find((row) => row.type !== 'personalized');
    const id = card?.market_insight_id;
    if (typeof id !== 'string' || !id) return null;
    const insight = normalizeInsight(await getInsightDetail(id));
    if (!insight.headline) return null;
    return serializeInsightToMarkdown(insight).slice(0, MAX_BRIEF_CHARS);
  } catch {
    return null;
  }
}

/** Read while the questions are answered, so the last one has it to hand. */
export const marketBriefQuery = {
  queryKey: queryKeys.insights.marketBrief(),
  queryFn: fetchMarketBrief,
  staleTime: 10 * 60_000,
  retry: false,
};
