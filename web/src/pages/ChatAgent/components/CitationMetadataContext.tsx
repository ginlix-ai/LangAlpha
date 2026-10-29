import React, { createContext, useContext, useMemo } from 'react';
import { parseDisplayableResults, buildRichResultMap, resolveSnippet } from './webSearchUtils';

export interface CitationMeta {
  title: string;
  url: string;
  snippet: string;
  date?: string;
  domain: string;
  source?: string;
}

const CitationMetadataContext = createContext<Map<string, CitationMeta>>(new Map());

export function useCitationMetadata(url: string): CitationMeta | undefined {
  const map = useContext(CitationMetadataContext);
  return map.get(url);
}

interface CitationMetadataProviderProps {
  toolCallProcesses: Record<string, Record<string, unknown>>;
  children: React.ReactNode;
}

export function CitationMetadataProvider({ toolCallProcesses, children }: CitationMetadataProviderProps): React.ReactElement {
  const metaMap = useMemo(() => {
    const map = new Map<string, CitationMeta>();

    for (const proc of Object.values(toolCallProcesses)) {
      const toolName = proc.toolName as string | undefined;
      if (toolName !== 'WebSearch' && toolName !== 'web_search') continue;

      const result = proc.toolCallResult as Record<string, unknown> | undefined;
      if (!result) continue;

      const raw = result.content;
      const displayable = parseDisplayableResults(raw);
      if (!displayable) continue;

      const artifact = result.artifact as Record<string, unknown> | undefined;
      const richByUrl = buildRichResultMap(artifact);

      for (const item of displayable) {
        const itemUrl = (item.url as string) || '';
        if (!itemUrl) continue;

        const rich = richByUrl.get(itemUrl);
        let domain = '';
        try { domain = new URL(itemUrl).hostname.replace(/^www\./, ''); } catch { /* skip */ }

        map.set(itemUrl, {
          title: (item.title as string) || '',
          url: itemUrl,
          snippet: resolveSnippet(item, rich),
          date: (item.date as string) || (item.publish_time as string) || undefined,
          domain,
          source: (item.source as string) || (item.site_name as string) || undefined,
        });
      }
    }

    return map;
  }, [toolCallProcesses]);

  return (
    <CitationMetadataContext value={metaMap}>
      {children}
    </CitationMetadataContext>
  );
}
