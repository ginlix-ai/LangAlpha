import React from 'react';
import { useAppFrame } from '@/lib/appBridge/useAppFrame';
import { appGrantKey } from '@/lib/appBridge/useAppScopes';
import { HostedFrame } from '../ChatAgent/components/viewers/HostedFrame';
import { ShareTopBar } from './ShareTopBar';
import type { SharedAppMetadata } from './api';
import './SharePage.css';

interface SharedAppViewProps {
  code: string;
  metadata: SharedAppMetadata;
}

/**
 * The owner's app behind its `/a/` link. The signed URL is minted by the
 * metadata read, which the page re-runs before it expires, so a long-open
 * tab keeps a URL that still answers.
 */
export default function SharedAppView({ code, metadata }: SharedAppViewProps): React.ReactElement {
  // An app link opens for its owner alone, so the page answers as them. The
  // URL and the command it is keyed by come in one answer, so a different
  // program on the link never reads under the grant given to the one before.
  const app = useAppFrame(metadata.url, { grantKey: appGrantKey({ code, command_key: metadata.command_key }) });
  return (
    <div className="share-page">
      <ShareTopBar name={metadata.title} />
      {app.bridge}
      <div className="share-content">
        <HostedFrame {...app.frame} className="share-frame" title={metadata.title} />
      </div>
    </div>
  );
}
