import React from 'react';
import { APP_PREVIEW_SANDBOX } from '../ChatAgent/components/viewers/html/sandbox';
import { ShareTopBar } from './ShareTopBar';
import type { SharedAppMetadata } from './api';
import './SharePage.css';

interface SharedAppViewProps {
  metadata: SharedAppMetadata;
}

/**
 * The owner's app behind its `/a/` link. The signed URL is minted by the
 * metadata read, which the page re-runs before it expires, so a long-open
 * tab keeps a URL that still answers.
 */
export default function SharedAppView({ metadata }: SharedAppViewProps): React.ReactElement {
  return (
    <div className="share-page">
      <ShareTopBar name={metadata.title} />
      <div className="share-content">
        <iframe src={metadata.url} className="share-frame" title={metadata.title} sandbox={APP_PREVIEW_SANDBOX} />
      </div>
    </div>
  );
}
