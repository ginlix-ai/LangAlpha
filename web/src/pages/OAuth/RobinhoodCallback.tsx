import { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { BROKERAGE_BROADCAST_CHANNEL, type BrokerageOAuthMessage } from '@/lib/oauthPopup';

export default function RobinhoodCallback() {
  const [params] = useSearchParams();

  useEffect(() => {
    const status = params.get('status') === 'success' ? 'success' : 'error';
    const error = params.get('error') ?? undefined;

    try {
      const channel = new BroadcastChannel(BROKERAGE_BROADCAST_CHANNEL);
      const msg: BrokerageOAuthMessage = { type: 'brokerage-oauth-complete', provider: 'robinhood', status, error };
      channel.postMessage(msg);
      channel.close();
    } catch {
      // BroadcastChannel unsupported — polling fallback handles cleanup
    }

    window.close();
  }, [params]);

  return null;
}
