import { MountOnceOpen } from '@/components/MountOnceOpen';
import { lazyWithMotion } from '@/lib/lazyWithMotion';
import { loadConnectConsent, useConnectQuestion } from '../connectQuestion';

const ConnectConsentLayer = lazyWithMotion(loadConnectConsent);

/**
 * The one place the question a connect raises is rendered, mounted once in
 * the app shell so it covers every surface that can start a connect: the
 * Plugins page, a workspace's MCP tab and the onboarding sheet. Mounted from
 * the first question on, so the answer animates out rather than
 * unmounting.
 */
export function ConnectConsentHost() {
  const question = useConnectQuestion();
  return (
    <MountOnceOpen open={!!question}>
      <ConnectConsentLayer />
    </MountOnceOpen>
  );
}
