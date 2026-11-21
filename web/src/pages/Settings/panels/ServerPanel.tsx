import { ServerSetupForm } from '@/pages/Setup/components/ServerSetupForm';

/** Where the agent runs and the vendor keys this server holds: what the setup
 *  step asked, to change later. Only a self-hosted build opens this. */
export default function ServerPanel() {
  return <ServerSetupForm layout="panel" />;
}
