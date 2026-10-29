import { useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router';
import type { LocationState } from './connectStep/shared';
import { SubscriptionConnect } from './connectStep/SubscriptionConnect';
import { CustomProviderConnect } from './connectStep/CustomProviderConnect';
import { ApiKeyConnect } from './connectStep/ApiKeyConnect';

// ---------------------------------------------------------------------------
// ConnectStep — Step 3: a subscription sign-in or an API key
// ---------------------------------------------------------------------------

export default function ConnectStep() {
  const navigate = useNavigate();
  const location = useLocation();

  const state = (location.state as LocationState | null) ?? {};

  // Redirect to method step if essential state is missing (e.g. browser refresh)
  // Custom provider flows don't have a provider yet — skip the guard for those.
  useEffect(() => {
    if (!state.provider && !state.isCustom) {
      navigate('/setup/method', { replace: true });
    }
  }, [state.provider, state.isCustom, navigate]);

  if (state.isCustom) return <CustomProviderConnect state={state} />;
  if (state.method === 'oauth') return <SubscriptionConnect state={state} />;
  return <ApiKeyConnect state={state} />;
}
