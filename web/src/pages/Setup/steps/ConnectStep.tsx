import { useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import type { LocationState } from './connectStep/shared';
import { ExistingCustomConnect } from './connectStep/ExistingCustomConnect';
import { CustomProviderConnect } from './connectStep/CustomProviderConnect';
import { OAuthConnect } from './connectStep/OAuthConnect';
import { ApiKeyConnect } from './connectStep/ApiKeyConnect';

// ---------------------------------------------------------------------------
// ConnectStep — Step 3: OAuth redirect or API key input
// ---------------------------------------------------------------------------

export default function ConnectStep() {
  const navigate = useNavigate();
  const location = useLocation();

  const state = (location.state as LocationState | null) ?? {};

  // Redirect to method step if essential state is missing (e.g. browser refresh)
  // Custom provider flows don't have a provider yet — skip the guard for those.
  useEffect(() => {
    if (!state.provider && !state.isCustom && !state.isExistingCustom) {
      navigate('/setup/method', { replace: true });
    }
  }, [state.provider, state.isCustom, state.isExistingCustom, navigate]);

  const method = state.method ?? 'api_key';
  const isCustom = state.isCustom ?? false;
  const isExistingCustom = state.isExistingCustom ?? false;

  if (isExistingCustom) return <ExistingCustomConnect state={state} />;
  if (isCustom) return <CustomProviderConnect state={state} />;
  if (method === 'oauth') return <OAuthConnect state={state} />;
  return <ApiKeyConnect state={state} />;
}
