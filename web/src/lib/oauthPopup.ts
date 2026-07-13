// Carries cross-tab auth wakeups — both OAuth popup completion and email-link
// confirmations. Name value is kept stable so tabs on older code still match.
export const AUTH_BROADCAST_CHANNEL = 'langalpha-oauth';
export const OAUTH_POPUP_WINDOW_NAME = 'langalpha-oauth';
export const OAUTH_POPUP_FEATURES = 'width=520,height=640,menubar=no,toolbar=no,location=no,status=no';

// The `oauth-complete` literal predates the email flows and is kept so tabs
// running older code still match; senders now include email-link confirmations.
export interface AuthBroadcastMessage {
  type: 'oauth-complete';
}
