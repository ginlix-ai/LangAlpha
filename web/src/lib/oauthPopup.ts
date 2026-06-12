export const OAUTH_BROADCAST_CHANNEL = 'langalpha-oauth';
export const OAUTH_POPUP_WINDOW_NAME = 'langalpha-oauth';
export const OAUTH_POPUP_FEATURES = 'width=520,height=640,menubar=no,toolbar=no,location=no,status=no';

export interface OAuthPopupMessage {
  type: 'oauth-complete';
}

export const BROKERAGE_BROADCAST_CHANNEL = 'langalpha-brokerage-oauth';

export interface BrokerageOAuthMessage {
  type: 'brokerage-oauth-complete';
  provider: string;
  status: 'success' | 'error';
  error?: string;
}
