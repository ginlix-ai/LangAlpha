// TODO: type properly — depends on backend preferences shape
export interface Preferences {
  risk_preference?: Record<string, unknown>;
  investment_preference?: Record<string, unknown>;
  agent_preference?: Record<string, unknown>;
  other_preference?: Record<string, unknown>;
}
