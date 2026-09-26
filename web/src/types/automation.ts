/** Automation types: the wire shapes of `/api/v1/automations*` (src/server/models/automation.py). */

export interface DeliveryConfig {
  methods: string[];
}

export interface PriceCondition {
  type: 'price_above' | 'price_below' | 'pct_change_above' | 'pct_change_below';
  value: number;
  reference?: 'previous_close' | 'day_open';
}

export interface PriceTriggerConfig {
  symbol: string;
  market?: 'stock' | 'index';
  conditions: PriceCondition[];
  retrigger: {
    mode: 'one_shot' | 'recurring';
    cooldown_seconds?: number;
  };
}

/** `executing` is a price automation mid-run; cron and once automations stay
 *  `active` while they run, and only their execution row says `running`. */
export type AutomationStatus = 'active' | 'paused' | 'completed' | 'disabled' | 'executing';

/** `waiting` is a firing held until the turn running in its thread ends;
 *  `skipped` is one that never ran, and is not a failure. */
export type ExecutionStatus = 'pending' | 'waiting' | 'running' | 'completed' | 'failed' | 'timeout' | 'skipped';

/** Why a run was skipped: the reader chose to, its thread stayed busy (or an
 *  earlier firing was already waiting), or the server stopped mid-wait. */
export type SkipReason = 'user' | 'thread_busy' | 'interrupted';

/** A failed run's cause, where the server knows it: a usage limit refused
 *  the firing or paused its run (`error_message` is then the quota service's
 *  own words), the model provider rejected the user's own key, the run failed
 *  on the server's side, or the server cut it off. Only the first two are the
 *  reader's to act on. */
export type FailureReason = 'usage_limit' | 'provider_auth' | 'server_error' | 'interrupted';

/** Why the server switched an automation off. Cleared by a resume. */
export type DisableReason = 'provider_auth' | 'max_failures';

export type TriggerType = 'cron' | 'once' | 'price';

export interface DeliveryAttempt {
  method: string;
  success: boolean;
  error?: string | null;
}

/** One run as the server sends it: a list row's newest execution, an
 *  automation's history and the cross-automation feed all read the same
 *  columns. */
export interface AutomationExecution {
  automation_execution_id: string;
  automation_id: string;
  status: ExecutionStatus;
  conversation_thread_id: string | null;
  scheduled_at: string;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
  skip_reason: SkipReason | null;
  failure_reason: FailureReason | null;
  delivery_result: DeliveryAttempt[] | null;
  created_at: string;
  /** The start of the run's final answer, as plain text, when it gave one. */
  excerpt: string | null;
}

export interface Automation {
  automation_id: string;
  user_id: string;
  name: string;
  description: string | null;

  trigger_type: TriggerType;
  cron_expression: string | null;
  timezone: string;
  trigger_config: PriceTriggerConfig | null;

  next_run_at: string | null;
  last_run_at: string | null;

  agent_mode: 'flash' | 'ptc';
  instruction: string;
  workspace_id: string | null;
  llm_model: string | null;

  thread_strategy: 'new' | 'continue';
  conversation_thread_id: string | null;

  status: AutomationStatus;
  max_failures: number;
  failure_count: number;
  disable_reason?: DisableReason | null;

  delivery_config: DeliveryConfig | null;
  metadata?: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;

  last_execution: AutomationExecution | null;
}

/** The settings the form writes: every field on a create, and the whole set
 *  again on an edit, which the server applies as a patch. */
export interface AutomationPayload {
  name: string;
  description?: string;
  trigger_type: TriggerType;
  cron_expression?: string;
  timezone: string;
  trigger_config?: PriceTriggerConfig;
  next_run_at?: string;
  agent_mode: Automation['agent_mode'];
  instruction: string;
  workspace_id?: string;
  thread_strategy: Automation['thread_strategy'];
  max_failures: number;
  delivery_config: DeliveryConfig;
}

/** What an edit sends, applied by the server as a patch: only the fields
 *  the edit changed need to be there. An automation keeps its kind of
 *  trigger for life, so there is no `trigger_type`. */
export type AutomationUpdatePayload = Partial<Omit<AutomationPayload, 'trigger_type'>>;

/** One entry of the cross-automation run feed: a run and the identity of
 *  the automation it belongs to. */
export interface AutomationRun extends AutomationExecution {
  automation_name: string;
  agent_mode: Automation['agent_mode'];
  trigger_type: TriggerType;
  workspace_id: string | null;
}
