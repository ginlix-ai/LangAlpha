import type { AgreementLevel, TradingLevel } from '@/types/api';

/** The Settings section's anchor, and the link every other page uses to reach
 *  it. Here rather than beside the section so a link to it does not pull the
 *  Settings page into another page's chunk. The agent sends users to the same
 *  address (`TRADING_SETTINGS_URL` in `src/server/services/trading_rule.py`),
 *  so a move here moves there too. */
export const TRADING_PERMISSION_ANCHOR = 'trading-permission';
export const TRADING_PERMISSION_HREF = `/settings?tab=preferences#${TRADING_PERMISSION_ANCHOR}`;

/** The version of the agreement text this bundle shows
 *  (`settings.tradingPermission.agreement.*`), and the one opting in sends.
 *  Bump it together with that copy. The server refuses any other version, so
 *  a tab still on an older bundle cannot accept text it never displayed. */
export const TRADING_AGREEMENT_VERSION = 1;

interface TradingLevelInfo<L extends TradingLevel> {
  /** Its name, in Settings, its agreement and on the brokerage page. */
  labelKey: string;
  /** What it lets the agent do, under its option in Settings. */
  descriptionKey: string;
  /** What signing up for it means, for a level that lets orders skip
   *  approval. Typed to the level, so a level cannot skip approval without
   *  copy for its agreement, nor carry one it is never asked for. */
  agreement: L extends AgreementLevel ? { descriptionKey: string } : null;
  /** Named in the warning tone wherever it appears: the one level that trades
   *  on the agent's own judgment. */
  warning: boolean;
}

/**
 * Everything the app says about each level, written least autonomy first
 * because that is the order they are listed in. The keys are spelled out so
 * the locale sweep sees each one.
 */
export const TRADING_LEVEL_INFO: { readonly [L in TradingLevel]: TradingLevelInfo<L> } = {
  no_trading: {
    labelKey: 'settings.tradingPermission.noTrading',
    descriptionKey: 'settings.tradingPermission.noTradingDesc',
    agreement: null,
    warning: false,
  },
  approve_each: {
    labelKey: 'settings.tradingPermission.approveEach',
    descriptionKey: 'settings.tradingPermission.approveEachDesc',
    agreement: null,
    warning: false,
  },
  plan_first: {
    labelKey: 'settings.tradingPermission.planFirst',
    descriptionKey: 'settings.tradingPermission.planFirstDesc',
    agreement: { descriptionKey: 'settings.tradingPermission.agreement.planFirst' },
    warning: false,
  },
  autonomous: {
    labelKey: 'settings.tradingPermission.autonomous',
    descriptionKey: 'settings.tradingPermission.autonomousDesc',
    agreement: { descriptionKey: 'settings.tradingPermission.agreement.autonomous' },
    warning: true,
  },
};

/** The table's levels in its order: string keys enumerate in the order they
 *  were written. */
export const TRADING_LEVELS = Object.keys(TRADING_LEVEL_INFO) as readonly TradingLevel[];

/** The level a user has until they choose one, tagged as the default. */
export const DEFAULT_TRADING_LEVEL: TradingLevel = 'approve_each';

/** The levels that let a live or staged order go without approval, which are
 *  the ones the server refuses without the signed agreement. */
export function skipsApproval(level: TradingLevel): level is AgreementLevel {
  return TRADING_LEVEL_INFO[level].agreement !== null;
}
