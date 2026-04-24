// Importing each module registers its widget as a side effect.
import './definitions/MarketsOverviewWidget';
import './definitions/InsightBriefWidget';
import './definitions/NewsFeedWidget';
import './definitions/EarningsCalendarWidget';
import './definitions/WatchlistWidget';
import './definitions/PortfolioWidget';
import './definitions/PortfolioWatchlistWidget';
import './definitions/AutomationsWidget';
import './definitions/ConversationWidget';
import './definitions/WorkspacePickerWidget';
import './definitions/RecentThreadsWidget';
import './definitions/MiniChartGridWidget';
// ChartWidget is registered lazily so lightweight-charts stays out of the dashboard chunk.
import './definitions/ChartWidget.register';
// TradingView embeds — under widgets/definitions/tv/. Most use the iframe
// `embed-widget-*.js` scripts; EconomicMap uses the `<tv-economic-map>` web
// component (one of 5 WCs in TV's catalog as of 2026-04-24, see tvConfig.ts).
import './definitions/tv/TickerTapeWidget';
import './definitions/tv/StockHeatmapWidget';
import './definitions/tv/CryptoHeatmapWidget';
import './definitions/tv/ForexHeatmapWidget';
import './definitions/tv/ETFHeatmapWidget';
import './definitions/tv/EconomicEventsWidget';
import './definitions/tv/EconomicMapWidget';
import './definitions/tv/TechnicalsWidget';
import './definitions/tv/MoversWidget';
import './definitions/tv/SymbolSpotlightWidget';
import './definitions/tv/SingleTickerWidget';
import './definitions/tv/SymbolInfoWidget';
import './definitions/tv/CompanyProfileWidget';
import './definitions/tv/CompanyFinancialsWidget';
import './definitions/tv/StockScreenerWidget';
import './definitions/tv/CryptoScreenerWidget';
import './definitions/tv/TopStoriesWidget';

export { listWidgets, listWidgetsByCategory, getWidget } from './framework/WidgetRegistry';
