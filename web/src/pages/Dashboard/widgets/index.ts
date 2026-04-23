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
// ChartWidget is registered lazily so lightweight-charts stays out of the dashboard chunk.
import './definitions/ChartWidget.register';

export { listWidgets, listWidgetsByCategory, getWidget } from './framework/WidgetRegistry';
