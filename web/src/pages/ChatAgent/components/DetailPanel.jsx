import { X, FileText, ArrowRight, Zap } from 'lucide-react';
import { getDisplayName, getToolIcon, stripLineNumbers, parseTruncatedResult } from './toolDisplayConfig';
import {
  StockPriceChart,
  CompanyOverviewCard,
  MarketIndicesChart,
  SectorPerformanceChart,
} from './charts/MarketDataCharts';
import Markdown from './Markdown';
import iconRoboSing from '../../../assets/img/icon-robo-sing.svg';

/**
 * DetailPanel Component
 *
 * Renders the detailed result of a single tool call in the right panel.
 * Routes artifact data to appropriate chart components when available,
 * otherwise falls back to markdown rendering.
 *
 * @param {Object} toolCallProcess - full tool call process object
 * @param {Function} onClose - close handler
 */
function DetailPanel({ toolCallProcess, planData, onClose, onOpenFile, onOpenSubagentTask }) {
  // Plan detail view
  if (planData) {
    return (
      <div
        className="h-full flex flex-col"
        style={{
          backgroundColor: 'transparent',
          borderLeft: '1px solid rgba(255, 255, 255, 0.1)',
        }}
      >
        <div
          className="flex items-center justify-between px-4 py-3 flex-shrink-0"
          style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}
        >
          <div className="flex items-center gap-2 min-w-0">
            <Zap className="h-4 w-4 flex-shrink-0" style={{ color: '#6155F5' }} />
            <span
              className="font-semibold truncate"
              style={{ color: '#FFFFFF', fontSize: 14 }}
            >
              Plan Details
            </span>
          </div>
          <button
            onClick={onClose}
            className="p-1 rounded hover:bg-white/10 transition-colors flex-shrink-0"
            style={{ color: 'var(--Labels-Secondary)' }}
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <div
          className="flex-1 overflow-y-auto px-4 py-4"
          style={{ minHeight: 0 }}
        >
          <Markdown variant="panel" content={planData.description || 'No plan description.'} className="text-sm" />
        </div>
      </div>
    );
  }

  if (!toolCallProcess) return null;

  const toolName = toolCallProcess.toolName || '';
  const isTaskTool = toolName === 'Task' || toolName === 'task';
  const displayName = isTaskTool ? 'Subagent Task' : getDisplayName(toolName);
  const IconComponent = getToolIcon(toolName);
  const artifact = toolCallProcess.toolCallResult?.artifact;
  const content = toolCallProcess.toolCallResult?.content;

  // Extract subagent info from Task tool args
  const subagentType = isTaskTool ? (toolCallProcess.toolCall?.args?.subagent_type || 'general-purpose') : '';
  const subagentDescription = isTaskTool ? (toolCallProcess.toolCall?.args?.description || toolCallProcess.toolCall?.args?.prompt || '') : '';
  const subagentId = isTaskTool ? toolCallProcess.toolCall?.id : null;

  return (
    <div
      className="h-full flex flex-col"
      style={{
        backgroundColor: 'transparent',
        borderLeft: '1px solid rgba(255, 255, 255, 0.1)',
      }}
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-4 py-3 flex-shrink-0"
        style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}
      >
        <div className="flex items-center gap-2 min-w-0">
          {isTaskTool ? (
            <img src={iconRoboSing} alt="Subagent" className="w-5 h-5 flex-shrink-0" />
          ) : (
            <IconComponent className="h-4 w-4 flex-shrink-0" style={{ color: '#6155F5' }} />
          )}
          <span
            className="font-semibold truncate"
            style={{ color: '#FFFFFF', fontSize: 14 }}
          >
            {displayName}
          </span>
          {isTaskTool && subagentType && (
            <span style={{ color: 'var(--Labels-Tertiary)', fontSize: 13 }}>
              — {subagentType}
            </span>
          )}
          {!isTaskTool && toolCallProcess.toolCall?.args?.symbol && (
            <span style={{ color: 'var(--Labels-Tertiary)', fontSize: 13 }}>
              — {toolCallProcess.toolCall.args.symbol}
            </span>
          )}
        </div>
        <button
          onClick={onClose}
          className="p-1 rounded hover:bg-white/10 transition-colors flex-shrink-0"
          style={{ color: 'var(--Labels-Secondary)' }}
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Content */}
      <div
        className="flex-1 overflow-y-auto px-4 py-4"
        style={{ minHeight: 0 }}
      >
        {isTaskTool ? (
          <TaskToolContent
            content={content}
            description={subagentDescription}
            type={subagentType}
            subagentId={subagentId}
            onOpenSubagentTask={onOpenSubagentTask}
          />
        ) : (
          <ArtifactOrMarkdown
            artifact={artifact}
            content={content}
            toolName={toolName}
            toolCallProcess={toolCallProcess}
            onOpenFile={onOpenFile}
          />
        )}
      </div>
    </div>
  );
}

function TaskToolContent({ content, description, type, subagentId, onOpenSubagentTask }) {
  const displayContent = typeof content === 'string' ? content : content ? String(content) : 'No result content';

  const handleGoToSubagent = () => {
    if (onOpenSubagentTask && subagentId) {
      onOpenSubagentTask({
        subagentId,
        description,
        type,
        status: 'completed',
      });
    }
  };

  return (
    <div className="space-y-4">
      {/* Markdown-rendered output */}
      <Markdown variant="panel" content={displayContent} className="text-sm" />

      {/* Footer link to subagent tab */}
      {onOpenSubagentTask && subagentId && (
        <button
          onClick={handleGoToSubagent}
          className="flex items-center gap-3 w-full px-3 py-2.5 rounded-lg transition-colors hover:brightness-110"
          style={{
            backgroundColor: 'rgba(97, 85, 245, 0.08)',
            borderLeft: '3px solid #6155F5',
            border: '1px solid rgba(97, 85, 245, 0.2)',
          }}
        >
          <img src={iconRoboSing} alt="Subagent" className="w-5 h-5 flex-shrink-0" />
          <div className="flex flex-col gap-0.5 min-w-0 flex-1 text-left">
            <span className="text-xs font-medium" style={{ color: '#FFFFFF', opacity: 0.8 }}>
              Go to subagent tab
            </span>
            {description && (
              <span className="text-xs truncate" style={{ color: '#FFFFFF', opacity: 0.5 }}>
                {description}
              </span>
            )}
          </div>
          <ArrowRight className="h-4 w-4 flex-shrink-0" style={{ color: '#6155F5' }} />
        </button>
      )}
    </div>
  );
}

function ArtifactOrMarkdown({ artifact, content, toolName, toolCallProcess, onOpenFile }) {
  // Check for truncated results first
  const rawContent = typeof content === 'string' ? content : content ? String(content) : '';
  const truncated = parseTruncatedResult(rawContent);
  if (truncated.isTruncated) {
    return (
      <TruncatedResultMessage
        filePath={truncated.filePath}
        preview={truncated.preview}
        onOpenFile={onOpenFile}
      />
    );
  }

  // Route by artifact type
  if (artifact?.type) {
    switch (artifact.type) {
      case 'stock_prices':
        return <StockPriceChart data={artifact} />;
      case 'company_overview':
        return <CompanyOverviewCard data={artifact} />;
      case 'market_indices':
        return <MarketIndicesChart data={artifact} />;
      case 'sector_performance':
        return <SectorPerformanceChart data={artifact} />;
    }
  }

  // WebSearch: formatted display
  if (toolName === 'WebSearch' || toolName === 'web_search') {
    const formatted = formatWebSearchContent(toolCallProcess);
    if (formatted) {
      return <Markdown variant="panel" content={formatted} className="text-sm" />;
    }
  }

  // Fallback: render content as markdown (strip line numbers from Read/SEC filing results)
  const displayContent = stripLineNumbers(rawContent || 'No result content');

  return <Markdown variant="panel" content={displayContent} className="text-sm" />;
}

function formatWebSearchContent(proc) {
  const raw = proc.toolCallResult?.content;
  if (!raw) return null;

  let results;
  try {
    results = JSON.parse(typeof raw === 'string' ? raw : String(raw));
    if (!Array.isArray(results)) return null;
  } catch {
    return null;
  }

  const artifact = proc.toolCallResult?.artifact;
  const richResults = artifact?.results;
  const lines = [];

  if (artifact?.answer) {
    lines.push(`> ${artifact.answer}`);
    lines.push('');
  }

  results.forEach((item, i) => {
    const title = item.title || 'Untitled';
    const url = item.url || '';
    const snippet = (richResults?.[i]?.snippet) || item.content || '';
    const date = item.date || '';

    lines.push(`**${i + 1}. [${title}](${url})**`);
    if (snippet) lines.push(snippet);
    if (date) lines.push(`*${date}*`);
    lines.push('');
  });

  return lines.join('\n').trim() || null;
}

function TruncatedResultMessage({ filePath, preview, onOpenFile }) {
  return (
    <div className="space-y-4">
      {/* Info card */}
      <div
        className="rounded-lg px-4 py-3"
        style={{
          backgroundColor: 'rgba(97, 85, 245, 0.1)',
          border: '1px solid rgba(97, 85, 245, 0.25)',
        }}
      >
        <div className="flex items-start gap-3">
          <FileText className="h-5 w-5 flex-shrink-0 mt-0.5" style={{ color: '#6155F5' }} />
          <div className="space-y-2 min-w-0">
            <p className="text-sm font-medium" style={{ color: '#FFFFFF' }}>
              Result too large to display inline
            </p>
            <p className="text-xs" style={{ color: 'rgba(255, 255, 255, 0.6)' }}>
              The full result has been saved to the workspace filesystem.
            </p>
            {filePath && onOpenFile && (
              <button
                onClick={() => onOpenFile(filePath)}
                className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm transition-colors hover:bg-white/10"
                style={{
                  color: '#6155F5',
                  border: '1px solid rgba(97, 85, 245, 0.4)',
                }}
              >
                <FileText className="h-3.5 w-3.5" />
                Open full result
              </button>
            )}
            {filePath && (
              <p className="text-xs font-mono truncate" style={{ color: 'rgba(255, 255, 255, 0.4)' }}>
                {filePath}
              </p>
            )}
          </div>
        </div>
      </div>

      {/* Preview */}
      {preview && (
        <div className="space-y-2">
          <p className="text-xs font-medium" style={{ color: 'rgba(255, 255, 255, 0.5)' }}>
            Preview
          </p>
          <Markdown variant="panel" content={stripLineNumbers(preview)} className="text-sm" />
        </div>
      )}
    </div>
  );
}

export default DetailPanel;
