import { ChevronDown, ChevronUp, Loader2, Wrench } from 'lucide-react';
import { useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

/**
 * File-related tool names that support opening in the file panel.
 * Backend uses PascalCase (LangChain SDK convention). Include both for backward compatibility
 * with older history that may have snake_case tool names.
 */
const FILE_TOOLS = ['Write', 'Edit', 'Read', 'Save', 'write_file', 'edit_file', 'read_file', 'save_file'];

function getFilePathFromToolCall(toolCall) {
  if (!toolCall?.args) return null;
  const args = toolCall.args;
  return args.file_path || args.filePath || args.path || args.filename || null;
}

/**
 * ToolCallMessageContent Component
 * 
 * Renders tool call information from tool_calls and tool_call_result events.
 * 
 * Features:
 * - Shows an icon indicating tool call status (loading when in progress, finished when complete)
 * - Displays tool name (e.g., "Write", "Read")
 * - For file tools: clicking opens the file in the right panel via onOpenFile callback
 * - For non-file tools: clicking toggles visibility of tool call details
 * - Displays tool_calls and tool_call_result with different visual styles
 * 
 * @param {Object} props
 * @param {string} props.toolCallId - Unique identifier for this tool call
 * @param {string} props.toolName - Name of the tool (e.g., "Write", "Read")
 * @param {Object} props.toolCall - Complete tool_calls event data
 * @param {Object} props.toolCallResult - tool_call_result event data
 * @param {boolean} props.isInProgress - Whether tool call is currently in progress
 * @param {boolean} props.isComplete - Whether tool call has completed
 * @param {boolean} props.isFailed - Whether tool call failed
 * @param {Function} props.onOpenFile - Callback to open a file in the file panel
 * @param {Array} [props.mergedProcesses] - When set (main chat merged block), expanded view shows all tool calls + results in order
 */
function ToolCallMessageContent({ 
  toolCallId, 
  toolName, 
  toolCall, 
  toolCallResult, 
  isInProgress, 
  isComplete,
  isFailed = false,
  onOpenFile,
  mergedProcesses
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Resolve display data: single from props or last of merged
  const processes = mergedProcesses && mergedProcesses.length > 0
    ? mergedProcesses
    : [{ toolName, toolCall, toolCallResult, isInProgress, isComplete, isFailed }];
  const displayProcess = processes[processes.length - 1];
  const displayName = displayProcess.toolName || displayProcess.toolCall?.name || 'Tool Call';
  const isFileTool = FILE_TOOLS.includes(displayName);
  const filePath = isFileTool ? getFilePathFromToolCall(displayProcess.toolCall) : null;

  // Don't render if there's no tool call data
  if (!displayName && !displayProcess.toolCall) {
    return null;
  }

  const handleToggle = () => {
    // For file tools with a valid path and onOpenFile callback, open in file panel
    if (isFileTool && filePath && onOpenFile) {
      onOpenFile(filePath);
      return;
    }
    // Otherwise, toggle expand/collapse as before
    setIsExpanded(!isExpanded);
  };

  return (
    <div className="mt-2">
      {/* Tool call indicator button */}
      <button
        onClick={handleToggle}
        className="transition-colors hover:bg-white/10"
        style={{
          boxSizing: 'border-box',
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          fontSize: '14px',
          lineHeight: '20px',
          color: isFailed ? '#FF383C' : 'var(--Labels-Secondary)',
          padding: '4px 12px',
          borderRadius: '6px',
          backgroundColor: displayProcess.isInProgress 
            ? 'rgba(97, 85, 245, 0.15)' 
            : 'transparent',
          border: displayProcess.isInProgress 
            ? '1px solid rgba(255, 255, 255, 0.1)' 
            : 'none',
          width: '100%',
        }}
        title={displayProcess.isInProgress ? 'Tool call in progress...' : 'View tool call details'}
      >
        {/* Icon: Wrench with loading spinner when active, static wrench when complete/failed */}
        <div className="relative flex-shrink-0">
          <Wrench 
            className="h-4 w-4" 
            style={{ color: displayProcess.isFailed ? '#FF383C' : 'var(--Labels-Secondary)' }} 
          />
          {displayProcess.isInProgress && (
            <Loader2 
              className="h-3 w-3 absolute -top-0.5 -right-0.5 animate-spin" 
              style={{ color: 'var(--Labels-Secondary)' }} 
            />
          )}
        </div>
        
        {/* Tool name label */}
        <span style={{ color: 'inherit' }}>
          {displayName}
        </span>
        
        {/* Status indicator */}
        {displayProcess.isComplete && !displayProcess.isInProgress && (
          <span 
            className="text-xs" 
            style={{ 
              color: 'inherit',
              opacity: 0.8
            }}
          >
            {displayProcess.isFailed ? '(failed)' : '(complete)'}
          </span>
        )}
        
        {/* Expand/collapse icon */}
        <div
          style={{
            flexShrink: 0,
            color: 'var(--Labels-Quaternary)',
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
          }}
        >
          {isExpanded ? (
            <ChevronUp className="h-4 w-4" />
          ) : (
            <ChevronDown className="h-4 w-4" />
          )}
        </div>
      </button>

      {/* Tool call details (shown when expanded): only result content, rendered as markdown; when merged, show each result in order */}
      {isExpanded && (
        <div className="mt-2 space-y-3">
          {processes.map((proc, idx) => {
            if (!proc.toolCallResult) return null;
            const content = typeof proc.toolCallResult.content === 'string'
              ? proc.toolCallResult.content
              : String(proc.toolCallResult.content ?? '');
            const isError = content.trim().startsWith('ERROR');
            const displayContent = content || 'No result content';
            return (
              <div key={idx} className="text-xs">
                {processes.length > 1 && (
                  <p className="mb-2" style={{ color: '#FFFFFF', opacity: 0.8 }}>
                    Result ({idx + 1}/{processes.length}):
                  </p>
                )}
                <div
                  className="px-3 py-2 rounded markdown-body"
                  style={{
                    backgroundColor: isError ? 'rgba(255, 56, 60, 0.15)' : 'rgba(15, 237, 190, 0.08)',
                    border: `1px solid ${isError ? 'rgba(255, 56, 60, 0.3)' : 'rgba(15, 237, 190, 0.25)'}`,
                    color: '#FFFFFF',
                    opacity: 0.9,
                  }}
                >
                  <ReactMarkdown
                    remarkPlugins={[remarkGfm]}
                    components={{
                      p: ({ node, ...props }) => (
                        <p className="my-[1px] py-[3px] whitespace-pre-wrap break-words first:mt-0 last:mb-0" style={{ color: '#FFFFFF' }} {...props} />
                      ),
                      strong: ({ node, ...props }) => (
                        <strong className="font-[600]" style={{ color: '#FFFFFF' }} {...props} />
                      ),
                      em: ({ node, ...props }) => (
                        <em className="italic" style={{ color: '#FFFFFF' }} {...props} />
                      ),
                      code: ({ node, className, children, ...props }) => (
                        <code className="font-mono" style={{ color: '#abb2bf', fontSize: 'inherit' }} {...props}>{children}</code>
                      ),
                      pre: ({ node, ...props }) => (
                        <pre className="rounded overflow-x-auto my-1 py-1 px-2 whitespace-pre-wrap break-words" style={{ backgroundColor: 'rgba(0,0,0,0.2)', margin: 0 }} {...props} />
                      ),
                      ul: ({ node, ...props }) => <ul className="list-disc ml-4 my-1" style={{ color: '#FFFFFF' }} {...props} />,
                      ol: ({ node, ...props }) => <ol className="list-decimal ml-4 my-1" style={{ color: '#FFFFFF' }} {...props} />,
                      li: ({ node, ...props }) => <li className="break-words" style={{ color: '#FFFFFF' }} {...props} />,
                      table: ({ node, ...props }) => (
                        <div className="my-2 overflow-x-auto rounded" style={{ border: '1px solid rgba(255,255,255,0.2)' }}>
                          <table className="w-full border-collapse text-left" style={{ minWidth: '100%' }} {...props} />
                        </div>
                      ),
                      thead: ({ node, ...props }) => (
                        <thead style={{ backgroundColor: 'rgba(0,0,0,0.25)' }} {...props} />
                      ),
                      tbody: ({ node, ...props }) => <tbody {...props} />,
                      tr: ({ node, ...props }) => (
                        <tr className="border-b border-white/10 last:border-b-0" {...props} />
                      ),
                      th: ({ node, ...props }) => (
                        <th className="px-3 py-2 font-[600] whitespace-nowrap" style={{ color: '#FFFFFF', borderBottom: '1px solid rgba(255,255,255,0.2)' }} {...props} />
                      ),
                      td: ({ node, ...props }) => (
                        <td className="px-3 py-2 break-words align-top" style={{ color: '#FFFFFF' }} {...props} />
                      ),
                    }}
                  >
                    {displayContent}
                  </ReactMarkdown>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export default ToolCallMessageContent;
