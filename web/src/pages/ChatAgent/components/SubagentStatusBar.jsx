import React from 'react';
import { CheckCircle2, Circle, Loader2 } from 'lucide-react';
import { cn } from '../../../lib/utils';
import iconRoboSing from '../../../assets/img/icon-robo-sing.svg';
import './AgentSidebar.css';

/**
 * SubagentStatusBar Component
 *
 * Replaces the chat input area when viewing a subagent tab.
 * Shows agent avatar, name, description, status, and current tool.
 *
 * @param {Object} agent - The agent object from the agents array
 */
function SubagentStatusBar({ agent }) {
  if (!agent) return null;

  const isActive = agent.status === 'active' && agent.isActive;
  const isCompleted = agent.status === 'completed';

  const getStatusIcon = () => {
    if (agent.currentTool) {
      return <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    if (agent.status === 'active' && agent.messages?.length > 0) {
      return <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    if (isCompleted) {
      return <CheckCircle2 className="h-4 w-4" style={{ color: '#0FEDBE' }} />;
    }
    return <Circle className="h-4 w-4" style={{ color: 'rgba(255, 255, 255, 0.3)' }} />;
  };

  const getStatusText = () => {
    if (agent.currentTool) {
      return `Running: ${agent.currentTool}`;
    }
    if (isCompleted) {
      if (agent.toolCalls > 0) {
        return `Completed (${agent.toolCalls} tool calls)`;
      }
      return 'Completed';
    }
    if (agent.status === 'active') {
      if (agent.messages?.length > 0) {
        return 'Running';
      }
      return 'Initializing';
    }
    return 'Initializing';
  };

  return (
    <div
      className="flex items-center gap-3 px-4 py-3 rounded-lg"
      style={{
        backgroundColor: 'rgba(255, 255, 255, 0.03)',
        border: '1px solid rgba(255, 255, 255, 0.06)',
      }}
    >
      {/* Agent avatar */}
      <div
        className={cn(
          "w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0",
          isActive && !isCompleted && "agent-tab-active-pulse"
        )}
        style={{
          backgroundColor: isActive && !isCompleted
            ? 'rgba(15, 237, 190, 0.25)'
            : 'rgba(255, 255, 255, 0.1)',
        }}
      >
        <img
          src={iconRoboSing}
          alt="Agent"
          className="h-5 w-5"
          style={{ filter: 'brightness(0) saturate(100%) invert(100%)' }}
        />
      </div>

      {/* Info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-white truncate">
            {agent.name}
          </span>
          <span
            className="text-xs px-1.5 py-0.5 rounded"
            style={{
              backgroundColor: 'rgba(255, 255, 255, 0.06)',
              color: 'rgba(255, 255, 255, 0.5)',
            }}
          >
            {agent.type}
          </span>
        </div>
        {agent.description && (
          <p
            className="text-xs mt-0.5 truncate"
            style={{
              color: 'rgba(255, 255, 255, 0.5)',
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden',
              whiteSpace: 'normal',
            }}
          >
            {agent.description}
          </p>
        )}
        <div className="flex items-center gap-1.5 mt-1">
          {getStatusIcon()}
          <span className="text-xs" style={{ color: isCompleted ? '#0FEDBE' : 'rgba(255, 255, 255, 0.5)' }}>
            {getStatusText()}
          </span>
        </div>
      </div>
    </div>
  );
}

export default SubagentStatusBar;
