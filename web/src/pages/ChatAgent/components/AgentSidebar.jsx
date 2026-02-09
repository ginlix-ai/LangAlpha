import React from 'react';
import { X, Check } from 'lucide-react';
import { cn } from '../../../lib/utils';
import iconRoboSing from '../../../assets/img/icon-robo-sing.svg';
import iconKing from '../../../assets/img/icon-king.svg';
import './AgentSidebar.css';

/**
 * AgentSidebar Component
 *
 * Narrow vertical bar (56px wide) at far left, full height below top bar.
 * Lists all agents (main + subagents) as clickable icons.
 *
 * @param {Array} agents - Array of agent objects (main agent first)
 * @param {string} activeAgentId - Currently active agent ID shown in main view
 * @param {Function} onSelectAgent - Callback when agent is clicked
 * @param {Function} onRemoveAgent - Callback to remove/hide an agent
 */
function AgentSidebar({ agents, activeAgentId, onSelectAgent, onRemoveAgent }) {
  return (
    <div
      className="flex-shrink-0 flex flex-col items-center py-2 overflow-y-auto overflow-x-hidden"
      style={{
        width: '56px',
        borderRight: '1px solid rgba(255, 255, 255, 0.06)',
        backgroundColor: 'transparent',
      }}
    >
      {agents.map((agent) => {
        const isSelected = activeAgentId === agent.id;
        const isActive = agent.status === 'active' && agent.isActive;
        const isCompleted = agent.status === 'completed';
        const isMainAgent = agent.id === 'main' || agent.isMainAgent;
        const agentIcon = isMainAgent ? iconKing : iconRoboSing;

        return (
          <div
            key={agent.id}
            className="relative group flex-shrink-0 mb-1"
            style={{ width: '48px' }}
          >
            <button
              onClick={() => onSelectAgent(agent.id)}
              className="flex flex-col items-center justify-center gap-0.5 p-1.5 rounded-lg transition-all w-full"
              style={{
                backgroundColor: isSelected ? 'rgba(255, 255, 255, 0.08)' : 'transparent',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = 'rgba(255, 255, 255, 0.08)';
              }}
              onMouseLeave={(e) => {
                if (!isSelected) {
                  e.currentTarget.style.backgroundColor = 'transparent';
                } else {
                  e.currentTarget.style.backgroundColor = 'rgba(255, 255, 255, 0.08)';
                }
              }}
              title={agent.name}
            >
              {/* Avatar */}
              <div className="relative">
                <div
                  className={cn(
                    "w-9 h-9 rounded-lg flex items-center justify-center transition-all",
                    isActive && !isCompleted && "agent-tab-active-pulse"
                  )}
                  style={{
                    backgroundColor: isActive && !isCompleted
                      ? 'rgba(15, 237, 190, 0.25)'
                      : 'rgba(255, 255, 255, 0.1)',
                  }}
                >
                  <img
                    src={agentIcon}
                    alt="Agent"
                    className="h-5 w-5"
                    style={{
                      filter: isSelected
                        ? 'brightness(0) saturate(100%) invert(100%)'
                        : 'brightness(0) saturate(100%) invert(100%) opacity(50%)',
                    }}
                  />
                </div>
                {isCompleted && (
                  <Check
                    className="absolute -bottom-0.5 -right-0.5 h-3 w-3"
                    style={{ color: '#FFFFFF', zIndex: 10 }}
                  />
                )}
              </div>

              {/* Name */}
              <span
                className={cn(
                  "text-[10px] font-medium text-center truncate w-full leading-tight",
                  isSelected ? "text-white" : "text-white/50"
                )}
              >
                {agent.name}
              </span>
            </button>

            {/* Close button - only for non-main agents, shown on hover */}
            {!isMainAgent && (
              <button
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  onRemoveAgent?.(agent.id);
                }}
                className={cn(
                  "absolute top-0 right-0 w-4 h-4 rounded-full flex items-center justify-center transition-all",
                  "opacity-0 group-hover:opacity-100"
                )}
                style={{
                  zIndex: 200,
                  transform: 'translate(2px, -2px)',
                  backgroundColor: 'transparent',
                }}
                title="Remove agent"
              >
                <X className="h-3 w-3" style={{ color: '#FFFFFF', strokeWidth: 2.5 }} />
              </button>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default AgentSidebar;
