import React from 'react';
import { Bot } from 'lucide-react';
import { cn } from '../../../lib/utils';

/**
 * AgentTabBar Component
 *
 * 底部 Tab 栏，显示所有 agents，可点击切换
 *
 * @param {Array} agents - agents 列表
 * @param {string} selectedAgentId - 当前选中的 agent ID
 * @param {Function} onSelectAgent - 切换 agent 回调
 */
function AgentTabBar({ agents, selectedAgentId, onSelectAgent }) {
  if (agents.length === 0) {
    return null;
  }

  return (
    <div className="flex gap-2 p-4 overflow-x-auto scrollbar-hide min-w-0 justify-center">
      {agents.map((agent, index) => {
        const isSelected = selectedAgentId === agent.id;

        return (
          <button
            key={agent.id}
            onClick={() => onSelectAgent(agent.id)}
            className={cn(
              "flex items-center gap-2 px-4 py-2.5 rounded-lg transition-all",
              "whitespace-nowrap flex-shrink-0",
              isSelected
                ? "bg-white/5 text-white"
                : "bg-transparent text-white/70 hover:bg-white/[0.02]"
            )}
          >
            {/* 头像 */}
            <div className={cn(
              "w-8 h-8 rounded-lg flex items-center justify-center",
              isSelected ? "bg-[#6155F5]/20" : "bg-white/10"
            )}>
              <Bot className={cn(
                "h-4 w-4",
                isSelected ? "text-[#6155F5]" : "text-white/50"
              )} />
            </div>

            {/* 编号 + 类型 */}
            <div className="flex flex-col items-start">
              <span className="text-xs font-semibold">
                {String(index + 1).padStart(2, '0')}
              </span>
              <span className="text-xs opacity-70">
                {agent.type || 'Agent'}
              </span>
            </div>
          </button>
        );
      })}
    </div>
  );
}

export default AgentTabBar;
