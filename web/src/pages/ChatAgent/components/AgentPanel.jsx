import { Bot } from 'lucide-react';
import { cn } from '../../../lib/utils';
import AgentTabBar from './AgentTabBar';
import SubagentCardContent from './SubagentCardContent';

/**
 * AgentPanel Component
 *
 * 右侧固定面板，显示 agent 详情和底部 Tab 栏
 *
 * @param {Array} agents - agents 列表
 * @param {string} selectedAgentId - 当前选中的 agent ID
 * @param {Function} onSelectAgent - 切换 agent 回调
 */
function AgentPanel({ agents, selectedAgentId, onSelectAgent }) {
  // 找到当前选中的 agent
  const selectedAgent = agents.find(agent => agent.id === selectedAgentId);

  // 调试日志
  console.log('[AgentPanel] Render:', {
    agentsCount: agents.length,
    selectedAgentId,
    selectedAgent: selectedAgent ? selectedAgent.name : 'none'
  });

  return (
    <div className="flex flex-col h-full w-full overflow-hidden">
      {/* 上方：Agent 详情区域 */}
      <div className="flex-1 flex flex-col p-4 min-h-0 overflow-hidden">
        {selectedAgent ? (
          // 卡片容器 - 灰色背景 + 圆角
          <div className="flex-1 flex flex-col bg-white/5 rounded-lg overflow-hidden">
            {/* Header */}
            <div className="flex items-center justify-between p-4 border-b border-white/10 min-w-0 flex-shrink-0">
              <div className="flex items-center gap-3 min-w-0 flex-1">
                <div className="w-10 h-10 rounded-lg bg-[#6155F5]/20 flex items-center justify-center flex-shrink-0">
                  <Bot className="h-5 w-5 text-[#6155F5]" />
                </div>
                <div className="min-w-0 flex-1">
                  <h2 className="text-base font-semibold text-white truncate">
                    {selectedAgent.name}
                  </h2>
                  <p className="text-xs text-white/50 truncate">
                    {selectedAgent.type}
                  </p>
                </div>
              </div>
              <span
                className={cn(
                  "text-xs px-3 py-1 rounded-full font-medium flex-shrink-0 ml-2",
                  selectedAgent.status === 'completed'
                    ? "bg-green-500/10 text-green-500"
                    : "bg-[#6155F5]/10 text-[#6155F5]"
                )}
              >
                {selectedAgent.status}
              </span>
            </div>

            {/* Content */}
            <div className="flex-1 p-4 overflow-y-auto min-h-0">
              <SubagentCardContent
                taskId={selectedAgent.taskId}
                description={selectedAgent.description}
                type={selectedAgent.type}
                toolCalls={selectedAgent.toolCalls}
                currentTool={selectedAgent.currentTool}
                status={selectedAgent.status}
                messages={selectedAgent.messages}
                isHistory={false}
              />
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-full text-white/40">
            <Bot className="h-12 w-12 mb-3 opacity-30" />
            <p className="text-sm">No agent selected</p>
          </div>
        )}
      </div>

      {/* 下方：Tab 栏 */}
      <div>
        <AgentTabBar
          agents={agents}
          selectedAgentId={selectedAgentId}
          onSelectAgent={onSelectAgent}
        />
      </div>
    </div>
  );
}

export default AgentPanel;
