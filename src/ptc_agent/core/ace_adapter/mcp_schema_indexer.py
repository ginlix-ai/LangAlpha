import logging
from typing import Dict, List, Set
from src.ptc_agent.core.mcp_registry import MCPToolInfo

logger = logging.getLogger(__name__)

class MCPSchemaIndexer:
    """
    Indexes MCP tools and builds a dependency graph.
    This graph is used to find related tools/servers when a query matches specific entrypoints.
    """
    def __init__(self):
        # Predefined domain connections for financial workflows
        self.workflow_connections = {
            # Tool name keywords -> related tool name keywords
            "dcf": ["financial_statements", "balance_sheet", "cash_flow", "historical_valuation", "ratios"],
            "valuation": ["financial_statements", "historical_valuation", "ratios"],
            "income": ["balance", "cash", "ratios"],
            "balance": ["income", "cash", "ratios"],
            "cash": ["income", "balance", "ratios"],
            "price": ["historical", "chart", "technical"],
            "option": ["price", "volatility"],
            "insider": ["executives", "shares"],
            "macro": ["interest", "inflation", "gdp"],
            "sentiment": ["news", "twitter"]
        }

    def _get_tool_id(self, tool: MCPToolInfo) -> str:
        return f"{tool.server_name}:{tool.name}"

    def build_graph(self, tools: List[MCPToolInfo]) -> Dict[str, List[str]]:
        """
        Builds a directed dependency graph among all tools.
        Returns: Dict mapping 'server:tool' to list of related 'server:tool' paths.
        """
        graph: Dict[str, List[str]] = {}
        tool_ids = [self._get_tool_id(t) for t in tools]
        
        # Initialize graph nodes
        for tid in tool_ids:
            graph[tid] = []

        # 1. Connect tools from the same server (server grouping)
        for i, t1 in enumerate(tools):
            tid1 = self._get_tool_id(t1)
            for j, t2 in enumerate(tools):
                if i == j:
                    continue
                tid2 = self._get_tool_id(t2)
                if t1.server_name == t2.server_name:
                    if tid2 not in graph[tid1]:
                        graph[tid1].append(tid2)

        # 2. Connect tools based on parameter sharing
        for i, t1 in enumerate(tools):
            tid1 = self._get_tool_id(t1)
            params1 = set(t1.get_parameters().keys())
            
            # Skip empty params
            if not params1:
                continue
                
            for j, t2 in enumerate(tools):
                if i == j:
                    continue
                tid2 = self._get_tool_id(t2)
                params2 = set(t2.get_parameters().keys())
                
                # Check intersection of params (e.g. sharing 'symbol', 'ticker', 'period')
                shared = params1.intersection(params2)
                
                # If they share specific domain parameters, connect them
                # Ignore very generic parameters like limit or query if not domain-specific
                domain_shared = {p for p in shared if p in ("period", "interval")}
                if domain_shared:
                    if tid2 not in graph[tid1]:
                        graph[tid1].append(tid2)

        # 3. Connect tools based on domain-specific workflow connections (keyword matching)
        for i, t1 in enumerate(tools):
            tid1 = self._get_tool_id(t1)
            name1_lower = t1.name.lower()
            
            for keyword, targets in self.workflow_connections.items():
                if keyword in name1_lower:
                    # Find all tools that match target keywords
                    for t2 in tools:
                        tid2 = self._get_tool_id(t2)
                        name2_lower = t2.name.lower()
                        for target in targets:
                            if target in name2_lower:
                                if tid2 not in graph[tid1]:
                                    graph[tid1].append(tid2)

        return graph
