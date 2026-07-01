import json
import logging
from typing import Dict, List, Optional
from src.utils.cache import get_cache_client

logger = logging.getLogger(__name__)

class AsyncCacheManager:
    """
    Manages the dependency graph in Redis asynchronously.
    Wraps the LangAlpha RedisCacheClient.
    """
    def __init__(self, prefix: str = 'ace:'):
        self.prefix = prefix
        self.graph_key = f"{self.prefix}dependency_graph"
        self.hash_key = f"{self.prefix}graph_nodes"

    async def is_available(self) -> bool:
        """Check if Redis is available."""
        try:
            client = get_cache_client()
            if client is None or not client.enabled:
                return False
            # Check ping
            return await client.redis.ping()
        except Exception as e:
            logger.warning(f"ACE AsyncCacheManager: Redis is not available: {e}")
            return False

    async def save_graph(self, graph: Dict[str, List[str]], ttl: Optional[int] = None):
        """Save the entire graph to redis as a JSON string."""
        try:
            client = get_cache_client()
            if client and client.enabled:
                # We serialize to JSON
                val = json.dumps(graph)
                await client.set(self.graph_key, val, expire=ttl)
        except Exception as e:
            logger.error(f"ACE AsyncCacheManager: Failed to save graph: {e}")

    async def get_graph(self) -> Dict[str, List[str]]:
        """Retrieve the entire graph from redis."""
        try:
            client = get_cache_client()
            if client and client.enabled:
                data = await client.get(self.graph_key)
                if data:
                    return json.loads(data)
        except Exception as e:
            logger.error(f"ACE AsyncCacheManager: Failed to get graph: {e}")
        return {}

    async def update_node(self, file_path: str, dependencies: List[str]):
        """Update a specific node incrementally in a hash."""
        try:
            client = get_cache_client()
            if client and client.enabled:
                # Use hash_set from RedisCacheClient or direct client.redis.hset
                await client.redis.hset(self.hash_key, file_path, json.dumps(dependencies))
        except Exception as e:
            logger.error(f"ACE AsyncCacheManager: Failed to update node {file_path}: {e}")

    async def get_node_dependencies(self, file_path: str) -> List[str]:
        """Get dependencies of a specific node."""
        try:
            client = get_cache_client()
            if client and client.enabled:
                data = await client.redis.hget(self.hash_key, file_path)
                if data:
                    return json.loads(data)
        except Exception as e:
            logger.error(f"ACE AsyncCacheManager: Failed to get dependencies for {file_path}: {e}")
        
        # Fallback to monolithic graph
        graph = await self.get_graph()
        return graph.get(file_path, [])
