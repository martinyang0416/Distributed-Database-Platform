"""
In-memory cache implementation for the coordinator.
"""
import time
import threading
from typing import Dict, Any, Optional, Tuple, List, Callable
from collections import OrderedDict
from distributed_db.common.utils import get_logger
from distributed_db.common.config import COORDINATOR_CACHE_SIZE

logger = get_logger(__name__)


class CacheManager:
    """
    Implements an in-memory LRU (Least Recently Used) cache for the coordinator.
    """
    
    def __init__(self, max_size: int = COORDINATOR_CACHE_SIZE):
        """
        Initialize the cache manager.
        
        Args:
            max_size: Maximum number of items to store in the cache
        """
        self._cache = OrderedDict()  # {key: (value, expiry_time)}
        self._max_size = max_size
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expirations': 0
        }
        
        # Start background thread for cache cleanup
        self._cleanup_thread = threading.Thread(target=self._cleanup_expired, daemon=True)
        self._cleanup_thread.start()
        
        logger.info(f"Cache manager initialized with max size {max_size}")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value if found and not expired, None otherwise
        """
        with self._lock:
            if key in self._cache:
                value, expiry_time = self._cache[key]
                
                # Check if expired
                if expiry_time is not None and time.time() > expiry_time:
                    self._cache.pop(key)
                    self._stats['expirations'] += 1
                    self._stats['misses'] += 1
                    return None
                
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                self._stats['hits'] += 1
                return value
            
            self._stats['misses'] += 1
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds, or None for no expiration
        """
        with self._lock:
            # Calculate expiry time if TTL is provided
            expiry_time = None
            if ttl is not None:
                expiry_time = time.time() + ttl
            
            # If key already exists, update it
            if key in self._cache:
                self._cache.pop(key)
            
            # Check if cache is full
            if len(self._cache) >= self._max_size:
                # Remove least recently used item
                self._cache.popitem(last=False)
                self._stats['evictions'] += 1
            
            # Add new item
            self._cache[key] = (value, expiry_time)
    
    def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if the key was found and deleted, False otherwise
        """
        with self._lock:
            if key in self._cache:
                self._cache.pop(key)
                return True
            return False
    
    def clear(self) -> None:
        """Clear the entire cache."""
        with self._lock:
            self._cache.clear()
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        with self._lock:
            stats = self._stats.copy()
            stats['size'] = len(self._cache)
            stats['max_size'] = self._max_size
            return stats
    
    def _cleanup_expired(self) -> None:
        """
        Background thread to clean up expired cache entries.
        """
        while True:
            time.sleep(60)  # Run cleanup every minute
            
            with self._lock:
                current_time = time.time()
                keys_to_remove = []
                
                # Find expired keys
                for key, (_, expiry_time) in self._cache.items():
                    if expiry_time is not None and current_time > expiry_time:
                        keys_to_remove.append(key)
                
                # Remove expired keys
                for key in keys_to_remove:
                    self._cache.pop(key)
                    self._stats['expirations'] += 1
                
                if keys_to_remove:
                    logger.debug(f"Cleaned up {len(keys_to_remove)} expired cache entries")
    
    def invalidate_by_prefix(self, prefix: str) -> int:
        """
        Invalidate all cache entries with keys starting with the given prefix.
        
        Args:
            prefix: Key prefix to match
            
        Returns:
            Number of entries invalidated
        """
        with self._lock:
            keys_to_remove = [key for key in self._cache.keys() if key.startswith(prefix)]
            for key in keys_to_remove:
                self._cache.pop(key)
            return len(keys_to_remove)
    
    def get_keys(self) -> List[str]:
        """
        Get all keys in the cache.
        
        Returns:
            List of cache keys
        """
        with self._lock:
            return list(self._cache.keys())
    
    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """
        Get multiple values from the cache.
        
        Args:
            keys: List of cache keys
            
        Returns:
            Dictionary mapping keys to values (only for keys that exist and are not expired)
        """
        result = {}
        with self._lock:
            for key in keys:
                value = self.get(key)
                if value is not None:
                    result[key] = value
        return result
    
    def set_many(self, items: Dict[str, Any], ttl: Optional[float] = None) -> None:
        """
        Set multiple values in the cache.
        
        Args:
            items: Dictionary mapping keys to values
            ttl: Time to live in seconds, or None for no expiration
        """
        with self._lock:
            for key, value in items.items():
                self.set(key, value, ttl)
    
    def delete_many(self, keys: List[str]) -> int:
        """
        Delete multiple values from the cache.
        
        Args:
            keys: List of cache keys
            
        Returns:
            Number of keys that were found and deleted
        """
        count = 0
        with self._lock:
            for key in keys:
                if self.delete(key):
                    count += 1
        return count
