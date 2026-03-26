"""Cache module for storing frpt query results."""

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """A cached result entry."""

    stdout: str
    stderr: str
    return_code: int
    success: bool
    timestamp: float
    command_hash: str

    def is_expired(self, ttl_seconds: int) -> bool:
        """Check if this cache entry has expired."""
        return (time.time() - self.timestamp) > ttl_seconds

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "CacheEntry":
        """Create from dictionary."""
        return cls(**data)


class FrptCache:
    """File-based cache for frpt query results."""

    DEFAULT_CACHE_DIR = Path.home() / ".cache" / "module_yield_dashboard"
    DEFAULT_TTL_SECONDS = 3600  # 1 hour default TTL

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ):
        """Initialize cache.

        Args:
            cache_dir: Directory to store cache files
            ttl_seconds: Time-to-live in seconds (default 1 hour)
        """
        self._cache_dir = cache_dir or self.DEFAULT_CACHE_DIR
        self._ttl_seconds = ttl_seconds
        self._ensure_cache_dir()

    def _ensure_cache_dir(self) -> None:
        """Create cache directory if it doesn't exist."""
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_key(
        self,
        step: str,
        form_factor: str,
        workweek: str,
        dbase: str,
        facility: str,
    ) -> str:
        """Generate a unique cache key from command parameters."""
        key_string = f"{step}|{form_factor}|{workweek}|{dbase}|{facility}"
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def _get_cache_file(self, cache_key: str) -> Path:
        """Get the cache file path for a key."""
        return self._cache_dir / f"{cache_key}.json"

    def get(
        self,
        step: str,
        form_factor: str,
        workweek: str,
        dbase: str,
        facility: str,
    ) -> Optional[CacheEntry]:
        """Get cached result if available and not expired.

        Args:
            step: Test step
            form_factor: Module form factor
            workweek: Work week
            dbase: Design ID
            facility: Test facility

        Returns:
            CacheEntry if found and valid, None otherwise
        """
        cache_key = self._get_cache_key(step, form_factor, workweek, dbase, facility)
        cache_file = self._get_cache_file(cache_key)

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r") as f:
                data = json.load(f)
            entry = CacheEntry.from_dict(data)

            if entry.is_expired(self._ttl_seconds):
                logger.info(f"Cache expired for {step}/{form_factor}/WW{workweek}")
                cache_file.unlink()  # Delete expired entry
                return None

            logger.info(f"Cache HIT for {step}/{form_factor}/WW{workweek}")
            return entry

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Invalid cache entry: {e}")
            cache_file.unlink()  # Delete invalid entry
            return None

    def set(
        self,
        step: str,
        form_factor: str,
        workweek: str,
        dbase: str,
        facility: str,
        stdout: str,
        stderr: str,
        return_code: int,
        success: bool,
    ) -> None:
        """Store a result in cache.

        Args:
            step: Test step
            form_factor: Module form factor
            workweek: Work week
            dbase: Design ID
            facility: Test facility
            stdout: Command stdout
            stderr: Command stderr
            return_code: Command return code
            success: Whether command succeeded
        """
        cache_key = self._get_cache_key(step, form_factor, workweek, dbase, facility)
        cache_file = self._get_cache_file(cache_key)

        entry = CacheEntry(
            stdout=stdout,
            stderr=stderr,
            return_code=return_code,
            success=success,
            timestamp=time.time(),
            command_hash=cache_key,
        )

        try:
            with open(cache_file, "w") as f:
                json.dump(entry.to_dict(), f)
            logger.info(f"Cached result for {step}/{form_factor}/WW{workweek}")
        except IOError as e:
            logger.warning(f"Failed to write cache: {e}")

    def clear(self) -> int:
        """Clear all cached entries.

        Returns:
            Number of entries cleared
        """
        count = 0
        for cache_file in self._cache_dir.glob("*.json"):
            try:
                cache_file.unlink()
                count += 1
            except IOError:
                pass
        logger.info(f"Cleared {count} cache entries")
        return count

    def clear_expired(self) -> int:
        """Clear only expired cache entries.

        Returns:
            Number of entries cleared
        """
        count = 0
        for cache_file in self._cache_dir.glob("*.json"):
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)
                entry = CacheEntry.from_dict(data)
                if entry.is_expired(self._ttl_seconds):
                    cache_file.unlink()
                    count += 1
            except (json.JSONDecodeError, KeyError, TypeError, IOError):
                # Delete invalid entries too
                cache_file.unlink()
                count += 1
        logger.info(f"Cleared {count} expired cache entries")
        return count

    def get_stats(self) -> dict:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        total = 0
        valid = 0
        expired = 0
        total_size = 0

        for cache_file in self._cache_dir.glob("*.json"):
            total += 1
            total_size += cache_file.stat().st_size
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)
                entry = CacheEntry.from_dict(data)
                if entry.is_expired(self._ttl_seconds):
                    expired += 1
                else:
                    valid += 1
            except (json.JSONDecodeError, KeyError, TypeError, IOError):
                expired += 1

        return {
            "total_entries": total,
            "valid_entries": valid,
            "expired_entries": expired,
            "total_size_mb": total_size / (1024 * 1024),
            "cache_dir": str(self._cache_dir),
            "ttl_seconds": self._ttl_seconds,
        }
