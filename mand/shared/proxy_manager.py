# shared/proxy_manager.py
import random
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set

class ProxyManager:
    """Thread-safe manager that assigns one proxy per 'session_id' until it fails."""

    def __init__(self, proxy_file: Optional[str] = None):
        # Default: project root / proxies.txt
        if proxy_file:
            self.proxy_file = Path(proxy_file)
        else:
            # assume project root is two levels up from this file (adjust if needed)
            self.proxy_file = Path(__file__).resolve().parents[2] / "proxies.txt"

        self._lock = threading.Lock()
        self._proxies: List[str] = []
        self._dead: Set[str] = set()
        self._session_map: Dict[str, str] = {}  # session_id -> proxy
        self.reload_proxies()

    def reload_proxies(self) -> None:
        """(Re)load proxies from file. Safe to call at runtime."""
        with self._lock:
            try:
                lines = self.proxy_file.read_text().splitlines()
            except FileNotFoundError:
                self._proxies = []
                return
            proxies = [p.strip() for p in lines if p.strip()]
            # remove duplicates, keep order
            seen = set()
            clean = []
            for p in proxies:
                if p not in seen:
                    seen.add(p)
                    clean.append(p)
            self._proxies = clean
            # if previously dead proxies are no longer in file, remove them
            self._dead.intersection_update(self._proxies)

    def _available_proxies(self) -> List[str]:
        return [p for p in self._proxies if p not in self._dead]

    def get_proxy_for_session(self, session_id: str) -> Optional[str]:
        """Return assigned proxy for a session_id, or assign a new one if none."""
        with self._lock:
            if session_id in self._session_map:
                return self._session_map[session_id]
            avail = self._available_proxies()
            if not avail:
                return None
            proxy = random.choice(avail)
            self._session_map[session_id] = proxy
            return proxy

    def rotate_proxy_for_session(self, session_id: str) -> Optional[str]:
        """Mark current proxy dead for this session and choose a new one."""
        with self._lock:
            old = self._session_map.get(session_id)
            if old:
                self._dead.add(old)
            avail = self._available_proxies()
            if not avail:
                # clear mapping so future get attempts may return None
                self._session_map.pop(session_id, None)
                return None
            new = random.choice(avail)
            self._session_map[session_id] = new
            return new

    def mark_proxy_bad(self, proxy: str) -> None:
        """Global mark a proxy as dead (e.g., if you detect it's banned)."""
        with self._lock:
            self._dead.add(proxy)
            # remove any sessions mapped to it
            for sid, p in list(self._session_map.items()):
                if p == proxy:
                    self._session_map.pop(sid, None)

    def free_session(self, session_id: str) -> None:
        """Remove session mapping when the session ends."""
        with self._lock:
            self._session_map.pop(session_id, None)

# module-level singleton
_DEFAULT_MANAGER: Optional[ProxyManager] = None

def get_proxy_manager(proxy_file: Optional[str] = None) -> ProxyManager:
    global _DEFAULT_MANAGER
    if _DEFAULT_MANAGER is None:
        _DEFAULT_MANAGER = ProxyManager(proxy_file=proxy_file)
    return _DEFAULT_MANAGER
