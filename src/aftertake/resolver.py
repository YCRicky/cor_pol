"""Scoped DNS resolution overrides for Polymarket public endpoints.

The Mac running this bot can intermittently resolve Polymarket hosts to an RPZ
landing IP that serves a self-signed certificate.  This module does not disable
TLS verification.  It only overrides DNS resolution for explicit hostnames while
leaving the original HTTPS/WSS hostname in place, so SNI, Host headers, and
certificate validation still use the official provider hostname.
"""

from __future__ import annotations

import contextlib
import random
import socket
import threading
from typing import Dict, Iterator, List, Mapping

ResolveOverrides = Dict[str, List[str]]


# ``socket.getaddrinfo`` is process-global, while Aftertake opens one market
# stream per asset.  A naive save/replace/restore context manager is therefore
# corruptible when two threads leave their contexts in a different order than
# they entered.  Keep one dispatcher installed while any override context is
# active and select the override from thread-local state instead.
_resolver_lock = threading.RLock()
_resolver_local = threading.local()
_resolver_base = socket.getaddrinfo
_resolver_contexts = 0


def _dispatched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):  # type: ignore[no-untyped-def]
    stack = getattr(_resolver_local, "stack", ())
    overrides = stack[-1] if stack else {}
    key = str(host or "").lower().rstrip(".")
    ips = overrides.get(key)
    with _resolver_lock:
        original = _resolver_base
    if not ips:
        return original(host, port, family, type, proto, flags)
    ordered = list(ips)
    random.shuffle(ordered)
    results = []
    for ip in ordered:
        results.extend(original(ip, port, family, type, proto, flags))
    return results


def parse_resolve_overrides(raw: str) -> ResolveOverrides:
    """Parse ``host=ip,ip;other=ip`` into a normalized override map."""

    overrides: ResolveOverrides = {}
    for entry in str(raw or "").split(";"):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError("resolve override must be host=ip[,ip]")
        host, ips = entry.split("=", 1)
        host = host.strip().lower().rstrip(".")
        parsed_ips = [item.strip() for item in ips.split(",") if item.strip()]
        if not host or not parsed_ips:
            raise ValueError("resolve override must include host and at least one IP")
        # Validate literals up front.  socket.create_connection will still decide
        # reachability; this only prevents accidental garbage config.
        for ip in parsed_ips:
            try:
                socket.inet_aton(ip)
            except OSError as exc:
                raise ValueError(f"illegal IP in resolve override for {host}: {ip}") from exc
        overrides[host] = parsed_ips
    return overrides


@contextlib.contextmanager
def scoped_getaddrinfo(overrides: Mapping[str, List[str]]) -> Iterator[None]:
    """Override selected hostnames without corrupting concurrent callers."""

    normalized = {key.lower().rstrip("."): list(value) for key, value in overrides.items() if value}
    if not normalized:
        yield
        return

    global _resolver_base, _resolver_contexts
    with _resolver_lock:
        if _resolver_contexts == 0:
            _resolver_base = socket.getaddrinfo
            socket.getaddrinfo = _dispatched_getaddrinfo  # type: ignore[assignment]
        _resolver_contexts += 1
    stack = list(getattr(_resolver_local, "stack", ()))
    stack.append(normalized)
    _resolver_local.stack = stack
    try:
        yield
    finally:
        stack = list(getattr(_resolver_local, "stack", ()))
        if stack:
            stack.pop()
        _resolver_local.stack = stack
        with _resolver_lock:
            _resolver_contexts -= 1
            if _resolver_contexts == 0:
                socket.getaddrinfo = _resolver_base  # type: ignore[assignment]
