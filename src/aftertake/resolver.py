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
from typing import Dict, Iterator, List, Mapping

ResolveOverrides = Dict[str, List[str]]


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
    """Temporarily override socket.getaddrinfo for selected hostnames only."""

    normalized = {key.lower().rstrip("."): list(value) for key, value in overrides.items() if value}
    if not normalized:
        yield
        return

    original = socket.getaddrinfo

    def patched(host, port, family=0, type=0, proto=0, flags=0):  # type: ignore[no-untyped-def]
        key = str(host or "").lower().rstrip(".")
        ips = normalized.get(key)
        if not ips:
            return original(host, port, family, type, proto, flags)
        ordered = list(ips)
        random.shuffle(ordered)
        results = []
        for ip in ordered:
            results.extend(original(ip, port, family, type, proto, flags))
        return results

    socket.getaddrinfo = patched  # type: ignore[assignment]
    try:
        yield
    finally:
        socket.getaddrinfo = original  # type: ignore[assignment]
