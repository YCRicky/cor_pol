import socket

from aftertake.resolver import parse_resolve_overrides, scoped_getaddrinfo


def test_parse_resolve_overrides():
    overrides = parse_resolve_overrides("Example.COM=1.1.1.1,1.0.0.1;other.test=8.8.8.8")

    assert overrides == {
        "example.com": ["1.1.1.1", "1.0.0.1"],
        "other.test": ["8.8.8.8"],
    }


def test_scoped_getaddrinfo_preserves_original_after_exit():
    original = socket.getaddrinfo

    with scoped_getaddrinfo({"example.com": ["127.0.0.1"]}):
        results = socket.getaddrinfo("example.com", 443, socket.AF_INET, socket.SOCK_STREAM)
        assert {row[4][0] for row in results} == {"127.0.0.1"}

    assert socket.getaddrinfo is original
