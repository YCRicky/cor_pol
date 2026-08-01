import socket
import threading

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


def test_scoped_getaddrinfo_restores_original_after_interleaved_thread_exits():
    original = socket.getaddrinfo
    first_entered = threading.Event()
    second_entered = threading.Event()
    first_exited = threading.Event()
    failures = []

    def first_worker():
        try:
            with scoped_getaddrinfo({"first.test": ["127.0.0.1"]}):
                first_entered.set()
                assert second_entered.wait(timeout=1.0)
            first_exited.set()
        except BaseException as exc:
            failures.append(exc)
            first_exited.set()

    def second_worker():
        try:
            assert first_entered.wait(timeout=1.0)
            with scoped_getaddrinfo({"second.test": ["127.0.0.2"]}):
                second_entered.set()
                assert first_exited.wait(timeout=1.0)
        except BaseException as exc:
            failures.append(exc)

    first = threading.Thread(target=first_worker, name="resolver-first")
    second = threading.Thread(target=second_worker, name="resolver-second")
    try:
        first.start()
        second.start()
        first.join(timeout=2.0)
        second.join(timeout=2.0)
        both_finished = not first.is_alive() and not second.is_alive()
        restored = socket.getaddrinfo is original
    finally:
        # A red test must not poison the remaining process-wide resolver tests.
        socket.getaddrinfo = original

    assert both_finished
    assert failures == []
    assert restored
