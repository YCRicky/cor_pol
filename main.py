"""Railway/Railpack process entrypoint."""

from aftertake.runner import main


if __name__ == "__main__":
    raise SystemExit(main(["--forever"]))
