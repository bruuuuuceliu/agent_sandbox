"""Command-line entry point for the sandbox manager."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from threading import Event, Thread
from types import FrameType

from .config import ConfigError, load_config
from .server import Readiness, create_server


def _log(level: str, message: str, **fields: object) -> None:
    record = {
        "time": datetime.now(UTC).isoformat(),
        "level": level,
        "msg": message,
        **fields,
    }
    print(json.dumps(record, separators=(",", ":")), flush=True)


def run(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="sandbox-manager")
    parser.add_argument(
        "--config", required=True, help="path to an environment .config file"
    )
    options = parser.parse_args(arguments)

    config = load_config(options.config)
    readiness = Readiness()
    server = create_server("", config.manager_port, readiness)
    shutting_down = Event()

    def request_shutdown(signum: int, _frame: FrameType | None) -> None:
        if shutting_down.is_set():
            return
        shutting_down.set()
        readiness.set(False)
        _log(
            "INFO", "shutting down sandbox manager", signal=signal.Signals(signum).name
        )
        Thread(target=server.shutdown, name="http-shutdown", daemon=True).start()

    previous_handlers = {
        signum: signal.signal(signum, request_shutdown)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    readiness.set(True)
    _log(
        "INFO",
        "sandbox manager started",
        environment=config.app_env.value,
        port=config.manager_port,
        max_sandboxes=config.max_sandboxes,
    )

    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        readiness.set(False)
        server.server_close()
        for signum, previous_handler in previous_handlers.items():
            signal.signal(signum, previous_handler)


def main() -> int:
    try:
        run()
    except ConfigError as error:
        _log("ERROR", "sandbox manager stopped", error=str(error))
        return 1
    except OSError as error:
        _log("ERROR", "sandbox manager stopped", error=str(error))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
