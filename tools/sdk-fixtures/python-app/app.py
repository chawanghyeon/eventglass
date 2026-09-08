#!/usr/bin/env python3
"""Minimal real Sentry Python SDK sender used by the capture fixtures."""

from __future__ import annotations

import logging
import os
import sys

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.scrubber import EventScrubber


LOGGER_NAME = "fixture.python"


def log_extra(**values: object) -> dict[str, object]:
    if secret := os.environ.get("SENTRY_FIXTURE_SECRET"):
        values["password"] = secret
    return values


def init_sdk(*, debug_logs: bool) -> None:
    live_mode = os.environ.get("SENTRY_FIXTURE_MODE") == "live-sequential"
    integration_options: dict[str, int] = {
        "level": logging.INFO,
        "event_level": logging.ERROR,
    }
    if debug_logs:
        integration_options["sentry_logs_level"] = logging.DEBUG

    live_options: dict[str, object] = {}
    if live_mode:
        # Only the localhost live gate disables the SDK's overlapping denylist so
        # the synthetic value reaches Eventglass and exercises server scrubbing.
        live_options["event_scrubber"] = EventScrubber(
            denylist=[], send_default_pii=True, pii_denylist=[]
        )

    sentry_sdk.init(
        dsn=os.environ["SENTRY_FIXTURE_DSN"],
        enable_logs=True,
        default_integrations=False,
        integrations=[LoggingIntegration(**integration_options)],
        environment="fixture",
        release="eventglass-sdk-fixture@1",
        server_name="fixture-python-host",
        send_client_reports=False,
        auto_session_tracking=False,
        **live_options,
    )
    sentry_sdk.set_user({"id": "fixture-user", "email": "fixture@example.invalid"})
    sentry_sdk.set_tag("service.name", "fixture-python")
    sentry_sdk.set_extra("fixture_unicode", "안녕하세요 👋")
    if secret := os.environ.get("SENTRY_FIXTURE_SECRET"):
        # The live Eventglass runner uses a synthetic sentinel to prove server-side
        # scrubbing. Offline capture generation deliberately leaves this unset.
        sentry_sdk.set_extra("password", secret)


def send_events() -> None:
    init_sdk(debug_logs=False)
    sentry_sdk.add_breadcrumb(category="fixture", message="before exception")
    try:
        raise RuntimeError("python fixture exception")
    except RuntimeError as error:
        sentry_sdk.capture_exception(error)
    sentry_sdk.capture_message("python fixture message", level="warning")


def send_default_logs() -> None:
    init_sdk(debug_logs=False)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.debug("python default debug omitted", extra=log_extra(fixture_case="default"))
    logger.info("python default info", extra=log_extra(order_id=381))
    logger.warning("python default warning", extra=log_extra(retry_count=2))
    logger.error("python default error", extra=log_extra(payment_id="pay_fixture"))
    logger.critical("python default critical", extra=log_extra(region="fixture-region"))


def send_debug_log() -> None:
    init_sdk(debug_logs=True)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.debug("python opt-in debug", extra=log_extra(query_rows=7))


def main() -> int:
    mode = sys.argv[1]
    if mode == "events":
        send_events()
    elif mode == "logging-default":
        send_default_logs()
    elif mode == "logging-debug":
        send_debug_log()
    else:
        raise SystemExit(f"unknown mode: {mode}")

    # Python's public flush API waits synchronously and returns None.
    sentry_sdk.flush(timeout=5.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
