from __future__ import annotations

import threading
from importlib import import_module
from queue import Queue
from typing import Any

from scrapy.crawler import CrawlerRunner
from scrapy.settings import Settings
from scrapy.utils.reactor import (
    install_reactor,
    is_asyncio_reactor_installed,
    is_reactor_installed,
    verify_installed_asyncio_event_loop,
    verify_installed_reactor,
)
from twisted.python.failure import Failure


_REACTOR_LOCK = threading.Lock()
_REACTOR_STARTED = threading.Event()
_REACTOR_THREAD: threading.Thread | None = None
_REACTOR: Any | None = None
_REACTOR_PATH: str | None = None
_ASYNCIO_EVENT_LOOP_PATH: str | None = None


def _coerce_settings(settings: dict[str, Any] | Settings) -> Settings:
    if isinstance(settings, Settings):
        return settings.copy()
    return Settings(settings)


def _verify_reactor_configuration(reactor_path: str | None, event_loop_path: str | None) -> None:
    if reactor_path:
        verify_installed_reactor(reactor_path)
    if event_loop_path and is_asyncio_reactor_installed():
        verify_installed_asyncio_event_loop(event_loop_path)


def _bootstrap_reactor(settings: dict[str, Any] | Settings) -> Any:
    global _REACTOR
    global _REACTOR_PATH
    global _ASYNCIO_EVENT_LOOP_PATH

    scrapy_settings = _coerce_settings(settings)
    reactor_path = scrapy_settings.get("TWISTED_REACTOR")
    event_loop_path = scrapy_settings.get("ASYNCIO_EVENT_LOOP")

    with _REACTOR_LOCK:
        if _REACTOR is None:
            if reactor_path and not is_reactor_installed():
                install_reactor(reactor_path, event_loop_path)
            _verify_reactor_configuration(reactor_path, event_loop_path)
            _REACTOR = import_module("twisted.internet").reactor
            _REACTOR_PATH = reactor_path
            _ASYNCIO_EVENT_LOOP_PATH = event_loop_path
            return _REACTOR

        _verify_reactor_configuration(reactor_path or _REACTOR_PATH, event_loop_path or _ASYNCIO_EVENT_LOOP_PATH)
        return _REACTOR


def _reactor_main(reactor: Any) -> None:
    _REACTOR_STARTED.set()
    reactor.run(installSignalHandlers=False)


def ensure_reactor_running(settings: dict[str, Any] | Settings) -> Any:
    global _REACTOR_THREAD

    reactor = _bootstrap_reactor(settings)
    if reactor.running:
        _REACTOR_STARTED.set()
        return reactor

    with _REACTOR_LOCK:
        if reactor.running:
            _REACTOR_STARTED.set()
            return reactor
        if _REACTOR_THREAD is None or not _REACTOR_THREAD.is_alive():
            _REACTOR_THREAD = threading.Thread(
                target=_reactor_main,
                args=(reactor,),
                name="scrapy-reactor",
                daemon=True,
            )
            _REACTOR_THREAD.start()

    _REACTOR_STARTED.wait(timeout=5.0)
    return reactor


def run_spider(
    spider_cls: type,
    *,
    settings: dict[str, Any] | Settings,
    **spider_kwargs: Any,
) -> None:
    reactor = ensure_reactor_running(settings)
    result_queue: Queue[tuple[str, Any]] = Queue(maxsize=1)

    def schedule() -> None:
        try:
            runner = CrawlerRunner(settings=settings)
            deferred = runner.crawl(spider_cls, **spider_kwargs)

            def on_success(result: Any) -> Any:
                result_queue.put(("success", result))
                return result

            def on_error(failure: Failure) -> Failure:
                result_queue.put(("error", failure))
                return failure

            deferred.addCallbacks(on_success, on_error)
        except Exception as exc:
            result_queue.put(("error", exc))

    reactor.callFromThread(schedule)
    status, payload = result_queue.get()
    if status == "success":
        return
    if isinstance(payload, Failure):
        payload.raiseException()
    raise payload
