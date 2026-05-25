from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scrapy.settings import Settings

import scrapers.scrapy_runner as scrapy_runner


class FakeEvent:
    def __init__(self) -> None:
        self.set_calls = 0
        self.wait_calls: list[float | None] = []

    def set(self) -> None:
        self.set_calls += 1

    def wait(self, timeout: float | None = None) -> bool:
        self.wait_calls.append(timeout)
        return True


class ScrapyRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = (
            scrapy_runner._REACTOR,
            scrapy_runner._REACTOR_PATH,
            scrapy_runner._ASYNCIO_EVENT_LOOP_PATH,
            scrapy_runner._REACTOR_THREAD,
            scrapy_runner._REACTOR_STARTED,
        )
        scrapy_runner._REACTOR = None
        scrapy_runner._REACTOR_PATH = None
        scrapy_runner._ASYNCIO_EVENT_LOOP_PATH = None
        scrapy_runner._REACTOR_THREAD = None
        scrapy_runner._REACTOR_STARTED = threading.Event()

    def tearDown(self) -> None:
        (
            scrapy_runner._REACTOR,
            scrapy_runner._REACTOR_PATH,
            scrapy_runner._ASYNCIO_EVENT_LOOP_PATH,
            scrapy_runner._REACTOR_THREAD,
            scrapy_runner._REACTOR_STARTED,
        ) = self.original_state

    def test_bootstrap_installs_default_reactor_before_import_and_caches_it(self):
        expected_reactor_path = Settings().get("TWISTED_REACTOR")
        fake_reactor = SimpleNamespace(running=False)
        call_order: list[str] = []

        def fake_import_module(name: str) -> SimpleNamespace:
            self.assertEqual(name, "twisted.internet")
            call_order.append("import")
            return SimpleNamespace(reactor=fake_reactor)

        with (
            patch.object(scrapy_runner, "is_reactor_installed", return_value=False),
            patch.object(
                scrapy_runner,
                "install_reactor",
                side_effect=lambda *args: call_order.append("install"),
            ) as install_mock,
            patch.object(
                scrapy_runner,
                "verify_installed_reactor",
                side_effect=lambda path: call_order.append(f"verify:{path}"),
            ) as verify_mock,
            patch.object(scrapy_runner, "import_module", side_effect=fake_import_module) as import_mock,
        ):
            reactor = scrapy_runner._bootstrap_reactor({})

        self.assertIs(reactor, fake_reactor)
        self.assertEqual(call_order, ["install", f"verify:{expected_reactor_path}", "import"])
        install_mock.assert_called_once_with(expected_reactor_path, None)
        verify_mock.assert_called_once_with(expected_reactor_path)
        import_mock.assert_called_once_with("twisted.internet")
        self.assertIs(scrapy_runner._REACTOR, fake_reactor)
        self.assertEqual(scrapy_runner._REACTOR_PATH, expected_reactor_path)

    def test_bootstrap_reuses_cached_reactor_without_reinstalling(self):
        expected_reactor_path = Settings().get("TWISTED_REACTOR")
        fake_reactor = SimpleNamespace(running=False)

        with (
            patch.object(scrapy_runner, "is_reactor_installed", return_value=False),
            patch.object(scrapy_runner, "install_reactor") as install_mock,
            patch.object(scrapy_runner, "verify_installed_reactor") as verify_mock,
            patch.object(
                scrapy_runner,
                "import_module",
                return_value=SimpleNamespace(reactor=fake_reactor),
            ) as import_mock,
        ):
            first = scrapy_runner._bootstrap_reactor({})
            second = scrapy_runner._bootstrap_reactor({})

        self.assertIs(first, fake_reactor)
        self.assertIs(second, fake_reactor)
        install_mock.assert_called_once_with(expected_reactor_path, None)
        self.assertEqual(verify_mock.call_count, 2)
        verify_mock.assert_called_with(expected_reactor_path)
        import_mock.assert_called_once_with("twisted.internet")

    def test_ensure_reactor_running_starts_thread_only_once(self):
        fake_reactor = SimpleNamespace(running=False)
        fake_event = FakeEvent()
        created_threads: list[object] = []

        class FakeThread:
            def __init__(self, *, target, args, name, daemon) -> None:
                self.target = target
                self.args = args
                self.name = name
                self.daemon = daemon
                self._alive = False
                created_threads.append(self)

            def start(self) -> None:
                self._alive = True
                self.args[0].running = True

            def is_alive(self) -> bool:
                return self._alive

        with (
            patch.object(scrapy_runner, "_bootstrap_reactor", return_value=fake_reactor),
            patch.object(scrapy_runner, "_REACTOR_STARTED", fake_event),
            patch.object(scrapy_runner.threading, "Thread", side_effect=FakeThread),
        ):
            first = scrapy_runner.ensure_reactor_running({})
            second = scrapy_runner.ensure_reactor_running({})

        self.assertIs(first, fake_reactor)
        self.assertIs(second, fake_reactor)
        self.assertEqual(len(created_threads), 1)
        self.assertEqual(fake_event.wait_calls, [5.0])
        self.assertEqual(fake_event.set_calls, 1)

    def test_bootstrap_raises_clear_error_for_conflicting_installed_reactor(self):
        with (
            patch.object(scrapy_runner, "is_reactor_installed", return_value=True),
            patch.object(
                scrapy_runner,
                "verify_installed_reactor",
                side_effect=RuntimeError("reactor conflict"),
            ),
            patch.object(scrapy_runner, "import_module") as import_mock,
            patch.object(scrapy_runner, "install_reactor") as install_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "reactor conflict"):
                scrapy_runner._bootstrap_reactor({})

        install_mock.assert_not_called()
        import_mock.assert_not_called()

    def test_run_spider_uses_cached_reactor_to_schedule_crawl(self):
        class FakeDeferred:
            def addCallbacks(self, on_success, on_error):
                on_success(None)
                return self

        class FakeRunner:
            def crawl(self, spider_cls, **kwargs):
                return FakeDeferred()

        class FakeReactor:
            def __init__(self) -> None:
                self.scheduled = 0

            def callFromThread(self, callback) -> None:
                self.scheduled += 1
                callback()

        fake_reactor = FakeReactor()

        with (
            patch.object(scrapy_runner, "ensure_reactor_running", return_value=fake_reactor) as ensure_mock,
            patch.object(scrapy_runner, "CrawlerRunner", return_value=FakeRunner()) as runner_mock,
        ):
            scrapy_runner.run_spider(object, settings={"LOG_LEVEL": "INFO"}, collector={})

        ensure_mock.assert_called_once_with({"LOG_LEVEL": "INFO"})
        runner_mock.assert_called_once_with(settings={"LOG_LEVEL": "INFO"})
        self.assertEqual(fake_reactor.scheduled, 1)


if __name__ == "__main__":
    unittest.main()
