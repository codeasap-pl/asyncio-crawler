from crawler import Worker, Base
from .testing import BaseTestCase

from unittest.mock import AsyncMock
import logging


class MyWorker(Worker):
    async def _run(self, *args, **kwargs):
        pass


class TestWorker(BaseTestCase):
    def test_abc(self):
        with self.assertRaises(TypeError):
            Worker()

    def test_init(self):
        obj = MyWorker()
        self.assertIsInstance(obj, Worker, "Worker")
        self.assertIsInstance(obj, Base, "Base")

    def test_logger(self):
        with self.assertLogs(level=logging.DEBUG) as logs:
            MyWorker()
        self.assertEqual(len(logs), 2)
        names = set([r.name for r in logs.records])
        self.assertEqual(names, set(["MyWorker"]), "Logger name")

    def test_callable(self):
        obj = MyWorker()
        self.assertTrue(callable(obj), "Implements __call__")

    async def test_run(self):
        obj = MyWorker()
        obj._run = AsyncMock()
        postitional_args = ("foo", "bar", 7, 9, 13)
        keyword_args = dict(something=17)

        # calling object (__call__) calls run()
        with self.assertLogs(level=logging.DEBUG) as logs:
            await obj(*postitional_args, **keyword_args)

        # check logs
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs.records[0].message, "Running")
        self.assertEqual(logs.records[1].message, "Done")

        # check run()
        self.assertTrue(obj._run.called, "Calls run")
        self.assertEqual(obj._run.call_count, 1, "Calls run only once")

        # check all passed arguments
        obj._run.assert_called_once_with(
            *postitional_args,
            **keyword_args,
        )
