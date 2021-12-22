from crawler import Worker, Base
from .testing import BaseTestCase

from unittest.mock import AsyncMock
import logging


class MyWorker(Worker):
    async def initialize(self, *args, **kwargs): ...
    async def shutdown(self, *args, **kwargs): ...
    async def _run(self, *args, **kwargs): ...


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


class TestCallOperator(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.postitional_args = (1, 2, "test", None, ["abc"])
        self.keyword_args = {"one": 1, "two": 2}

    async def test_initialize(self):
        obj = MyWorker()
        obj.initialize = AsyncMock()
        await obj(*self.postitional_args, **self.keyword_args)
        self.assertEqual(obj.initialize.call_count, 1, "Calls initialize()")
        obj.initialize.assert_called_once_with(
            *self.postitional_args,
            **self.keyword_args,
        )

    async def test_shutdown(self):
        obj = MyWorker()
        obj.shutdown = AsyncMock()
        await obj(*self.postitional_args, **self.keyword_args)
        self.assertEqual(obj.shutdown.call_count, 1, "Calls shutdown()")
        obj.shutdown.assert_called_once_with(
            *self.postitional_args,
            **self.keyword_args,
        )

    async def test_run(self):
        obj = MyWorker()
        obj._run = AsyncMock()

        # calling object (__call__) calls run()
        with self.assertLogs(level=logging.DEBUG) as logs:
            await obj(*self.postitional_args, **self.keyword_args)

        # check logs
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs.records[0].message, "Running")
        self.assertEqual(logs.records[1].message, "Done")

        # check run()
        self.assertTrue(obj._run.called, "Calls run")
        self.assertEqual(obj._run.call_count, 1, "Calls run only once")

        # check all passed arguments
        obj._run.assert_called_once_with(
            *self.postitional_args,
            **self.keyword_args,
        )

    async def test_run_retval(self):
        value = "Hello"
        obj = MyWorker()
        obj._run = AsyncMock()
        obj._run.return_value = value
        retval = await obj()
        self.assertEqual(retval, value, "Returned valued")
