from unittest import TestCase
from crawler import Base

import abc
import logging


class MyDerived(Base):
    ...


class TestBase(TestCase):
    def test_inherits_abc(self):
        self.assertTrue(issubclass(Base, abc.ABC), "Subclass")
        obj = Base()
        self.assertIsInstance(obj, abc.ABC, "Instance")

    def test_str(self):
        o = Base()
        self.assertEqual(str(o), "Base", "__str__")
        o = MyDerived()
        self.assertEqual(str(o), MyDerived.__name__, "__str__")

    def test_logging(self):
        for cls in [Base, MyDerived]:
            with self.assertLogs(level=logging.DEBUG) as logs:
                cls()
            self.assertEqual(len(logs.records), 1)
            record = logs.records[0]
            self.assertEqual(record.name, cls.__name__, "Logger name")
            self.assertEqual(record.message, "Initialized")
