from crawler import Base
from .testing import BaseTestCase

import abc
import logging


class TestBase(BaseTestCase):
    def test_inherits_abc(self):
        obj = Base()
        self.assertIsInstance(obj, abc.ABC)

    def test_str(self):
        o = Base()
        self.assertEqual(str(o), "Base", "__str__")

    def test_logging(self):
        with self.assertLogs(level=logging.DEBUG) as logs:
            Base()
        self.assertEqual(len(logs), 2)
        names = set([r.name for r in logs.records])
        self.assertEqual(names, set(["Base"]), "Logger name")
        self.assertEqual(logs.records[0].message, "Initialized")
