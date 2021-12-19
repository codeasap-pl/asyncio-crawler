from crawler import Cache, Base
from .testing import BaseTestCase

import logging
import sqlite3


class TestCacheBasics(BaseTestCase):
    def test_init(self):
        dbconn = sqlite3.connect(":memory:")
        with self.assertLogs(level=logging.DEBUG) as logs:
            obj = Cache(dbconn)
        self.assertIsInstance(obj, Base, "Inherits Base")
        self.assertIsInstance(obj.db, type(dbconn), "Stores db connection")

        self.assertEqual(len(logs), 2)
        names = set([r.name for r in logs.records])
        self.assertEqual(names, set(["Cache"]), "Logger name")

        # check logs
        self.assertEqual(logs.records[0].message, "Initialized")
        expected = "sqlite3.Connection object at 0x"
        self.assertIn(expected, logs.records[1].message, "Logs db connection")


class TestCacheOperations(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dbconn = sqlite3.connect(":memory:")
        self.dbconn.row_factory = sqlite3.Row

    def tearDown(self):
        self.dbconn.close()
        super().tearDown()

    def test_initdb(self):
        obj = Cache(self.dbconn)
        obj.initdb()

        # check db schema
        cur = self.dbconn.cursor()
        cur.execute("SELECT * FROM urls")
        self.assertEqual(cur.fetchall(), [], "Empty urls table")

    def test_store_links(self):
        obj = Cache(self.dbconn)
        obj.initdb()
        links = ["http://localhost/%d" % i for i in range(8)]
        obj.store_links(links)

        # check db records
        cur = self.dbconn.cursor()
        cur.execute("SELECT * FROM urls")
        records = cur.fetchall()
        self.assertEqual(len(records), len(links), "All links were stored")

        urls = [r["url"] for r in records]
        self.assertFalse(set(links).difference(urls), "Links")

        null_fields = ["mtime", "content_type", "status_code", "size"]
        for r in records:
            self.assertIsNotNone(r["ctime"], "ctime has a timestamp")
            # check expected null_fields are NULL
            for field in null_fields:
                self.assertIsNone(r[field], "Expexted NULL: %s" % field)

    def test_store_links_duplicates(self):
        """Checks if store_links() ignores already existing links"""
        obj = Cache(self.dbconn)
        obj.initdb()
        links = ["http://localhost/%d" % i for i in range(8)]

        # Add first time
        obj.store_links(links)

        # Add second time
        obj.store_links(links)

        cur = self.dbconn.cursor()
        cur.execute("SELECT * FROM urls")
        records = cur.fetchall()
        self.assertEqual(len(records), len(links), "Ignores duplicates")

    def test_store_response(self):
        """Checks storing a response. Repeats it twice to check UPDATE."""
        obj = Cache(self.dbconn)
        obj.initdb()

        # NOTE: lowercase keys.
        # Regular headers in a response are a 'case-insensitive mapping'.
        response = dict(
            url="http://localhost",
            status_code=200,
            headers={
                "content-type": "text/html",
                "content-length": 123,
            }
        )

        # FIRST TIME
        obj.store_response(**response)

        cur = self.dbconn.cursor()
        cur.execute("SELECT * FROM urls")
        records = cur.fetchall()
        self.assertEqual(len(records), 1, "Stored")

        # check record values
        r = records[0]
        self.assertEqual(
            r["content_type"],
            response["headers"]["content-type"],
            "Stores content type"
        )

        self.assertEqual(
            r["size"],
            response["headers"]["content-length"],
            "Stores size (content-length)"
        )

        # SECOND TIME /update/
        response["headers"].update({
            "content-type": "application/xml",
            "content-length": 234,
        })
        obj.store_response(**response)

        cur = self.dbconn.cursor()
        cur.execute("SELECT * FROM urls")
        records = cur.fetchall()
        self.assertEqual(len(records), 1, "Updated")

        # check record values
        r = records[0]
        self.assertEqual(
            r["content_type"],
            response["headers"]["content-type"],
            "Stores content type"
        )

        self.assertEqual(
            r["size"],
            response["headers"]["content-length"],
            "Stores size (content-length)"
        )
