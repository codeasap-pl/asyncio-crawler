# testing
import unittest
import asyncio
import sqlite3

# fake web app
import os
import flask
import logging
import multiprocessing

# crawler
from crawler import Cache


class BaseTestCase(unittest.IsolatedAsyncioTestCase):
    pass


class WorkerTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.stop_ev = asyncio.Event()
        self.dbconn = sqlite3.connect(":memory:")
        self.dbconn.row_factory = sqlite3.Row
        self.cache = Cache(self.dbconn)
        self.cache.initdb()

    def tearDown(self):
        self.dbconn.close()


class FakeApp:
    def __init__(self, host="localhost", port=65432):
        self.host = host
        self.port = port
        self.app = app = flask.Flask(self.__class__.__name__)
        self.process = None
        self.link_seq_id = 0

        @app.route("/error/<int:code>")
        def make_error_response(code):
            return flask.Response(status=code)

        @app.route("/html/<int:n_links>")
        def get_html_with_links(n_links):
            urls = []
            for _ in range(n_links):
                urls.append(
                    "http://{host}:{port}/{link_seq_id}".format(
                        **self.__dict__
                    )
                )
                self.link_seq_id += 1
            urls = ["<a href='%s'></a>" % url for url in urls]
            return "<body>%s</body>" % "\n".join(urls)

    def start(self, debug=False):
        logging.getLogger("werkzeug").disabled = not debug
        os.environ["WERKZEUG_RUN_MAIN"] = "false" if debug else "true"
        self.process = multiprocessing.Process(
            target=self.app.run,
            args=(self.host, self.port),
            kwargs={"debug": debug}
        )
        self.process.start()

    def stop(self):
        if self.process:
            self.process.terminate()
