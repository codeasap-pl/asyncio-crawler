from crawler import Downloader, DownloaderError, Worker
from .testing import WorkerTestCase, FakeApp

import asyncio
import aiohttp
from urllib.parse import urljoin


class DownloaderTestCase(WorkerTestCase):
    def setUp(self):
        super().setUp()
        # reset class attribute
        Downloader.SEQ_ID = 0
        self.url_queue = asyncio.Queue()

    async def asyncSetUp(self):
        super().setUp()
        self.http_client = aiohttp.ClientSession(
            headers={"User-Agent": "Test Client"},
        )

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self.http_client.close()


class TestDownloaderBasic(DownloaderTestCase):
    def test_init(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client
        )
        self.assertIsInstance(downloader, Worker, "Is a Worker")

    def test_object_enumeration(self):
        for i in range(1, 8):
            downloader = Downloader(
                self.stop_ev,
                self.cache,
                self.url_queue,
                self.http_client
            )
            self.assertEqual(downloader.seq_id, i, "sequential id")
            self.assertEqual(str(downloader), "Downloader-%d" % i, "__str__")


class TestDownloaderOperations(DownloaderTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.webapp = FakeApp()
        cls.webapp.start()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.webapp.stop()

    def make_url(self, uri):
        return "http://%s:%d/%s" % (
            self.webapp.host,
            self.webapp.port,
            uri.lstrip("/")
        )

    def test_extract_links(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client,
        )

        page_url = "http://localhost"
        paths = ["/test/%d" % i for i in range(16)]
        links = ['<a href="%s"></a>' % p for p in paths]

        text = "<body>%s</body>" % "".join(links)
        urls = downloader.extract_urls(page_url, {}, text)
        absurls = [urljoin(page_url, p) for p in paths]
        self.assertEqual(urls, absurls, "Returns absolute urls")

    async def test_errors(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client,
        )

        error_codes = [400, 401, 402, 403, 404, 422, 500, 502]
        for ec in error_codes:
            job = {"url": self.make_url("/error/%d" % ec)}
            response = await downloader.HEAD(job)
            self.assertEqual(response.status, ec, "Handler error: %d" % ec)

    async def test_HEAD(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client,
            size_limit_kb=64,
        )

        requested_url = self.make_url("/html/1")
        job = {"url": requested_url}
        response = await downloader.HEAD(job)
        self.assertEqual(response.status, 200, "HEAD")

    async def test_GET(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client,
            size_limit_kb=64,
        )

        requested_url = self.make_url("/html/10")
        job = {"url": requested_url}
        url, headers, content = await downloader.GET(job)
        self.assertEqual(url, requested_url)
        self.assertTrue(headers)
        self.assertTrue(content)

    async def test_size_limit(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client,
            size_limit_kb=1,
        )

        requested_url = self.make_url("/html/1000")
        job = {"url": requested_url}
        with self.assertRaises(DownloaderError):
            await downloader.GET(job)

    async def test_reacts_to_stop(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client,
            size_limit_kb=1,
        )

        task = asyncio.create_task(downloader())
        self.stop_ev.set()
        try:
            await asyncio.wait_for(asyncio.gather(task), timeout=1)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            self.fail("Did not react to stop event")

    async def test_run(self):
        downloader = Downloader(
            self.stop_ev,
            self.cache,
            self.url_queue,
            self.http_client,
            size_limit_kb=128,
            whitelist=["localhost"],
        )

        n = 128
        for i in range(n):
            url = self.make_url("/html/%d" % i)
            self.url_queue.put_nowait({"url": url})

        tasks = [
            asyncio.create_task(downloader())
            for _ in range(8)
        ]

        total_processed = 0
        wait_interval = 0.1
        max_wait_loops = 3 / wait_interval

        while max_wait_loops and total_processed < n:
            max_wait_loops -= 1
            cur = self.dbconn.cursor()
            cur.execute("SELECT COUNT(*) FROM urls WHERE mtime IS NOT NULL")
            total_processed = cur.fetchone()[0]
            await asyncio.sleep(wait_interval)

        self.stop_ev.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
        self.assertEqual(total_processed, n, "Processed all")
