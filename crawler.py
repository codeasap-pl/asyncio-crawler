#!/usr/bin/env python3

import abc
import aiohttp
import argparse
import asyncio
import bs4
import coloredlogs
import logging
import signal
import sqlite3
import time
from urllib.parse import (
    urlparse,
    urljoin,
)


DEFAULT_USER_AGENT = "CodeASAP.pl: Crawler"
DEFAULT_DB = ":memory:"
DEFAULT_SIZE_LIMIT_KB = 128
DEFAULT_CONCURRENCY = 32
DEFAULT_QUEUE_SIZE = DEFAULT_CONCURRENCY
DEFAULT_MAX_IDLE_SEC = 3
DEFAULT_SCHEDULING_INTERVAL_SEC = 1


LOG_FORMAT = " ".join([
    "%(asctime)s %(levelname)-8s [%(process)-5d]",
    "%(name)-16s %(funcName)-12s %(message)s"
])

LOG_FORMAT_DEBUG = " ".join([
    "%(asctime)s %(levelname)-8s [%(process)d]",
    "%(module)-16s %(name)-16s %(funcName)-12s %(message)s"
])


class Base(abc.ABC):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(str(self))
        self.logger.debug("Initialized")

    def __str__(self):
        return self.__class__.__name__


class Worker(Base):
    async def initialize(self, *args, **kwargs): ...
    async def shutdown(self, *args, **kwargs): ...

    async def __call__(self, *args, **kwargs):
        self.logger.debug("Running")
        await self.initialize()
        try:
            await self._run(*args, **kwargs)
        finally:
            await self.shutdown()
        self.logger.debug("Done")

    @abc.abstractmethod
    async def _run(self, *args, **kwargs): ...


class Cache(Base):
    def __init__(self, dbconn: sqlite3.Connection, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = dbconn
        self.logger.debug(dbconn)

    def initdb(self):
        schema = (
            """
            CREATE TABLE IF NOT EXISTS urls(
                url_id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                mtime TIMESTAMP DEFAULT NULL,
                content_type TEXT DEFAULT NULL,
                status_code INTEGER DEFAULT NULL,
                size INTEGER DEFAULT NULL
            );
            """
        )

        self.logger.debug("Creating schema")
        cur = self.db.cursor()
        cur.execute(schema)
        cur.connection.commit()

    def store_links(self, links):
        stmt = "INSERT INTO urls(url) VALUES(?) ON CONFLICT(url) DO NOTHING"
        cur = self.db.cursor()
        cur.executemany(stmt, [(link,) for link in links])
        cur.connection.commit()

    def store_response(self, url, status_code=None, headers=None):
        headers = (headers or {})
        stmt = (
            """
            INSERT INTO urls(url, content_type, status_code, size, mtime)
            VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(url) DO UPDATE SET
                content_type=EXCLUDED.content_type,
                status_code=EXCLUDED.status_code,
                size=EXCLUDED.size,
                mtime=CURRENT_TIMESTAMP
            """
        )

        record = (
            url,
            headers.get("content-type"),
            status_code,
            (int(headers.get("content-length", 0)) or None),
        )
        cur = self.db.cursor()
        cur.execute(stmt, record)
        cur.connection.commit()


class Scheduler(Worker):
    def __init__(self,
                 stop_ev: asyncio.Event,
                 cache: Cache,
                 url_queue: asyncio.Queue,
                 /,
                 schedule_interval_sec: float = 1,
                 max_idle_sec: float = 1):
        super().__init__()
        self.stop_ev = stop_ev
        self.cache = cache
        self.url_queue = url_queue
        self.schedule_interval_sec = schedule_interval_sec
        self.max_idle_sec = max_idle_sec

    async def _run(self, *args, **kwargs):
        cte = (
            """
            WITH select_stmt AS (
                SELECT url_id FROM urls U
                    WHERE (
                        U.mtime IS NULL
                        OR
                        datetime(U.mtime, "+3600 seconds") <= CURRENT_TIMESTAMP
                    )
                ORDER BY RANDOM()
                LIMIT ?
            )
            UPDATE urls SET mtime = CURRENT_TIMESTAMP WHERE url_id IN (
                SELECT url_id FROM select_stmt
            ) RETURNING *
            """
        )
        last_success = time.monotonic()
        while not self.stop_ev.is_set():
            cur = self.cache.db.cursor()
            cur.execute(cte, (self.url_queue.maxsize,))
            cur.connection.commit()
            records = cur.fetchall()
            if records:
                last_success = time.monotonic()
                self.logger.debug("Scheduling %d url(s)", len(records))
                for record in records:
                    await self.url_queue.put(dict(record))
            elif (time.monotonic() - last_success) > self.max_idle_sec:
                self.logger.info("Nothing to do. Stop.")
                self.stop_ev.set()
                break
            else:
                self.logger.debug("No urls to schedule.")
            await asyncio.sleep(self.schedule_interval_sec)


class Downloader(Worker):
    SEQ_ID = 0
    SUPPORTED_PROTOCOLS = ["http", "https", "ftp", "mailto"]
    SUPPORTED_CONTENT_TYPES = ["text/html", "application/xml"]

    def __init__(self,
                 stop_ev: asyncio.Event,
                 cache: Cache,
                 url_queue: asyncio.Queue,
                 http_client: aiohttp.ClientSession,
                 /,
                 whitelist=None,
                 size_limit_kb=None):

        # tracking id
        self.seq_id = Downloader.SEQ_ID + 1
        Downloader.SEQ_ID += 1

        super().__init__()
        self.stop_ev = stop_ev
        self.cache = cache
        self.url_queue = url_queue
        self.client = http_client
        self.whitelist = (whitelist or [])
        self.size_limit_kb = size_limit_kb

    def __str__(self):
        return "%s-%d" % (self.__class__.__name__, self.seq_id)

    async def _run(self, *args, **kwargs):
        while not self.stop_ev.is_set():
            try:
                job = await asyncio.wait_for(self.url_queue.get(), 0.1)
                parsed = urlparse(job["url"])
                assert parsed.scheme in self.SUPPORTED_PROTOCOLS, "unsupported"

                response = await self.HEAD(job)
                self.cache.store_response(
                    str(response.real_url),
                    response.status,
                    response.headers,
                )
                content_type = response.headers["content-type"].split(";")[0]
                if content_type not in self.SUPPORTED_CONTENT_TYPES:
                    continue
                fqdn, *garbage = parsed.netloc.split(":")
                if self.whitelist and fqdn not in self.whitelist:
                    self.logger.debug("SKIP: %s", fqdn)
                    continue
                url, headers, content = await self.GET(job)
                links = self.extract_urls(url, content, headers)
                self.cache.store_links(links)
            except (asyncio.QueueEmpty, asyncio.TimeoutError) as e:
                self.logger.debug("Nothing to do: %s", e)
            except (KeyError, AssertionError) as e:
                self.logger.error("SKIP: Invalid job: %s", e)
            except aiohttp.client_exceptions.ClientError as e:
                self.logger.error(e)
            except DownloaderError as e:
                self.logger.error(e)
                self.cache.store_response(str(response.real_url), -1)
            except asyncio.CancelledError as e:
                self.logger.warning("Cancelled. %s", e)
            except Exception as e:
                self.logger.exception(e)

    async def HEAD(self, job):
        url = job["url"]
        timeout = aiohttp.ClientTimeout(connect=5, total=10)
        response = await self.client.head(url,
                                          allow_redirects=True,
                                          timeout=timeout)
        self.logger.info("%d HEAD: %s", response.status, url)
        return response

    async def GET(self, job):
        url = job["url"]
        response = await self.client.get(url,
                                         allow_redirects=True,
                                         timeout=5)
        self.logger.info("%d GET: %s", response.status, url)
        try:
            content = await self.stream_content(response)
            return (str(response.real_url), response.headers, content)
        finally:
            response.close() if not response.closed else ...

    def extract_urls(self, url, text, headers=None):
        headers = (headers or None)
        soup = bs4.BeautifulSoup(text, "html.parser")
        links = soup.find_all("a")
        hrefs = [link.attrs.get("href") for link in links]
        hrefs = [href for href in hrefs if href and not href.startswith("#")]
        parsed = urlparse(url)
        return [urljoin(parsed.geturl(), href) for href in hrefs]

    async def stream_content(self, response):
        chunks = bytearray()
        total_size = 0
        async for chunk in response.content.iter_chunked(4096):
            total_size += len(chunk)
            if self.size_limit_kb and total_size > self.size_limit_kb * 1024:
                raise DownloaderError("Size limit exceeded.")
            chunks.extend(chunk)
        return chunks.decode()


class DownloaderError(Exception):
    ...


async def main(po: argparse.Namespace):
    logger = logging.getLogger(__name__)
    stop_ev = asyncio.Event()

    db = sqlite3.connect(po.db)
    db.row_factory = sqlite3.Row

    cache = Cache(db)
    cache.initdb()

    url_queue = asyncio.Queue(maxsize=po.queue_size)
    [url_queue.put_nowait({"url": url}) for url in po.url]

    terminate = (lambda *args, **kwargs: stop_ev.set())
    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)

    http_client = aiohttp.ClientSession(
        headers={"User-Agent": po.user_agent},
    )

    scheduler = Scheduler(
        stop_ev,
        cache,
        url_queue,
        max_idle_sec=po.max_idle_sec,
        schedule_interval_sec=po.schedule_interval_sec
    )

    scheduler_task = asyncio.create_task(scheduler())
    downloader_tasks = []
    for _ in range(po.concurrency):
        downloader = Downloader(
            stop_ev,
            cache,
            url_queue,
            http_client,
            whitelist=po.whitelist,
            size_limit_kb=po.size_limit_kb,
        )
        downloader_tasks.append(asyncio.create_task(downloader()))

    try:
        await asyncio.gather(*downloader_tasks)
        logger.info("Tasks done.")
    finally:
        logger.info("Cancelling Scheduler task.")
        scheduler_task.cancel()
        logger.info("Closing HTTP client.")
        await http_client.close()

    list_broken_urls(db)
    db.close()


def list_broken_urls(db):
    stmt = (
        """
        SELECT status_code, url FROM urls
        WHERE status_code IS NOT NULL AND status_code != 200
        """
    )
    cur = db.cursor()
    cur.execute(stmt)
    for r in cur.fetchall():
        print("%3d %s" % (r["status_code"], r["url"]))


def setup_logging(color=False) -> None:
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_format = LOG_FORMAT_DEBUG if args.debug else LOG_FORMAT
    logging.basicConfig(level=log_level, format=log_format)

    if color:
        coloredlogs.install(level=log_level, fmt=log_format)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    # Flags
    flags = parser.add_argument_group("Flags")
    flags.add_argument(
        "-C", "--color", action="store_true",
        help="Color logs",
    )
    flags.add_argument(
        "-D", "--debug", action="store_true",
        help="Display debug information",
    )

    # Cache
    cache = parser.add_argument_group("Cache")
    cache.add_argument(
        "-d", "--db", type=str,
        help="Database path (default: %s)" % DEFAULT_DB,
        default=DEFAULT_DB,
    )

    # Scheduler
    scheduler = parser.add_argument_group("Scheduler")
    scheduler.add_argument(
        "-i", "--max-idle-sec", type=float,
        help="Exit if idle for N seconds (default %f)" % DEFAULT_MAX_IDLE_SEC,
        default=DEFAULT_MAX_IDLE_SEC,
    )
    scheduler.add_argument(
        "-q", "--queue-size", type=int,
        help="URL Queue maxsize (default: %d)" % DEFAULT_QUEUE_SIZE,
        default=DEFAULT_QUEUE_SIZE,
    )
    scheduler.add_argument(
        "-t", "--schedule-interval-sec", type=float,
        help="Scheduling interval (default: %f)" % (
            DEFAULT_SCHEDULING_INTERVAL_SEC,
        ),
        default=DEFAULT_SCHEDULING_INTERVAL_SEC,
    )

    # Downloader
    downloader = parser.add_argument_group("Downloader")
    downloader.add_argument(
        "-c", "--concurrency", type=int,
        help="Number of downloaders (default: %d)" % DEFAULT_CONCURRENCY,
        default=DEFAULT_CONCURRENCY,
    )
    downloader.add_argument(
        "-s", "--size-limit-kb", type=float,
        help="Download size limit (default: %f)" % DEFAULT_SIZE_LIMIT_KB,
        default=DEFAULT_SIZE_LIMIT_KB,
    )
    downloader.add_argument(
        "-u", "--user-agent", type=str,
        help="User-Agent header (default: %s)" % DEFAULT_USER_AGENT,
        default=DEFAULT_USER_AGENT,
    )
    downloader.add_argument(
        "-w", "--whitelist", action="append",
        metavar="DOMAIN",
        help="Allow http GET from this domain",
    )

    # Positional
    parser.add_argument(
        "url", nargs="*", action="extend",
        help="Start url(s)",
    )

    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    setup_logging(args.color)
    asyncio.run(main(args))
