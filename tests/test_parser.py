from unittest import TestCase

from crawler import (
    create_parser,
    DEFAULT_USER_AGENT,
    DEFAULT_DB,
    DEFAULT_SIZE_LIMIT_KB,
    DEFAULT_CONCURRENCY,
    DEFAULT_QUEUE_SIZE,
    DEFAULT_MAX_IDLE_SEC,
    DEFAULT_SCHEDULING_INTERVAL_SEC,
)


class TestParser(TestCase):
    def test_defaults(self):
        parser = create_parser()
        args = parser.parse_args("")

        # Flags
        self.assertFalse(args.color, "Color off")
        self.assertFalse(args.debug, "Debug off")

        # Cache
        self.assertEqual(args.db, DEFAULT_DB)

        # SCheduler
        self.assertEqual(args.max_idle_sec, DEFAULT_MAX_IDLE_SEC, "Idle sec")
        self.assertEqual(args.queue_size, DEFAULT_QUEUE_SIZE, "Queue size")
        self.assertEqual(args.schedule_interval_sec,
                         DEFAULT_SCHEDULING_INTERVAL_SEC, "Interval")

        # Downloader
        self.assertEqual(args.concurrency, DEFAULT_CONCURRENCY)
        self.assertEqual(args.size_limit_kb, DEFAULT_SIZE_LIMIT_KB, "Limit")
        self.assertEqual(args.user_agent, DEFAULT_USER_AGENT, "User agent")
        self.assertIsNone(args.whitelist, "Whitelist is empty")

        # Positional
        self.assertEqual(args.url, [], "No default urls")

    def test_with_options(self):
        cmdline = " ".join([
            "-C -D -d test.db -i 3.14 -s 7.3 -c 13 -q 17 -t 21.7",
            "-w localhost -w dev.lan",
            "-u my-browser",
            "http://localhost https://dev.lan"
        ])

        parser = create_parser()
        args = parser.parse_args(cmdline.split())

        # Flags
        self.assertTrue(args.color, "Color on")
        self.assertTrue(args.debug, "Debug on")

        # Cache
        self.assertEqual(args.db, "test.db", "Database")

        # SCheduler
        self.assertEqual(args.max_idle_sec, 3.14, "Idle")
        self.assertEqual(args.queue_size, 17, "Queue size")
        self.assertEqual(args.schedule_interval_sec, 21.7, "Interval")

        # Downloader
        self.assertEqual(args.concurrency, 13, "Concurency")
        self.assertEqual(args.size_limit_kb, 7.3, "Size limit")
        self.assertEqual(args.user_agent, "my-browser", "User agent")
        self.assertEqual(args.whitelist, ["localhost", "dev.lan"], "Whitelist")

        # Positional
        self.assertEqual(args.url,
                         ["http://localhost", "https://dev.lan"], "Urls")
