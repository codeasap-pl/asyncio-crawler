from unittest import TestCase
from crawler import create_parser


class TestParser(TestCase):
    def test_parser(self):
        cmdline = " ".join([
            "-C -D -d test.db -i 7 -s 9 -c 13",
            "-w localhost -w dev.lan",
            "http://localhost https://dev.lan"
        ])

        parser = create_parser()
        args = parser.parse_args(cmdline.split())
        self.assertTrue(args.color)
        self.assertTrue(args.debug)
        self.assertEqual(args.db, "test.db")
        self.assertEqual(args.max_idle_sec, 7)
        self.assertEqual(args.size_limit_kb, 9)
        self.assertEqual(args.concurrency, 13)
        self.assertEqual(args.whitelist, ["localhost", "dev.lan"])
        self.assertEqual(args.url, ["http://localhost", "https://dev.lan"])
