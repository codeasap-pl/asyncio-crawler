# Crawler (asyncio)

## Python: educational asyncio crawler

[blog.codeasap.pl](https://blog.codeasap.pl/posts/crawler/1-setup/)

python3, asyncio, aiohttp, sqlite3, BeautifulSoup (bs4)


## Options

```text
$ ./crawler.py --help

positional arguments:
  url                   Start url(s)

optional arguments:
  -h, --help            show this help message and exit

Flags:
  -C, --color           Color logs
  -D, --debug           Display debug information

Cache:
  -d DB, --db DB        Database path (default: :memory:)

Scheduler:
  -i MAX_IDLE_SEC, --max-idle-sec MAX_IDLE_SEC
                        Exit if idle for N seconds (default 3)
  -q QUEUE_SIZE, --queue-size QUEUE_SIZE
                        URL Queue maxsize (default: 32)
  -t SCHEDULE_INTERVAL_SEC, --schedule-interval-sec SCHEDULE_INTERVAL_SEC
                        Scheduling interval (default: 1)

Downloader:
  -c CONCURRENCY, --concurrency CONCURRENCY
                        Number of downloaders (default: 32)
  -s SIZE_LIMIT_KB, --size-limit-kb SIZE_LIMIT_KB
                        Download size limit (default: 128.000000)
  -w DOMAIN, --whitelist DOMAIN
                        Allow http GET from this domain
  -u USER_AGENT, --user-agent USER_AGENT
                        User-Agent header (default: CodeASAP.pl: Crawler)

```

## Run

```text
FQDN=mysite.lan URLS=http://mysite.lan make run 
```

## Tests

```
make test
```

Or

```
make test-pytest
```

## Coverage

```
make coverage
```

## Docs

Doxygen.

```
make doxygen
```

## BUGS

Always.
