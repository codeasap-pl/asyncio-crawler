from crawler import Scheduler, Worker
from .testing import WorkerTestCase

import asyncio


class TestScheduler(WorkerTestCase):
    def test_init(self):
        scheduler = Scheduler(
            self.stop_ev,
            self.cache,
            asyncio.Queue(),
        )
        self.assertIsInstance(scheduler, Worker, "Is a Worker")


class TestSchedulerOperations(WorkerTestCase):
    def _insert_urls(self, *urls):
        cur = self.dbconn.cursor()
        cur.executemany(
            "INSERT INTO urls(url) VALUES(?)",
            [(url,) for url in urls]
        )
        cur.connection.commit()

    async def _queue_consumer(self,
                              stop_ev,
                              url_queue,
                              result_queue,
                              /,
                              timeout=1,
                              total_expected=-1
                              ):
        while not stop_ev.is_set():
            if result_queue.qsize() == total_expected:
                break
            try:
                job = await asyncio.wait_for(url_queue.get(), timeout=timeout)
                url_queue.task_done()
                result_queue.put_nowait(job)
            except (asyncio.QueueEmpty, asyncio.TimeoutError):
                pass

    async def test_stop_when_idle(self):
        url_queue = asyncio.Queue(maxsize=8)

        scheduler = Scheduler(
            self.stop_ev,
            self.cache,
            url_queue,
            max_idle_sec=1,
        )

        task = asyncio.create_task(scheduler())
        try:
            await asyncio.wait_for(asyncio.gather(task), timeout=2)
            self.assertTrue(self.stop_ev.is_set(), "Stop event is set.")
        except (asyncio.TimeoutError, asyncio.CancelledError):
            self.fail("Scheduler did not finish.")

    async def test_reacts_to_stop(self):
        url_queue = asyncio.Queue(maxsize=16)
        scheduler = Scheduler(
            self.stop_ev,
            self.cache,
            url_queue,
            max_idle_sec=1
        )
        task = asyncio.create_task(scheduler())
        self.stop_ev.set()
        try:
            await asyncio.wait_for(asyncio.gather(task), timeout=3)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            self.fail("Scheduler did not finish.")
        finally:
            await task

    async def test_scheduling_simple(self):
        url_queue = asyncio.Queue(maxsize=8)

        scheduler = Scheduler(
            self.stop_ev,
            self.cache,
            url_queue,
            max_idle_sec=2,
        )
        task = asyncio.create_task(scheduler())

        url = "http://localhost/1"
        self._insert_urls(url)
        try:
            job = await asyncio.wait_for(url_queue.get(), timeout=1)
            self.assertEqual(job["url"], url, "Queued url")
            self.stop_ev.set()
        finally:
            await task

    async def test_scheduling(self):
        url_queue = asyncio.Queue(maxsize=4)

        scheduler = Scheduler(
            self.stop_ev,
            self.cache,
            url_queue,
            schedule_interval_sec=0.01,
            max_idle_sec=2,
        )

        total_jobs = 512
        result_queue = asyncio.Queue()

        urls = ["http://localhost/%d" % i for i in range(total_jobs)]
        self._insert_urls(*urls)

        scheduler_task = asyncio.create_task(scheduler())
        consumer_task = asyncio.create_task(
            self._queue_consumer(
                self.stop_ev,
                url_queue,
                result_queue,
                timeout=0.1,
                total_expected=total_jobs
            )
        )

        wait_interval = 0.005
        max_wait_loops = 5 / wait_interval  # 5 seconds / interval

        try:
            while max_wait_loops and result_queue.qsize() < total_jobs:
                await asyncio.sleep(wait_interval)
                max_wait_loops -= 1

            self.stop_ev.set()
            await asyncio.wait_for(
                asyncio.gather(scheduler_task, consumer_task),
                timeout=1
            )
            self.assertTrue(scheduler_task.done())
            self.assertTrue(consumer_task.done())
            self.assertEqual(result_queue.qsize(), total_jobs, "All schduled")

            # make sure these are correct urls
            # Note: this is impossible in Python. :-)
            #     [result_queue.get_nowait() while result_queue.qsize()]

            jobs = []
            while result_queue.qsize():
                job = result_queue.get_nowait()
                result_queue.task_done()
                jobs.append(job["url"])
            self.assertFalse(set(jobs) - set(urls), "All urls are correct")
        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            self.fail("Scheduling is broken: %s" % e)

    async def test_scheduling_visited(self):
        url_queue = asyncio.Queue(maxsize=8)
        result_queue = asyncio.Queue()

        url = "http://localhost/1"
        self._insert_urls(url)

        scheduler = Scheduler(
            self.stop_ev,
            self.cache,
            url_queue,
            schedule_interval_sec=0.1,
            max_idle_sec=2,
        )

        scheduler_task = asyncio.create_task(scheduler())

        consumer_task = asyncio.create_task(
            self._queue_consumer(
                self.stop_ev,
                url_queue,
                result_queue,
                timeout=0.1,
                total_expected=1
            )
        )

        try:
            await asyncio.wait_for(consumer_task, timeout=1)
            self.assertEqual(result_queue.qsize(), 1, "Got job")
            result_queue.get_nowait()
            result_queue.task_done()

            # update processed url's mtime, so it's old
            cur = self.dbconn.cursor()
            cur.execute("UPDATE urls SET mtime = 0")
            cur.connection.commit()

            # start a consumer again
            consumer_task = asyncio.create_task(
                self._queue_consumer(
                    self.stop_ev,
                    url_queue,
                    result_queue,
                    timeout=0.1,
                    total_expected=1
                )
            )
            await asyncio.wait_for(consumer_task, timeout=1)
            self.assertEqual(result_queue.qsize(), 1, "Got job")
            result_queue.get_nowait()
            result_queue.task_done()
            self.stop_ev.set()
            await scheduler_task
        except asyncio.TimeoutError as e:
            self.fail("Scheduling is broken (aged tasks): %r" % e)
