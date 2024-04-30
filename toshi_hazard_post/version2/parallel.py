import logging
import multiprocessing
import queue
import threading
import traceback
from typing import Callable, Tuple, Union

log = logging.getLogger(__name__)


def setup_parallel(
    num_workers: int, func: Callable
) -> Tuple[Union[queue.Queue, multiprocessing.JoinableQueue], Union[queue.Queue, multiprocessing.Queue],]:
    """
    Create the task and results queus and setup workers for either parallel (multiprocessing) or serial mode

    Parameters:
        num_workers: the number of workers to create. If <1, then a serial process will be used
        func: the callable object to be invoked

    Returns:
        task_queue: the task queue
        results_queue: the results queue
    """

    if num_workers > 1:
        return setup_multiproc(num_workers, func)
    return setup_serial(func)


def setup_multiproc(num_workers: int, func: Callable) -> Tuple[multiprocessing.JoinableQueue, multiprocessing.Queue]:
    log.info(f"creating {num_workers} multiprocessing workers")
    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    workers = [AggregationWorkerMP(task_queue, result_queue, func) for i in range(num_workers)]
    for w in workers:
        w.start()
    return task_queue, result_queue


def setup_serial(func: Callable) -> Tuple[queue.Queue, queue.Queue]:
    log.info("creating one serial worker")
    task_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    worker = AggregationWorkerSerial(task_queue, result_queue, func)
    worker.start()
    return task_queue, result_queue


class AggregationWorkerSerial(threading.Thread):
    """A serial worker"""

    def __init__(self, task_queue: queue.Queue, result_queue: queue.Queue, func: Callable):
        threading.Thread.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.func = func

    def run(self):
        log.info("worker %s running." % self.name)
        proc_name = self.name

        while True:
            task_args = self.task_queue.get()
            if task_args is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                log.info('%s: Exiting' % proc_name)
                break
            log.info(f"worker {self.name} working on hazard for site: {task_args.site}, imt: {task_args.imt}")

            try:
                self.func(task_args)  # calc_aggregation
                self.task_queue.task_done()
                log.info('%s task done.' % self.name)
                self.result_queue.put(str(task_args.imt))
            except Exception:
                log.error(traceback.format_exc())
                args = f"{task_args.site}, {task_args.imt}"
                self.result_queue.put(f'FAILED {args} {traceback.format_exc()}')
                self.task_queue.task_done()


class AggregationWorkerMP(multiprocessing.Process):
    """A worker that batches aggregation processing."""

    def __init__(self, task_queue: multiprocessing.JoinableQueue, result_queue: multiprocessing.Queue, func: Callable):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.func = func

    def run(self):
        log.info("worker %s running." % self.name)
        proc_name = self.name

        while True:
            task_args = self.task_queue.get()
            if task_args is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                log.info('%s: Exiting' % proc_name)
                break
            log.info(f"worker {self.name} working on hazard for site: {task_args.site}, imt: {task_args.imt}")

            try:
                self.func(task_args)  # calc_aggregation
                self.task_queue.task_done()
                log.info('%s task done.' % self.name)
                self.result_queue.put(str(task_args.imt))
            except Exception:
                log.error(traceback.format_exc())
                args = f"{task_args.site}, {task_args.imt}"
                self.result_queue.put(f'FAILED {args} {traceback.format_exc()}')
                self.task_queue.task_done()
