import logging
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from loky import get_reusable_executor

logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

out = {}
startstamp = time.time()


class MultiFutures:

    def __init__(self, fun, iterablelist, workers):
        self.fun = fun
        self.iterablelist = iterablelist
        self.workers = workers

    def futureIteration(self, executor):

        futures = {}
        for i, futItem in enumerate(self.iterablelist):
            itemOfList = futItem
            logger.info(f"\nScheduling future for item: {itemOfList}...\n")
            futures[executor.submit(self.fun, itemOfList)] = itemOfList
        counter = 0
        for future in as_completed(futures):
            counter += 1
            futItem = futures[future]
            res = future.result()
            out[futItem] = res
            logger.info(f"Completed check for {counter} out of {len(self.iterablelist)} items.")
            timestamp = time.time()
            logger.info(time.strftime('%H:%M:%S', time.gmtime(timestamp - startstamp)) + ", " +
                        time.strftime('%H:%M:%S', time.localtime(time.time())))
            logger.info(f"Aggregated item: {futItem} to dictionary.")
        endstamp = time.time()
        logger.info("ALL DONE\n")
        logger.info(time.strftime('%H:%M:%S', time.gmtime(endstamp - startstamp)) + ", " +
                    time.strftime('%H:%M:%S', time.localtime(time.time())))
        return out

    def processing(self):
        with get_reusable_executor(max_workers=self.workers) as e:
            self.futureIteration(executor=e)

    def threading(self):
        with ThreadPoolExecutor(max_workers=self.workers) as e:
            self.futureIteration(executor=e)
