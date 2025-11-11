from queue import Queue
import threading
import time

from common import is_prime


class ThreadedETL:
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
        self.start_batch = 0

        self.database = []  # Simula una DB

        self.transformer_queue = Queue()
        self.loader_queue = Queue()

    def extract(self) -> iter:
        for i in range(0, 1000, self.batch_size):
            time.sleep(0.1)
            batch = list(range(i + 1, min(i + self.batch_size + 1, 1001)))
            yield batch
        self.transformer_queue.put(None)

    def transform(self, batch) -> list:
        # now processes a single batch at a time
        transformed = []
        for num in batch:
            transformed.append(
                {
                    "number": num,
                    "is_prime": is_prime(num**2 + 1),
                    "sqrt": sum(x**0.5 for x in range(1, 1000)),
                }
            )
        return transformed

    def load(self, batch):
        # now processes a single batch at a time
        time.sleep(0.05)  # Simula escritura a DB
        self.database.extend(batch)

    # queues are now managed by threads (because they are the ones that use them)
    def extractor_thread(self):
        for batch in self.extract():
            self.transformer_queue.put(batch)
            # print(f"Extracted batch: {batch[0]}-{batch[-1]}")

    # queues are now managed by threads (because they are the ones that use them)
    def transformer_thread(self):
        while True:
            batch = self.transformer_queue.get()

            if batch is None:
                self.loader_queue.put(None)
                break

            # print(f"Transforming batch: {batch[0]}-{batch[-1]}")
            transformed = self.transform(batch)
            self.loader_queue.put(transformed)

    def loader_thread(self):
        while True:
            batch = self.loader_queue.get()

            if batch is None:
                break

            # print(f"Loading batch: {batch[0]['number']}-{batch[-1]['number']}")
            self.load(batch)

    def run_with_3_threads(self):
        start = time.time()

        # Create threads for each stage
        extractor_thread = threading.Thread(target=self.extractor_thread, name="Extractor")
        transformer_thread = threading.Thread(target=self.transformer_thread, name="Transformer")
        loader_thread = threading.Thread(target=self.loader_thread, name="Loader")

        # start threads
        extractor_thread.start()
        transformer_thread.start()
        loader_thread.start()

        # wait for completion
        extractor_thread.join()
        transformer_thread.join()
        loader_thread.join()

        elapsed = time.time() - start
        print(f"\n✅ Completed! Total records: {len(self.database)}")
        print(f"⏱️  Time: {elapsed:.2f}s")
        return elapsed

    def run(self):
        start = time.time()

        # Create threads for each stage
        transformer_thread = threading.Thread(target=self.transformer_thread, name="Transformer")
        loader_thread = threading.Thread(target=self.loader_thread, name="Loader")

        # start threads
        transformer_thread.start()
        loader_thread.start()

        try:
            self.extractor_thread()
        except Exception as e:
            print(f"Extractor thread failed: {e}")

        # wait for completion
        transformer_thread.join()
        loader_thread.join()

        elapsed = time.time() - start
        print(f"\n✅ Completed! Total records: {len(self.database)}")
        print(f"⏱️  Time: {elapsed:.2f}s")
        return elapsed


if __name__ == "__main__":
    etl = ThreadedETL(batch_size=50)
    etl.run_with_3_threads()
    # etl.run()
