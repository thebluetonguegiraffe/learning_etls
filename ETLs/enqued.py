from queue import Queue
import time

from common import is_prime


class QueuedETL:
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
        self.start_batch = 0

        self.database = []  # Simula una DB

        self.transformer_queue = Queue()
        self.loader_queue = Queue()

    def extract(self) -> None:
        for i in range(0, 1000, self.batch_size):
            time.sleep(0.1)
            batch = list(range(i + 1, min(i + self.batch_size + 1, 1001)))
            self.transformer_queue.put(batch)
            print(f"  Extracted batch: {batch[0]}-{batch[-1]}")

        self.transformer_queue.put(None)

    def transform(self) -> None:
        while True:
            batch = self.transformer_queue.get()

            if batch is None:
                self.loader_queue.put(None)
                break

            print(f"  Transforming batch: {batch[0]}-{batch[-1]}")
            transformed = []
            for num in batch:
                transformed.append({
                    "number": num,
                    "is_prime": is_prime(num**2 + 1),
                    "sqrt": sum(x**0.5 for x in range(1, 1000)),
                })

            self.loader_queue.put(transformed)

    def load(self):
        while True:
            batch = self.loader_queue.get()

            if batch is None:
                break

            print(f"  Loading batch: {batch[0]['number']}-{batch[-1]['number']}")

            time.sleep(0.05)  # Simula escritura a DB
            self.database.extend(batch)

    def run(self):
        start = time.time()

        # no need to declare variables as output is inside queues
        self.extract()
        self.transform()
        self.load()

        elapsed = time.time() - start
        print(f"\n✅ Completed! Total records: {len(self.database)}")
        print(f"⏱️  Time: {elapsed:.2f}s")
        return elapsed


if __name__ == "__main__":
    etl = QueuedETL(batch_size=50)
    etl.run()
